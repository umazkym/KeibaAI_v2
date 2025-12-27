#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
σモデル V2 訓練スクリプト

目的: μ_time_v1の残差（|y_true - mu_pred|）を予測するモデル

【過学習対策】
1. 時系列分割（μモデルと同じTrain/Test期間）
2. 正則化パラメータ
3. Early Stopping

【リーク対策】
1. μモデルの予測結果のみを使用（ターゲット情報なし）
2. 馬の過去安定性と経験を特徴量化

Usage:
    python scripts/training/train_sigma_v2.py
"""

import sys
import logging
import json
import pickle
import yaml
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import lightgbm as lgb

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# ログ設定
log_dir = project_root / "outputs" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"train_sigma_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(log_file), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def load_mu_model():
    """
    μ_time_v1モデルと特徴量リストをロード
    """
    model_dir = project_root / "keibaai" / "models" / "mu_time_v1"
    
    model_path = model_dir / "mu_time_model.pkl"
    features_path = model_dir / "feature_names.json"
    
    logger.info(f"μモデルロード: {model_path}")
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    with open(features_path, 'r', encoding='utf-8') as f:
        feature_cols = json.load(f)
    
    logger.info(f"  特徴量数: {len(feature_cols)}")
    return model, feature_cols


def load_and_prepare_data():
    """
    データ読み込みとμ予測の計算
    """
    logger.info("データ読み込み開始...")
    
    races_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races"
    corners_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "corners"
    pedigrees_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "pedigrees"
    
    # races.parquetを読み込み
    races_files = list(races_path.glob("*.parquet"))
    races_df = pd.concat([pd.read_parquet(f) for f in races_files], ignore_index=True)
    logger.info(f"  races: {len(races_df):,}行")
    
    # corners.parquetを読み込み
    corners_df = None
    if corners_path.exists():
        corners_files = list(corners_path.glob("*.parquet"))
        if corners_files:
            corners_df = pd.concat([pd.read_parquet(f) for f in corners_files], ignore_index=True)
            logger.info(f"  corners: {len(corners_df):,}行")
    
    # pedigrees.parquetを読み込み
    pedigrees_df = None
    if pedigrees_path.exists():
        pedigrees_files = list(pedigrees_path.glob("*.parquet"))
        if pedigrees_files:
            pedigrees_df = pd.concat([pd.read_parquet(f) for f in pedigrees_files], ignore_index=True)
            logger.info(f"  pedigrees: {len(pedigrees_df):,}行")
    
    return races_df, corners_df, pedigrees_df


def generate_features_and_predictions(races_df, corners_df, pedigrees_df, mu_model, mu_feature_cols):
    """
    特徴量生成とμ予測の計算
    """
    from keibaai.src.features.leak_free_feature_engineer_v15 import (
        LeakFreeFeatureEngineerV15, FeatureConfigV15
    )
    
    logger.info("特徴量生成とμ予測計算...")
    
    # 期間設定（μモデルと同じ）
    train_end = pd.Timestamp('2023-01-01')
    
    # 日付型に変換
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    # 欠損値除外
    races_df = races_df.dropna(subset=['finish_position', 'finish_time_seconds'])
    
    # 重複排除
    races_df = races_df.drop_duplicates(subset=['race_id', 'horse_number'], keep='first')
    races_df = races_df.reset_index(drop=True)
    logger.info(f"  データ整形後: {len(races_df):,}行")
    
    # V15特徴量エンジンでfit/transform
    config = FeatureConfigV15()
    fe = LeakFreeFeatureEngineerV15(config)
    
    train_mask = races_df['race_date'] < train_end
    train_races = races_df[train_mask].copy()
    
    fe.fit(races_df=train_races, pedigrees_df=pedigrees_df, corners_df=corners_df)
    df = fe.transform(races_df.copy())
    
    # μ予測
    available_features = [c for c in mu_feature_cols if c in df.columns]
    X = df[available_features].fillna(-999)
    df['mu_pred'] = mu_model.predict(X)
    
    # 残差計算（σのターゲット）
    df['residual'] = df['finish_time_seconds'] - df['mu_pred']
    df['abs_residual'] = df['residual'].abs()
    df['log_abs_residual'] = np.log(df['abs_residual'] + 1e-6)
    
    logger.info(f"  μ予測完了: 残差平均={df['abs_residual'].mean():.4f}秒")
    
    return df


def prepare_sigma_features(df):
    """
    σモデル用の特徴量を準備
    
    【設計思想】
    - 馬の「安定性」を示す特徴量
    - 過去成績のばらつき、経験値、調子
    """
    logger.info("σモデル用特徴量準備...")
    
    # 既存の特徴量から安定性関連を抽出
    sigma_feature_candidates = [
        # 過去走の統計
        'horse_cum_win_rate',
        'horse_cum_place_rate',
        'horse_avg_finish',
        'career_starts',
        
        # 騎手・調教師
        'jockey_cum_win_rate',
        'trainer_cum_win_rate',
        
        # 馬の特性
        'age',
        'sex',
        'horse_weight',
        'basis_weight',
        
        # 距離・コース適性
        'distance_m',
        'track_surface',
        
        # 最近の調子
        'prev_finish_position',
        'prev_2_finish_position',
        'prev_3_finish_position',
        
        # 脚質
        'horse_front_runner_rate',
        'horse_c4_gap_avg',
    ]
    
    # 実際に存在する特徴量のみ使用
    sigma_features = [c for c in sigma_feature_candidates if c in df.columns]
    
    # カテゴリ変数を処理
    for col in ['track_surface', 'sex']:
        if col in df.columns:
            df[col] = df[col].astype('category').cat.codes
    
    logger.info(f"  σ特徴量数: {len(sigma_features)}")
    return sigma_features


def train_sigma_model(df, sigma_features, train_start, train_end, test_start, test_end):
    """
    σモデルの訓練
    """
    logger.info("σモデル訓練開始...")
    logger.info(f"  Train: {train_start} - {train_end}")
    logger.info(f"  Test: {test_start} - {test_end}")
    
    # 時系列分割
    train_mask = (df['race_date'] >= train_start) & (df['race_date'] < train_end)
    test_mask = (df['race_date'] >= test_start) & (df['race_date'] < test_end)
    
    train_df = df[train_mask].copy()
    test_df = df[test_mask].copy()
    
    logger.info(f"  Train: {len(train_df):,}行, Test: {len(test_df):,}行")
    
    # 特徴量とターゲット
    X_train = train_df[sigma_features].fillna(-999)
    y_train = train_df['log_abs_residual']
    X_test = test_df[sigma_features].fillna(-999)
    y_test = test_df['log_abs_residual']
    
    # ハイパーパラメータ
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'boosting_type': 'gbdt',
        'learning_rate': 0.01,
        'num_leaves': 31,
        'max_depth': 6,
        'min_child_samples': 100,
        'lambda_l1': 1.0,
        'lambda_l2': 1.0,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'n_estimators': 1000,
        'verbose': -1,
    }
    
    model = lgb.LGBMRegressor(**params)
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[
            lgb.early_stopping(stopping_rounds=50, verbose=True),
            lgb.log_evaluation(period=50)
        ]
    )
    
    # 評価
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    train_rmse = np.sqrt(np.mean((y_train - y_train_pred) ** 2))
    test_rmse = np.sqrt(np.mean((y_test - y_test_pred) ** 2))
    
    # 実際のσ値に変換して評価
    sigma_train_pred = np.exp(y_train_pred)
    sigma_test_pred = np.exp(y_test_pred)
    sigma_train_true = np.exp(y_train)
    sigma_test_true = np.exp(y_test)
    
    sigma_train_rmse = np.sqrt(np.mean((sigma_train_true - sigma_train_pred) ** 2))
    sigma_test_rmse = np.sqrt(np.mean((sigma_test_true - sigma_test_pred) ** 2))
    
    metrics = {
        'log_train_rmse': train_rmse,
        'log_test_rmse': test_rmse,
        'sigma_train_rmse': sigma_train_rmse,
        'sigma_test_rmse': sigma_test_rmse,
        'n_train': len(train_df),
        'n_test': len(test_df),
        'n_features': len(sigma_features),
        'n_estimators_used': model.n_estimators_,
    }
    
    logger.info("=== σモデル評価結果 ===")
    logger.info(f"  Log Train RMSE: {train_rmse:.4f}")
    logger.info(f"  Log Test RMSE:  {test_rmse:.4f}")
    logger.info(f"  Sigma Train RMSE: {sigma_train_rmse:.4f}秒")
    logger.info(f"  Sigma Test RMSE:  {sigma_test_rmse:.4f}秒")
    
    return model, metrics


def save_model(model, sigma_features, metrics):
    """
    モデル保存
    """
    output_dir = project_root / "keibaai" / "models" / "sigma_v2"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # モデル保存
    model_path = output_dir / "sigma_model.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    logger.info(f"モデル保存: {model_path}")
    
    # 特徴量リスト保存
    features_path = output_dir / "feature_names.json"
    with open(features_path, 'w', encoding='utf-8') as f:
        json.dump(sigma_features, f, ensure_ascii=False, indent=2)
    logger.info(f"特徴量リスト保存: {features_path}")
    
    # 設定保存
    config = {
        'version': 'sigma_v2',
        'created_at': datetime.now().isoformat(),
        'target': 'log(|residual| + 1e-6)',
        'base_model': 'mu_time_v1',
        'n_features': len(sigma_features),
        'metrics': metrics,
    }
    config_path = output_dir / "config.yaml"
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
    logger.info(f"設定保存: {config_path}")
    
    return output_dir


def main():
    """
    メイン処理
    """
    logger.info("=" * 60)
    logger.info("σモデル V2 訓練スクリプト開始")
    logger.info("=" * 60)
    
    try:
        # 1. μモデルロード
        mu_model, mu_feature_cols = load_mu_model()
        
        # 2. データ読み込み
        races_df, corners_df, pedigrees_df = load_and_prepare_data()
        
        # 3. 特徴量生成とμ予測
        df = generate_features_and_predictions(
            races_df, corners_df, pedigrees_df,
            mu_model, mu_feature_cols
        )
        
        # 4. σ特徴量準備
        sigma_features = prepare_sigma_features(df)
        
        # 5. モデル訓練
        train_start = pd.Timestamp('2020-01-01')
        train_end = pd.Timestamp('2023-01-01')
        test_start = pd.Timestamp('2023-01-01')
        test_end = pd.Timestamp('2024-01-01')
        
        model, metrics = train_sigma_model(
            df, sigma_features,
            train_start, train_end,
            test_start, test_end
        )
        
        # 6. 保存
        output_dir = save_model(model, sigma_features, metrics)
        
        logger.info("=" * 60)
        logger.info(f"訓練完了。出力: {output_dir}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

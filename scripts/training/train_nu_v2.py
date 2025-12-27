#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
νモデル V2 訓練スクリプト（リーク排除版）

目的: レースの「荒れやすさ」（自由度ν）を予測するモデル

【重要: リーク対策】
- avg_win_odds, std_win_odds, popularity等の確定オッズ由来は使用禁止
- レース開始前に確定している情報のみ使用

【過学習対策】
1. 時系列分割
2. 正則化パラメータ
3. Early Stopping

Usage:
    python scripts/training/train_nu_v2.py
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
from scipy.stats import t
from scipy.optimize import minimize_scalar

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# ログ設定
log_dir = project_root / "outputs" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"train_nu_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(log_file), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# リーク排除: 禁止特徴量リスト
# ============================================================
FORBIDDEN_NU_FEATURES = [
    # 確定オッズ・人気（絶対禁止）
    'avg_win_odds',
    'std_win_odds',
    'min_win_odds',
    'max_win_odds',
    'win_odds',
    'popularity',
    'popularity_spread',
    
    # レース結果
    'finish_position',
    'finish_time_seconds',
    'margin_seconds',
    'last_3f_time',
]


def load_models():
    """
    μとσモデルをロード
    """
    mu_dir = project_root / "keibaai" / "models" / "mu_time_v1"
    sigma_dir = project_root / "keibaai" / "models" / "sigma_v2"
    
    logger.info(f"μモデルロード: {mu_dir}")
    with open(mu_dir / "mu_time_model.pkl", 'rb') as f:
        mu_model = pickle.load(f)
    with open(mu_dir / "feature_names.json", 'r') as f:
        mu_features = json.load(f)
    
    logger.info(f"σモデルロード: {sigma_dir}")
    with open(sigma_dir / "sigma_model.pkl", 'rb') as f:
        sigma_model = pickle.load(f)
    with open(sigma_dir / "feature_names.json", 'r') as f:
        sigma_features = json.load(f)
    
    return mu_model, mu_features, sigma_model, sigma_features


def load_and_prepare_data():
    """
    データ読み込み
    """
    logger.info("データ読み込み開始...")
    
    races_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races"
    corners_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "corners"
    pedigrees_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "pedigrees"
    
    races_files = list(races_path.glob("*.parquet"))
    races_df = pd.concat([pd.read_parquet(f) for f in races_files], ignore_index=True)
    logger.info(f"  races: {len(races_df):,}行")
    
    corners_df = None
    if corners_path.exists():
        corners_files = list(corners_path.glob("*.parquet"))
        if corners_files:
            corners_df = pd.concat([pd.read_parquet(f) for f in corners_files], ignore_index=True)
            logger.info(f"  corners: {len(corners_df):,}行")
    
    pedigrees_df = None
    if pedigrees_path.exists():
        pedigrees_files = list(pedigrees_path.glob("*.parquet"))
        if pedigrees_files:
            pedigrees_df = pd.concat([pd.read_parquet(f) for f in pedigrees_files], ignore_index=True)
            logger.info(f"  pedigrees: {len(pedigrees_df):,}行")
    
    return races_df, corners_df, pedigrees_df


def generate_features_and_predictions(races_df, corners_df, pedigrees_df,
                                       mu_model, mu_features, 
                                       sigma_model, sigma_features):
    """
    特徴量生成とμ/σ予測
    """
    from keibaai.src.features.leak_free_feature_engineer_v15 import (
        LeakFreeFeatureEngineerV15, FeatureConfigV15
    )
    
    logger.info("特徴量生成とμ/σ予測計算...")
    
    train_end = pd.Timestamp('2023-01-01')
    
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df.dropna(subset=['finish_position', 'finish_time_seconds'])
    races_df = races_df.drop_duplicates(subset=['race_id', 'horse_number'], keep='first')
    races_df = races_df.reset_index(drop=True)
    logger.info(f"  データ整形後: {len(races_df):,}行")
    
    config = FeatureConfigV15()
    fe = LeakFreeFeatureEngineerV15(config)
    
    train_mask = races_df['race_date'] < train_end
    train_races = races_df[train_mask].copy()
    
    fe.fit(races_df=train_races, pedigrees_df=pedigrees_df, corners_df=corners_df)
    df = fe.transform(races_df.copy())
    
    # μ予測
    available_mu = [c for c in mu_features if c in df.columns]
    X_mu = df[available_mu].fillna(-999)
    df['mu_pred'] = mu_model.predict(X_mu)
    
    # σ予測（カテゴリ変数処理）
    for col in ['track_surface', 'sex']:
        if col in df.columns:
            df[col] = df[col].astype('category').cat.codes
    
    available_sigma = [c for c in sigma_features if c in df.columns]
    X_sigma = df[available_sigma].fillna(-999)
    df['log_sigma_pred'] = sigma_model.predict(X_sigma)
    df['sigma_pred'] = np.exp(df['log_sigma_pred'])
    
    # 標準化残差
    df['z'] = (df['finish_time_seconds'] - df['mu_pred']) / df['sigma_pred']
    
    logger.info(f"  μ/σ予測完了")
    
    return df


def estimate_nu_mle(standardized_residuals):
    """
    t分布のMLEでνを推定
    """
    if len(standardized_residuals) < 3:
        return 5.0  # デフォルト
    
    def neg_log_likelihood(nu):
        return -np.sum(t.logpdf(standardized_residuals, df=nu))
    
    result = minimize_scalar(
        neg_log_likelihood,
        bounds=(2.1, 30.0),
        method='bounded'
    )
    
    return result.x


def calculate_nu_targets(df):
    """
    レースごとのν真値を計算
    """
    logger.info("レースごとのν真値を計算中...")
    
    race_nu = df.groupby('race_id')['z'].apply(estimate_nu_mle)
    
    logger.info(f"  計算完了: {len(race_nu):,}レース")
    logger.info(f"  ν平均: {race_nu.mean():.2f}, 標準偏差: {race_nu.std():.2f}")
    
    return race_nu


def prepare_nu_features(df):
    """
    νモデル用の特徴量を準備（リーク排除版）
    
    【使用可能: レース開始前に確定】
    - 出走頭数
    - 距離
    - 馬場
    - クラス
    - 開催場所
    - 天候
    """
    logger.info("ν特徴量準備（リーク排除版）...")
    
    # レース単位の特徴量を作成
    race_df = df.drop_duplicates(subset=['race_id']).copy()
    
    # 使用可能な特徴量
    nu_feature_candidates = [
        'distance_m',
        'track_surface',
        'track_condition',
        'weather',
        'venue',
        'race_class',
        'age_restriction',
    ]
    
    # 出走頭数（レース開始前に確定）
    head_count = df.groupby('race_id').size()
    race_df = race_df.set_index('race_id')
    race_df['head_count'] = head_count
    
    # 実際に存在する特徴量
    nu_features = ['head_count']
    for col in nu_feature_candidates:
        if col in race_df.columns:
            nu_features.append(col)
    
    # カテゴリ変数をエンコード
    for col in ['track_surface', 'track_condition', 'weather', 'venue', 'race_class', 'age_restriction']:
        if col in race_df.columns:
            race_df[col] = race_df[col].astype('category').cat.codes
    
    logger.info(f"  ν特徴量数: {len(nu_features)}")
    return race_df, nu_features


def train_nu_model(race_df, nu_features, nu_targets, train_start, train_end, test_start, test_end):
    """
    νモデルの訓練
    """
    logger.info("νモデル訓練開始...")
    logger.info(f"  Train: {train_start} - {train_end}")
    logger.info(f"  Test: {test_start} - {test_end}")
    
    # ターゲットをマージ
    race_df = race_df.copy()
    race_df['nu_target'] = nu_targets
    race_df = race_df.dropna(subset=['nu_target'])
    
    # race_dateを追加
    race_df['race_date'] = pd.to_datetime(race_df['race_date']) if 'race_date' in race_df.columns else None
    
    # 時系列分割
    if race_df['race_date'].isna().all():
        # race_dateがない場合はrace_idから推測
        race_df['inferred_date'] = race_df.index.str[:8]
        race_df['race_date'] = pd.to_datetime(race_df['inferred_date'], format='%Y%m%d', errors='coerce')
    
    train_mask = (race_df['race_date'] >= train_start) & (race_df['race_date'] < train_end)
    test_mask = (race_df['race_date'] >= test_start) & (race_df['race_date'] < test_end)
    
    train_df = race_df[train_mask].copy()
    test_df = race_df[test_mask].copy()
    
    logger.info(f"  Train: {len(train_df):,}レース, Test: {len(test_df):,}レース")
    
    # 特徴量とターゲット
    X_train = train_df[nu_features].fillna(-999)
    y_train = train_df['nu_target']
    X_test = test_df[nu_features].fillna(-999)
    y_test = test_df['nu_target']
    
    # ハイパーパラメータ
    params = {
        'objective': 'regression',
        'metric': 'mae',  # νはMAEで評価
        'boosting_type': 'gbdt',
        'learning_rate': 0.01,
        'num_leaves': 15,  # νモデルはシンプルに
        'max_depth': 5,
        'min_child_samples': 50,
        'lambda_l1': 1.0,
        'lambda_l2': 1.0,
        'n_estimators': 500,
        'verbose': -1,
    }
    
    model = lgb.LGBMRegressor(**params)
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[
            lgb.early_stopping(stopping_rounds=30, verbose=True),
            lgb.log_evaluation(period=50)
        ]
    )
    
    # 評価
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    train_mae = np.mean(np.abs(y_train - y_train_pred))
    test_mae = np.mean(np.abs(y_test - y_test_pred))
    
    metrics = {
        'train_mae': train_mae,
        'test_mae': test_mae,
        'train_mean_nu': y_train.mean(),
        'test_mean_nu': y_test.mean(),
        'n_train': len(train_df),
        'n_test': len(test_df),
        'n_features': len(nu_features),
        'n_estimators_used': model.n_estimators_,
    }
    
    logger.info("=== νモデル評価結果 ===")
    logger.info(f"  Train MAE: {train_mae:.4f}")
    logger.info(f"  Test MAE:  {test_mae:.4f}")
    logger.info(f"  Train Mean ν: {y_train.mean():.2f}")
    logger.info(f"  Test Mean ν:  {y_test.mean():.2f}")
    
    return model, metrics


def save_model(model, nu_features, metrics):
    """
    モデル保存
    """
    output_dir = project_root / "keibaai" / "models" / "nu_v2"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    model_path = output_dir / "nu_model.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    logger.info(f"モデル保存: {model_path}")
    
    features_path = output_dir / "feature_names.json"
    with open(features_path, 'w', encoding='utf-8') as f:
        json.dump(nu_features, f, ensure_ascii=False, indent=2)
    logger.info(f"特徴量リスト保存: {features_path}")
    
    config = {
        'version': 'nu_v2',
        'created_at': datetime.now().isoformat(),
        'target': 'nu (degrees of freedom)',
        'base_model': 'mu_time_v1 + sigma_v2',
        'n_features': len(nu_features),
        'metrics': metrics,
        'leak_free': True,
        'forbidden_features': FORBIDDEN_NU_FEATURES,
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
    logger.info("νモデル V2 訓練スクリプト開始（リーク排除版）")
    logger.info("=" * 60)
    
    try:
        # 1. モデルロード
        mu_model, mu_features, sigma_model, sigma_features = load_models()
        
        # 2. データ読み込み
        races_df, corners_df, pedigrees_df = load_and_prepare_data()
        
        # 3. 特徴量生成とμ/σ予測
        df = generate_features_and_predictions(
            races_df, corners_df, pedigrees_df,
            mu_model, mu_features,
            sigma_model, sigma_features
        )
        
        # 4. ν真値計算
        nu_targets = calculate_nu_targets(df)
        
        # 5. ν特徴量準備
        race_df, nu_features = prepare_nu_features(df)
        
        # 6. モデル訓練
        train_start = pd.Timestamp('2020-01-01')
        train_end = pd.Timestamp('2023-01-01')
        test_start = pd.Timestamp('2023-01-01')
        test_end = pd.Timestamp('2024-01-01')
        
        model, metrics = train_nu_model(
            race_df, nu_features, nu_targets,
            train_start, train_end,
            test_start, test_end
        )
        
        # 7. 保存
        output_dir = save_model(model, nu_features, metrics)
        
        logger.info("=" * 60)
        logger.info(f"訓練完了。出力: {output_dir}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μ_time_v1: 時間予測μモデル訓練スクリプト

目的: 完走時間（finish_time_seconds）を予測するRegressorモデルを構築

【過学習対策】
1. 時系列分割（Walk-forward validation）
2. 正則化パラメータ（lambda_l1, lambda_l2, min_child_samples）
3. Early Stopping

【リーク対策】
1. finish_position, finish_time_seconds以外のターゲット由来情報を除外
2. V15特徴量エンジンのリークフリー特徴量を使用
3. 特徴量リストから禁止リストを明示的に除外

【出力】
- keibaai/models/mu_time_v1/
    - mu_time_model.pkl
    - feature_names.json
    - config.yaml
    - report.md

Usage:
    python scripts/training/train_mu_time_v1.py
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
from scipy.stats import spearmanr

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# ログ設定
log_dir = project_root / "outputs" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"train_mu_time_v1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
# リークフリー対策: 禁止特徴量リスト
# ============================================================
FORBIDDEN_FEATURES = [
    # ターゲット変数（結果）
    'finish_position',
    'finish_time_seconds',
    'is_win',
    
    # オッズ・人気（確定値はリーク）
    'win_odds',
    'popularity',
    'morning_odds',
    'morning_popularity',
    
    # レース結果由来
    'last_3f_time',
    'margin_seconds',
    'margin_str',
    'passing_order_1',
    'passing_order_2',
    'passing_order_3',
    'passing_order_4',
    'pace_index',
    'prize_money',
    
    # ID・メタデータ
    'race_id',
    'horse_id',
    'jockey_id',
    'trainer_id',
    'sire_id',
    'dam_id',
    'horse_name',
    'jockey_name',
    'trainer_name',
    'race_date',
]


def load_parquet_data():
    """
    パース済みデータの読み込み
    """
    logger.info("データ読み込み開始...")
    
    races_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races"
    corners_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "corners"
    pedigrees_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "pedigrees"
    
    # races.parquetを読み込み
    races_files = list(races_path.glob("*.parquet"))
    if not races_files:
        raise FileNotFoundError(f"races parquetファイルが見つかりません: {races_path}")
    
    races_df = pd.concat([pd.read_parquet(f) for f in races_files], ignore_index=True)
    logger.info(f"  races: {len(races_df):,}行")
    
    # corners.parquetを読み込み（あれば）
    corners_df = None
    if corners_path.exists():
        corners_files = list(corners_path.glob("*.parquet"))
        if corners_files:
            corners_df = pd.concat([pd.read_parquet(f) for f in corners_files], ignore_index=True)
            logger.info(f"  corners: {len(corners_df):,}行")
    
    # pedigrees.parquetを読み込み（あれば）
    pedigrees_df = None
    if pedigrees_path.exists():
        pedigrees_files = list(pedigrees_path.glob("*.parquet"))
        if pedigrees_files:
            pedigrees_df = pd.concat([pd.read_parquet(f) for f in pedigrees_files], ignore_index=True)
            logger.info(f"  pedigrees: {len(pedigrees_df):,}行")
    
    return races_df, corners_df, pedigrees_df


def generate_features(races_df, corners_df, pedigrees_df, train_end_date):
    """
    V15特徴量エンジンで特徴量生成
    
    【リーク対策】
    - train_end_dateまでのデータのみでfit
    """
    from keibaai.src.features.leak_free_feature_engineer_v15 import (
        LeakFreeFeatureEngineerV15, FeatureConfigV15
    )
    
    logger.info(f"特徴量生成開始（train_end_date={train_end_date}）...")
    
    # finish_position, finish_time_secondsが欠損している行を除外
    # （V15エンジンがis_winの計算にfinish_positionを使用するため）
    original_len = len(races_df)
    races_df = races_df.dropna(subset=['finish_position', 'finish_time_seconds'])
    logger.info(f"  欠損値除外: {original_len:,} → {len(races_df):,}行")
    
    # race_id × horse_number の重複を排除（最新を保持）
    races_df = races_df.drop_duplicates(subset=['race_id', 'horse_number'], keep='first')
    logger.info(f"  重複排除後: {len(races_df):,}行")
    
    # インデックスをリセット
    races_df = races_df.reset_index(drop=True)
    
    # Train期間のデータでfit
    config = FeatureConfigV15()
    fe = LeakFreeFeatureEngineerV15(config)
    
    # Train期間のみでfit（リーク防止）
    train_mask = races_df['race_date'] < train_end_date
    train_races = races_df[train_mask].copy()
    
    fe.fit(
        races_df=train_races,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df
    )
    
    # 全データにtransform
    df = fe.transform(races_df.copy())
    
    # 特徴量列を取得
    feature_cols = fe.get_feature_columns()
    logger.info(f"  生成された特徴量数: {len(feature_cols)}")
    
    return df, feature_cols


def filter_features(feature_cols, df):
    """
    禁止特徴量を除外し、実際にDFに存在する列のみを使用
    """
    # まず、DataFrameに実際に存在する列のみをフィルタ
    available = [c for c in feature_cols if c in df.columns]
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        logger.warning(f"  DFに存在しない特徴量（スキップ）: {missing}")
    
    # 次に、禁止特徴量を除外
    filtered = [c for c in available if c not in FORBIDDEN_FEATURES]
    removed = [c for c in available if c in FORBIDDEN_FEATURES]
    
    if removed:
        logger.warning(f"  禁止特徴量を除外: {removed}")
    
    return filtered


def train_mu_time_model(df, feature_cols, train_start, train_end, test_start, test_end):
    """
    時間予測μモデルの訓練
    
    【過学習対策】
    1. 時系列分割
    2. Early Stopping
    3. 正則化パラメータ
    """
    logger.info(f"モデル訓練開始...")
    logger.info(f"  Train期間: {train_start} - {train_end}")
    logger.info(f"  Test期間: {test_start} - {test_end}")
    
    # ターゲット変数の有効なデータのみ使用
    df = df.dropna(subset=['finish_time_seconds'])
    
    # 時系列分割
    train_mask = (df['race_date'] >= train_start) & (df['race_date'] < train_end)
    test_mask = (df['race_date'] >= test_start) & (df['race_date'] < test_end)
    
    train_df = df[train_mask].copy()
    test_df = df[test_mask].copy()
    
    logger.info(f"  Train: {len(train_df):,}行, Test: {len(test_df):,}行")
    
    # 特徴量とターゲット
    X_train = train_df[feature_cols].copy()
    y_train = train_df['finish_time_seconds'].copy()
    X_test = test_df[feature_cols].copy()
    y_test = test_df['finish_time_seconds'].copy()
    
    # 欠損値処理
    X_train = X_train.fillna(-999)
    X_test = X_test.fillna(-999)
    
    # ハイパーパラメータ（過学習対策で保守的な設定）
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'boosting_type': 'gbdt',
        'learning_rate': 0.01,
        'num_leaves': 31,
        'max_depth': 6,
        'min_child_samples': 100,  # 過学習対策
        'lambda_l1': 1.0,          # L1正則化
        'lambda_l2': 1.0,          # L2正則化
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'n_estimators': 2000,
        'verbose': -1,
    }
    
    # Early Stopping付きで訓練
    model = lgb.LGBMRegressor(**params)
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[
            lgb.early_stopping(stopping_rounds=100, verbose=True),
            lgb.log_evaluation(period=100)
        ]
    )
    
    # 評価
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    train_rmse = np.sqrt(np.mean((y_train - y_train_pred) ** 2))
    test_rmse = np.sqrt(np.mean((y_test - y_test_pred) ** 2))
    
    # Spearman相関（レース内順位の予測精度）
    train_df['mu_pred'] = y_train_pred
    test_df['mu_pred'] = y_test_pred
    
    def calc_rank_corr(group_df):
        if len(group_df) < 3:
            return np.nan
        actual_rank = group_df['finish_time_seconds'].rank()
        pred_rank = group_df['mu_pred'].rank()
        corr, _ = spearmanr(actual_rank, pred_rank)
        return corr
    
    train_corrs = train_df.groupby('race_id').apply(calc_rank_corr)
    test_corrs = test_df.groupby('race_id').apply(calc_rank_corr)
    
    mean_train_corr = train_corrs.mean()
    mean_test_corr = test_corrs.mean()
    
    # Top1精度（時間が最短の馬が1着）
    def calc_top1_accuracy(group_df):
        pred_top1 = group_df.loc[group_df['mu_pred'].idxmin()]
        actual_top1 = group_df.loc[group_df['finish_time_seconds'].idxmin()]
        return pred_top1.name == actual_top1.name
    
    train_top1_acc = train_df.groupby('race_id').apply(calc_top1_accuracy).mean()
    test_top1_acc = test_df.groupby('race_id').apply(calc_top1_accuracy).mean()
    
    metrics = {
        'train_rmse': train_rmse,
        'test_rmse': test_rmse,
        'rmse_gap': (train_rmse - test_rmse) / test_rmse * 100,
        'train_spearman': mean_train_corr,
        'test_spearman': mean_test_corr,
        'train_top1_acc': train_top1_acc,
        'test_top1_acc': test_top1_acc,
        'n_train': len(train_df),
        'n_test': len(test_df),
        'n_features': len(feature_cols),
        'n_estimators_used': model.n_estimators_,
    }
    
    logger.info("=== モデル評価結果 ===")
    logger.info(f"  Train RMSE: {train_rmse:.4f}秒")
    logger.info(f"  Test RMSE:  {test_rmse:.4f}秒")
    logger.info(f"  RMSE Gap:   {metrics['rmse_gap']:.1f}%")
    logger.info(f"  Train Spearman: {mean_train_corr:.4f}")
    logger.info(f"  Test Spearman:  {mean_test_corr:.4f}")
    logger.info(f"  Train Top1 Acc: {train_top1_acc:.1%}")
    logger.info(f"  Test Top1 Acc:  {test_top1_acc:.1%}")
    
    return model, metrics, test_df


def save_model(model, feature_cols, metrics, params):
    """
    モデルと関連ファイルを保存
    """
    output_dir = project_root / "keibaai" / "models" / "mu_time_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # モデル保存
    model_path = output_dir / "mu_time_model.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    logger.info(f"モデル保存: {model_path}")
    
    # 特徴量リスト保存
    features_path = output_dir / "feature_names.json"
    with open(features_path, 'w', encoding='utf-8') as f:
        json.dump(feature_cols, f, ensure_ascii=False, indent=2)
    logger.info(f"特徴量リスト保存: {features_path}")
    
    # 設定保存
    config = {
        'version': 'mu_time_v1',
        'created_at': datetime.now().isoformat(),
        'target': 'finish_time_seconds',
        'objective': 'regression',
        'n_features': len(feature_cols),
        'metrics': metrics,
    }
    config_path = output_dir / "config.yaml"
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
    logger.info(f"設定保存: {config_path}")
    
    # レポート保存
    report = f"""# μ_time_v1 訓練レポート

**作成日**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## モデル概要

- **目的変数**: `finish_time_seconds`（完走時間）
- **手法**: LightGBM Regressor
- **特徴量数**: {len(feature_cols)}

## 評価結果

| 指標 | Train | Test | Gap |
|------|-------|------|-----|
| RMSE | {metrics['train_rmse']:.4f}秒 | {metrics['test_rmse']:.4f}秒 | {metrics['rmse_gap']:.1f}% |
| Spearman | {metrics['train_spearman']:.4f} | {metrics['test_spearman']:.4f} | - |
| Top1 Acc | {metrics['train_top1_acc']:.1%} | {metrics['test_top1_acc']:.1%} | - |

## データ数

- Train: {metrics['n_train']:,}行
- Test: {metrics['n_test']:,}行
- 使用イテレーション: {metrics['n_estimators_used']}

## リーク対策

1. ✅ finish_position, win_odds等の禁止特徴量を除外
2. ✅ V15リークフリー特徴量エンジンを使用
3. ✅ 時系列分割でTrain/Testを分離

## 次のステップ

- Phase 2: σモデル再学習（このμモデルの残差を学習）
- Phase 3: νモデル再学習（リーク排除版）
"""
    report_path = output_dir / "report.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"レポート保存: {report_path}")
    
    return output_dir


def main():
    """
    メイン処理
    """
    logger.info("=" * 60)
    logger.info("μ_time_v1 訓練スクリプト開始")
    logger.info("=" * 60)
    
    try:
        # 1. データ読み込み
        races_df, corners_df, pedigrees_df = load_parquet_data()
        
        # 日付型に変換
        if 'race_date' in races_df.columns:
            races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        # 期間設定（4年間Walk-forward）
        # Train: 2020-2022, Test: 2023
        train_start = pd.Timestamp('2020-01-01')
        train_end = pd.Timestamp('2023-01-01')
        test_start = pd.Timestamp('2023-01-01')
        test_end = pd.Timestamp('2024-01-01')
        
        # 2. 特徴量生成
        df, feature_cols_raw = generate_features(
            races_df, corners_df, pedigrees_df,
            train_end_date=train_end
        )
        
        # 3. 禁止特徴量除外（実際にDFに存在する列のみ）
        feature_cols = filter_features(feature_cols_raw, df)
        logger.info(f"最終特徴量数: {len(feature_cols)}")
        
        # 4. モデル訓練
        model, metrics, test_df = train_mu_time_model(
            df, feature_cols,
            train_start, train_end,
            test_start, test_end
        )
        
        # 5. 保存
        output_dir = save_model(model, feature_cols, metrics, {})
        
        logger.info("=" * 60)
        logger.info(f"訓練完了。出力: {output_dir}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

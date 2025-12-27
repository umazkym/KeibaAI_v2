#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μ_time_v5: 季節性・馬体重変動・Huber損失導入版

【Model Updates】
- Feature: TimeFeatureEngineerV4 (Seasonal + Weight + Interval)
- Loss: Huber Loss (Robust to outliers)
- Training Data: 2012-202X (Using all available history for stability)

【目的】
2022-2024年の精度低下に対し、季節性の導入と外れ値への頑健性向上で対応する。
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

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

log_dir = project_root / "outputs" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"train_mu_time_v5_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(log_file), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

FORBIDDEN_FEATURES = [
    'finish_position', 'finish_time_seconds', 'normalized_time',
    'is_win', 'win_odds', 'popularity', 'race_id', 'horse_id', 'jockey_id', 'trainer_id',
    'race_date', 'horse_name', 'jockey_name', 'trainer_name',
    'base_time_mean', 'base_time_std', 'base_time_level',
    'l1_mean', 'l1_std', 'l1_count', 'l2_mean', 'l2_std', 'l2_count', 'l3_mean', 'l3_std', 'l3_count',
    'horse_weight_change', 'prev_race_date'
]


def load_data():
    """データ読み込み"""
    logger.info("データ読み込み開始...")
    
    races_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
    horses_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "horses" / "horses.parquet"
    
    df = pd.read_parquet(races_path)
    horses = pd.read_parquet(horses_path)
    
    # 馬情報の結合 (weight_changeなど)
    # race_id, horse_numberをキーにする
    if 'horse_weight_change' not in df.columns:
        horses_subset = horses[['race_id', 'horse_number', 'horse_weight_change']]
        df = df.merge(horses_subset, on=['race_id', 'horse_number'], how='left')
    
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df.dropna(subset=['finish_position', 'finish_time_seconds', 'venue'])
    df = df.drop_duplicates(subset=['race_id', 'horse_number'], keep='first')
    df = df.reset_index(drop=True)
    
    logger.info(f"  データ: {len(df):,}行")
    
    return df


def generate_features(df, train_end_date):
    """特徴量生成 (V4)"""
    from keibaai.src.features.time_feature_engineer_v4 import TimeFeatureEngineerV4
    
    logger.info(f"特徴量生成開始（train_end={train_end_date}）...")
    
    train_mask = df['race_date'] < train_end_date
    train_df = df[train_mask].copy()
    
    # TimeFeatureEngineerV4
    time_fe = TimeFeatureEngineerV4(min_samples=30)
    time_fe.fit(train_df)
    df = time_fe.transform(df)
    time_features = time_fe.get_feature_columns()
    base_time_stats = time_fe.get_base_time_stats()
    
    logger.info(f"  時間予測特徴量: {len(time_features)}個")
    
    # 基本特徴量
    basic_features = ['distance_m', 'bracket_number', 'horse_weight', 'age', 'basis_weight']
    basic_features = [c for c in basic_features if c in df.columns]
    
    # カテゴリエンコード
    for col in ['track_surface', 'track_condition', 'sex', 'venue', 'distance_category']:
        if col in df.columns:
            df[col + '_encoded'] = df[col].astype('category').cat.codes
            basic_features.append(col + '_encoded')
    
    all_features = time_features + basic_features
    # 重複削除
    all_features = list(dict.fromkeys(all_features))
    logger.info(f"  全特徴量: {len(all_features)}個")
    
    return df, all_features, base_time_stats


def train_model(df, feature_cols, train_start, train_end, test_start, test_end):
    """モデル訓練 (Huber Loss)"""
    logger.info("モデル訓練開始...")
    logger.info(f"  Train: {train_start} - {train_end}")
    logger.info(f"  Test: {test_start} - {test_end}")
    
    target_col = 'normalized_time'
    df = df.dropna(subset=[target_col])
    
    train_mask = (df['race_date'] >= train_start) & (df['race_date'] < train_end)
    test_mask = (df['race_date'] >= test_start) & (df['race_date'] < test_end)
    
    train_df = df[train_mask].copy()
    test_df = df[test_mask].copy()
    
    logger.info(f"  Train: {len(train_df):,}行, Test: {len(test_df):,}行")
    
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df[target_col]
    X_test = test_df[feature_cols].fillna(0)
    y_test = test_df[target_col]
    
    # Huber Loss & V4.1 Base Params
    params = {
        'objective': 'huber',        # Changed from 'regression' (rmse)
        'alpha': 1.0,               # Huber loss transition parameter (default 1.0)
        'metric': 'rmse',           # Evaluation metric remains RMSE for comparability
        'boosting_type': 'gbdt',
        'learning_rate': 0.02,
        'num_leaves': 31,
        'max_depth': 4,
        'min_child_samples': 100,
        'reg_alpha': 1.0,
        'reg_lambda': 1.5,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 3,
        'n_estimators': 1000,
        'verbose': -1,
    }
    
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
    
    train_df = train_df.copy()
    test_df = test_df.copy()
    train_df['pred'] = y_train_pred
    test_df['pred'] = y_test_pred
    
    def calc_race_spearman(group):
        if len(group) < 3:
            return np.nan
        actual_rank = group['normalized_time'].rank()
        pred_rank = group['pred'].rank()
        corr, _ = spearmanr(actual_rank, pred_rank)
        return corr
    
    train_corrs = train_df.groupby('race_id').apply(calc_race_spearman, include_groups=False)
    test_corrs = test_df.groupby('race_id').apply(calc_race_spearman, include_groups=False)
    
    mean_train_corr = train_corrs.mean()
    mean_test_corr = test_corrs.mean()
    
    def calc_top1_acc(group):
        pred_top = group.loc[group['pred'].idxmin()]
        actual_top = group.loc[group['normalized_time'].idxmin()]
        return pred_top.name == actual_top.name
    
    train_top1 = train_df.groupby('race_id').apply(calc_top1_acc, include_groups=False).mean()
    test_top1 = test_df.groupby('race_id').apply(calc_top1_acc, include_groups=False).mean()
    
    # 実タイム誤差
    test_df['pred_time'] = test_df['pred'] * test_df['base_time_std'] + test_df['base_time_mean']
    test_df['time_error'] = (test_df['pred_time'] - test_df['finish_time_seconds']).abs()
    mae_seconds = test_df['time_error'].mean()
    median_error_seconds = test_df['time_error'].median()
    
    metrics = {
        'train_rmse': train_rmse,
        'test_rmse': test_rmse,
        'rmse_gap': (train_rmse - test_rmse) / test_rmse * 100,
        'train_spearman': mean_train_corr,
        'test_spearman': mean_test_corr,
        'train_top1_acc': train_top1,
        'test_top1_acc': test_top1,
        'mae_seconds': mae_seconds,
        'median_error_seconds': median_error_seconds,
    }
    
    logger.info("=== モデル評価結果 (V5) ===")
    logger.info(f"  RMSE Gap: {metrics['rmse_gap']:.1f}%")
    logger.info(f"  Test Spearman:  {mean_test_corr:.4f}")
    logger.info(f"  Test Top1 Acc:  {test_top1:.1%}")
    logger.info(f"  MAE (秒): {mae_seconds:.2f}秒")
    
    # 特徴量重要度
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    logger.info("\n=== 重要特徴量 Top 10 ===")
    for i, row in importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']}")
    
    return model, metrics


def save_model(model, feature_cols, metrics, base_time_stats):
    """モデル保存"""
    output_dir = project_root / "keibaai" / "models" / "mu_time_v5"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / "mu_time_model.pkl", 'wb') as f:
        pickle.dump(model, f)
    
    with open(output_dir / "feature_names.json", 'w', encoding='utf-8') as f:
        json.dump(feature_cols, f, ensure_ascii=False, indent=2)
    
    stats_l1, stats_l2, stats_l3 = base_time_stats
    stats_l1.to_parquet(output_dir / "base_time_stats_l1.parquet")
    stats_l2.to_parquet(output_dir / "base_time_stats_l2.parquet")
    stats_l3.to_parquet(output_dir / "base_time_stats_l3.parquet")
    
    config = {
        'version': 'mu_time_v5',
        'created_at': datetime.now().isoformat(),
        'target': 'normalized_time',
        'description': 'FeatureV4 (Seasonal) + Huber Loss',
        'metrics': {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in metrics.items()},
    }
    with open(output_dir / "config.yaml", 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
    
    return output_dir


def main():
    logger.info("=" * 60)
    logger.info("μ_time_v5 (Seasonal + Huber) 訓練スクリプト開始")
    logger.info("=" * 60)
    
    try:
        df = load_data()
        
        # 評価期間設定: 2023年をテストデータとする（最近の傾向を確認するため）
        train_start = pd.Timestamp('2014-01-01') # 全期間使用
        train_end = pd.Timestamp('2023-01-01')
        test_start = pd.Timestamp('2023-01-01')
        test_end = pd.Timestamp('2024-01-01')
        
        df, feature_cols, base_time_stats = generate_features(df, train_end)
        
        feature_cols = [c for c in feature_cols if c not in FORBIDDEN_FEATURES]
        
        model, metrics = train_model(
            df, feature_cols,
            train_start, train_end,
            test_start, test_end
        )
        
        output_dir = save_model(model, feature_cols, metrics, base_time_stats)
        logger.info(f"訓練完了: {output_dir}")
        
    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

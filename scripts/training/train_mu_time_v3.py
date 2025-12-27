#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μ_time_v3: 課題解決版時間予測モデル

【V2からの改善】
1. 芝・不良馬場のバイアス補正
2. 上がり3F特徴量の強化
3. レースラップ特徴量の活用

【リーク対策】
- 全特徴量は shift(1) + expanding() で計算
- バイアス補正もTrain期間のデータのみで計算

【過学習対策】
- V15参考の強正則化
- Early Stopping

Usage:
    python scripts/training/train_mu_time_v3.py
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
log_file = log_dir / f"train_mu_time_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
    'finish_position', 'finish_time_seconds', 'normalized_time', 'normalized_time_corrected',
    'is_win', 'win_odds', 'popularity', 'race_id', 'horse_id', 'jockey_id', 'trainer_id',
    'race_date', 'horse_name', 'jockey_name', 'trainer_name', 'base_time_mean', 'base_time_std',
    'base_time_mean_corrected', 'venue_base_time_mean', 'venue_base_time_std', 'base_time_count',
]


def load_data():
    """データ読み込み"""
    logger.info("データ読み込み開始...")
    
    races_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
    race_details_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "race_details" / "race_details.parquet"
    
    df = pd.read_parquet(races_path)
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df.dropna(subset=['finish_position', 'finish_time_seconds'])
    df = df.drop_duplicates(subset=['race_id', 'horse_number'], keep='first')
    df = df.reset_index(drop=True)
    
    logger.info(f"  races: {len(df):,}行")
    
    race_details_df = None
    if race_details_path.exists():
        race_details_df = pd.read_parquet(race_details_path)
        logger.info(f"  race_details: {len(race_details_df):,}行")
    
    return df, race_details_df


def generate_features(df, race_details_df, train_end_date):
    """特徴量生成"""
    from keibaai.src.features.time_feature_engineer_v2 import TimeFeatureEngineerV2
    
    logger.info(f"特徴量生成開始（train_end={train_end_date}）...")
    
    train_mask = df['race_date'] < train_end_date
    train_df = df[train_mask].copy()
    
    # 時間予測特化特徴量V2
    time_fe = TimeFeatureEngineerV2()
    time_fe.fit(train_df, race_details_df)
    df = time_fe.transform(df)
    time_features = time_fe.get_feature_columns()
    base_time_stats = time_fe.get_base_time_stats()
    track_condition_bias = time_fe.get_track_condition_bias()
    
    logger.info(f"  時間予測特徴量: {len(time_features)}個")
    logger.info(f"  馬場バイアス: {track_condition_bias}")
    
    # 基本特徴量
    basic_features = ['distance_m', 'bracket_number', 'horse_weight', 'age', 'basis_weight']
    basic_features = [c for c in basic_features if c in df.columns]
    
    # カテゴリエンコード
    for col in ['track_surface', 'track_condition', 'sex', 'venue', 'distance_category']:
        if col in df.columns:
            df[col + '_encoded'] = df[col].astype('category').cat.codes
            basic_features.append(col + '_encoded')
    
    all_features = time_features + basic_features
    logger.info(f"  全特徴量: {len(all_features)}個")
    
    return df, all_features, base_time_stats, track_condition_bias


def train_model(df, feature_cols, train_start, train_end, test_start, test_end):
    """モデル訓練"""
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
    
    # V15参考の強正則化
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'boosting_type': 'gbdt',
        'learning_rate': 0.02,
        'num_leaves': 31,
        'max_depth': 4,
        'min_child_samples': 100,
        'reg_alpha': 2.0,
        'reg_lambda': 3.0,
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
    
    # 実タイム誤差も計算
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
        'n_train': len(train_df),
        'n_test': len(test_df),
        'n_features': len(feature_cols),
        'n_estimators_used': model.n_estimators_,
    }
    
    logger.info("=== モデル評価結果 ===")
    logger.info(f"  Train RMSE (標準化): {train_rmse:.4f}")
    logger.info(f"  Test RMSE (標準化):  {test_rmse:.4f}")
    logger.info(f"  RMSE Gap: {metrics['rmse_gap']:.1f}%")
    logger.info(f"  Train Spearman: {mean_train_corr:.4f}")
    logger.info(f"  Test Spearman:  {mean_test_corr:.4f}")
    logger.info(f"  Train Top1 Acc: {train_top1:.1%}")
    logger.info(f"  Test Top1 Acc:  {test_top1:.1%}")
    logger.info(f"  MAE (秒): {mae_seconds:.2f}秒")
    logger.info(f"  Median Error (秒): {median_error_seconds:.2f}秒")
    
    return model, metrics


def save_model(model, feature_cols, metrics, base_time_stats, track_condition_bias):
    """モデル保存"""
    output_dir = project_root / "keibaai" / "models" / "mu_time_v3"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / "mu_time_model.pkl", 'wb') as f:
        pickle.dump(model, f)
    logger.info(f"モデル保存: {output_dir / 'mu_time_model.pkl'}")
    
    with open(output_dir / "feature_names.json", 'w', encoding='utf-8') as f:
        json.dump(feature_cols, f, ensure_ascii=False, indent=2)
    
    base_time_stats.to_parquet(output_dir / "base_time_stats.parquet")
    
    with open(output_dir / "track_condition_bias.json", 'w', encoding='utf-8') as f:
        bias_serializable = {f"{k[0]}_{k[1]}": v for k, v in track_condition_bias.items()}
        json.dump(bias_serializable, f, ensure_ascii=False, indent=2)
    
    config = {
        'version': 'mu_time_v3',
        'created_at': datetime.now().isoformat(),
        'target': 'normalized_time',
        'description': '芝不良バイアス補正 + 上がり3F強化',
        'metrics': {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in metrics.items()},
    }
    with open(output_dir / "config.yaml", 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
    
    report = f"""# μ_time_v3 訓練レポート

**作成日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}

## V2からの改善点

1. **芝不良バイアス補正**: 馬場状態別の時間補正係数を学習
2. **上がり3F強化**: 距離別・馬場別・改善傾向など詳細特徴量

## 評価結果

| 指標 | V2 | V3 |
|------|-----|-----|
| Spearman | 0.40 | **{metrics['test_spearman']:.4f}** |
| Top1 Acc | 21.6% | **{metrics['test_top1_acc']:.1%}** |
| MAE (秒) | 1.20秒 | **{metrics['mae_seconds']:.2f}秒** |
| Median Error | 0.82秒 | **{metrics['median_error_seconds']:.2f}秒** |

## 馬場バイアス（学習結果）

{track_condition_bias}
"""
    with open(output_dir / "report.md", 'w', encoding='utf-8') as f:
        f.write(report)
    
    return output_dir


def main():
    logger.info("=" * 60)
    logger.info("μ_time_v3 訓練スクリプト開始")
    logger.info("=" * 60)
    
    try:
        # データ読み込み
        df, race_details_df = load_data()
        
        # 期間設定
        train_start = pd.Timestamp('2020-01-01')
        train_end = pd.Timestamp('2023-01-01')
        test_start = pd.Timestamp('2023-01-01')
        test_end = pd.Timestamp('2024-01-01')
        
        # 特徴量生成
        df, feature_cols, base_time_stats, track_condition_bias = generate_features(
            df, race_details_df, train_end
        )
        
        # 禁止特徴量除外
        feature_cols = [c for c in feature_cols if c not in FORBIDDEN_FEATURES]
        logger.info(f"最終特徴量数: {len(feature_cols)}")
        
        # モデル訓練
        model, metrics = train_model(
            df, feature_cols,
            train_start, train_end,
            test_start, test_end
        )
        
        # 保存
        output_dir = save_model(model, feature_cols, metrics, base_time_stats, track_condition_bias)
        
        logger.info("=" * 60)
        logger.info(f"訓練完了。出力: {output_dir}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

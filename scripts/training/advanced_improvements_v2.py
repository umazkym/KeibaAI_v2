# -*- coding: utf-8 -*-
"""
Advanced Model Improvements - V15公式設定版

【V15公式設定】
- Train: ~2024-12-31
- Test: 2025-01-01~2025-10-31

【3つの改善】
1. 新特徴量追加（venue_avg_pace, horse_c4_gap_std）
2. アンサンブル学習（Binary + LambdaRank + StrongReg）
3. 確率キャリブレーション（Isotonic Regression）
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.isotonic import IsotonicRegression
from sklearn.model_selection import train_test_split
from pathlib import Path
import logging
import sys
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    data_dir = Path("keibaai/data/parsed/parquet")
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    return races, pedigrees, corners, race_details, horses


def calc_roi(df, preds):
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def add_new_features_v16(df, race_details_df, corners_df, train_df):
    """新特徴量をリークフリーで追加"""
    
    # 1. ペース特徴量（Train期間のみで統計計算）
    train_race_ids = set(train_df['race_id'].unique())
    train_paces = race_details_df[race_details_df['race_id'].isin(train_race_ids)]
    
    # 会場×距離ごとのペース統計
    train_with_info = train_df[['race_id', 'venue', 'distance_m']].drop_duplicates()
    train_with_info = train_with_info.merge(
        train_paces[['race_id', 'first_half', 'second_half']], 
        on='race_id', how='left'
    )
    train_with_info['pace_diff'] = train_with_info['first_half'] - train_with_info['second_half']
    
    venue_pace_stats = train_with_info.groupby(['venue', 'distance_m'])['pace_diff'].mean().to_dict()
    
    # マッピング
    df['venue_avg_pace'] = df.apply(
        lambda x: venue_pace_stats.get((x['venue'], x['distance_m']), 0), axis=1
    )
    
    # 2. C4馬身差の安定性
    c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'gap_from_leader']].copy()
    race_info = train_df[['race_id', 'horse_id', 'horse_number', 'race_date']].drop_duplicates()
    c4 = c4.merge(race_info, on=['race_id', 'horse_number'], how='inner')
    c4 = c4.sort_values(['horse_id', 'race_date'])
    
    # 過去5走のstd（shift済み）
    c4['gap_std'] = c4.groupby('horse_id')['gap_from_leader'].transform(
        lambda x: x.rolling(5, min_periods=2).std().shift(1)
    )
    gap_std_stats = c4.groupby('horse_id')['gap_std'].last().to_dict()
    
    df['horse_c4_gap_std'] = df['horse_id'].map(gap_std_stats)
    median_val = df[df['horse_c4_gap_std'].notna()]['horse_c4_gap_std'].median()
    df['horse_c4_gap_std'] = df['horse_c4_gap_std'].fillna(median_val if pd.notna(median_val) else 0)
    
    return df


def main():
    logger.info("=" * 70)
    logger.info("Advanced Model Improvements - V15公式設定版")
    logger.info("=" * 70)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # V15公式期間
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # 特徴量
    logger.info("\n特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    v15_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"  V15特徴量数: {len(v15_cols)}")
    
    X_train = train_f[v15_cols].fillna(0)
    X_test = test_f[v15_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
    # V15パラメータ
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    # ========== Baseline ==========
    logger.info("\n" + "=" * 70)
    logger.info("Baseline: V15")
    logger.info("=" * 70)
    
    model_v15 = lgb.train(params, lgb.Dataset(X_train, y_train), num_boost_round=200)
    pred_train = model_v15.predict(X_train)
    pred_test = model_v15.predict(X_test)
    
    train_roi, train_hit = calc_roi(train_f, pred_train)
    test_roi, test_hit = calc_roi(test_f, pred_test)
    
    logger.info(f"  Train: ROI={train_roi:.1f}%, Hit={train_hit:.1f}%")
    logger.info(f"  Test:  ROI={test_roi:.1f}%, Hit={test_hit:.1f}%")
    baseline_roi = test_roi
    
    # ========== Phase 1: 新特徴量 ==========
    logger.info("\n" + "=" * 70)
    logger.info("Phase 1: 新特徴量追加")
    logger.info("=" * 70)
    
    train_f_v16 = add_new_features_v16(train_f.copy(), race_details, corners, train_f)
    test_f_v16 = add_new_features_v16(test_f.copy(), race_details, corners, train_f)  # Train期間の統計を使用
    
    v16_cols = v15_cols + ['venue_avg_pace', 'horse_c4_gap_std']
    X_train_v16 = train_f_v16[v16_cols].fillna(0)
    X_test_v16 = test_f_v16[v16_cols].fillna(0)
    
    logger.info(f"  V16特徴量数: {len(v16_cols)}")
    logger.info(f"  venue_avg_pace非ゼロ(Test): {(test_f_v16['venue_avg_pace'] != 0).sum()}/{len(test_f_v16)}")
    
    model_v16 = lgb.train(params, lgb.Dataset(X_train_v16, y_train), num_boost_round=200)
    pred_test_v16 = model_v16.predict(X_test_v16)
    
    test_roi_v16, test_hit_v16 = calc_roi(test_f_v16, pred_test_v16)
    logger.info(f"  Test:  ROI={test_roi_v16:.1f}%, Hit={test_hit_v16:.1f}%")
    logger.info(f"  改善:  {test_roi_v16 - baseline_roi:+.1f}%")
    
    # ========== Phase 2: アンサンブル ==========
    logger.info("\n" + "=" * 70)
    logger.info("Phase 2: アンサンブル学習")
    logger.info("=" * 70)
    
    # 内部Valid分割（アンサンブル重み調整用）
    train_idx, valid_idx = train_test_split(
        range(len(train_f)), test_size=0.15, random_state=42
    )
    
    X_tr = X_train.iloc[train_idx]
    X_val = X_train.iloc[valid_idx]
    y_tr = y_train.iloc[train_idx]
    y_val = y_train.iloc[valid_idx]
    valid_df_for_roi = train_f.iloc[valid_idx]
    
    # Model 1: Binary
    model1 = lgb.train(params, lgb.Dataset(X_tr, y_tr), num_boost_round=200)
    
    # Model 2: Strong Reg
    params_strong = params.copy()
    params_strong.update({'num_leaves': 12, 'max_depth': 2, 'min_child_samples': 200, 'reg_alpha': 5.0, 'reg_lambda': 8.0})
    model2 = lgb.train(params_strong, lgb.Dataset(X_tr, y_tr), num_boost_round=200)
    
    # Valid ROIで重み計算
    pred_val1 = model1.predict(X_val)
    pred_val2 = model2.predict(X_val)
    
    roi1, _ = calc_roi(valid_df_for_roi, pred_val1)
    roi2, _ = calc_roi(valid_df_for_roi, pred_val2)
    
    logger.info(f"  Model1 (Binary): Valid ROI={roi1:.1f}%")
    logger.info(f"  Model2 (StrongReg): Valid ROI={roi2:.1f}%")
    
    # 重み正規化
    w1 = max(roi1 - 80, 0.1)
    w2 = max(roi2 - 80, 0.1)
    total = w1 + w2
    w1, w2 = w1/total, w2/total
    
    # 全Trainで再学習
    model1_full = lgb.train(params, lgb.Dataset(X_train, y_train), num_boost_round=200)
    model2_full = lgb.train(params_strong, lgb.Dataset(X_train, y_train), num_boost_round=200)
    
    pred_test_ens = w1 * model1_full.predict(X_test) + w2 * model2_full.predict(X_test)
    test_roi_ens, test_hit_ens = calc_roi(test_f, pred_test_ens)
    
    logger.info(f"  Ensemble: weights=({w1:.2f}, {w2:.2f})")
    logger.info(f"  Test:  ROI={test_roi_ens:.1f}%, Hit={test_hit_ens:.1f}%")
    logger.info(f"  改善:  {test_roi_ens - baseline_roi:+.1f}%")
    
    # ========== Phase 3: キャリブレーション ==========
    logger.info("\n" + "=" * 70)
    logger.info("Phase 3: 確率キャリブレーション")
    logger.info("=" * 70)
    
    # Isotonic Regressionでキャリブレーション
    iso_reg = IsotonicRegression(out_of_bounds='clip')
    iso_reg.fit(pred_train, y_train.values)
    
    pred_test_calib = iso_reg.transform(pred_test)
    test_roi_calib, test_hit_calib = calc_roi(test_f, pred_test_calib)
    
    logger.info(f"  Test:  ROI={test_roi_calib:.1f}%, Hit={test_hit_calib:.1f}%")
    logger.info(f"  改善:  {test_roi_calib - baseline_roi:+.1f}%")
    
    # ========== 結果比較 ==========
    logger.info("\n" + "=" * 70)
    logger.info("結果比較")
    logger.info("=" * 70)
    
    results = [
        ('V15 Baseline', baseline_roi, test_hit),
        ('Phase 1: 新特徴量', test_roi_v16, test_hit_v16),
        ('Phase 2: アンサンブル', test_roi_ens, test_hit_ens),
        ('Phase 3: キャリブレーション', test_roi_calib, test_hit_calib),
    ]
    
    logger.info(f"\n{'Method':<30} {'Test ROI':>10} {'改善幅':>10} {'的中率':>10}")
    logger.info("-" * 60)
    for name, roi, hit in results:
        diff = roi - baseline_roi if name != 'V15 Baseline' else 0
        logger.info(f"{name:<30} {roi:>9.1f}% {diff:>+9.1f}% {hit:>9.1f}%")
    
    best = max(results, key=lambda x: x[1])
    logger.info(f"\nBest: {best[0]} (ROI={best[1]:.1f}%)")
    
    return {r[0]: r[1] for r in results}


if __name__ == "__main__":
    main()

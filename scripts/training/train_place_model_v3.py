# -*- coding: utf-8 -*-
"""
複勝専用モデル - V15公式設定版

【正しい設定】
- Train: ~2024-12-31
- Test: 2025-01-01 ~ 2025-10-31
- モデル: Binary Classification (objective='binary')
- ラベル: is_place (3着以内)

【目標】
- 複勝ROI: 83.7% (V15単勝モデル流用時) → 88%+ (専用モデル)
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
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
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns


def calc_tansho_roi(df, preds):
    """単勝ROI"""
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def calc_fukusho_roi(df, preds, returns):
    """複勝ROI"""
    fukusho = returns[returns['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1][['race_id', 'horse_number', 'finish_position']].copy()
    top1 = top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    
    total_bets = len(top1) * 100
    total_payout = top1['payout'].fillna(0).sum()
    
    hit_rate = top1['payout'].notna().sum() / len(top1) * 100 if len(top1) > 0 else 0
    roi = total_payout / total_bets * 100 if total_bets > 0 else 0
    
    return roi, hit_rate


def main():
    logger.info("=" * 60)
    logger.info("複勝専用モデル (V15公式設定版)")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # V15公式期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2024-12-31)")
    logger.info(f"  Test:  {len(test):,}件 (2025-01-01~)")
    
    # 特徴量
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    
    # ========== ベースライン (単勝ラベル) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. ベースライン (is_win ラベル = V15相当)")
    logger.info("=" * 60)
    
    y_train_win = (train_f['finish_position'] == 1).astype(int)
    
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds_win = lgb.Dataset(X_train, y_train_win)
    model_win = lgb.train(params, train_ds_win, num_boost_round=200)
    
    pred_train_win = model_win.predict(X_train)
    pred_test_win = model_win.predict(X_test)
    
    train_tansho_roi, train_tansho_hit = calc_tansho_roi(train_f, pred_train_win)
    test_tansho_roi, test_tansho_hit = calc_tansho_roi(test_f, pred_test_win)
    
    train_fukusho_roi_base, train_fukusho_hit_base = calc_fukusho_roi(train_f, pred_train_win, returns)
    test_fukusho_roi_base, test_fukusho_hit_base = calc_fukusho_roi(test_f, pred_test_win, returns)
    
    logger.info(f"  【単勝】Train: 的中率={train_tansho_hit:.1f}%, ROI={train_tansho_roi:.1f}%")
    logger.info(f"  【単勝】Test:  的中率={test_tansho_hit:.1f}%, ROI={test_tansho_roi:.1f}%")
    logger.info(f"  【複勝】Train: 的中率={train_fukusho_hit_base:.1f}%, ROI={train_fukusho_roi_base:.1f}%")
    logger.info(f"  【複勝】Test:  的中率={test_fukusho_hit_base:.1f}%, ROI={test_fukusho_roi_base:.1f}%")
    
    # ========== 複勝専用モデル (is_place ラベル) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. 複勝専用モデル (is_place ラベル = 3着以内)")
    logger.info("=" * 60)
    
    y_train_place = (train_f['finish_position'] <= 3).astype(int)
    
    train_ds_place = lgb.Dataset(X_train, y_train_place)
    model_place = lgb.train(params, train_ds_place, num_boost_round=200)
    
    pred_train_place = model_place.predict(X_train)
    pred_test_place = model_place.predict(X_test)
    
    train_fukusho_roi_new, train_fukusho_hit_new = calc_fukusho_roi(train_f, pred_train_place, returns)
    test_fukusho_roi_new, test_fukusho_hit_new = calc_fukusho_roi(test_f, pred_test_place, returns)
    
    logger.info(f"  【複勝】Train: 的中率={train_fukusho_hit_new:.1f}%, ROI={train_fukusho_roi_new:.1f}%")
    logger.info(f"  【複勝】Test:  的中率={test_fukusho_hit_new:.1f}%, ROI={test_fukusho_roi_new:.1f}%")
    
    # ========== 比較 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較")
    logger.info("=" * 60)
    
    improvement = test_fukusho_roi_new - test_fukusho_roi_base
    logger.info(f"                   ベースライン    複勝専用    改善幅")
    logger.info(f"  Test 複勝ROI:    {test_fukusho_roi_base:7.1f}%       {test_fukusho_roi_new:7.1f}%     {improvement:+.1f}%")
    logger.info(f"  Test 的中率:     {test_fukusho_hit_base:7.1f}%       {test_fukusho_hit_new:7.1f}%")
    
    gap_base = train_fukusho_roi_base - test_fukusho_roi_base
    gap_new = train_fukusho_roi_new - test_fukusho_roi_new
    logger.info(f"  Train-Test Gap:  {gap_base:7.1f}%       {gap_new:7.1f}%")
    
    target = 88.0
    if test_fukusho_roi_new >= target:
        logger.info(f"\n  ✅ 目標達成! Test 複勝ROI {test_fukusho_roi_new:.1f}% >= {target}%")
    else:
        logger.info(f"\n  ❌ 目標未達 Test 複勝ROI {test_fukusho_roi_new:.1f}% < {target}%")
        logger.info(f"     あと {target - test_fukusho_roi_new:.1f}% 必要")
    
    # 特徴量重要度
    logger.info("")
    logger.info("複勝モデル Top 10 特徴量:")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model_place.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for i, row in importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")
    
    return model_win, model_place, test_fukusho_roi_base, test_fukusho_roi_new


if __name__ == "__main__":
    main()

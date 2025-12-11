# -*- coding: utf-8 -*-
"""
単勝オッズ加重学習 - V15公式設定版

【正しい設定】
- Train: ~2024-12-31
- Test: 2025-01-01 ~ 2025-10-31
- モデル: Binary Classification

【オッズ加重戦略】
- sample_weight = log1p(win_odds)
- 高配当馬の1着を重視して学習
- win_oddsは特徴量には使用しない（リーク防止）
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
    avg_odds = hits['win_odds'].mean() if len(hits) > 0 else 0
    return roi, hit_rate, avg_odds


def main():
    logger.info("=" * 60)
    logger.info("単勝オッズ加重学習 (V15公式設定版)")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
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
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    # リーク確認
    assert 'win_odds' not in feature_cols, "リーク: win_oddsが特徴量に含まれています！"
    logger.info("  ✅ win_oddsは特徴量に含まれていません（リーク防止OK）")
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
    # sample_weight作成
    train_f['sample_weight'] = np.log1p(train_f['win_odds'])
    logger.info(f"  sample_weight: min={train_f['sample_weight'].min():.2f}, max={train_f['sample_weight'].max():.2f}")
    
    # V15公式パラメータ
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
    
    # ========== ベースライン (重みなし) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. ベースライン（重みなし = V15相当）")
    logger.info("=" * 60)
    
    train_ds_base = lgb.Dataset(X_train, y_train)
    model_base = lgb.train(params, train_ds_base, num_boost_round=200)
    
    pred_train_base = model_base.predict(X_train)
    pred_test_base = model_base.predict(X_test)
    
    train_roi_base, train_hit_base, train_odds_base = calc_roi(train_f, pred_train_base)
    test_roi_base, test_hit_base, test_odds_base = calc_roi(test_f, pred_test_base)
    
    logger.info(f"  Train: 的中率={train_hit_base:.1f}%, ROI={train_roi_base:.1f}%, 平均オッズ={train_odds_base:.1f}")
    logger.info(f"  Test:  的中率={test_hit_base:.1f}%, ROI={test_roi_base:.1f}%, 平均オッズ={test_odds_base:.1f}")
    
    # ========== オッズ加重 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. オッズ加重学習（sample_weight=log1p(odds)）")
    logger.info("=" * 60)
    
    train_ds_weighted = lgb.Dataset(X_train, y_train, weight=train_f['sample_weight'])
    model_weighted = lgb.train(params, train_ds_weighted, num_boost_round=200)
    
    pred_train_weighted = model_weighted.predict(X_train)
    pred_test_weighted = model_weighted.predict(X_test)
    
    train_roi_weighted, train_hit_weighted, train_odds_weighted = calc_roi(train_f, pred_train_weighted)
    test_roi_weighted, test_hit_weighted, test_odds_weighted = calc_roi(test_f, pred_test_weighted)
    
    logger.info(f"  Train: 的中率={train_hit_weighted:.1f}%, ROI={train_roi_weighted:.1f}%, 平均オッズ={train_odds_weighted:.1f}")
    logger.info(f"  Test:  的中率={test_hit_weighted:.1f}%, ROI={test_roi_weighted:.1f}%, 平均オッズ={test_odds_weighted:.1f}")
    
    # ========== より強い加重 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. 強加重（sample_weight=odds / 10）")
    logger.info("=" * 60)
    
    train_f['sample_weight_strong'] = train_f['win_odds'] / 10  # より強い加重
    
    train_ds_strong = lgb.Dataset(X_train, y_train, weight=train_f['sample_weight_strong'])
    model_strong = lgb.train(params, train_ds_strong, num_boost_round=200)
    
    pred_train_strong = model_strong.predict(X_train)
    pred_test_strong = model_strong.predict(X_test)
    
    train_roi_strong, train_hit_strong, train_odds_strong = calc_roi(train_f, pred_train_strong)
    test_roi_strong, test_hit_strong, test_odds_strong = calc_roi(test_f, pred_test_strong)
    
    logger.info(f"  Train: 的中率={train_hit_strong:.1f}%, ROI={train_roi_strong:.1f}%, 平均オッズ={train_odds_strong:.1f}")
    logger.info(f"  Test:  的中率={test_hit_strong:.1f}%, ROI={test_roi_strong:.1f}%, 平均オッズ={test_odds_strong:.1f}")
    
    # ========== 比較 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較")
    logger.info("=" * 60)
    
    logger.info(f"                   ベースライン    log1p加重    強加重")
    logger.info(f"  Test ROI:        {test_roi_base:7.1f}%       {test_roi_weighted:7.1f}%     {test_roi_strong:7.1f}%")
    logger.info(f"  Test 的中率:     {test_hit_base:7.1f}%       {test_hit_weighted:7.1f}%     {test_hit_strong:7.1f}%")
    logger.info(f"  平均オッズ:      {test_odds_base:7.1f}        {test_odds_weighted:7.1f}      {test_odds_strong:7.1f}")
    
    # 最良結果
    best_roi = max(test_roi_base, test_roi_weighted, test_roi_strong)
    target = 95.0
    
    if best_roi >= target:
        logger.info(f"\n  ✅ 目標達成! Best Test ROI {best_roi:.1f}% >= {target}%")
    else:
        logger.info(f"\n  ❌ 目標未達 Best Test ROI {best_roi:.1f}% < {target}%")
        logger.info(f"     あと {target - best_roi:.1f}% 必要")
    
    return model_base, model_weighted, model_strong, test_roi_base


if __name__ == "__main__":
    main()

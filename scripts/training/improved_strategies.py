# -*- coding: utf-8 -*-
"""
改善戦略の検証

【テストする戦略】
1. オッズ帯フィルタリング: 予測1位 AND オッズ10-50倍のみ購入
2. レース選別: Top2/Top1オッズ比が高いレースのみ
3. EV最適化: EV 1.5-2.0帯のベットのみ
4. 組み合わせ戦略: 上記の組み合わせ
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


def calc_strategy_roi(df, mask, strategy_name):
    """戦略のROIを計算"""
    bets = df[mask].copy()
    if len(bets) == 0:
        return 0, 0, 0
    
    wins = bets[bets['finish_position'] == 1]
    roi = wins['win_odds'].sum() / len(bets) * 100
    hit_rate = len(wins) / len(bets) * 100
    return roi, hit_rate, len(bets)


def main():
    logger.info("=" * 60)
    logger.info("改善戦略の検証")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # V15特徴量
    logger.info("")
    logger.info("V15特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
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
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    test_f['pred'] = model.predict(X_test)
    test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    test_f['ev'] = test_f['pred'] * test_f['win_odds']
    
    # Top2/Top1オッズ比を計算
    def calc_odds_ratio(group):
        sorted_g = group.sort_values('pred_rank')
        if len(sorted_g) >= 2:
            top1_odds = sorted_g.iloc[0]['win_odds']
            top2_odds = sorted_g.iloc[1]['win_odds']
            return top2_odds / top1_odds if top1_odds > 0 else 1
        return 1
    
    odds_ratio = test_f.groupby('race_id').apply(calc_odds_ratio)
    odds_ratio.name = 'odds_ratio'
    test_f = test_f.merge(odds_ratio.reset_index(), on='race_id')
    
    # ========== ベースライン ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("ベースライン vs 改善戦略")
    logger.info("=" * 60)
    
    baseline_mask = test_f['pred_rank'] == 1
    roi, hit, n = calc_strategy_roi(test_f, baseline_mask, "ベースライン")
    logger.info(f"\n  【ベースライン】予測1位全購入")
    logger.info(f"    ROI: {roi:.1f}%, 的中率: {hit:.1f}%, ベット数: {n:,}")
    
    # ========== 戦略1: オッズ帯フィルタリング ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("戦略1: オッズ帯フィルタリング")
    logger.info("=" * 60)
    
    for odds_low, odds_high in [(10, 20), (10, 50), (20, 50), (5, 20)]:
        mask = (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= odds_low) & (test_f['win_odds'] < odds_high)
        roi, hit, n = calc_strategy_roi(test_f, mask, f"オッズ{odds_low}-{odds_high}")
        logger.info(f"  オッズ{odds_low}-{odds_high}倍: ROI={roi:.1f}%, 的中率={hit:.1f}%, n={n:,}")
    
    # ========== 戦略2: レース選別（オッズ比） ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("戦略2: Top2/Top1オッズ比フィルタリング")
    logger.info("=" * 60)
    
    for threshold in [3.0, 5.0, 7.0, 10.0]:
        mask = (test_f['pred_rank'] == 1) & (test_f['odds_ratio'] >= threshold)
        roi, hit, n = calc_strategy_roi(test_f, mask, f"オッズ比>{threshold}")
        logger.info(f"  オッズ比>={threshold:.1f}: ROI={roi:.1f}%, 的中率={hit:.1f}%, n={n:,}")
    
    # ========== 戦略3: EV帯フィルタリング ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("戦略3: EV帯フィルタリング")
    logger.info("=" * 60)
    
    for ev_low, ev_high in [(1.0, 1.5), (1.5, 2.0), (1.0, 2.0), (0.8, 1.5)]:
        mask = (test_f['pred_rank'] == 1) & (test_f['ev'] >= ev_low) & (test_f['ev'] < ev_high)
        roi, hit, n = calc_strategy_roi(test_f, mask, f"EV{ev_low}-{ev_high}")
        logger.info(f"  EV{ev_low}-{ev_high}: ROI={roi:.1f}%, 的中率={hit:.1f}%, n={n:,}")
    
    # ========== 戦略4: 組み合わせ ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("戦略4: 組み合わせ戦略")
    logger.info("=" * 60)
    
    # オッズ10-50 AND オッズ比>5
    mask = (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 50) & (test_f['odds_ratio'] >= 5)
    roi, hit, n = calc_strategy_roi(test_f, mask, "オッズ10-50 AND オッズ比>5")
    logger.info(f"  オッズ10-50 AND オッズ比>=5: ROI={roi:.1f}%, 的中率={hit:.1f}%, n={n:,}")
    
    # オッズ10-50 AND EV>1.0
    mask = (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 50) & (test_f['ev'] >= 1.0)
    roi, hit, n = calc_strategy_roi(test_f, mask, "オッズ10-50 AND EV>1.0")
    logger.info(f"  オッズ10-50 AND EV>=1.0: ROI={roi:.1f}%, 的中率={hit:.1f}%, n={n:,}")
    
    # EV>1.0 AND オッズ比>5
    mask = (test_f['pred_rank'] == 1) & (test_f['ev'] >= 1.0) & (test_f['odds_ratio'] >= 5)
    roi, hit, n = calc_strategy_roi(test_f, mask, "EV>1.0 AND オッズ比>5")
    logger.info(f"  EV>=1.0 AND オッズ比>=5: ROI={roi:.1f}%, 的中率={hit:.1f}%, n={n:,}")
    
    # 全条件
    mask = (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 50) & (test_f['ev'] >= 1.0) & (test_f['odds_ratio'] >= 5)
    roi, hit, n = calc_strategy_roi(test_f, mask, "全条件")
    logger.info(f"  オッズ10-50 AND EV>=1.0 AND オッズ比>=5: ROI={roi:.1f}%, 的中率={hit:.1f}%, n={n:,}")
    
    # ========== サマリー ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("サマリー: 有望な戦略")
    logger.info("=" * 60)
    
    strategies = [
        ("ベースライン(予測1位全購入)", (test_f['pred_rank'] == 1)),
        ("オッズ10-50倍のみ", (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 50)),
        ("オッズ比>=5のみ", (test_f['pred_rank'] == 1) & (test_f['odds_ratio'] >= 5)),
        ("EV>=1.0のみ", (test_f['pred_rank'] == 1) & (test_f['ev'] >= 1.0)),
        ("オッズ10-50 + EV>=1.0", (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 50) & (test_f['ev'] >= 1.0)),
    ]
    
    logger.info(f"\n  {'戦略':30} {'ROI':>8} {'的中率':>8} {'ベット数':>10}")
    logger.info("  " + "-" * 60)
    
    for name, mask in strategies:
        roi, hit, n = calc_strategy_roi(test_f, mask, name)
        logger.info(f"  {name:30} {roi:>7.1f}% {hit:>7.1f}% {n:>10,}")


if __name__ == "__main__":
    main()

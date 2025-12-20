"""
V15+オッズフィルタ戦略の最適化

目的:
- 異なるオッズ帯のROI比較
- レース条件との組み合わせ分析
- 最適なフィルタ条件を発見
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    return train, test, pedigrees, corners, horses, race_details


def calc_roi_with_filter(df: pd.DataFrame, preds: np.ndarray, 
                         min_odds: float, max_odds: float,
                         race_class_filter: list = None,
                         min_field_size: int = None) -> dict:
    """フィルタ付きROI計算"""
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    # 基本フィルタ: モデル予測1位
    bets = df[df['rank'] == 1].copy()
    
    # オッズフィルタ
    bets = bets[(bets['win_odds'] >= min_odds) & (bets['win_odds'] < max_odds)]
    
    # レースクラスフィルタ
    if race_class_filter is not None:
        bets = bets[~bets['race_class'].isin(race_class_filter)]
    
    # 頭数フィルタ
    if min_field_size is not None:
        # 頭数計算
        field_sizes = df.groupby('race_id').size().reset_index(name='field_size')
        bets = bets.merge(field_sizes, on='race_id', how='left')
        bets = bets[bets['field_size'] >= min_field_size]
    
    if len(bets) == 0:
        return {'roi': 0, 'hit_rate': 0, 'n_bets': 0, 'avg_odds': 0}
    
    hits = bets[bets['finish_position'] == 1]
    
    return {
        'roi': hits['win_odds'].sum() / len(bets) * 100,
        'hit_rate': len(hits) / len(bets) * 100,
        'n_bets': len(bets),
        'avg_odds': bets['win_odds'].mean()
    }


def main():
    logger.info("=" * 70)
    logger.info("V15 + Odds Filter Optimization")
    logger.info("=" * 70)
    
    train, test, pedigrees, corners, horses, race_details = load_data()
    logger.info(f"Train: {len(train):,}, Test: {len(test):,}")
    
    # V15モデル訓練
    logger.info("\n--- Training V15 Model ---")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
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
    
    test_pred = model.predict(X_test)
    train_pred = model.predict(X_train)
    
    # ===== オッズ帯別ROI分析 =====
    logger.info("\n" + "=" * 50)
    logger.info("1. Odds Range Analysis (Test Data)")
    logger.info("=" * 50)
    
    odds_ranges = [
        (5, 15),
        (5, 20),
        (10, 30),
        (10, 50),
        (15, 50),
        (20, 50),
        (10, 100),
    ]
    
    results = []
    for min_o, max_o in odds_ranges:
        res = calc_roi_with_filter(test_f, test_pred, min_o, max_o)
        results.append({
            'filter': f'{min_o}-{max_o}倍',
            'roi': res['roi'],
            'hit_rate': res['hit_rate'],
            'n_bets': res['n_bets'],
            'avg_odds': res['avg_odds']
        })
        logger.info(f"  {min_o:2d}-{max_o:3d}倍: ROI={res['roi']:6.1f}%, 的中率={res['hit_rate']:5.1f}%, ベット数={res['n_bets']:4d}")
    
    # ===== レース条件フィルタ分析 =====
    logger.info("\n" + "=" * 50)
    logger.info("2. Race Condition Filter Analysis (10-50x Odds)")
    logger.info("=" * 50)
    
    # 重賞除外
    res_no_graded = calc_roi_with_filter(test_f, test_pred, 10, 50, 
                                         race_class_filter=['G1', 'G2', 'G3', 'OP'])
    logger.info(f"  重賞・OP除外: ROI={res_no_graded['roi']:.1f}%, ベット数={res_no_graded['n_bets']}")
    
    # 頭数12頭以上
    res_min12 = calc_roi_with_filter(test_f, test_pred, 10, 50, min_field_size=12)
    logger.info(f"  12頭以上: ROI={res_min12['roi']:.1f}%, ベット数={res_min12['n_bets']}")
    
    # 組み合わせ（重賞除外 + 12頭以上）
    res_combo = calc_roi_with_filter(test_f, test_pred, 10, 50,
                                     race_class_filter=['G1', 'G2', 'G3', 'OP'],
                                     min_field_size=12)
    logger.info(f"  重賞除外+12頭以上: ROI={res_combo['roi']:.1f}%, ベット数={res_combo['n_bets']}")
    
    # ===== 年別安定性分析 =====
    logger.info("\n" + "=" * 50)
    logger.info("3. Year-by-Year Stability (10-50x Filter, Train Data)")
    logger.info("=" * 50)
    
    for year in [2020, 2021, 2022, 2023, 2024]:
        year_mask = train_f['race_date'].dt.year == year
        if year_mask.sum() == 0:
            continue
        
        year_df = train_f[year_mask]
        year_pred = train_pred[year_mask]
        
        res = calc_roi_with_filter(year_df, year_pred, 10, 50)
        logger.info(f"  {year}: ROI={res['roi']:6.1f}%, ベット数={res['n_bets']:4d}")
    
    # ===== ベースラインとの比較 =====
    logger.info("\n" + "=" * 50)
    logger.info("4. Baseline Comparison")
    logger.info("=" * 50)
    
    # ベースライン: 全購入
    baseline = calc_roi_with_filter(test_f, test_pred, 0, 1000)
    logger.info(f"  全購入: ROI={baseline['roi']:.1f}%, ベット数={baseline['n_bets']}")
    
    # 最適フィルタ
    best = calc_roi_with_filter(test_f, test_pred, 10, 50)
    logger.info(f"  10-50倍フィルタ: ROI={best['roi']:.1f}%, ベット数={best['n_bets']}")
    
    logger.info(f"\n  ROI改善: +{best['roi'] - baseline['roi']:.1f}%")
    logger.info(f"  ベット削減: {baseline['n_bets']} → {best['n_bets']} ({best['n_bets']/baseline['n_bets']*100:.0f}%)")
    
    # ===== 結論 =====
    logger.info("\n" + "=" * 70)
    logger.info("CONCLUSION")
    logger.info("=" * 70)
    
    df_results = pd.DataFrame(results)
    best_row = df_results.loc[df_results['roi'].idxmax()]
    
    logger.info(f"\n  最高ROIフィルタ: {best_row['filter']}")
    logger.info(f"  ROI: {best_row['roi']:.1f}%")
    logger.info(f"  ベット数: {int(best_row['n_bets'])}")
    logger.info(f"  的中率: {best_row['hit_rate']:.1f}%")
    
    if best_row['roi'] > 100:
        logger.info(f"\n  ✅ ROI > 100% achieved with {best_row['filter']} filter!")
    else:
        logger.info(f"\n  ⚠️ Best ROI is {best_row['roi']:.1f}% (< 100%)")


if __name__ == "__main__":
    main()

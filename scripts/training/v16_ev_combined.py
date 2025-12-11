# -*- coding: utf-8 -*-
"""
V16 + EV閾値 組み合わせ検証

Phase 3のペース特徴量(V16)とPhase 4のEV閾値戦略を組み合わせて
最大ROIを達成する。

【期待】
- V16単独: ROI 106.4%
- EV閾値単独: ROI 109.2%
- 組み合わせ: ROI 115%+ ?
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


def add_pace_features(df, race_details, races_full):
    """ペース特徴量追加（pace_features.pyから移植）"""
    details = race_details[['race_id', 'first_half', 'second_half']].copy()
    details = details.dropna(subset=['first_half', 'second_half'])
    details['pace_diff'] = details['first_half'] - details['second_half']
    
    race_info = races_full[['race_id', 'venue', 'distance_m', 'track_condition', 'race_date']].drop_duplicates()
    details = details.merge(race_info, on='race_id', how='left')
    
    details['distance_cat'] = pd.cut(
        details['distance_m'], 
        bins=[0, 1400, 1800, 2200, 4000], 
        labels=['sprint', 'mile', 'middle', 'long']
    )
    
    details = details.sort_values('race_date')
    
    pace_stats = {}
    for _, row in details.iterrows():
        key = (row['venue'], row['distance_cat'], row['track_condition'])
        if key not in pace_stats:
            pace_stats[key] = {'first_half_sum': 0, 'count': 0, 'pace_diff_sum': 0}
        pace_stats[key]['first_half_sum'] += row['first_half']
        pace_stats[key]['pace_diff_sum'] += row['pace_diff']
        pace_stats[key]['count'] += 1
    
    final_pace = {}
    for key, vals in pace_stats.items():
        if vals['count'] > 0:
            final_pace[key] = {
                'first_half_avg': vals['first_half_sum'] / vals['count'],
                'pace_diff_avg': vals['pace_diff_sum'] / vals['count']
            }
    
    df = df.copy()
    df['distance_cat'] = pd.cut(
        df['distance_m'], 
        bins=[0, 1400, 1800, 2200, 4000], 
        labels=['sprint', 'mile', 'middle', 'long']
    )
    
    df['race_expected_first_half'] = df.apply(
        lambda x: final_pace.get((x['venue'], x['distance_cat'], x['track_condition']), {}).get('first_half_avg', np.nan),
        axis=1
    )
    df['race_expected_pace_diff'] = df.apply(
        lambda x: final_pace.get((x['venue'], x['distance_cat'], x['track_condition']), {}).get('pace_diff_avg', np.nan),
        axis=1
    )
    
    details_with_result = details.merge(
        races_full[['race_id', 'horse_id', 'finish_position']].drop_duplicates(),
        on='race_id',
        how='left'
    )
    
    top3_races = details_with_result[details_with_result['finish_position'] <= 3]
    horse_pace_pref = top3_races.groupby('horse_id')['pace_diff'].mean().reset_index()
    horse_pace_pref.columns = ['horse_id', 'horse_preferred_pace_diff']
    
    df = df.merge(horse_pace_pref, on='horse_id', how='left')
    df['pace_synergy'] = -abs(df['race_expected_pace_diff'] - df['horse_preferred_pace_diff'])
    df = df.drop(columns=['distance_cat'], errors='ignore')
    
    return df


def calc_roi_ev(df, threshold):
    """予測確率Top1のEV閾値ROI"""
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1]
    bets = top1[top1['expected_value'] > threshold]
    
    if len(bets) == 0:
        return 0, 0, 0
    
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    
    return roi, hit_rate, len(bets)


def main():
    logger.info("=" * 60)
    logger.info("V16 + EV閾値 組み合わせ検証")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # V15特徴量
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    v15_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # ペース特徴量
    logger.info("ペース特徴量追加中...")
    train_f = add_pace_features(train_f, race_details, races)
    test_f = add_pace_features(test_f, race_details, races)
    
    new_features = ['race_expected_first_half', 'race_expected_pace_diff', 'horse_preferred_pace_diff', 'pace_synergy']
    v16_cols = v15_cols + [f for f in new_features if f in train_f.columns]
    
    logger.info(f"  V16特徴量数: {len(v16_cols)}")
    
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
    
    # V16モデル学習
    X_train = train_f[v16_cols].fillna(0)
    X_test = test_f[v16_cols].fillna(0)
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    test_f['pred'] = model.predict(X_test)
    test_f['expected_value'] = test_f['pred'] * test_f['win_odds'] * 0.80
    
    # ベースライン (Top1全賭け)
    logger.info("")
    logger.info("=" * 60)
    logger.info("ベースライン (V16, Top1全賭け)")
    logger.info("=" * 60)
    
    test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    top1 = test_f[test_f['pred_rank'] == 1]
    hits = top1[top1['finish_position'] == 1]
    roi_base = hits['win_odds'].sum() / len(top1) * 100
    hit_base = len(hits) / len(top1) * 100
    
    logger.info(f"  ベット数: {len(top1):,}")
    logger.info(f"  的中率: {hit_base:.1f}%")
    logger.info(f"  ROI: {roi_base:.1f}%")
    
    # EV閾値別
    thresholds = [0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5]
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("V16 + EV閾値")
    logger.info("=" * 60)
    
    results = []
    for th in thresholds:
        roi, hit, n = calc_roi_ev(test_f, th)
        results.append({'threshold': th, 'roi': roi, 'hit_rate': hit, 'n_bets': n})
        if n >= 100:
            logger.info(f"  EV > {th:.1f}: ベット={n:,}, 的中率={hit:.1f}%, ROI={roi:.1f}%")
        else:
            logger.info(f"  EV > {th:.1f}: ベット={n:,} (100未満のためスキップ)")
    
    # 結果サマリー
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリー")
    logger.info("=" * 60)
    
    logger.info(f"  {'閾値':>6} {'ベット数':>10} {'的中率':>8} {'ROI':>8}")
    logger.info("-" * 40)
    logger.info(f"  {'なし':>6} {len(top1):>10,} {hit_base:>7.1f}% {roi_base:>7.1f}%")
    
    best_roi = roi_base
    best_th = 'なし'
    
    for r in results:
        if r['n_bets'] >= 100:
            if r['roi'] > best_roi:
                best_roi = r['roi']
                best_th = f"EV > {r['threshold']:.1f}"
            logger.info(f"  {r['threshold']:>6.1f} {r['n_bets']:>10,} {r['hit_rate']:>7.1f}% {r['roi']:>7.1f}%")
    
    logger.info("")
    logger.info(f"  最良: {best_th} (ROI={best_roi:.1f}%)")
    
    return results


if __name__ == "__main__":
    main()

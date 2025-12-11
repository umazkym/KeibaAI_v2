# -*- coding: utf-8 -*-
"""
馬連オッズ帯組み合わせ戦略 - 複数年検証

発見した有望な組み合わせ:
- Top1 10-20倍 × Top2 5-10倍: ROI 250%
- Top1 20-50倍 × Top2 ~5倍: ROI 237%
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


def calc_umaren_roi_by_combo(df, returns_df):
    """馬連オッズ帯組み合わせ別ROI計算"""
    df = df.copy()
    
    # 馬連払戻データ
    umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    umaren = umaren.dropna(subset=['horse_1', 'horse_2'])
    umaren['winner_set'] = umaren.apply(
        lambda x: frozenset([int(x['horse_1']), int(x['horse_2'])]),
        axis=1
    )
    payout_dict = dict(zip(
        zip(umaren['race_id'], umaren['winner_set']),
        umaren['payout']
    ))
    
    # 各レースのTop1, Top2のオッズを取得
    race_ids = df['race_id'].unique()
    results = []
    
    # ベースライン
    total_bet = 0
    total_payout = 0
    hit_count = 0
    
    for race_id in race_ids:
        race_df = df[df['race_id'] == race_id]
        top1 = race_df[race_df['pred_rank'] == 1]
        top2 = race_df[race_df['pred_rank'] == 2]
        
        if len(top1) == 0 or len(top2) == 0:
            continue
            
        top_horses = race_df[race_df['pred_rank'] <= 2]['horse_number'].astype(int).tolist()
        if len(top_horses) < 2:
            continue
            
        bet_set = frozenset(top_horses)
        payout = payout_dict.get((race_id, bet_set), 0)
        
        total_bet += 100
        total_payout += payout
        if payout > 0:
            hit_count += 1
    
    baseline_roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    baseline_hit = hit_count / len(race_ids) * 100
    results.append(('ベースライン', baseline_roi, baseline_hit, len(race_ids)))
    
    # 有望な組み合わせ
    combos = [
        ("Top1 10-50 x Top2 ~10", 10, 50, 0, 10),
        ("Top1 10-50 x Top2 5-20", 10, 50, 5, 20),
        ("Top1 10-20 x Top2 5-10", 10, 20, 5, 10),
        ("Top1 20-50 x Top2 ~10", 20, 50, 0, 10),
        ("Top1 穴馬 x Top2 人気馬", 10, 50, 0, 5),
    ]
    
    for name, t1_low, t1_high, t2_low, t2_high in combos:
        total_bet = 0
        total_payout = 0
        hit_count = 0
        bet_count = 0
        
        for race_id in race_ids:
            race_df = df[df['race_id'] == race_id]
            top1 = race_df[race_df['pred_rank'] == 1]
            top2 = race_df[race_df['pred_rank'] == 2]
            
            if len(top1) == 0 or len(top2) == 0:
                continue
                
            top1_odds = top1.iloc[0]['win_odds']
            top2_odds = top2.iloc[0]['win_odds']
            
            # フィルタ条件
            if not (t1_low <= top1_odds < t1_high and t2_low <= top2_odds < t2_high):
                continue
            
            top_horses = race_df[race_df['pred_rank'] <= 2]['horse_number'].astype(int).tolist()
            if len(top_horses) < 2:
                continue
                
            bet_set = frozenset(top_horses)
            payout = payout_dict.get((race_id, bet_set), 0)
            
            total_bet += 100
            total_payout += payout
            bet_count += 1
            if payout > 0:
                hit_count += 1
        
        if bet_count > 0:
            roi = total_payout / total_bet * 100
            hit_rate = hit_count / bet_count * 100
            results.append((name, roi, hit_rate, bet_count))
    
    return results


def main():
    logger.info("=" * 60)
    logger.info("馬連オッズ帯組み合わせ戦略 - 複数年検証")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 年別のテスト期間
    test_periods = [
        ('2025', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
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
    
    all_results = {}
    
    for year, train_end, test_start, test_end in test_periods:
        logger.info("")
        logger.info(f"=" * 60)
        logger.info(f"{year} Test (Train~{train_end})")
        logger.info(f"=" * 60)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        logger.info(f"  Train: {len(train):,}件, Test: {len(test):,}件")
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        X_train = train_f[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        y_train = (train_f['finish_position'] == 1).astype(int)
        
        train_ds = lgb.Dataset(X_train, y_train)
        model = lgb.train(params, train_ds, num_boost_round=200)
        
        test_f['pred'] = model.predict(X_test)
        test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
        
        # ROI計算
        results = calc_umaren_roi_by_combo(test_f, returns)
        all_results[year] = {r[0]: (r[1], r[2], r[3]) for r in results}
        
        logger.info(f"\n  {'戦略':25} {'ROI':>8} {'的中率':>8} {'n':>6}")
        logger.info("  " + "-" * 55)
        for name, roi, hit, n in results:
            logger.info(f"  {name:25} {roi:>7.1f}% {hit:>7.1f}% {n:>6,}")
    
    # サマリー
    logger.info("")
    logger.info("=" * 60)
    logger.info("年別・戦略別ROIサマリー")
    logger.info("=" * 60)
    
    strategies = ['ベースライン', 'Top1 10-50 x Top2 ~10', 'Top1 10-50 x Top2 5-20', 
                 'Top1 10-20 x Top2 5-10', 'Top1 20-50 x Top2 ~10', 'Top1 穴馬 x Top2 人気馬']
    
    for strategy in strategies:
        logger.info(f"\n  【{strategy}】")
        logger.info(f"  {'年':6} {'ROI':>10} {'的中率':>10} {'n':>8}")
        logger.info("  " + "-" * 40)
        
        rois = []
        for year in ['2025', '2024', '2023', '2022']:
            if year in all_results and strategy in all_results[year]:
                roi, hit, n = all_results[year][strategy]
                rois.append(roi)
                logger.info(f"  {year:6} {roi:>9.1f}% {hit:>9.1f}% {n:>8,}")
        
        if rois:
            avg_roi = np.mean(rois)
            logger.info(f"  {'平均':6} {avg_roi:>9.1f}%")


if __name__ == "__main__":
    main()

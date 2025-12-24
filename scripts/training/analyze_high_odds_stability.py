#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高オッズフィルタの安定性分析

オッズ50倍以上でROI 150-200%という結果の再現性を検証。
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import sys
import pickle
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def calc_roi_stats(df):
    if len(df) == 0:
        return {'n_bets': 0, 'n_hits': 0, 'roi': 0, 'hit_rate': 0}
    hits = df[df['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(df) * 100
    hit_rate = len(hits) / len(df) * 100
    return {'n_bets': len(df), 'n_hits': len(hits), 'roi': roi, 'hit_rate': hit_rate}


def main():
    logger.info("=" * 60)
    logger.info("高オッズフィルタ安定性分析")
    logger.info("=" * 60)
    
    # 直接データ読み込みと予測を行う
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races_df = races_df[races_df['finish_position'].notna()].copy()
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    # データ分割
    test_years = [2020, 2021, 2022, 2023, 2024]
    test_data = races_df[(races_df['race_date'].dt.year >= 2020) & 
                         (races_df['race_date'].dt.year <= 2024)].copy()
    train_data = races_df[races_df['race_date'] < '2020-01-01'].copy()
    
    # モデル読み込みまたは訓練
    model_path = project_root / "keibaai/models/v15/v15_model.pkl"
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    # 特徴量生成
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train_data, pedigrees, corners, race_details, horses_df=horses)
    
    test_f = engine.transform(test_data)
    feature_cols = [c for c in engine.get_feature_columns() if c in test_f.columns]
    
    X_test = test_f[feature_cols].fillna(0)
    test_f['pred'] = model.predict(X_test)
    
    # AI予測ランク
    test_f['ai_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first').astype(int)
    test_f['year'] = test_f['race_date'].dt.year
    
    # AI予測1位のみ
    ai_top1 = test_f[test_f['ai_rank'] == 1].copy()
    
    # ==============================
    # 1. 年別×オッズ帯分析
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. 年別×オッズ帯ROI分析")
    logger.info("=" * 60)
    
    odds_ranges = [
        (0, 10, "オッズ0-10"),
        (10, 30, "オッズ10-30"),
        (30, 50, "オッズ30-50"),
        (50, 100, "オッズ50-100"),
        (100, 999, "オッズ100+"),
    ]
    
    for low, high, label in odds_ranges:
        logger.info(f"\n  {label}:")
        logger.info(f"    {'Year':<6} {'ROI':>8} {'Hit%':>8} {'件数':>8}")
        logger.info(f"    {'-'*6} {'-'*8} {'-'*8} {'-'*8}")
        
        yearly_roi = []
        for year in sorted(ai_top1['year'].unique()):
            subset = ai_top1[(ai_top1['year'] == year) & 
                            (ai_top1['win_odds'] >= low) & 
                            (ai_top1['win_odds'] < high)]
            stats = calc_roi_stats(subset)
            if stats['n_bets'] > 0:
                yearly_roi.append(stats['roi'])
                logger.info(f"    {year:<6} {stats['roi']:>7.1f}% {stats['hit_rate']:>7.1f}% {stats['n_bets']:>8}")
        
        if yearly_roi:
            avg = np.mean(yearly_roi)
            std = np.std(yearly_roi)
            logger.info(f"    {'平均':>6} {avg:>7.1f}% (±{std:.1f}%)")
    
    # ==============================
    # 2. 高オッズ帯の詳細分析
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. 高オッズ帯（30倍以上）の詳細分析")
    logger.info("=" * 60)
    
    high_odds = ai_top1[ai_top1['win_odds'] >= 30].copy()
    
    # 的中したレースの詳細
    hits = high_odds[high_odds['finish_position'] == 1]
    logger.info(f"\n  高オッズ的中: {len(hits)}件 / {len(high_odds)}件")
    logger.info(f"  的中オッズ合計: {hits['win_odds'].sum():.1f}倍")
    
    if len(hits) > 0:
        logger.info(f"\n  的中オッズ分布:")
        for q in [25, 50, 75, 90, 95]:
            pct = np.percentile(hits['win_odds'], q)
            logger.info(f"    {q}%ile: {pct:.1f}倍")
        
        # 最高オッズ的中
        top5 = hits.nlargest(5, 'win_odds')
        logger.info(f"\n  最高オッズ的中 Top5:")
        for _, row in top5.iterrows():
            logger.info(f"    {row['race_date'].strftime('%Y-%m-%d')} オッズ{row['win_odds']:.1f}倍")
    
    # ==============================
    # 3. オッズ30-60倍の安定性
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. オッズ30-60倍帯の年別安定性")
    logger.info("=" * 60)
    
    mid_high = ai_top1[(ai_top1['win_odds'] >= 30) & (ai_top1['win_odds'] < 60)]
    
    logger.info(f"\n  {'Year':<6} {'ROI':>8} {'Hit%':>8} {'件数':>8} {'的中数':>8}")
    logger.info(f"  {'-'*6} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    
    all_bets = 0
    all_return = 0
    
    for year in sorted(mid_high['year'].unique()):
        subset = mid_high[mid_high['year'] == year]
        stats = calc_roi_stats(subset)
        all_bets += stats['n_bets']
        all_return += stats['roi'] * stats['n_bets'] / 100
        logger.info(f"  {year:<6} {stats['roi']:>7.1f}% {stats['hit_rate']:>7.1f}% {stats['n_bets']:>8} {stats['n_hits']:>8}")
    
    overall_roi = all_return / all_bets * 100 if all_bets > 0 else 0
    logger.info(f"  {'全体':>6} {overall_roi:>7.1f}%")
    
    # ==============================
    # 4. 低オッズ（本命）の安定性
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("4. 低オッズ帯（3倍以下）の年別安定性")
    logger.info("=" * 60)
    
    low_odds = ai_top1[ai_top1['win_odds'] <= 3]
    
    logger.info(f"\n  {'Year':<6} {'ROI':>8} {'Hit%':>8} {'件数':>8}")
    logger.info(f"  {'-'*6} {'-'*8} {'-'*8} {'-'*8}")
    
    for year in sorted(low_odds['year'].unique()):
        subset = low_odds[low_odds['year'] == year]
        stats = calc_roi_stats(subset)
        logger.info(f"  {year:<6} {stats['roi']:>7.1f}% {stats['hit_rate']:>7.1f}% {stats['n_bets']:>8}")
    
    # ==============================
    # 5. 最適戦略の提案
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("5. 戦略別シミュレーション（2020-2024）")
    logger.info("=" * 60)
    
    strategies = [
        ("全賭け", ai_top1),
        ("オッズ3倍以下", ai_top1[ai_top1['win_odds'] <= 3]),
        ("オッズ3-10倍", ai_top1[(ai_top1['win_odds'] > 3) & (ai_top1['win_odds'] <= 10)]),
        ("オッズ10-30倍", ai_top1[(ai_top1['win_odds'] > 10) & (ai_top1['win_odds'] <= 30)]),
        ("オッズ30-60倍", ai_top1[(ai_top1['win_odds'] > 30) & (ai_top1['win_odds'] <= 60)]),
        ("オッズ60倍以上", ai_top1[ai_top1['win_odds'] > 60]),
    ]
    
    logger.info(f"\n  {'戦略':<15} {'ROI':>8} {'Hit%':>8} {'件数':>8} {'年間平均利益':>12}")
    logger.info(f"  {'-'*15} {'-'*8} {'-'*8} {'-'*8} {'-'*12}")
    
    for name, subset in strategies:
        stats = calc_roi_stats(subset)
        annual_bets = stats['n_bets'] / 5
        annual_profit = (stats['roi'] - 100) * annual_bets / 100  # 1単位=100円換算
        logger.info(f"  {name:<15} {stats['roi']:>7.1f}% {stats['hit_rate']:>7.1f}% {stats['n_bets']:>8} {annual_profit:>+11.0f}円")
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("分析完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

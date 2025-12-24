#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多角的ROI分析スクリプト
- 条件別ROI分析（クラス、距離、馬場、競馬場）
- Gap特徴量の有効性検証
- コース特徴（坂、直線長）の影響分析
"""
import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def log(msg):
    print(f"[分析] {msg}")

def calc_roi(df):
    """Top1のROIを計算"""
    if len(df) == 0:
        return 0, 0, 0
    df = df.copy()
    df['rank'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    if len(bets) == 0:
        return 0, 0, 0
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate, len(bets)

def main():
    log("=" * 60)
    log("多角的ROI分析")
    log("=" * 60)
    
    # データ読み込み
    data_dir = Path("keibaai/data/parsed/parquet")
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[races['finish_position'].notna() & races['win_odds'].notna()]
    
    log(f"データ: {len(races):,}件 ({races['race_date'].min().date()} - {races['race_date'].max().date()})")
    
    # 2025年のみをTest期間として分析
    test = races[races['race_date'] >= '2025-01-01'].copy()
    log(f"Test期間: {len(test):,}件 (2025年〜)")
    
    # 人気順で簡易スコアを付与（本来はモデル予測だが、ベースライン分析用）
    test['score'] = -test['popularity']  # 人気順が低い方がスコア高
    
    log("")
    log("=" * 60)
    log("1. 競馬場別ROI (人気順ベース)")
    log("=" * 60)
    venues = test['venue'].dropna().unique()
    venue_results = []
    for venue in sorted(venues):
        subset = test[test['venue'] == venue]
        roi, hit, n = calc_roi(subset)
        venue_results.append({'venue': venue, 'roi': roi, 'hit_rate': hit, 'bets': n})
        log(f"  {venue}: ROI={roi:6.1f}%, 的中={hit:5.1f}%, レース={n}")
    
    log("")
    log("=" * 60)
    log("2. 距離カテゴリ別ROI")
    log("=" * 60)
    test['dist_category'] = pd.cut(
        test['distance_m'], 
        bins=[0, 1400, 1800, 2200, 9999],
        labels=['短距離(~1400)', 'マイル(1400-1800)', '中距離(1800-2200)', '長距離(2200~)']
    )
    for cat in test['dist_category'].dropna().unique():
        subset = test[test['dist_category'] == cat]
        roi, hit, n = calc_roi(subset)
        log(f"  {cat}: ROI={roi:6.1f}%, 的中={hit:5.1f}%, レース={n}")
    
    log("")
    log("=" * 60)
    log("3. 馬場状態別ROI")
    log("=" * 60)
    for cond in test['track_condition'].dropna().unique():
        subset = test[test['track_condition'] == cond]
        roi, hit, n = calc_roi(subset)
        log(f"  {cond}: ROI={roi:6.1f}%, 的中={hit:5.1f}%, レース={n}")
    
    log("")
    log("=" * 60)
    log("4. クラス別ROI")
    log("=" * 60)
    for rc in sorted(test['race_class'].dropna().unique()):
        subset = test[test['race_class'] == rc]
        if len(subset) < 100:
            continue
        roi, hit, n = calc_roi(subset)
        log(f"  {rc}: ROI={roi:6.1f}%, 的中={hit:5.1f}%, レース={n}")
    
    log("")
    log("=" * 60)
    log("5. 人気別ROI（1着率とオッズの関係）")
    log("=" * 60)
    test['pop_bin'] = pd.cut(test['popularity'], bins=[0, 1, 2, 3, 5, 10, 99], labels=['1人気', '2人気', '3人気', '4-5人気', '6-10人気', '11人気~'])
    for pop in ['1人気', '2人気', '3人気', '4-5人気', '6-10人気', '11人気~']:
        subset = test[test['pop_bin'] == pop]
        if len(subset) == 0:
            continue
        wins = subset[subset['finish_position'] == 1]
        win_rate = len(wins) / len(subset) * 100
        avg_odds = wins['win_odds'].mean() if len(wins) > 0 else 0
        # 期待値 = 1着確率 × 平均オッズ
        ev = win_rate / 100 * avg_odds
        log(f"  {pop}: 1着率={win_rate:5.1f}%, 平均オッズ={avg_odds:5.1f}, 期待値={ev:.2f}")
    
    log("")
    log("=" * 60)
    log("6. Gap分析（オッズ vs 実際の1着率）")
    log("=" * 60)
    # オッズから暗黙の勝率を計算
    test['implied_prob'] = 0.8 / test['win_odds']  # 控除率20%を考慮
    test['actual_win'] = (test['finish_position'] == 1).astype(float)
    
    # オッズ帯別に集計
    test['odds_bin'] = pd.cut(test['win_odds'], bins=[0, 3, 5, 10, 20, 50, 999], labels=['1-3倍', '3-5倍', '5-10倍', '10-20倍', '20-50倍', '50倍~'])
    for odds_bin in ['1-3倍', '3-5倍', '5-10倍', '10-20倍', '20-50倍', '50倍~']:
        subset = test[test['odds_bin'] == odds_bin]
        if len(subset) == 0:
            continue
        implied = subset['implied_prob'].mean() * 100
        actual = subset['actual_win'].mean() * 100
        gap = actual - implied
        log(f"  {odds_bin}: 暗黙={implied:5.1f}%, 実際={actual:5.1f}%, Gap={gap:+5.1f}%")
    
    log("")
    log("=" * 60)
    log("7. 芝/ダート×距離の詳細分析")
    log("=" * 60)
    for surface in ['芝', 'ダート']:
        log(f"\n  【{surface}】")
        subset_s = test[test['track_surface'] == surface]
        for dist in [1200, 1400, 1600, 1800, 2000, 2400]:
            subset = subset_s[(subset_s['distance_m'] >= dist - 100) & (subset_s['distance_m'] <= dist + 100)]
            if len(subset) < 50:
                continue
            roi, hit, n = calc_roi(subset)
            log(f"    {dist}m: ROI={roi:6.1f}%, 的中={hit:5.1f}%, レース={n}")
    
    log("")
    log("=" * 60)
    log("分析完了")
    log("=" * 60)

if __name__ == "__main__":
    main()

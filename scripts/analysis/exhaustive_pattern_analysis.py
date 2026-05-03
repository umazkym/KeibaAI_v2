#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全レースデータ徹底分析

目的:
- 41,502件の予測データから傾向を体系的に抽出
- 結論を急がず、データが示すパターンを丁寧に読み取る
- 複数の切り口からの交差分析

作成日: 2026-01-12
"""

import pandas as pd
import numpy as np
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent

# 予測データ読み込み
preds = pd.read_csv(project_root / 'outputs/analysis/race_level_deep_analysis.csv')
print(f"予測データ: {len(preds):,}件")
print(f"レース数: {preds['race_id'].nunique():,}件")

# ============================================================
# 分析1: 予測順位と着順の基本関係
# ============================================================
print("\n" + "="*80)
print("【分析1】予測順位と着順の基本関係")
print("="*80)

print("\n■ 予測順位別の着順分布（全データ）")
for rank in range(1, 11):
    rank_df = preds[preds['rank'] == rank]
    if len(rank_df) == 0:
        continue
    
    avg_finish = rank_df['finish_position'].mean()
    median_finish = rank_df['finish_position'].median()
    first_rate = (rank_df['finish_position'] == 1).mean() * 100
    top3_rate = (rank_df['finish_position'] <= 3).mean() * 100
    
    print(f"  Top{rank:2}: 平均着順{avg_finish:5.2f}, 中央値{median_finish:4.1f}, "
          f"1着率{first_rate:5.1f}%, 3着以内{top3_rate:5.1f}% (n={len(rank_df):,})")

print("\n■ 予測順位と着順の相関係数")
corr = preds['rank'].corr(preds['finish_position'])
print(f"  相関係数: {corr:.4f}")

# ============================================================
# 分析2: オッズ帯別の予測精度
# ============================================================
print("\n" + "="*80)
print("【分析2】オッズ帯別の予測精度")
print("="*80)

odds_bins = [(1, 2), (2, 3), (3, 5), (5, 10), (10, 20), (20, 50), (50, 100), (100, 500)]

print("\n■ オッズ帯別: Top1が1着になる確率")
for low, high in odds_bins:
    top1_in_band = preds[(preds['rank'] == 1) & (preds['win_odds'] >= low) & (preds['win_odds'] < high)]
    if len(top1_in_band) == 0:
        continue
    
    hit_rate = (top1_in_band['finish_position'] == 1).mean() * 100
    avg_odds = top1_in_band['win_odds'].mean()
    roi = top1_in_band[top1_in_band['finish_position'] == 1]['win_odds'].sum() / len(top1_in_band) * 100
    
    print(f"  {low:3}-{high:3}倍: 件数{len(top1_in_band):4}, 的中率{hit_rate:5.1f}%, "
          f"平均オッズ{avg_odds:5.1f}, ROI{roi:6.1f}%")

print("\n■ オッズ帯別: 実際の1着馬のAI予測順位分布")
for low, high in odds_bins:
    winners_in_band = preds[(preds['finish_position'] == 1) & (preds['win_odds'] >= low) & (preds['win_odds'] < high)]
    if len(winners_in_band) == 0:
        continue
    
    avg_rank = winners_in_band['rank'].mean()
    top3_pred = (winners_in_band['rank'] <= 3).mean() * 100
    top5_pred = (winners_in_band['rank'] <= 5).mean() * 100
    
    print(f"  {low:3}-{high:3}倍: 1着馬{len(winners_in_band):3}頭, "
          f"AI平均順位{avg_rank:4.1f}, Top3予測率{top3_pred:5.1f}%, Top5予測率{top5_pred:5.1f}%")

# ============================================================
# 分析3: 人気と予測の関係
# ============================================================
print("\n" + "="*80)
print("【分析3】人気と予測の関係")
print("="*80)

print("\n■ 人気順別: AI Top1に選ばれる確率")
for pop in range(1, 11):
    pop_df = preds[preds['popularity'] == pop]
    if len(pop_df) == 0:
        continue
    
    selected_as_top1 = (pop_df['rank'] == 1).mean() * 100
    selected_as_top3 = (pop_df['rank'] <= 3).mean() * 100
    actual_win_rate = (pop_df['finish_position'] == 1).mean() * 100
    
    print(f"  {pop:2}番人気: AI Top1率{selected_as_top1:5.1f}%, AI Top3率{selected_as_top3:5.1f}%, "
          f"実際1着率{actual_win_rate:5.1f}% (n={len(pop_df):,})")

print("\n■ 人気とAI予測が異なるケースの分析")
# AIがTop1に選んだが、その馬の人気順が何番だったか
top1_df = preds[preds['rank'] == 1]
print("  AI Top1の人気順分布:")
pop_dist = top1_df['popularity'].value_counts().sort_index()
for pop in range(1, 11):
    count = pop_dist.get(pop, 0)
    pct = count / len(top1_df) * 100
    win_rate = (top1_df[top1_df['popularity'] == pop]['finish_position'] == 1).mean() * 100 if count > 0 else 0
    print(f"    {pop:2}番人気: {count:4}件 ({pct:5.1f}%), 実際1着率{win_rate:5.1f}%")

# ============================================================
# 分析4: 距離別の予測精度
# ============================================================
print("\n" + "="*80)
print("【分析4】距離別の予測精度")
print("="*80)

if 'distance' in preds.columns:
    distance_bins = [(1000, 1400), (1400, 1800), (1800, 2200), (2200, 3000), (3000, 4000)]
    
    print("\n■ 距離帯別: Top1の1着率")
    for low, high in distance_bins:
        subset = preds[(preds['distance'] >= low) & (preds['distance'] < high)]
        if len(subset) == 0:
            continue
        
        top1 = subset[subset['rank'] == 1]
        hit_rate = (top1['finish_position'] == 1).mean() * 100
        roi = top1[top1['finish_position'] == 1]['win_odds'].sum() / len(top1) * 100 if len(top1) > 0 else 0
        
        print(f"  {low:4}-{high:4}m: 件数{len(top1):4}, 1着率{hit_rate:5.1f}%, ROI{roi:6.1f}%")

# ============================================================
# 分析5: 馬場（芝/ダート）別の予測精度
# ============================================================
print("\n" + "="*80)
print("【分析5】馬場（芝/ダート）別の予測精度")
print("="*80)

if 'track_surface' in preds.columns:
    for surface in preds['track_surface'].dropna().unique():
        subset = preds[preds['track_surface'] == surface]
        top1 = subset[subset['rank'] == 1]
        
        if len(top1) == 0:
            continue
        
        hit_rate = (top1['finish_position'] == 1).mean() * 100
        roi = top1[top1['finish_position'] == 1]['win_odds'].sum() / len(top1) * 100
        
        print(f"  {surface:8}: 件数{len(top1):4}, 1着率{hit_rate:5.1f}%, ROI{roi:6.1f}%")

# ============================================================
# 分析6: 頭数別の予測精度
# ============================================================
print("\n" + "="*80)
print("【分析6】頭数別の予測精度")
print("="*80)

preds['num_horses'] = preds.groupby('race_id')['horse_number'].transform('count')
head_bins = [(5, 8), (8, 12), (12, 16), (16, 19)]

print("\n■ 頭数別: Top1の1着率")
for low, high in head_bins:
    subset = preds[(preds['num_horses'] >= low) & (preds['num_horses'] < high)]
    top1 = subset[subset['rank'] == 1]
    
    if len(top1) == 0:
        continue
    
    hit_rate = (top1['finish_position'] == 1).mean() * 100
    roi = top1[top1['finish_position'] == 1]['win_odds'].sum() / len(top1) * 100
    
    print(f"  {low:2}-{high:2}頭: 件数{len(top1):4}, 1着率{hit_rate:5.1f}%, ROI{roi:6.1f}%")

# ============================================================
# 分析7: 騎手勝率と予測精度の関係
# ============================================================
print("\n" + "="*80)
print("【分析7】騎手勝率と予測精度の関係")
print("="*80)

if 'jockey_win_rate' in preds.columns:
    jw_bins = [(0, 0.05), (0.05, 0.10), (0.10, 0.15), (0.15, 0.20), (0.20, 0.30), (0.30, 1.0)]
    
    print("\n■ 騎手勝率帯別: Top1の1着率")
    for low, high in jw_bins:
        subset = preds[(preds['jockey_win_rate'] >= low) & (preds['jockey_win_rate'] < high)]
        top1 = subset[subset['rank'] == 1]
        
        if len(top1) < 10:
            continue
        
        hit_rate = (top1['finish_position'] == 1).mean() * 100
        roi = top1[top1['finish_position'] == 1]['win_odds'].sum() / len(top1) * 100
        
        print(f"  JW{low:.2f}-{high:.2f}: 件数{len(top1):4}, 1着率{hit_rate:5.1f}%, ROI{roi:6.1f}%")

# ============================================================
# 分析8: 予測スコアと着順の関係
# ============================================================
print("\n" + "="*80)
print("【分析8】予測スコアと着順の関係")
print("="*80)

if 'ensemble_score' in preds.columns:
    score_quantiles = preds['ensemble_score'].quantile([0.2, 0.4, 0.6, 0.8, 1.0])
    
    print("\n■ 予測スコア帯別: 実際の平均着順")
    prev = 0
    for q_val, score in score_quantiles.items():
        subset = preds[(preds['ensemble_score'] > prev) & (preds['ensemble_score'] <= score)]
        if len(subset) == 0:
            continue
        
        avg_finish = subset['finish_position'].mean()
        first_rate = (subset['finish_position'] == 1).mean() * 100
        
        print(f"  スコア{prev:.2f}-{score:.2f}: 平均着順{avg_finish:5.2f}, 1着率{first_rate:5.1f}% (n={len(subset):,})")
        prev = score

# ============================================================
# 分析9: 予測が大きく外れるパターン
# ============================================================
print("\n" + "="*80)
print("【分析9】予測が大きく外れるパターン")
print("="*80)

# Top1が10着以下になったケース
big_miss = preds[(preds['rank'] == 1) & (preds['finish_position'] >= 10)]
print(f"\n■ Top1が10着以下になったケース: {len(big_miss)}件")

if len(big_miss) > 0:
    print("  人気順分布:")
    for pop in range(1, 11):
        count = (big_miss['popularity'] == pop).sum()
        if count > 0:
            print(f"    {pop}番人気: {count}件")
    
    print(f"  平均オッズ: {big_miss['win_odds'].mean():.1f}倍")
    print(f"  オッズ中央値: {big_miss['win_odds'].median():.1f}倍")
    
    if 'jockey_win_rate' in big_miss.columns:
        print(f"  平均騎手勝率: {big_miss['jockey_win_rate'].mean():.3f}")

# ============================================================
# 分析10: レース全体の荒れ具合と予測精度
# ============================================================
print("\n" + "="*80)
print("【分析10】レース全体の荒れ具合と予測精度")
print("="*80)

# レースごとに1着馬の人気を計算
race_winner_pop = preds[preds['finish_position'] == 1].groupby('race_id')['popularity'].first()
preds['winner_pop'] = preds['race_id'].map(race_winner_pop)

print("\n■ 1着馬の人気別: Top1の的中率")
for pop in range(1, 11):
    races_with_pop_winner = preds[(preds['winner_pop'] == pop) & (preds['rank'] == 1)]
    if len(races_with_pop_winner) < 10:
        continue
    
    hit = (races_with_pop_winner['finish_position'] == 1).mean() * 100
    print(f"  {pop}番人気が勝ったレース: Top1的中率{hit:5.1f}% (n={len(races_with_pop_winner)})")

# ============================================================
# 分析11: 予測順位の確信度（スコア差）
# ============================================================
print("\n" + "="*80)
print("【分析11】予測順位の確信度（Top1とTop2のスコア差）")
print("="*80)

# レースごとにTop1とTop2のスコア差を計算
def calc_score_gap(race_df):
    sorted_df = race_df.sort_values('ensemble_score', ascending=False)
    if len(sorted_df) < 2:
        return np.nan
    return sorted_df.iloc[0]['ensemble_score'] - sorted_df.iloc[1]['ensemble_score']

score_gaps = preds.groupby('race_id').apply(calc_score_gap)
preds['score_gap'] = preds['race_id'].map(score_gaps)

gap_quantiles = preds['score_gap'].quantile([0.25, 0.5, 0.75, 1.0])
print("\n■ Top1-Top2スコア差別: Top1の1着率")
prev = 0
for q_val, gap in gap_quantiles.items():
    subset = preds[(preds['score_gap'] > prev) & (preds['score_gap'] <= gap) & (preds['rank'] == 1)]
    if len(subset) == 0:
        continue
    
    hit_rate = (subset['finish_position'] == 1).mean() * 100
    roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
    
    print(f"  差{prev:.3f}-{gap:.3f}: 1着率{hit_rate:5.1f}%, ROI{roi:6.1f}% (n={len(subset)})")
    prev = gap

# ============================================================
# 分析12: 特徴量値と着順の詳細関係
# ============================================================
print("\n" + "="*80)
print("【分析12】主要特徴量と着順の関係")
print("="*80)

key_features = ['win_rate', 'place_rate', 'jockey_win_rate', 'days_since_last_race']
key_features = [f for f in key_features if f in preds.columns]

for feat in key_features:
    print(f"\n■ {feat}:")
    
    # 欠損値を除外
    valid = preds[preds[feat].notna()]
    
    # 5分位に分割
    try:
        quantiles = valid[feat].quantile([0.2, 0.4, 0.6, 0.8, 1.0])
        prev = valid[feat].min() - 0.001
        for q_val, val in quantiles.items():
            subset = valid[(valid[feat] > prev) & (valid[feat] <= val)]
            if len(subset) < 100:
                continue
            
            avg_finish = subset['finish_position'].mean()
            first_rate = (subset['finish_position'] == 1).mean() * 100
            
            print(f"    {prev:.3f}-{val:.3f}: 平均着順{avg_finish:5.2f}, 1着率{first_rate:4.1f}% (n={len(subset):,})")
            prev = val
    except Exception as e:
        print(f"    (分析不可: {e})")

print("\n" + "="*80)
print("データ分析完了")
print("="*80)

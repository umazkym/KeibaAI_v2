"""
ROI改善戦略の深層検証スクリプト
"""
import pandas as pd
import numpy as np
from scipy import stats

# データ読み込み
races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races['year'] = pd.to_datetime(races['race_date']).dt.year
shutuba = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')

print('=' * 70)
print('検証1: 勝馬のオッズ分布')
print('=' * 70)

winners = races[races['finish_position'] == 1]
print(f'全勝馬数: {len(winners)}')
print(f'勝馬の平均オッズ: {winners["win_odds"].mean():.2f}倍')
print()

print('勝馬のオッズ分布:')
odds_ranges = [(1, 3), (3, 5), (5, 10), (10, 20), (20, 50), (50, 100), (100, 500)]
for low, high in odds_ranges:
    count = len(winners[(winners['win_odds'] >= low) & (winners['win_odds'] < high)])
    pct = count / len(winners) * 100
    print(f'  {low}-{high}倍: {count}件 ({pct:.1f}%)')

print()
print('=' * 70)
print('検証2: 朝オッズの可用性確認')
print('=' * 70)
print(f'morning_odds非NULL件数: {shutuba["morning_odds"].notna().sum()}')
print(f'morning_popularity非NULL件数: {shutuba["morning_popularity"].notna().sum()}')
print()

print('=' * 70)
print('検証3: レース条件別のベースラインROI（人気1位購入）')
print('=' * 70)

# クラス別
print('\nクラス別ROI:')
for race_class in ['G1', 'G2', 'G3', 'OP', '3勝', '2勝', '1勝', '未勝利', '新馬']:
    class_data = races[races['race_class'].str.contains(race_class, na=False) if race_class in ['G1', 'G2', 'G3'] else races['race_class'] == race_class]
    if len(class_data) > 0:
        pop1 = class_data[class_data['popularity'] == 1]
        pop1_wins = pop1[pop1['finish_position'] == 1]
        if len(pop1) > 0:
            roi = pop1_wins['win_odds'].sum() / len(pop1) * 100
            win_rate = len(pop1_wins) / len(pop1) * 100
            print(f'  {race_class}: ROI={roi:.1f}%, 勝率={win_rate:.1f}%, n={len(pop1)}')

# 距離別
print('\n距離カテゴリ別ROI:')
races['dist_cat'] = pd.cut(races['distance_m'], bins=[0, 1400, 1800, 2200, 3000, 5000], 
                           labels=['sprint', 'mile', 'middle', 'long', 'stayer'])
for cat in ['sprint', 'mile', 'middle', 'long', 'stayer']:
    cat_data = races[races['dist_cat'] == cat]
    if len(cat_data) > 0:
        pop1 = cat_data[cat_data['popularity'] == 1]
        pop1_wins = pop1[pop1['finish_position'] == 1]
        if len(pop1) > 0:
            roi = pop1_wins['win_odds'].sum() / len(pop1) * 100
            win_rate = len(pop1_wins) / len(pop1) * 100
            print(f'  {cat}: ROI={roi:.1f}%, 勝率={win_rate:.1f}%, n={len(pop1)}')

# 馬場状態別
print('\n馬場状態別ROI:')
for condition in ['良', '稍重', '重', '不良']:
    cond_data = races[races['track_condition'] == condition]
    if len(cond_data) > 0:
        pop1 = cond_data[cond_data['popularity'] == 1]
        pop1_wins = pop1[pop1['finish_position'] == 1]
        if len(pop1) > 0:
            roi = pop1_wins['win_odds'].sum() / len(pop1) * 100
            win_rate = len(pop1_wins) / len(pop1) * 100
            print(f'  {condition}: ROI={roi:.1f}%, 勝率={win_rate:.1f}%, n={len(pop1)}')

# 頭数別
print('\n出走頭数別ROI:')
races['head_cat'] = pd.cut(races.groupby('race_id')['horse_number'].transform('max'), 
                           bins=[0, 8, 12, 14, 16, 20], labels=['少(~8)', '中(9-12)', '多(13-14)', '多(15-16)', '超多(17+)'])
for cat in ['少(~8)', '中(9-12)', '多(13-14)', '多(15-16)', '超多(17+)']:
    cat_data = races[races['head_cat'] == cat]
    if len(cat_data) > 0:
        pop1 = cat_data[cat_data['popularity'] == 1]
        pop1_wins = pop1[pop1['finish_position'] == 1]
        if len(pop1) > 0:
            roi = pop1_wins['win_odds'].sum() / len(pop1) * 100
            win_rate = len(pop1_wins) / len(pop1) * 100
            print(f'  {cat}: ROI={roi:.1f}%, 勝率={win_rate:.1f}%, n={len(pop1)}')

print()
print('=' * 70)
print('検証4: 確定オッズに頼らない代替指標の探索')
print('=' * 70)

# 人気順位の分布
print('\n人気順位と勝率・平均オッズ:')
for pop in range(1, 11):
    pop_data = races[races['popularity'] == pop]
    pop_wins = pop_data[pop_data['finish_position'] == 1]
    if len(pop_data) > 0:
        win_rate = len(pop_wins) / len(pop_data) * 100
        avg_odds = pop_data['win_odds'].mean()
        roi = pop_wins['win_odds'].sum() / len(pop_data) * 100 if len(pop_data) > 0 else 0
        print(f'  {pop}番人気: 勝率={win_rate:.1f}%, 平均オッズ={avg_odds:.1f}倍, ROI={roi:.1f}%, n={len(pop_data)}')

print()
print('=' * 70)
print('結論')
print('=' * 70)
print('''
【重要な発見】
1. 朝オッズ（morning_odds）は全件NULL
   → 本番では確定オッズに依存する戦略は使用不可

2. 「オッズ10-50倍フィルタ」戦略はバックテスト専用
   → 本番実装には「確定オッズに頼らない」代替指標が必要

3. レース条件（クラス、距離、馬場、頭数）によるROI差
   → これらは事前に判明するため、レース選択に使える可能性

4. 人気順位は確定オッズと強く相関するが、事前に推定可能
   → モデル予測順位を「疑似人気」として活用できる
''')

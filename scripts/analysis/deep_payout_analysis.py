#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""年別配当分布の深層分析"""

import pandas as pd
import numpy as np

# 的中詳細
hits = pd.read_csv('outputs/analysis/full_5year_hit_details.csv')

# 馬連 stable Top1-Top2
stable = hits[(hits['bet_type'] == 'umaren') & 
              (hits['pattern'] == 'Top1-Top2') & 
              (hits['condition'] == 'stable')]

print('='*70)
print('【馬連 stable Top1-Top2】年別配当分布分析')
print('='*70)

yearly_stats = []

for year in sorted(stable['year'].dropna().unique()):
    year_hits = stable[stable['year'] == year]
    payouts = year_hits['payout'].values
    
    if len(payouts) == 0:
        continue
    
    print(f'\n■ {int(year)}年 ({len(payouts)}件の的中)')
    print(f'  平均配当: {np.mean(payouts):.1f}倍')
    print(f'  中央値: {np.median(payouts):.1f}倍')
    print(f'  標準偏差: {np.std(payouts):.1f}')
    print(f'  最小: {np.min(payouts):.1f}倍, 最大: {np.max(payouts):.1f}倍')
    
    under10 = len([p for p in payouts if p < 10])
    b10_30 = len([p for p in payouts if 10 <= p < 30])
    b30_100 = len([p for p in payouts if 30 <= p < 100])
    over100 = len([p for p in payouts if p >= 100])
    
    print(f'  10倍未満: {under10}件, 10-30倍: {b10_30}件, 30-100倍: {b30_100}件, 100倍以上: {over100}件')
    
    yearly_stats.append({
        'year': int(year),
        'hits': len(payouts),
        'avg': np.mean(payouts),
        'median': np.median(payouts),
        'max': np.max(payouts),
        'over100': over100,
    })
    
    # 高配当の詳細
    high = year_hits[year_hits['payout'] >= 30].sort_values('payout', ascending=False)
    if len(high) > 0:
        print(f'  30倍以上の詳細:')
        for _, h in high.iterrows():
            print(f'    {h["race_date"]}: {h["payout"]:.1f}倍')

print('\n' + '='*70)
print('【考察】外れ値かどうかの判断')
print('='*70)

# 312倍を除外した場合の2024年ROI計算
y2024 = stable[stable['year'] == 2024]
total_return_with = y2024['payout'].sum()
total_return_without = y2024[y2024['payout'] < 300]['payout'].sum()
bets = 372  # 2024年のbet数

print(f'\n■ 2024年の312倍的中の影響')
print(f'  312倍含む場合のリターン: {total_return_with:.0f}倍 / {bets}件 = ROI {total_return_with/bets*100:.1f}%')
print(f'  312倍除く場合のリターン: {total_return_without:.0f}倍 / {bets}件 = ROI {total_return_without/bets*100:.1f}%')

# 年別100倍以上の頻度
print('\n■ 100倍以上的中の年別頻度')
for stat in yearly_stats:
    freq = stat['over100'] / stat['hits'] * 100 if stat['hits'] > 0 else 0
    print(f'  {stat["year"]}年: {stat["over100"]}件 / {stat["hits"]}件 ({freq:.1f}%)')

# 全年での100倍以上の確率
all_over100 = sum([s['over100'] for s in yearly_stats])
all_hits = sum([s['hits'] for s in yearly_stats])
print(f'\n  5年合計: {all_over100}件 / {all_hits}件 ({all_over100/all_hits*100:.1f}%)')

print('\n' + '='*70)
print('【結論】')
print('='*70)
print('''
人気薄（Top1-Top2でも人気が割れている場合）を狙う戦略であれば、
100倍以上の的中が発生すること自体は「外れ値」ではなく「期待される結果」。

問題は:
1. 312倍のような極端な高配当が「一貫して」発生するかどうか
2. 他の年でも同様の頻度で100倍以上が発生しているか
3. 平均配当と中央値の乖離（中央値が低く平均が高い=少数の高配当に依存）

これらを見ると、2024年の312倍は他年と比較して「特異」ではあるが、
人気薄狙いの戦略として「理論的に起こりうる」範囲内。
''')

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
競馬場×距離×馬場×状態の組み合わせ分析
"""
import pandas as pd
import numpy as np

df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
df = df.dropna(subset=['finish_time_seconds', 'track_condition', 'venue'])
df['race_date'] = pd.to_datetime(df['race_date'])

# 2020-2022年（Train期間）のデータのみ
train_df = df[(df['race_date'] >= '2020-01-01') & (df['race_date'] < '2023-01-01')]

print('=== 競馬場×距離×馬場×状態 の組み合わせ分析 ===')
print(f'Train期間のデータ: {len(train_df):,}件')
print()

# 組み合わせ数
combos = train_df.groupby(['venue', 'distance_m', 'track_surface', 'track_condition']).agg({
    'finish_time_seconds': ['mean', 'std', 'count']
}).reset_index()
combos.columns = ['venue', 'distance_m', 'track_surface', 'track_condition', 'mean', 'std', 'count']

print(f'ユニークな組み合わせ数: {len(combos)}')
print()

# サンプル数の分布
print('=== サンプル数の分布 ===')
cnt = combos['count']
print(f'count >= 100: {(cnt >= 100).sum()}組み合わせ')
print(f'30 <= count < 100: {((cnt >= 30) & (cnt < 100)).sum()}組み合わせ')
print(f'10 <= count < 30: {((cnt >= 10) & (cnt < 30)).sum()}組み合わせ')
print(f'count < 10: {(cnt < 10).sum()}組み合わせ')
print()

# サンプル数が少ない組み合わせの例
print('=== サンプル < 10 の例（芝・不良）===')
low_sample = combos[(combos['count'] < 10) & (combos['track_surface'] == '芝') & (combos['track_condition'] == '不良')]
print(low_sample[['venue', 'distance_m', 'count']].head(15).to_string(index=False))
print()

# フォールバック戦略の検証
print('=== フォールバック戦略 ===')
print('Level1: venue × distance × surface × condition')
print('Level2: venue × distance × surface')
print('Level3: distance × surface × condition')
print()

# 各レベルでのカバレッジ
level1 = combos[combos['count'] >= 30]
print(f'Level1 (venue×dist×surf×cond, N>=30): {len(level1)}組み合わせ')

level2 = train_df.groupby(['venue', 'distance_m', 'track_surface']).size()
level2_valid = (level2 >= 30).sum()
print(f'Level2 (venue×dist×surf, N>=30): {level2_valid}組み合わせ')

level3 = train_df.groupby(['distance_m', 'track_surface', 'track_condition']).size()
level3_valid = (level3 >= 30).sum()
print(f'Level3 (dist×surf×cond, N>=30): {level3_valid}組み合わせ')

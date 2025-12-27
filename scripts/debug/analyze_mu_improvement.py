#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μモデル改善のための深層分析
"""
import pandas as pd
import numpy as np
import json
from pathlib import Path

project_root = Path(__file__).parent.parent.parent

# データ読み込み
df = pd.read_parquet(project_root / 'keibaai/data/parsed/parquet/races/races.parquet')

# 現在のμモデルの特徴量を確認
with open(project_root / 'keibaai/models/mu_time_v1/feature_names.json', 'r') as f:
    features = json.load(f)

print('=== 現在のμモデル特徴量分析 ===')
print(f'特徴量総数: {len(features)}')
print()

# 時間関連特徴量
time_features = [f for f in features if 'time' in f.lower() or 'last3f' in f.lower() or 'pace' in f.lower()]
print(f'時間関連特徴量: {time_features}')
print()

# 過去タイム特徴量の有無
past_time = [f for f in features if 'prev' in f.lower()]
print(f'過去走関連特徴量: {past_time}')
print()

# ===== 核心的な問題分析 =====
print('=' * 60)
print('核心的な問題分析')
print('=' * 60)

# 時間予測に必要な情報
mask = (df['distance_m'] == 1200) & (df['track_surface'] == '芝') & (df['track_condition'] == '良')
sub = df[mask][['horse_id', 'race_id', 'race_date', 'finish_time_seconds', 'last_3f_time', 'finish_position']].dropna()

print()
print('1. last_3f_time（上がり3F）と順位の相関')
corr1 = sub['last_3f_time'].corr(sub['finish_position'])
print(f'   相関係数: {corr1:.3f}')
print('   → 上がりが速い馬ほど順位が良い（当たり前）')

print()
print('2. finish_time_secondsと順位の相関')  
corr2 = sub['finish_time_seconds'].corr(sub['finish_position'])
print(f'   相関係数: {corr2:.3f}')
print('   → タイムが速い馬ほど順位が良い（当たり前）')

print()
print('3. 問題の本質')
print('   現在のμモデルは「勝つかどうか」を予測するV15特徴量を使っている')
print('   しかし「どれくらい速く走れるか」を予測する特徴量が不足している')

print()
print('=' * 60)
print('必要な時間予測特徴量')
print('=' * 60)

print("""
【時間予測に必要だが現在欠如している特徴量】

1. 馬の過去タイム（同距離・同馬場）
   - horse_avg_time_same_distance: 同距離での平均タイム
   - horse_best_time_same_distance: 同距離でのベストタイム
   - horse_last_time_same_distance: 同距離での直近タイム
   → shift(1)で安全に計算可能

2. レースのペース予測
   - venue_distance_avg_time: その競馬場・距離の平均タイム
   - track_condition_time_effect: 馬場状態によるタイム補正
   → 過去データから計算可能

3. 上がり3F予測
   - horse_avg_last3f: 馬の平均上がり3F
   - horse_best_last3f: 馬のベスト上がり3F
   → これは既存のhorse_last3f_avgとして存在

4. 距離適性とタイムの関係
   - horse_distance_time_deviation: 各距離での馬のタイム偏差
   → 同距離での相対的なタイム
""")

# 欠如特徴量の具体例
print()
print('=' * 60)
print('改善案: 距離・馬場正規化された過去タイム特徴量')
print('=' * 60)

# 距離・馬場ごとの基準タイム計算
base_time = df.groupby(['distance_m', 'track_surface', 'track_condition'])['finish_time_seconds'].agg(['mean', 'std'])
base_time = base_time.reset_index()
base_time.columns = ['distance_m', 'track_surface', 'track_condition', 'base_time_mean', 'base_time_std']

print('距離・馬場・馬場状態ごとの基準タイム（例: 1200m芝）')
print(base_time[(base_time['distance_m'] == 1200) & (base_time['track_surface'] == '芝')])

print()
print('【改善版μモデルの設計方針】')
print("""
1. 目的変数を標準化タイムに変更
   target = (finish_time - base_time_mean) / base_time_std
   → 異なる距離・馬場のレースを統一的に扱える
   → 実際の時間への逆変換も容易

2. 時間予測特化特徴量の追加
   - 過去3走の標準化タイム平均
   - 同距離での標準化タイム平均
   - 同馬場状態での標準化タイム平均

3. ペース予測特徴量
   - 逃げ馬の数（既存）
   - 距離・競馬場の平均ペース
""")

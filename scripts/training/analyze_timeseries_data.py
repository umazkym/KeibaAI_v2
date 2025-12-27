# -*- coding: utf-8 -*-
"""
時系列要素のデータ深層分析

実際のParquetデータを多角的に分析し、
汎用的かつ再現性のある時系列改善要素を特定する
"""

import pandas as pd
import numpy as np
from pathlib import Path

# データ読み込み
data_dir = Path("keibaai/data/parsed/parquet")
races = pd.read_parquet(data_dir / "races/races.parquet")
races['race_date'] = pd.to_datetime(races['race_date'])
races = races[races['finish_position'].notna()].copy()

print("=" * 80)
print("1. 競走馬の出走頻度分析")
print("=" * 80)

# 馬ごとの年間出走数
races['year'] = races['race_date'].dt.year
horse_yearly = races.groupby(['horse_id', 'year']).size().reset_index(name='runs')
print("\n年間出走数の統計:")
print(horse_yearly['runs'].describe())
print(f"\n年間10走以上の馬の割合: {(horse_yearly['runs'] >= 10).mean()*100:.1f}%")
print(f"年間5走以上の馬の割合: {(horse_yearly['runs'] >= 5).mean()*100:.1f}%")
print(f"年間3走以下の馬の割合: {(horse_yearly['runs'] <= 3).mean()*100:.1f}%")

# 馬の通算出走数
horse_total = races.groupby('horse_id').size().reset_index(name='total_runs')
print("\n通算出走数の統計:")
print(horse_total['total_runs'].describe())
print(f"通算10走以上の馬の割合: {(horse_total['total_runs'] >= 10).mean()*100:.1f}%")
print(f"通算20走以上の馬の割合: {(horse_total['total_runs'] >= 20).mean()*100:.1f}%")
print(f"通算30走以上の馬の割合: {(horse_total['total_runs'] >= 30).mean()*100:.1f}%")

print("\n" + "=" * 80)
print("2. レース間隔（days_since_last_race）の分析")
print("=" * 80)

# レース間隔を計算
races_sorted = races.sort_values(['horse_id', 'race_date'])
races_sorted['prev_race_date'] = races_sorted.groupby('horse_id')['race_date'].shift(1)
races_sorted['days_since_last'] = (races_sorted['race_date'] - races_sorted['prev_race_date']).dt.days

print("\nレース間隔の統計:")
print(races_sorted['days_since_last'].describe())

# レース間隔帯別の勝率
def calc_win_rate_by_interval(df):
    intervals = [(0, 14), (14, 28), (28, 56), (56, 90), (90, 180), (180, 365), (365, 9999)]
    print("\nレース間隔帯別の勝率:")
    for low, high in intervals:
        mask = (df['days_since_last'] >= low) & (df['days_since_last'] < high)
        subset = df[mask]
        if len(subset) > 100:
            win_rate = (subset['finish_position'] == 1).mean() * 100
            avg_finish = subset['finish_position'].mean()
            print(f"  {low:>3}-{high:<4}日: {len(subset):>8,}件, 勝率 {win_rate:.2f}%, 平均着順 {avg_finish:.2f}")

calc_win_rate_by_interval(races_sorted)

print("\n" + "=" * 80)
print("3. 年齢別の成績変化（成長曲線）")
print("=" * 80)

print("\n年齢別の成績:")
for age in range(2, 10):
    mask = races['age'] == age
    subset = races[mask]
    if len(subset) > 1000:
        win_rate = (subset['finish_position'] == 1).mean() * 100
        avg_finish = subset['finish_position'].mean()
        print(f"  {age}歳: {len(subset):>8,}件, 勝率 {win_rate:.2f}%, 平均着順 {avg_finish:.2f}")

print("\n" + "=" * 80)
print("4. 競馬場別の開催特性")
print("=" * 80)

print("\n競馬場別のレース数と月別分布:")
venue_monthly = races.groupby(['venue', races['race_date'].dt.month]).size().unstack(fill_value=0)
print(venue_monthly)

print("\n" + "=" * 80)
print("5. 開催週（round_of_year, day_of_meeting）の分析")
print("=" * 80)

if 'day_of_meeting' in races.columns:
    print("\nday_of_meeting（開催日）の統計:")
    print(races['day_of_meeting'].describe())
    print("\nday_of_meeting別の勝率:")
    for dom in sorted(races['day_of_meeting'].dropna().unique())[:12]:
        mask = races['day_of_meeting'] == dom
        subset = races[mask]
        if len(subset) > 1000:
            win_rate = (subset['finish_position'] == 1).mean() * 100
            avg_finish = subset['finish_position'].mean()
            print(f"  開催{int(dom):>2}日目: {len(subset):>8,}件, 勝率 {win_rate:.2f}%, 平均着順 {avg_finish:.2f}")

if 'round_of_year' in races.columns:
    print("\nround_of_year（開催回）の分布:")
    print(races['round_of_year'].value_counts().head(10))

print("\n" + "=" * 80)
print("6. レースデータのカラム一覧")
print("=" * 80)
print("\n利用可能なカラム:")
for col in sorted(races.columns):
    non_null = races[col].notna().sum()
    non_null_pct = non_null / len(races) * 100
    print(f"  {col}: {non_null:,}件 ({non_null_pct:.1f}%)")

print("\n" + "=" * 80)
print("7. 馬場状態と開催日の関係")
print("=" * 80)

if 'track_condition' in races.columns:
    print("\n馬場状態の分布:")
    print(races['track_condition'].value_counts())
    
    print("\n開催日ごとの馬場状態分布:")
    if 'day_of_meeting' in races.columns:
        cross = pd.crosstab(races['day_of_meeting'], races['track_condition'], normalize='index') * 100
        print(cross.head(12))

print("\n" + "=" * 80)
print("8. 血統データの確認")
print("=" * 80)

try:
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    print(f"\n血統データ件数: {len(pedigrees):,}")
    print("\n血統カラム:")
    for col in pedigrees.columns:
        print(f"  {col}")
except Exception as e:
    print(f"血統データ読み込みエラー: {e}")

print("\n" + "=" * 80)
print("9. 季節・月別の成績傾向")
print("=" * 80)

races['month'] = races['race_date'].dt.month
print("\n月別の平均着順と勝率:")
for month in range(1, 13):
    mask = races['month'] == month
    subset = races[mask]
    win_rate = (subset['finish_position'] == 1).mean() * 100
    avg_finish = subset['finish_position'].mean()
    print(f"  {month:>2}月: {len(subset):>8,}件, 勝率 {win_rate:.2f}%, 平均着順 {avg_finish:.2f}")

print("\n" + "=" * 80)
print("10. race_detailsの内容確認")
print("=" * 80)

try:
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    print(f"\nrace_details件数: {len(race_details):,}")
    print("\nrace_detailsカラム:")
    for col in race_details.columns:
        non_null = race_details[col].notna().sum()
        print(f"  {col}: {non_null:,}件")
except Exception as e:
    print(f"race_details読み込みエラー: {e}")

print("\n" + "=" * 80)
print("分析完了")
print("=" * 80)

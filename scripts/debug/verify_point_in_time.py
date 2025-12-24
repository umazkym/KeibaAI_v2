"""
V7.1の時点考慮計算を検証

検証内容:
- Testデータの2025年6月15日のレース → 2025年6月14日以前のデータで統計計算されているか
- 同じ馬でも日付が異なれば異なる統計値になるか
"""
import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7

# データ読み込み
races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races_df['race_date'] = pd.to_datetime(races_df['race_date'])
races_df = races_df.dropna(subset=['finish_position', 'win_odds'])

# 分割
train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()

print("="*70)
print("V7.1 時点考慮計算の検証")
print("="*70)
print(f"\nTrain期間: {train_df['race_date'].min()} - {train_df['race_date'].max()}")
print(f"Test期間:  {test_df['race_date'].min()} - {test_df['race_date'].max()}")

# V7.1 fit
fe = LeakFreeFeatureEngineerV7()
fe.fit(races_df=train_df)

# Test期間の2つの異なる日に出走した同じ馬を探す
test_df = test_df.sort_values('race_date')
horse_counts = test_df.groupby('horse_id').size()
repeat_horses = horse_counts[horse_counts >= 2].index[:5].tolist()

print(f"\n複数回出走した馬を検証: {repeat_horses[:3]}")

# Transform
test_features = fe.transform(test_df)

# 検証: 同じ馬の異なる出走日で統計が異なるか
print("\n" + "="*70)
print("検証1: 同じ馬の統計が日付ごとに更新されているか")
print("="*70)

for horse_id in repeat_horses[:3]:
    horse_data = test_features[test_features['horse_id'] == horse_id].sort_values('race_date')
    if len(horse_data) >= 2:
        print(f"\n馬ID: {horse_id}")
        for idx, row in horse_data.head(3).iterrows():
            total_races = row.get('horse_total_races', 'N/A')
            win_rate = row.get('horse_win_rate')
            win_rate_str = f"{win_rate:.3f}" if pd.notna(win_rate) else "N/A"
            print(f"  {row['race_date'].date()}: horse_total_races={total_races}, horse_win_rate={win_rate_str}")

# 検証2: 手動計算との比較
print("\n" + "="*70)
print("検証2: 手動計算との比較（1頭を詳細確認）")
print("="*70)

# 適当な馬を1頭選んで手動検証
sample_horse = repeat_horses[0] if repeat_horses else test_df['horse_id'].iloc[0]
horse_history = train_df[train_df['horse_id'] == sample_horse].sort_values('race_date')
horse_test = test_features[test_features['horse_id'] == sample_horse].sort_values('race_date')

print(f"\n馬ID: {sample_horse}")
print(f"  Train期間のレース数: {len(horse_history)}")
if len(horse_history) > 0:
    train_wins = (horse_history['finish_position'] == 1).sum()
    print(f"  Train期間の勝利数: {train_wins}")
    print(f"  Train期間の勝率: {train_wins / len(horse_history):.3f}")

if len(horse_test) > 0:
    first_test = horse_test.iloc[0]
    print(f"\n  Test初出走日: {first_test['race_date'].date()}")
    print(f"  V7が計算したhorse_total_races: {first_test.get('horse_total_races', 'N/A')}")
    print(f"  V7が計算したhorse_win_rate: {first_test.get('horse_win_rate', 'N/A')}")
    
    # 期待値と比較
    expected_races = len(horse_history)
    if expected_races >= 3:
        expected_win_rate = train_wins / expected_races
        v7_win_rate = first_test.get('horse_win_rate', np.nan)
        if pd.notna(v7_win_rate):
            match = abs(expected_win_rate - v7_win_rate) < 0.001
            print(f"\n  期待値 horse_win_rate: {expected_win_rate:.3f}")
            print(f"  V7計算値 horse_win_rate: {v7_win_rate:.3f}")
            print(f"  一致: {'✓ YES' if match else '✗ NO'}")

print("\n" + "="*70)
print("検証完了")
print("="*70)

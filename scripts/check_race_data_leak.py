"""
レース結果系データの使用確認

重要: 以下が「現在のレース」の値ではなく「過去レース」の値であることを確認
- horse_last3f_avg
- horse_avg_c4_pos
- horse_avg_c4_gap
"""
import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7

print("="*80)
print("レース結果データの使用確認")
print("="*80)

# データ読み込み
races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races_df['race_date'] = pd.to_datetime(races_df['race_date'])
races_df = races_df.dropna(subset=['finish_position', 'win_odds'])

corners_df = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')

train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()

# 特定のレースを選択
sample_race_id = test_df['race_id'].iloc[100]
sample_race = test_df[test_df['race_id'] == sample_race_id].iloc[0]
sample_horse_id = sample_race['horse_id']
sample_date = sample_race['race_date']

print(f"\n【検証対象レース】")
print(f"  race_id: {sample_race_id}")
print(f"  race_date: {sample_date.date()}")
print(f"  horse_id: {sample_horse_id}")

# 現在のレースの実際のデータ
print(f"\n【現在のレースの実際の値（元データから）】")
print(f"  着順 (finish_position): {sample_race['finish_position']}")
print(f"  上り3ハロン (last_3f_time): {sample_race.get('last_3f_time', 'N/A')}")
print(f"  タイム (finish_time_seconds): {sample_race.get('finish_time_seconds', 'N/A')}")

# 4コーナー通過順（現在のレース）
sample_corner = corners_df[(corners_df['race_id'] == sample_race_id) & 
                            (corners_df['horse_number'] == sample_race['horse_number']) &
                            (corners_df['corner'] == 4)]
if len(sample_corner) > 0:
    print(f"  4コーナー通過順 (corner=4 position): {sample_corner.iloc[0]['position']}")
    print(f"  4コーナー先頭差 (gap_from_leader): {sample_corner.iloc[0]['gap_from_leader']}")
else:
    print(f"  4コーナーデータなし")

# この馬の過去レースの上り3ハロン
all_data = pd.concat([train_df, test_df])
horse_history = all_data[(all_data['horse_id'] == sample_horse_id) & 
                         (all_data['race_date'] < sample_date)].sort_values('race_date')

print(f"\n【過去レースの上り3ハロン（{len(horse_history)}レース）】")
if len(horse_history) > 0:
    past_3f = horse_history['last_3f_time'].dropna()
    print(f"  過去の上り3F値: {past_3f.values[-5:] if len(past_3f) > 0 else 'なし'}")
    print(f"  過去の上り3F平均: {past_3f.mean():.1f}" if len(past_3f) > 0 else "  平均: N/A")

# V7の計算結果
fe = LeakFreeFeatureEngineerV7()
pedigrees_df = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
race_details_df = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')

fe.fit(
    races_df=train_df,
    pedigrees_df=pedigrees_df,
    corners_df=corners_df,
    race_details_df=race_details_df
)
test_features = fe.transform(test_df)

# V7の計算値を取得
sample_feature_row = test_features[test_features['race_id'] == sample_race_id]
sample_feature_row = sample_feature_row[sample_feature_row['horse_number'] == sample_race['horse_number']]
if len(sample_feature_row) > 0:
    sample_feature_row = sample_feature_row.iloc[0]
    
    print(f"\n【V7が計算した特徴量】")
    print(f"  horse_last3f_avg: {sample_feature_row.get('horse_last3f_avg', 'N/A')}")
    print(f"  horse_avg_c4_pos: {sample_feature_row.get('horse_avg_c4_pos', 'N/A')}")
    print(f"  horse_avg_c4_gap: {sample_feature_row.get('horse_avg_c4_gap', 'N/A')}")
    
    # 比較
    print(f"\n【リークチェック】")
    v7_3f = sample_feature_row.get('horse_last3f_avg', np.nan)
    current_3f = sample_race.get('last_3f_time', np.nan)
    if pd.notna(v7_3f) and pd.notna(current_3f):
        if abs(v7_3f - current_3f) < 0.01:
            print(f"  ⚠️ 警告: horse_last3f_avg ({v7_3f:.1f}) = 現在の上り3F ({current_3f:.1f})")
        else:
            print(f"  ✓ horse_last3f_avg ({v7_3f:.1f}) ≠ 現在の上り3F ({current_3f:.1f})")
    
    # 過去レースの4コーナー順位を確認
    horse_corners = corners_df[(corners_df['race_id'].isin(horse_history['race_id'])) &
                               (corners_df['horse_number'] == sample_race['horse_number']) &
                               (corners_df['corner'] == 4)]
    if len(horse_corners) > 0:
        past_c4_pos = horse_corners['position'].mean()
        past_c4_gap = horse_corners['gap_from_leader'].mean()
        print(f"  過去の4コーナー順位平均: {past_c4_pos:.1f}")
        print(f"  V7 horse_avg_c4_pos: {sample_feature_row.get('horse_avg_c4_pos', 'N/A')}")

print("\n" + "="*80)
print("検証完了")
print("="*80)

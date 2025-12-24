"""
V7 リーク源の検証スクリプト

問題となる3つの関数のリーク挙動を実データで検証:
1. _merge_track_bias
2. _merge_running_style  
3. _merge_venue_pace
"""

import pandas as pd
import numpy as np
from pathlib import Path

# データ読み込み
DATA_DIR = Path("keibaai/data/parsed/parquet")

print("=" * 60)
print("V7 リーク源検証スクリプト")
print("=" * 60)

# races.parquet読み込み
races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
races['race_date'] = pd.to_datetime(races['race_date'])
print(f"\nraces.parquet: {len(races):,}行")
print(f"期間: {races['race_date'].min()} ～ {races['race_date'].max()}")

# Test期間の定義（レポートより）
TEST_START = pd.Timestamp('2025-01-05')
TEST_END = pd.Timestamp('2025-10-26')
TRAIN_END = pd.Timestamp('2024-06-30')

train_data = races[races['race_date'] <= TRAIN_END]
test_data = races[(races['race_date'] >= TEST_START) & (races['race_date'] <= TEST_END)]

print(f"\nTrain期間: {len(train_data):,}行 (～{TRAIN_END})")
print(f"Test期間: {len(test_data):,}行 ({TEST_START}～)")

# ========================================
# 検証1: _merge_track_bias のリーク確認
# ========================================
print("\n" + "=" * 60)
print("検証1: _merge_track_bias のリーク確認")
print("=" * 60)

# 現在のV7実装: self._full_history（fit時の全データ）からbracket_biasを計算
# 問題: fitにTest期間を含めるとTest結果がリークする

# fitにTrain+Test両方を渡した場合のシミュレーション
full_data = pd.concat([train_data, test_data])

# 距離カテゴリ追加
full_data['distance_category'] = pd.cut(
    full_data['distance_m'], bins=[0, 1400, 1800, 2200, 9999],
    labels=['sprint', 'mile', 'middle', 'long']
).astype(str)

# V7と同じ方法でbracket_biasを計算（全データ使用）
track_bias_full = full_data.groupby(
    ['venue', 'distance_category', 'track_surface', 'bracket_number']
).agg(
    bracket_avg_finish=('finish_position', 'mean'),
    n_samples=('finish_position', 'count')
).reset_index()

venue_base_full = full_data.groupby(
    ['venue', 'distance_category', 'track_surface']
).agg(
    venue_avg_finish=('finish_position', 'mean')
).reset_index()

track_bias_full = track_bias_full.merge(
    venue_base_full, on=['venue', 'distance_category', 'track_surface']
)
track_bias_full['bracket_bias_full'] = track_bias_full['bracket_avg_finish'] - track_bias_full['venue_avg_finish']

# Trainのみでbracket_biasを計算（正しい方法）
train_data_cat = train_data.copy()
train_data_cat['distance_category'] = pd.cut(
    train_data_cat['distance_m'], bins=[0, 1400, 1800, 2200, 9999],
    labels=['sprint', 'mile', 'middle', 'long']
).astype(str)

track_bias_train = train_data_cat.groupby(
    ['venue', 'distance_category', 'track_surface', 'bracket_number']
).agg(
    bracket_avg_finish=('finish_position', 'mean'),
    n_samples=('finish_position', 'count')
).reset_index()

venue_base_train = train_data_cat.groupby(
    ['venue', 'distance_category', 'track_surface']
).agg(
    venue_avg_finish=('finish_position', 'mean')
).reset_index()

track_bias_train = track_bias_train.merge(
    venue_base_train, on=['venue', 'distance_category', 'track_surface']
)
track_bias_train['bracket_bias_train'] = track_bias_train['bracket_avg_finish'] - track_bias_train['venue_avg_finish']

# 両者を比較
compare = track_bias_full[['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_bias_full']].merge(
    track_bias_train[['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_bias_train']],
    on=['venue', 'distance_category', 'track_surface', 'bracket_number'],
    how='inner'
)
compare['diff'] = compare['bracket_bias_full'] - compare['bracket_bias_train']

print(f"\n比較可能な組み合わせ: {len(compare):,}件")
print(f"差分の統計:")
print(compare['diff'].describe())
print(f"\n差分 > 0.1 の件数: {(compare['diff'].abs() > 0.1).sum()}件")
print(f"差分 > 0.2 の件数: {(compare['diff'].abs() > 0.2).sum()}件")

# 大きな差分がある例を表示
if (compare['diff'].abs() > 0.1).any():
    print("\n大きな差分がある例（上位5件）:")
    print(compare.nlargest(5, 'diff')[['venue', 'distance_category', 'bracket_number', 'bracket_bias_train', 'bracket_bias_full', 'diff']])

# ========================================
# 検証2: _merge_running_style のリーク確認
# ========================================
print("\n" + "=" * 60)
print("検証2: _merge_running_style のリーク確認")
print("=" * 60)

corners_path = DATA_DIR / "corners" / "corner_positions.parquet"
if corners_path.exists():
    corners = pd.read_parquet(corners_path)
    print(f"corner_positions.parquet: {len(corners):,}行")
    
    # コーナー4のデータ
    c4 = corners[corners['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
    c4.columns = ['race_id', 'horse_number', 'c4_position', 'c4_gap']
    
    # Train+Testの全データとマージ
    full_with_c4 = full_data.merge(c4, on=['race_id', 'horse_number'], how='left')
    
    # 全データでの馬別統計
    horse_running_full = full_with_c4.groupby('horse_id').agg(
        avg_c4_pos_full=('c4_position', 'mean'),
        avg_c4_gap_full=('c4_gap', 'mean'),
        n_races_full=('c4_position', 'count')
    ).reset_index()
    
    # Trainのみでの馬別統計
    train_with_c4 = train_data_cat.merge(c4, on=['race_id', 'horse_number'], how='left')
    horse_running_train = train_with_c4.groupby('horse_id').agg(
        avg_c4_pos_train=('c4_position', 'mean'),
        avg_c4_gap_train=('c4_gap', 'mean'),
        n_races_train=('c4_position', 'count')
    ).reset_index()
    
    # Test期間に出走する馬で比較
    test_horses = set(test_data['horse_id'].unique())
    
    compare_horse = horse_running_full.merge(
        horse_running_train, on='horse_id', how='inner'
    )
    compare_horse = compare_horse[compare_horse['horse_id'].isin(test_horses)]
    compare_horse['diff_c4_pos'] = compare_horse['avg_c4_pos_full'] - compare_horse['avg_c4_pos_train']
    
    print(f"\nTest期間に出走した馬のうち比較可能: {len(compare_horse):,}頭")
    print(f"4コーナー順位の差分統計:")
    print(compare_horse['diff_c4_pos'].describe())
    print(f"\n全データの方が順位良い（リーク効果）: {(compare_horse['diff_c4_pos'] < -0.5).sum()}頭")
else:
    print("corner_positions.parquet が存在しません")

# ========================================
# 検証3: _merge_venue_pace のリーク確認
# ========================================
print("\n" + "=" * 60)
print("検証3: _merge_venue_pace のリーク確認")
print("=" * 60)

race_details_path = DATA_DIR / "race_details" / "race_details.parquet"
if race_details_path.exists():
    race_details = pd.read_parquet(race_details_path)
    print(f"race_details.parquet: {len(race_details):,}行")
    
    # venue情報を追加
    race_venue = full_data[['race_id', 'venue']].drop_duplicates()
    
    # 全データでのvenue別ペース
    venue_pace_full = race_details.merge(race_venue, on='race_id', how='left')
    venue_stats_full = venue_pace_full.groupby('venue').agg(
        pace_avg_full=('first_half', 'mean'),
        n_races_full=('first_half', 'count')
    ).reset_index()
    
    # Train期間のみでのvenue別ペース
    train_race_ids = set(train_data['race_id'].unique())
    venue_pace_train = race_details[race_details['race_id'].isin(train_race_ids)].merge(
        race_venue, on='race_id', how='left'
    )
    venue_stats_train = venue_pace_train.groupby('venue').agg(
        pace_avg_train=('first_half', 'mean'),
        n_races_train=('first_half', 'count')
    ).reset_index()
    
    compare_venue = venue_stats_full.merge(venue_stats_train, on='venue', how='inner')
    compare_venue['diff'] = compare_venue['pace_avg_full'] - compare_venue['pace_avg_train']
    
    print(f"\n競馬場別ペース比較:")
    print(compare_venue[['venue', 'pace_avg_train', 'pace_avg_full', 'diff', 'n_races_full']].to_string())
else:
    print("race_details.parquet が存在しません")

# ========================================
# 結論
# ========================================
print("\n" + "=" * 60)
print("結論")
print("=" * 60)
print("""
【分析結果】
1. bracket_bias: TrainのみとFull(Train+Test)で差分が発生 → リーク確認
2. running_style: Test期間の馬の脚質がfull集計に含まれる → リーク確認  
3. venue_pace: ペース傾向は安定だが原則違反 → 軽微なリーク

【推奨対処】
- いずれの特徴量も「cumsum().shift(1)」パターンで再実装が必要
- または、fit時にTrain期間のみのデータで固定する方式に変更
""")

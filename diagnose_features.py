"""
特徴量生成の診断：過去走データがなぜゼ

ロになるのかを調査
"""
import pandas as pd
from pathlib import Path

# データパス
races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')

# 読み込み
print("Loading races.parquet...")
races_df = pd.read_parquet(races_path)
races_df['race_date'] = pd.to_datetime(races_df['race_date'])
print(f"  Loaded: {len(races_df):,} rows")

print("\nLoading shutuba.parquet...")
shutuba_df = pd.read_parquet(shutuba_path)
shutuba_df['race_date'] = pd.to_datetime(shutuba_df['race_date'])
print(f"  Loaded: {len(shutuba_df):,} rows")

# 2024年1月のデータでテスト
test_date_start = pd.Timestamp('2024-01-01')
test_date_end = pd.Timestamp('2024-01-31')

shutuba_2024_01 = shutuba_df[(shutuba_df['race_date'] >= test_date_start) & 
                              (shutuba_df['race_date'] <= test_date_end)]
print(f"\n2024年1月の出馬表: {len(shutuba_2024_01)}行")

# 過去走データ（2024年1月まで）
history_df = races_df[races_df['race_date'] < test_date_end]
print(f"2024年1月以前の過去走: {len(history_df):,}行")

# 重複チェック
duplicates = history_df.duplicated(subset=['race_id', 'horse_id'], keep=False).sum()
print(f"  重複: {duplicates}行")

# horse_idのマッチング確認
shutuba_horses = set(shutuba_2024_01['horse_id'].unique())
history_horses = set(history_df['horse_id'].unique())

common_horses = shutuba_horses & history_horses
print(f"\n馬IDマッチング:")
print(f"  出馬表の馬: {len(shutuba_horses)}")
print(f"  過去走の馬: {len(history_horses)}")
print(f"  共通の馬: {len(common_horses)}")
print(f"  マッチ率: {len(common_horses)/len(shutuba_horses)*100:.1f}%")

# サンプル馬で確認
if common_horses:
    sample_horse = list(common_horses)[0]
    print(f"\nサンプル馬 {sample_horse} の過去走:")
    horse_history = history_df[history_df['horse_id'] == sample_horse].sort_values('race_date')
    print(f"  過去走数: {len(horse_history)}")
    if len(horse_history) > 0:
        print(f"  最初: {horse_history['race_date'].min()}")
        print(f"  最後: {horse_history['race_date'].max()}")
        
        # finish_position, last_3f_time などがあるか
        for col in ['finish_position', 'last_3f_time', 'margin_seconds', 'passing_order_1']:
            if col in horse_history.columns:
                non_null = horse_history[col].notna().sum()
                print(f"  {col}: {non_null}/{len(horse_history)} non-null")

# 結合テスト
print("\n\n=== 結合テスト ===")
history_df_copy = history_df.copy()
history_df_copy['is_target_race'] = 0
shutuba_copy = shutuba_2024_01.copy()
shutuba_copy['is_target_race'] = 1

combined = pd.concat([history_df_copy, shutuba_copy], ignore_index=True, sort=False)
combined = combined.drop_duplicates(subset=['race_id', 'horse_id'], keep='last')
combined = combined.sort_values(by=['horse_id', 'race_date'])

print(f"結合後: {len(combined):,}行")
print(f"  対象レース: {combined['is_target_race'].sum()}行")
print(f"  過去走: {(combined['is_target_race']==0).sum()}行")

# 対象レースのデータで、過去走があるものを確認
target_races = combined[combined['is_target_race'] == 1].copy()

# groupby('horse_id').shift(1) の動作確認
target_races['prev_finish_position'] = combined.groupby('horse_id')['finish_position'].shift(1)

non_null_prev = target_races['prev_finish_position'].notna().sum()
print(f"\n対象レースで前走finish_positionがある: {non_null_prev}/{len(target_races)}")

# finish_positionがある過去走の数
history_with_finish = history_df_copy['finish_position'].notna().sum()
print(f"過去走でfinish_positionがある: {history_with_finish:,}/{len(history_df_copy)}")

# 列の存在確認
print(f"\n\nraces.parquetのカラム:")
for col in ['finish_position', 'last_3f_time', 'margin_seconds', 'passing_order_1', 'prize_money']:
    if col in races_df.columns:
        non_null = races_df[col].notna().sum()
        print(f"  {col}: あり ({non_null:,}/{len(races_df)} non-null)")
    else:
        print(f"  {col}: なし")

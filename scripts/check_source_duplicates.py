import pandas as pd
from pathlib import Path

data_dir = Path('data')

# 1. horses_performance の重複チェック
perf_path = data_dir / 'horses_performance/horses_performance_fixed.parquet'
df_perf = pd.read_parquet(perf_path)
print(f"horses_performance_fixed.parquet:")
print(f"  Total rows: {len(df_perf):,}")
dupes = df_perf.duplicated(subset=['race_id', 'horse_id']).sum()
print(f"  Duplicates (race_id, horse_id): {dupes:,}")
print(f"  Duplicate rate: {dupes/len(df_perf)*100:.2f}%")
print()

# 2. races.parquet の重複チェック
races_path = data_dir / 'races/races.parquet'
if races_path.exists():
    df_races = pd.read_parquet(races_path)
    print(f"races.parquet:")
    print(f"  Total rows: {len(df_races):,}")
    race_dupes = df_races.duplicated(subset=['race_id']).sum()
    print(f"  Duplicates (race_id): {race_dupes:,}")
    
    # race_id, horse_id の組み合わせでの重複
    if 'horse_id' in df_races.columns:
        combo_dupes = df_races.duplicated(subset=['race_id', 'horse_id']).sum()
        print(f"  Duplicates (race_id, horse_id): {combo_dupes:,}")
    print(f"  Duplicate rate: {race_dupes/len(df_races)*100:.2f}%")

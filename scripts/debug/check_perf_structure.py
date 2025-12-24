import pandas as pd
from pathlib import Path

data_dir = Path('keibaai/data/parsed/parquet')

# horses_performance の列を確認
perf_path = data_dir / 'horses_performance/horses_performance_fixed.parquet'
if not perf_path.exists():
    perf_path = data_dir / 'horses_performance/horses_performance.parquet'

df_perf = pd.read_parquet(perf_path)
print(f"horses_performance columns ({len(df_perf.columns)}):")
print(df_perf.columns.tolist())
print(f"\nTotal rows: {len(df_perf):,}")

# 重複チェック
dupes = df_perf.duplicated(subset=['race_id', 'horse_id']).sum()
print(f"Duplicates (race_id, horse_id): {dupes:,}")

# win_odds, popularity が含まれているか確認
print(f"\nwin_odds in columns: {'win_odds' in df_perf.columns}")
print(f"popularity in columns: {'popularity' in df_perf.columns}")

# サンプルレースの行数確認（同じrace_idが何行あるか）
sample_race = df_perf['race_id'].iloc[0]
same_race_rows = len(df_perf[df_perf['race_id'] == sample_race])
print(f"\nSample race {sample_race}: {same_race_rows} rows")

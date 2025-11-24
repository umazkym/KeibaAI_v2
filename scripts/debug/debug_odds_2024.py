import pandas as pd
from pathlib import Path

# 2024年のオッズデータを確認
perf_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')
df = pd.read_parquet(perf_path)

# 2024年のデータのみフィルタ
df['race_date'] = pd.to_datetime(df['race_date'])
df_2024 = df[(df['race_date'] >= '2024-01-01') & (df['race_date'] <= '2024-12-31')]

print(f"2024年データ:")
print(f"  Total rows: {len(df_2024):,}")
print(f"  win_odds non-null: {df_2024['win_odds'].notna().sum():,}")
print(f"  win_odds null: {df_2024['win_odds'].isna().sum():,}")
print(f"  Coverage: {df_2024['win_odds'].notna().sum() / len(df_2024) * 100:.1f}%")

print(f"\nSample 2024 data (first 10 rows with win_odds):")
sample = df_2024[df_2024['win_odds'].notna()][['race_id', 'horse_id', 'win_odds', 'finish_position']].head(10)
print(sample.to_string())

print(f"\nSample 2024 data (first 10 rows with NULL win_odds):")
sample_null = df_2024[df_2024['win_odds'].isna()][['race_id', 'horse_id', 'finish_position']].head(10)
print(sample_null.to_string())

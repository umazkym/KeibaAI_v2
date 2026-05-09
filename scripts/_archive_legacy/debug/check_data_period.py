import pandas as pd

df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
df['race_date'] = pd.to_datetime(df['race_date'])

print('=== races.parquet のデータ期間 ===')
print(f'最古: {df["race_date"].min()}')
print(f'最新: {df["race_date"].max()}')
print(f'レコード数: {len(df):,}')
print()

print('年別レコード数:')
print(df.groupby(df['race_date'].dt.year).size())

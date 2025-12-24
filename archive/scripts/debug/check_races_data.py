"""レースデータの内容確認スクリプト"""
import pandas as pd

df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
print(f'Shape: {df.shape}')
print(f'\nColumns ({len(df.columns)}):')
for i, col in enumerate(df.columns):
    print(f'  {i+1:3d}. {col}')

if 'race_date' in df.columns:
    print(f'\nDate range: {df["race_date"].min()} to {df["race_date"].max()}')
else:
    print('\nNo race_date column')

print(f'\nSample data:')
print(df.head(2).T)

"""
races.parquetの賞金列を確認
"""
import pandas as pd

parquet_path = 'keibaai/data/parsed/parquet/races/races.parquet'
df = pd.read_parquet(parquet_path)

# 2024年のデータだけ確認
df_2024 = df[df['race_id'].str.startswith('2024')]
print(f"2024年のレコード: {len(df_2024)}件")

# 賞金関連の列を確認
prize_cols = [col for col in df_2024.columns if 'prize' in col.lower()]
print(f"\n賞金関連の列: {prize_cols}")

for col in prize_cols:
    non_null = df_2024[col].notna().sum()
    non_zero = (df_2024[col] > 0).sum() if pd.api.types.is_numeric_dtype(df_2024[col]) else 0
    print(f"  {col}: 非null={non_null}, 非ゼロ={non_zero}")

# サンプルデータを表示
print(f"\n2024年データのサンプル（1-5位）:")
sample = df_2024[df_2024['finish_position'] <= 5][['race_id', 'finish_position', 'horse_name', 'prize_money']].head(20)
print(sample)

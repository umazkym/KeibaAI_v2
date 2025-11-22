"""
generate_features.pyが読み込むraces.parquetの列を確認
"""
import pandas as pd
from datetime import datetime

# generate_features.pyと同じロジックでデータ読み込み
races_dir = 'keibaai/data/parsed/parquet/races'

print("=" * 60)
print("races.parquetの列を確認")
print("=" * 60)

# 全データを読み込み
df = pd.read_parquet(races_dir)

print(f"\n総レコード数: {len(df):,}件")
print(f"列数: {len(df.columns)}")

# 賞金関連の列
prize_cols = [col for col in df.columns if 'prize' in col.lower()]
print(f"\n賞金関連の列: {prize_cols}")

# 各列の統計
for col in prize_cols:
    if pd.api.types.is_numeric_dtype(df[col]):
        non_null = df[col].notna().sum()
        non_zero = (df[col] > 0).sum()
        print(f"\n{col}:")
        print(f"  非null: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        print(f"  非ゼロ: {non_zero:,}/{len(df):,} ({non_zero/len(df)*100:.1f}%)")

# 2024年のデータだけ確認
df_2024 = df[df['race_id'].str.startswith('2024')]
print(f"\n2024年データ: {len(df_2024):,}件")

prize_cols_2024 = [col for col in df_2024.columns if 'prize' in col.lower()]
print(f"2024年の賞金関連列: {prize_cols_2024}")

for col in prize_cols_2024:
    if pd.api.types.is_numeric_dtype(df_2024[col]):
        non_zero = (df_2024[col] > 0).sum()
        print(f"  {col}: 非ゼロ={non_zero:,}/{len(df_2024):,} ({non_zero/len(df_2024)*100:.1f}%)")

# サンプルデータ
print(f"\nサンプルデータ（2024年、賞金あり）:")
sample = df_2024[df_2024['prize_money'] > 0][['race_id', 'horse_name', 'finish_position', 'prize_1st', 'prize_2nd', 'prize_money']].head(10)
print(sample)

# 全列リスト
print(f"\n全列（{len(df.columns)}個）:")
for col in sorted(df.columns):
    print(f"  - {col}")

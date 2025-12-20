"""データ状況チェックスクリプト"""
import pandas as pd
from pathlib import Path

# パース済みParquet
races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
shutuba = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')

print("=== Parquet File Status ===")
print(f"races.parquet: {len(races):,} rows")
print(f"  Date range: {races['race_date'].min()} to {races['race_date'].max()}")
print(f"shutuba.parquet: {len(shutuba):,} rows")
print(f"  Date range: {shutuba['race_date'].min()} to {shutuba['race_date'].max()}")

# 生HTMLファイル数
raw_base = Path('keibaai/data/raw/html')
race_files = len(list((raw_base / 'race').glob('*.bin')))
shutuba_files = len(list((raw_base / 'shutuba').glob('*.bin')))
horse_files = len(list((raw_base / 'horse').glob('*.bin')))
ped_files = len(list((raw_base / 'ped').glob('*.bin')))

print("\n=== Raw HTML File Status ===")
print(f"race: {race_files:,} files")
print(f"shutuba: {shutuba_files:,} files")
print(f"horse: {horse_files:,} files")
print(f"ped: {ped_files:,} files")

# ギャップ分析
print("\n=== Gap Analysis ===")
print(f"Race files vs Parquet: {race_files - len(races.groupby('race_id').first()):+,} difference")

# 期間別分布
races['race_date'] = pd.to_datetime(races['race_date'])
print("\n=== Yearly Distribution in Parquet ===")
yearly = races.groupby(races['race_date'].dt.year).size()
for year, count in yearly.items():
    print(f"  {year}: {count:,} records")

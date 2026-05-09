import sys
sys.path.insert(0, 'keibaai/src')

from pathlib import Path
import pandas as pd
from datetime import datetime

print("=== data_utils.py load_parquet_data_by_date 動作テスト ===\n")

# 実際のload関数をテスト
from utils.data_utils import load_parquet_data_by_date

features_dir = Path('keibaai/data/features/parquet')

# テスト1: 実際のデータ範囲で読み込み
print("Test 1: 2020-01-01 データ")
target_dt = datetime(2020, 1, 1)
df1 = load_parquet_data_by_date(features_dir, target_dt, target_dt, date_col='race_date')
print(f"Result: {len(df1):,} rows")
if not df1.empty:
    print(f"Date range: {df1['race_date'].min()} ~ {df1['race_date'].max()}")
print()

# テスト2: 2020年1月全体
print("Test 2: 2020-01 全体 (start=2020-01-01, end=2020-01-31)")
start_dt = datetime(2020, 1, 1)
end_dt = datetime(2020, 1, 31)
df2 = load_parquet_data_by_date(features_dir, start_dt, end_dt, date_col='race_date')
print(f"Result: {len(df2):,} rows")
if not df2.empty:
    print(f"Date range: {df2['race_date'].min()} ~ {df2['race_date'].max()}")
    print(f"Unique dates: {df2['race_date'].nunique()}")
print()

# テスト3: フィルタなしで全データ
print("Test 3: フィルタなし（全データ）")
df3 = load_parquet_data_by_date(features_dir, None, None, date_col='race_date')
print(f"Result: {len(df3):,} rows")
if not df3.empty:
    print(f"Date range: {df3['race_date'].min()} ~ {df3['race_date'].max()}")
    print(f"Unique dates: {df3['race_date'].nunique()}")

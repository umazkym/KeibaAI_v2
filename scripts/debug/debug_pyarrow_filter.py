import sys
sys.path.insert(0, 'keibaai/src')

from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
from datetime import datetime

print("=== PyArrow Dataset読み込みテスト ===")
features_dir = Path('keibaai/data/features/parquet')

# ファイルリスト取得
target_files = list(features_dir.rglob("*.parquet"))
print(f"Total files: {len(target_files)}")

# PyArrow Datasetとして読み込み
dataset = ds.dataset(target_files, format="parquet", partitioning="hive")
print(f"Dataset schema: {dataset.schema}")

# フィルタなしで読み込み
table = dataset.to_table()
df = table.to_pandas()
print(f"\nTotal rows (no filter): {len(df):,}")

if 'race_date' in df.columns:
    print(f"race_date type: {df['race_date'].dtype}")
    print(f"Date range: {df['race_date'].min()} ~ {df['race_date'].max()}")
    
    # 2020-01-15のデータをフィルタ
    target_date = datetime(2020, 1, 15)
    df_filtered = df[df['race_date'].dt.date == target_date.date()]
    print(f"\n2020-01-15 rows: {len(df_filtered):,}")
    
    # PyArrowフィルタでテスト
    print(f"\n=== PyArrow Filter Test ===")
    field = ds.field('race_date')
    filter_expr = (field >= target_date) & (field <= target_date)
    print(f"Filter: {filter_expr}")
    
    try:
        table_filtered = dataset.to_table(filter=filter_expr)
        df_pa_filtered = table_filtered.to_pandas()
        print(f"PyArrow filtered rows: {len(df_pa_filtered):,}")
    except Exception as e:
        print(f"PyArrow filter error: {e}")
else:
    print("race_date column not found!")

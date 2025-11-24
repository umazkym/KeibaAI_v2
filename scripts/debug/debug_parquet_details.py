import sys
sys.path.insert(0, 'keibaai/src')

from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

print("=== Parquetファイル詳細調査 ===\n")

features_dir = Path('keibaai/data/features/parquet')
parquet_files = list(features_dir.rglob('*.parquet'))

print(f"Total parquet files: {len(parquet_files)}\n")

# 最初の5ファイルを詳細調査
for i, pf in enumerate(parquet_files[:5], 1):
    print(f"--- File {i}: {pf.relative_to(features_dir)} ---")
    
    # PyArrowで読み込み
    try:
        parquet_file = pq.ParquetFile(pf)
        schema = parquet_file.schema_arrow
        metadata = parquet_file.metadata
        
        print(f"  Rows: {metadata.num_rows:,}")
        print(f"  Row groups: {metadata.num_row_groups}")
        print(f"  Columns: {metadata.num_columns}")
        
        # race_dateカラムの型を確認
        if 'race_date' in schema.names:
            race_date_field = schema.field('race_date')
            print(f"  race_date type: {race_date_field.type}")
        else:
            print(f"  race_date: NOT FOUND")
            print(f"  Available columns: {schema.names[:10]}")
        
        # 実際のデータをサンプル読み込み
        df_sample = pd.read_parquet(pf)
        if 'race_date' in df_sample.columns:
            print(f"  Pandas race_date dtype: {df_sample['race_date'].dtype}")
            print(f"  Date range: {df_sample['race_date'].min()} to {df_sample['race_date'].max()}")
            print(f"  Sample dates: {df_sample['race_date'].head(3).tolist()}")
        
        print()
        
    except Exception as e:
        print(f"  Error: {e}\n")

# パーティション構造を確認
print("\n=== パーティション構造 ===")
year_dirs = sorted(features_dir.glob('year=*'))
for year_dir in year_dirs:
    month_dirs = sorted(year_dir.glob('month=*'))
    print(f"{year_dir.name}: {len(month_dirs)} months")
    for month_dir in month_dirs[:3]:  # 最初の3ヶ月のみ
        files = list(month_dir.glob('*.parquet'))
        print(f"  {month_dir.name}: {len(files)} files")

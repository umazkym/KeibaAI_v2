import sys
sys.path.insert(0, 'keibaai/src')

from pathlib import Path
import pandas as pd

print("=== 特徴量データ確認（簡易版） ===")
features_dir = Path('keibaai/data/features/parquet')

# parquetファイル数
parquet_files = list(features_dir.rglob('*.parquet'))
print(f"Parquet files: {len(parquet_files)}")

if len(parquet_files) > 0:
    # 最初のファイルを読み込み
    sample_file = parquet_files[0]
    print(f"\nSample file: {sample_file}")
    
    try:
        sample_df = pd.read_parquet(sample_file)
        print(f"Rows: {len(sample_df):,}")
        print(f"Columns: {len(sample_df.columns)}")
        if 'race_date' in sample_df.columns:
            print(f"Has race_date: Yes")
            print(f"Date range: {sample_df['race_date'].min()} to {sample_df['race_date'].max()}")
        else:
            print(f"Has race_date: No")
            print(f"Available columns: {list(sample_df.columns[:10])}")
    except Exception as e:
        print(f"Error reading sample: {e}")
    
    # 全データを読み込み（サイズが小さい場合のみ）
    if len(parquet_files) < 30:
        print(f"\n=== 全データ読み込み ===")
        try:
            df_all = pd.read_parquet(features_dir)
            print(f"Total rows: {len(df_all):,}")
            if 'race_date' in df_all.columns:
                print(f"Date range: {df_all['race_date'].min()} to {df_all['race_date'].max()}")
                print(f"Unique dates: {df_all['race_date'].nunique():,}")
        except Exception as e:
            print(f"Error: {e}")
else:
    print("No parquet files found!")

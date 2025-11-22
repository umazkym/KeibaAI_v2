import sys
sys.path.insert(0, 'keibaai/src')

from pathlib import Path
import pandas as pd

print("=== 特徴量データ確認 ===")
features_dir = Path('keibaai/data/features/parquet')
print(f"Directory exists: {features_dir.exists()}")

if features_dir.exists():
    # パーティションディレクトリを確認
    year_dirs = sorted(features_dir.glob('year=*'))
    print(f"\nYear directories: {[d.name for d in year_dirs]}")
    
    # 2023年データを確認
    year_2023 = features_dir / 'year=2023'
    if year_2023.exists():
        month_dirs = sorted(year_2023.glob('month=*'))
        print(f"\n2023 Months: {[d.name for d in month_dirs]}")
        
        # 12月データがあるか確認
        dec_2023 = year_2023 / 'month=12'
        if dec_2023.exists():
            parquet_files = list(dec_2023.glob('*.parquet'))
            print(f"\n2023-12 Parquet files: {len(parquet_files)}")
            if parquet_files:
                sample_df = pd.read_parquet(parquet_files[0])
                print(f"Sample rows: {len(sample_df)}")
                print(f"Columns: {list(sample_df.columns[:10])}")
                print(f"Has race_date: {'race_date' in sample_df.columns}")
                if 'race_date' in sample_df.columns:
                    print(f"Date range: {sample_df['race_date'].min()} to {sample_df['race_date'].max()}")
        else:
            print("\n2023-12 does not exist")
    
    # 全体をpandasで読み込んでテスト
    print("\n=== Pandas read_parquet test ===")
    try:
        df_all = pd.read_parquet(features_dir)
        print(f"Total rows: {len(df_all):,}")
        if 'race_date' in df_all.columns:
            df_2023_12 = df_all[df_all['race_date'].astype(str).str.startswith('2023-12')]
            print(f"2023-12 rows: {len(df_2023_12):,}")
    except Exception as e:
        print(f"Error: {e}")

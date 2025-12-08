"""特徴量データの内容確認スクリプト"""
import pandas as pd
from pathlib import Path

features_dir = Path('keibaai/data/features/parquet')
print(f'Features directory: {features_dir}')
print(f'Exists: {features_dir.exists()}')

# パーティション化されたデータを読み込み
try:
    df = pd.read_parquet(features_dir)
    print(f'\nShape: {df.shape}')
    print(f'Columns ({len(df.columns)}):')
    for col in df.columns[:30]:
        print(f'  - {col}')
    if len(df.columns) > 30:
        print(f'  ... and {len(df.columns) - 30} more')
except Exception as e:
    print(f'Error reading parquet: {e}')
    
    # 年単位で読み込んでみる
    for year_dir in sorted(features_dir.glob('year=*')):
        try:
            year_df = pd.read_parquet(year_dir)
            print(f'{year_dir.name}: {len(year_df)} rows')
        except Exception as e2:
            print(f'{year_dir.name}: Error - {e2}')

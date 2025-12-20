"""全Parquetファイルの最新情報を確認"""
import pandas as pd
from pathlib import Path

data_dir = Path('keibaai/data/parsed/parquet')

files = {
    'races': data_dir / 'races/races.parquet',
    'shutuba': data_dir / 'shutuba/shutuba.parquet',
    'horses': data_dir / 'horses/horses.parquet',
    'pedigrees': data_dir / 'pedigrees/pedigrees.parquet',
    'returns': data_dir / 'returns/returns.parquet',
    'race_details': data_dir / 'race_details/race_details.parquet',
    'corners': data_dir / 'corners/corner_positions.parquet',
}

print('=== Parquetファイル最新状況 ===')
for name, path in files.items():
    df = pd.read_parquet(path)
    print(f"\n{name}:")
    print(f"  件数: {len(df):,}")
    print(f"  カラム数: {len(df.columns)}")
    if 'race_date' in df.columns:
        print(f"  期間: {df['race_date'].min()} ~ {df['race_date'].max()}")
    if 'race_id' in df.columns:
        print(f"  ユニークrace_id: {df['race_id'].nunique():,}")

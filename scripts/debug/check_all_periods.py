"""全ファイルの期間を年別で確認"""
import pandas as pd
from pathlib import Path

data_dir = Path('keibaai/data/parsed/parquet')

print("=" * 80)
print("全Parquetファイルの期間確認（年別レコード数）")
print("=" * 80)

# 各ファイルを読み込み（日付カラムがあるもの）
files_with_date = {
    'races': (data_dir / 'races/races.parquet', 'race_date'),
    'shutuba': (data_dir / 'shutuba/shutuba.parquet', 'race_date'),
    'returns': (data_dir / 'returns/returns.parquet', None),  # race_idから年を抽出
    'race_details': (data_dir / 'race_details/race_details.parquet', None),
    'corners': (data_dir / 'corners/corner_positions.parquet', None),
}

# racesから日付情報を取得（マスター）
races = pd.read_parquet(data_dir / 'races/races.parquet')
races['race_date'] = pd.to_datetime(races['race_date'])
races['year'] = races['race_date'].dt.year
race_year_map = races[['race_id', 'year']].drop_duplicates('race_id')

print("\n年別レコード数:")
print("-" * 80)
print(f"{'ファイル':<15} | " + " | ".join([f"{y}" for y in range(2014, 2026)]) + " | 合計")
print("-" * 80)

for name, (path, date_col) in files_with_date.items():
    df = pd.read_parquet(path)
    
    if date_col and date_col in df.columns:
        df['_date'] = pd.to_datetime(df[date_col])
        df['_year'] = df['_date'].dt.year
    else:
        # race_idから年を取得
        df = df.merge(race_year_map, on='race_id', how='left')
        df['_year'] = df['year']
    
    year_counts = df.groupby('_year').size()
    
    counts = []
    for y in range(2014, 2026):
        c = year_counts.get(y, 0)
        counts.append(f"{c:>6,}" if c > 0 else "     0")
    
    total = len(df)
    print(f"{name:<15} | " + " | ".join(counts) + f" | {total:>10,}")

# horses と pedigrees は日付がないので別途確認
print("\n" + "=" * 80)
print("日付カラムがないファイル")
print("=" * 80)

horses = pd.read_parquet(data_dir / 'horses/horses.parquet')
pedigrees = pd.read_parquet(data_dir / 'pedigrees/pedigrees.parquet')

print(f"\nhorses: {len(horses):,}頭")
if 'birth_year' in horses.columns:
    print(f"  birth_year範囲: {horses['birth_year'].min()} ~ {horses['birth_year'].max()}")
    print(f"  年別分布:")
    birth_counts = horses['birth_year'].value_counts().sort_index()
    for y in range(2010, 2024):
        print(f"    {y}: {birth_counts.get(y, 0):,}頭")

print(f"\npedigrees: {len(pedigrees):,}件")
print(f"  ユニークhorse_id: {pedigrees['horse_id'].nunique():,}")
print(f"  generation分布: {pedigrees['generation'].value_counts().sort_index().to_dict()}")

# HTMLファイルの期間も確認
print("\n" + "=" * 80)
print("HTMLファイルの期間確認")
print("=" * 80)

html_dir = Path('keibaai/data/raw/html')

for folder in ['race', 'shutuba', 'horse', 'ped']:
    files = sorted((html_dir / folder).glob('*.bin'))
    if files:
        first = files[0].stem
        last = files[-1].stem
        print(f"{folder:<10}: {len(files):>6,}件 | {first[:4]} ~ {last[:4]}")

import pandas as pd
from pathlib import Path

# races.parquet（過去走データ）を読み込み
races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
print(f"Loading {races_path}...")
races_df = pd.read_parquet(races_path)

print(f"読み込み完了: {len(races_df):,}行")
print(f"カラム数: {len(races_df.columns)}")
print(f"カラム: {list(races_df.columns)[:15]}")

if 'race_date' in races_df.columns:
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    print(f"\n期間: {races_df['race_date'].min()} - {races_df['race_date'].max()}")
    
    # 年別の件数
    print("\n年別レース数:")
    year_counts = races_df['race_date'].dt.year.value_counts().sort_index()
    for year, count in year_counts.items():
        print(f"  {year}年: {count:,}行")

if 'horse_id' in races_df.columns:
    print(f"\nhorse_id ユニーク数: {races_df['horse_id'].nunique():,}")
    print(f"horse_id サンプル: {races_df['horse_id'].head().tolist()}")
    print(f"horse_id type: {races_df['horse_id'].dtype}")

# shutuba.parquet（出馬表）も確認
shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
print(f"\n\nLoading {shutuba_path}...")
shutuba_df = pd.read_parquet(shutuba_path)

print(f"読み込み完了: {len(shutuba_df):,}行")
if 'race_date' in shutuba_df.columns:
    shutuba_df['race_date'] = pd.to_datetime(shutuba_df['race_date'])
    print(f"期間: {shutuba_df['race_date'].min()} - {shutuba_df['race_date'].max()}")
    
    year_counts = shutuba_df['race_date'].dt.year.value_counts().sort_index()
    print("\n年別出馬表数:")
    for year, count in year_counts.items():
        print(f"  {year}年: {count:,}行")

if 'horse_id' in shutuba_df.columns:
    print(f"\nhorse_id ユニーク数: {shutuba_df['horse_id'].nunique():,}")
    print(f"horse_id type: {shutuba_df['horse_id'].dtype}")

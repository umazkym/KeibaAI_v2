"""Parquet DBの全カラム・サンプルデータを確認するデバッグスクリプト"""
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "keibaai", "data", "parsed", "parquet")

parquet_files = {
    'races': os.path.join(DATA_DIR, "races", "races.parquet"),
    'race_details': os.path.join(DATA_DIR, "race_details", "race_details.parquet"),
    'corners': os.path.join(DATA_DIR, "corners", "corner_positions.parquet"),
}

for name, path in parquet_files.items():
    print(f"\n{'='*60}")
    print(f"📁 {name}: {path}")
    if not os.path.exists(path):
        print("  ❌ FILE NOT FOUND")
        continue
    
    df = pd.read_parquet(path)
    print(f"  行数: {len(df):,}")
    print(f"  カラム ({len(df.columns)}):")
    for col in df.columns:
        dtype = df[col].dtype
        non_null = df[col].notna().sum()
        sample = df[col].dropna().iloc[0] if non_null > 0 else 'N/A'
        # truncate sample
        sample_str = str(sample)[:60]
        print(f"    {col:30s} {str(dtype):12s} non-null:{non_null:>10,}  sample: {sample_str}")

# 中山ダート1400のサンプルを確認
print(f"\n{'='*60}")
print("🔍 中山ダート1400のサンプルデータ")
races_df = pd.read_parquet(parquet_files['races'])
target = races_df[
    (races_df['venue'] == '中山') &
    (races_df['distance_m'] == 1400) &
    (races_df['track_surface'] == 'ダート')
]
print(f"  該当レース数: {len(target)}")
if not target.empty:
    sample_ids = target['race_id'].head(3).tolist()
    print(f"  サンプルrace_id: {sample_ids}")
    
    # race_detailsからサンプル確認
    details_df = pd.read_parquet(parquet_files['race_details'])
    sample_details = details_df[details_df['race_id'] == sample_ids[0]]
    print(f"\n  race_id={sample_ids[0]} の出走馬データ ({len(sample_details)}頭):")
    if not sample_details.empty:
        row = sample_details.iloc[0]
        for col in sample_details.columns:
            val = row[col]
            val_str = str(val)[:80]
            print(f"    {col:30s}: {val_str}")

    # cornersからサンプル確認
    if os.path.exists(parquet_files['corners']):
        corners_df = pd.read_parquet(parquet_files['corners'])
        sample_corners = corners_df[corners_df['race_id'] == sample_ids[0]]
        print(f"\n  corners for race_id={sample_ids[0]} ({len(sample_corners)}行):")
        if not sample_corners.empty:
            print(sample_corners.head(8).to_string())

# 追加: race_detailsに着順データがあるか確認
print(f"\n{'='*60}")
print("🔍 race_details に着順関連カラムがあるか確認")
details_df = pd.read_parquet(parquet_files['race_details'])
rank_cols = [c for c in details_df.columns if any(k in c.lower() for k in ['rank', 'order', 'finish', 'result', 'position', '着'])]
print(f"  着順関連カラム候補: {rank_cols}")

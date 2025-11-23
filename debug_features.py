import pandas as pd
from pathlib import Path

# 2024年の特徴量データを確認
features_path = Path('keibaai/data/features/parquet')
partition_path = features_path / 'year=2024' / 'month=1'

print(f"Loading features from: {partition_path}")
features_df = pd.read_parquet(partition_path)

print(f"\nFeatures DataFrame:")
print(f"  Total rows: {len(features_df):,}")
print(f"  Columns ({len(features_df.columns)}): {list(features_df.columns)[:25]}")

# win_odds の存在確認
if 'win_odds' in features_df.columns:
    print(f"\n✓ win_odds in features_df")
    print(f"  Non-null: {features_df['win_odds'].notna().sum():,}")
    print(f"  Null: {features_df['win_odds'].isna().sum():,}")
else:
    print(f"\n❌ win_odds NOT in features_df")

# ターゲット列の確認
print(f"\nTarget columns:")
print(f"  finish_position exists: {'finish_position' in features_df.columns}")
print(f"  finish_time_seconds exists: {'finish_time_seconds' in features_df.columns}")

if 'finish_position' in features_df.columns:
    print(f"  finish_position non-null: {features_df['finish_position'].notna().sum():,}/{len(features_df):,}")
if 'finish_time_seconds' in features_df.columns:
    print(f"  finish_time_seconds non-null: {features_df['finish_time_seconds'].notna().sum():,}/{len(features_df):,}")

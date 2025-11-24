#!/usr/bin/env python3
"""
Simple end-to-end test: Generate features for 2024-01 with all fixes
"""
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent))

from keibaai.src.features.feature_engine import FeatureEngine
from keibaai.src.utils.data_utils import load_parquet_data_by_date

print("=== Feature Generation Test (2024-01) ===\n")

# 1. Load source data
print("1. Loading source data...")
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 31)

# Load shutuba (directly from partition)
shutuba_file = Path('keibaai/data/parsed/parquet/shutuba/year=2024/month=1/shutuba.parquet')
if not shutuba_file.exists():
    print(f"ERROR: Shutuba file not found: {shutuba_file}")
    sys.exit(1)

shutuba_df = pd.read_parquet(shutuba_file)
print(f"   Shutuba: {len(shutuba_df)} rows")

# Load races (single file only to avoid schema conflicts)
races_file = Path('keibaai/data/parsed/parquet/races/races.parquet')
races_df = pd.read_parquet(races_file)

# Filter races by date
races_df['race_date'] = pd.to_datetime(races_df['race_date'])
races_df = races_df[(races_df['race_date'] >= start_date) & (races_df['race_date'] <= end_date)]
print(f"   Races (filtered): {len(races_df)} rows")

# Check venue column
if 'venue' not in races_df.columns:
    print("ERROR: 'venue' column missing in races_df!")
    sys.exit(1)
print(f"   ✓ 'venue' column exists ({races_df['venue'].nunique()} unique values)")

# Load horse profiles
horse_profiles_file = Path('keibaai/data/parsed/parquet/horse_profiles/horse_profiles.parquet')
horse_profiles_df = pd.read_parquet(horse_profiles_file) if horse_profiles_file.exists() else pd.DataFrame()
print(f"   Horse profiles: {len(horse_profiles_df)} rows")

# 2. Generate features
print("\n2. Generating features...")
config_path = Path('keibaai/configs/features.yaml')
engine = FeatureEngine(config_path)

features_df = engine.generate_features(
    shutuba_df=shutuba_df,
    results_history_df=races_df,
    horse_profiles_df=horse_profiles_df
)

print(f"   Generated features: {features_df.shape}")
print(f"   Feature count: {len(features_df.columns)}")

# 3. Check for venue-based features
print("\n3. Checking venue-based interaction features...")
venue_features = [col for col in features_df.columns 
                  if ('jockey_' in col or 'trainer_' in col) and '_win_rate' in col 
                  and any(venue in col for venue in ['中山', '東京', '京都', '阪神', 'venue'])]

if venue_features:
    print(f"   ✓ Found {len(venue_features)} venue-based features!")
    print("   Sample:")
    for feat in venue_features[:5]:
        print(f"      - {feat}")
    print("\n=== SUCCESS ===")
else:
    print("   ✗ No venue-based features found")
    print("   All columns:", features_df.columns.tolist()[:30])
    print("\n=== FAILED ===")

# 4. Save features
print("\n4. Saving features...")
output_dir = Path('keibaai/data/features/parquet')
engine.save_features(features_df, str(output_dir), partition_cols=['year', 'month'])
print(f"   Saved to: {output_dir}")

print("\nDone!")

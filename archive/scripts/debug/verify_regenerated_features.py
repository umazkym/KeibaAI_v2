#!/usr/bin/env python3
"""
Verify regenerated features contain venue-based interaction features
"""
import pandas as pd
from pathlib import Path

features_path = Path('keibaai/data/features/parquet/year=2024/month=1')

if not features_path.exists():
    print(f"ERROR: Features path not found: {features_path}")
    exit(1)

print(f"Reading features from: {features_path}")
df = pd.read_parquet(features_path)

print(f"\nTotal rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")

# Find venue-related features
venue_features = [col for col in df.columns 
                  if ('jockey_' in col or 'trainer_' in col or 'sire_' in col) 
                  and ('_win_rate' in col or '_avg_finish' in col)
                  and any(x in col for x in ['中山', '東京', '京都', '阪神', '新潟', '福島', '中京', '小倉', '札幌', '函館'])]

print(f"\nVenue-based features found: {len(venue_features)}")

if venue_features:
    print("\nSample venue-based features:")
    for feat in sorted(venue_features)[:15]:
        non_null = df[feat].notna().sum()
        print(f"  {feat}: {non_null:,} non-null values")
    print("\n✅ SUCCESS: Venue-based interaction features generated correctly!")
else:
    print("\n❌ FAILED: No venue-based features found")
    print("\nAll feature columns:")
    for col in df.columns[:50]:
        print(f"  {col}")

#!/usr/bin/env python3
"""
Quick venue feature check - ASCII only output
"""
import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent))

from keibaai.src.features.feature_engine import FeatureEngine
from datetime import datetime

# Load data
shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/year=2024/month=1/shutuba.parquet')
races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
horse_profiles_df = pd.read_parquet('keibaai/data/parsed/parquet/horse_profiles/horse_profiles.parquet')

# Generate features
engine = FeatureEngine('keibaai/configs/features.yaml')
features_df = engine.generate_features(shutuba_df, races_df, horse_profiles_df)

# Check for venue features
venue_cols = [c for c in features_df.columns if 'venue' in c.lower() or 'jockey_' in c and ('_win_rate' in c or '_avg_finish' in c) or 'trainer_' in c and ('_win_rate' in c)]

print(f"Total features: {len(features_df.columns)}")
print(f"Venue-related features found: {len(venue_cols)}")

if len(venue_cols) > 0:
    print("\nSample venue features (first 10):")
    for col in venue_cols[:10]:
        print(f"  {col}")
    print("\nSUCCESS: Venue features generated!")
else:
    print("\nFAILURE: No venue features found")
    print("Sample columns:")
    for col in features_df.columns[:20]:
        print(f"  {col}")

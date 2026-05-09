#!/usr/bin/env python3
"""
テスト: venue カラムで特徴量生成が動作するか確認
"""
import sys
from pathlib import Path
import pandas as pd

# プロジェクトルート追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from keibaai.src.features.feature_engine import FeatureEngine
from keibaai.src.utils.data_utils import load_parquet_data_by_date
from datetime import datetime

print("=" * 60)
print("Venue-based Feature Generation Test")
print("=" * 60)

# 1. データロード
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 31)

shutuba_path = Path('keibaai/data/parsed/parquet/shutuba')
races_path = Path('keibaai/data/parsed/parquet/races')
horse_profiles_path = Path('keibaai/data/parsed/parquet/horse_profiles/horse_profiles.parquet')

print("\n1. Loading data...")
shutuba_df = load_parquet_data_by_date(shutuba_path, start_date, end_date, 'race_date')
races_df = pd.read_parquet(races_path / 'races.parquet')  # 直接読み込み
horse_profiles_df = pd.read_parquet(horse_profiles_path) if horse_profiles_path.exists() else pd.DataFrame()

print(f"   Shutuba: {len(shutuba_df)} rows")
print(f"   Races: {len(races_df)} rows")
print(f"   Horse profiles: {len(horse_profiles_df)} rows")

# venueカラムの確認
if 'venue' in races_df.columns:
    print(f"\n✅ 'venue' column EXISTS in races_df")
    print(f"   Unique venues: {races_df['venue'].nunique()}")
    print(f"   Sample venues: {races_df['venue'].dropna().unique()[:5].tolist()}")
else:
    print(f"\n❌ 'venue' column MISSING in races_df")
    print("Exiting test.")
    sys.exit(1)

# 2. 特徴量生成
print("\n2. Generating features with FeatureEngine...")
config_path = Path('keibaai/configs/features.yaml')
engine = FeatureEngine(config_path)

features_df = engine.generate_features(
    shutuba_df=shutuba_df,
    results_history_df=races_df,
    horse_profiles_df=horse_profiles_df
)

print(f"   Generated features: {features_df.shape}")
print(f"   Total columns: {len(features_df.columns)}")

# 3. venue関連の特徴量を確認
print("\n3. Checking venue-interaction features...")
venue_features = [col for col in features_df.columns if 'venue' in col.lower() or '中山' in col or '東京' in col]

if venue_features:
    print(f"✅ Found {len(venue_features)} venue-related features:")
    for feat in venue_features[:10]:
        print(f"   - {feat}")
    print("\n✅ TEST PASSED: Venue-based interaction features generated successfully!")
else:
    print("❌ No venue-related features found.")
    print("\nSample of generated features:")
    print(features_df.columns[:20].tolist())
    print("\n❌ TEST FAILED")

print("\n" + "=" * 60)

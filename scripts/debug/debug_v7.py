"""Debug V7 in sequence like test script"""
import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7

races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races_df['race_date'] = pd.to_datetime(races_df['race_date'])
races_df = races_df.dropna(subset=['finish_position', 'win_odds'])

# Same splits as test script
train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & (races_df['race_date'] < '2025-01-01')].copy()
test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()

print(f"Train: {len(train_df)}, Valid: {len(valid_df)}, Test: {len(test_df)}")

fe = LeakFreeFeatureEngineerV7()
pedigrees_df = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
corners_df = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
race_details_df = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')

fe.fit(
    races_df=train_df,
    pedigrees_df=pedigrees_df,
    corners_df=corners_df,
    race_details_df=race_details_df
)

# Transform in sequence like test script
print("\nTransforming train...")
train_features = fe.transform(train_df)
print(f"Train finish_position NA: {train_features['finish_position'].isna().sum()} / {len(train_features)}")

print("\nTransforming valid...")  
valid_features = fe.transform(valid_df)
print(f"Valid finish_position NA: {valid_features['finish_position'].isna().sum()} / {len(valid_features)}")

print("\nTransforming test...")
test_features = fe.transform(test_df)
print(f"Test finish_position NA: {test_features['finish_position'].isna().sum()} / {len(test_features)}")
print(f"Test sample: {test_features['finish_position'].head().tolist()}")

import pandas as pd
from pathlib import Path

# 2024年の特徴量データを確認
features_path = Path('keibaai/data/features/parquet/year=2024/month=1')
print(f"Loading features from: {features_path}")
try:
    features_df = pd.read_parquet(features_path)
    
    if 'finish_position' in features_df.columns:
        print(f"\nfinish_position stats:")
        print(features_df['finish_position'].describe())
        print(f"\nSample values:")
        print(features_df[['race_id', 'horse_id', 'finish_position']].head(20))
    else:
        print("finish_position NOT found")
        
except Exception as e:
    print(f"Error: {e}")

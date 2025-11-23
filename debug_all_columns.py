import pandas as pd
from pathlib import Path

# 2024年の特徴量データを確認
features_path = Path('keibaai/data/features/parquet/year=2024/month=1')
print(f"Loading features from: {features_path}")
try:
    features_df = pd.read_parquet(features_path)
    print(f"Columns ({len(features_df.columns)}):")
    print(list(features_df.columns))
    
    # Check for partial matches
    matches = [c for c in features_df.columns if 'finish' in c]
    print(f"\nColumns matching 'finish': {matches}")
        
except Exception as e:
    print(f"Error: {e}")

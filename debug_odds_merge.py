import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime

# 1. Load Config
try:
    project_root = Path.cwd() / 'keibaai'  # Simulate evaluate_model.py's project_root
    with open('keibaai/configs/default.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Path substitution logic from evaluate_model.py
    data_path = project_root / config.get('data_path', 'data')
    for key, value in config.items():
        if isinstance(value, str):
            config[key] = value.replace('${data_path}', str(data_path))
            
    print("Config loaded and paths substituted.")
except Exception as e:
    print(f"Config load failed: {e}")
    config = {}

# 2. Load Features (2024-01)
features_path = Path('keibaai/data/features/parquet/year=2024/month=1')
print(f"Loading features from: {features_path}")
features_df = pd.read_parquet(features_path)
print(f"Features loaded: {len(features_df)} rows")

# 3. Load Performance Data (Logic from evaluate_model.py)
perf_path = None
if 'parsed_parquet_path' in config:
    perf_path = Path(config['parsed_parquet_path']) / 'horses_performance' / 'horses_performance.parquet'
elif 'parsed_data_path' in config:
    perf_path = Path(config['parsed_data_path']) / 'parquet' / 'horses_performance' / 'horses_performance.parquet'
else:
    perf_path = Path(config.get('data_path', 'data')) / 'parsed' / 'parquet' / 'horses_performance' / 'horses_performance.parquet'

print(f"Performance path resolved to: {perf_path}")

if perf_path.exists():
    perf_df = pd.read_parquet(perf_path)
    print(f"Performance data loaded: {len(perf_df)} rows")
    
    # Filter by date (2024-01)
    start_dt = datetime(2024, 1, 1)
    end_dt = datetime(2024, 1, 31)
    
    if 'race_date' in perf_df.columns:
        perf_df['race_date'] = pd.to_datetime(perf_df['race_date'])
        perf_df = perf_df[(perf_df['race_date'] >= start_dt) & (perf_df['race_date'] <= end_dt)]
        print(f"Filtered performance data (2024-01): {len(perf_df)} rows")
    
    # Key normalization
    features_df['race_id'] = features_df['race_id'].astype(str).str.strip()
    features_df['horse_id'] = features_df['horse_id'].astype(str).str.strip()
    
    perf_df['race_id'] = perf_df['race_id'].astype(str).str.strip()
    perf_df['horse_id'] = perf_df['horse_id'].astype(str).str.strip()
    
    # Check sample keys
    print(f"\nSample keys (Features):")
    print(features_df[['race_id', 'horse_id']].head().to_string())
    print(f"\nSample keys (Performance):")
    print(perf_df[['race_id', 'horse_id']].head().to_string())
    
    # Merge
    odds_df = perf_df[['race_id', 'horse_id', 'win_odds']].copy()
    merged_df = features_df.merge(odds_df, on=['race_id', 'horse_id'], how='left')
    
    print(f"\nMerged DataFrame: {len(merged_df)} rows")
    print(f"win_odds non-null: {merged_df['win_odds'].notna().sum()}")
    print(f"Match rate: {merged_df['win_odds'].notna().sum() / len(merged_df) * 100:.1f}%")
    
    if merged_df['win_odds'].notna().sum() == 0:
        print("\n❌ Merge failed completely. Checking for key mismatches...")
        # Check for intersection
        common_races = set(features_df['race_id']) & set(perf_df['race_id'])
        common_horses = set(features_df['horse_id']) & set(perf_df['horse_id'])
        print(f"Common race_ids: {len(common_races)}")
        print(f"Common horse_ids: {len(common_horses)}")
        
        if len(common_races) > 0:
            sample_race = list(common_races)[0]
            print(f"\nDeep dive into race_id: {sample_race}")
            print("Features:")
            print(features_df[features_df['race_id'] == sample_race][['race_id', 'horse_id']].to_string())
            print("Performance:")
            print(perf_df[perf_df['race_id'] == sample_race][['race_id', 'horse_id']].to_string())

else:
    print("❌ Performance file not found")

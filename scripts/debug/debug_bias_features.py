import pandas as pd
import sys
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from keibaai.src.features.advanced_features import AdvancedFeatureEngine

def debug_bias_features():
    data_dir = project_root / 'keibaai/data/parsed/parquet'
    perf_path = data_dir / 'horses_performance/horses_performance_fixed.parquet'
    
    print(f"Loading data from {perf_path}...")
    try:
        df = pd.read_parquet(perf_path)
    except FileNotFoundError:
        print("horses_performance_fixed.parquet not found. Trying horses_performance.parquet")
        perf_path = data_dir / 'horses_performance/horses_performance.parquet'
        df = pd.read_parquet(perf_path)

    print(f"Data loaded. Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    print(f"Data loaded. Shape: {df.shape}")
    
    # FIX: Merge missing columns from races.parquet
    required_cols = ['round_of_year', 'day_of_meeting']
    missing = [c for c in required_cols if c not in df.columns]
    
    if missing:
        print(f"MISSING COLUMNS: {missing}. Attempting to merge from races.parquet...")
        races_path = data_dir / 'races/races.parquet'
        if races_path.exists():
            races_df = pd.read_parquet(races_path)
            # Select only needed columns + race_id
            cols_to_merge = ['race_id'] + missing
            # Check if columns exist in races
            available_cols = [c for c in cols_to_merge if c in races_df.columns]
            
            if len(available_cols) > 1: # at least race_id and one other
                df = df.merge(races_df[available_cols], on='race_id', how='left')
                print(f"Merged {available_cols[1:]} from races.parquet")
            else:
                print("Required columns not found in races.parquet")
        else:
            print("races.parquet not found.")
            
    # Convert race_date
    df['race_date'] = pd.to_datetime(df['race_date'])
    
    # Split into target (2023) and history (2020-2022) for testing
    target_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2023-02-01')
    history_mask = (df['race_date'] < '2023-01-01')
    
    target_df = df[target_mask].copy()
    history_df = df[history_mask].copy()
    
    print(f"Target DF (Jan 2023): {len(target_df)} rows")
    print(f"History DF (Pre-2023): {len(history_df)} rows")
    
    engine = AdvancedFeatureEngine()
    
    print("\n--- Testing generate_seasonal_bias_features ---")
    try:
        res_seasonal = engine.generate_seasonal_bias_features(target_df.copy(), history_df)
        print("Result Columns:", res_seasonal.columns.tolist())
        if 'bias_seasonal_score' in res_seasonal.columns:
            print("bias_seasonal_score stats:")
            print(res_seasonal['bias_seasonal_score'].describe())
            print("NaN count:", res_seasonal['bias_seasonal_score'].isna().sum())
        else:
            print("bias_seasonal_score NOT generated.")
    except Exception as e:
        print(f"Error in seasonal bias: {e}")

    print("\n--- Testing generate_dynamic_bias_features ---")
    try:
        res_dynamic = engine.generate_dynamic_bias_features(target_df.copy(), history_df)
        print("Result Columns:", res_dynamic.columns.tolist())
        if 'bias_dynamic_score' in res_dynamic.columns:
            print("bias_dynamic_score stats:")
            print(res_dynamic['bias_dynamic_score'].describe())
            print("NaN count:", res_dynamic['bias_dynamic_score'].isna().sum())
        else:
            print("bias_dynamic_score NOT generated.")
    except Exception as e:
        print(f"Error in dynamic bias: {e}")

if __name__ == "__main__":
    debug_bias_features()

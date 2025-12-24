import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def check_leakage():
    print("Loading training data...")
    df = pd.read_parquet('keibaai/models/mu_v2/train_data_mu_v2.parquet')
    
    # Target columns
    target_cols = ['finish_position', 'target', 'finish_time_seconds']
    targets = [c for c in target_cols if c in df.columns]
    
    if not targets:
        print("No target columns found in dataset to check against.")
        # Try to reconstruct target if missing
        if 'finish_position' not in df.columns:
             # We might need to load performance data to get targets if they were excluded from the save
             pass

    # We need the target to check correlation. 
    # If finish_position was excluded from the parquet, we need to merge it back.
    if 'finish_position' not in df.columns:
        print("Merging target data for validation...")
        perf_path = 'keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet'
        perf_df = pd.read_parquet(perf_path)
        # Ensure types
        df['race_id'] = df['race_id'].astype(str)
        df['horse_id'] = df['horse_id'].astype(str)
        perf_df['race_id'] = perf_df['race_id'].astype(str)
        perf_df['horse_id'] = perf_df['horse_id'].astype(str)
        
        df = df.merge(perf_df[['race_id', 'horse_id', 'finish_position']], on=['race_id', 'horse_id'], how='left')
    
    # Create binary target
    df['target'] = (df['finish_position'] == 1).astype(int)
    
    print(f"Analyzing {len(df.columns)} columns...")
    
    suspicious_features = []
    
    # Exclude non-numeric and ID columns
    exclude_cols = ['race_id', 'horse_id', 'race_date', 'race_name', 'horse_name', 'jockey_name', 'trainer_name', 'finish_position', 'target']
    feature_cols = [c for c in df.columns if c not in exclude_cols and pd.api.types.is_numeric_dtype(df[c])]
    
    print(f"Checking {len(feature_cols)} numeric features for correlation with finish_position...")
    
    for col in feature_cols:
        # Correlation with finish_position (lower is better, so negative correlation is expected for good features)
        # But if it's too high (e.g. > 0.8 or < -0.8), it's suspicious.
        corr = df[col].corr(df['finish_position'])
        
        # Also check correlation with binary target
        corr_bin = df[col].corr(df['target'])
        
        # Check for "perfect" predictors
        if abs(corr) > 0.5 or abs(corr_bin) > 0.5:
            suspicious_features.append({
                'feature': col,
                'corr_finish': corr,
                'corr_win': corr_bin
            })
            
    # Sort by absolute correlation
    suspicious_features.sort(key=lambda x: abs(x['corr_finish']), reverse=True)
    
    print("\n--- Suspicious High Correlation Features ---")
    if not suspicious_features:
        print("None found. Highest correlations are within safe limits.")
    else:
        for item in suspicious_features:
            print(f"{item['feature']}: Corr(Finish)={item['corr_finish']:.4f}, Corr(Win)={item['corr_win']:.4f}")
            
    # Check for "Future" leakage in past_ features
    # If past_1_finish_position is exactly equal to current finish_position, that's a leak (shift failed)
    print("\n--- Checking for Shift Failures (Identity Leakage) ---")
    past_finish_cols = [c for c in df.columns if 'past_' in c and 'finish_position' in c and 'mean' in c]
    
    for col in past_finish_cols:
        # Check if feature equals target exactly (or very close)
        # Note: mean of 1 race (past_1) might equal current if shift failed AND it's the same race? 
        # No, if shift failed, past_1 would be the current race's finish position.
        
        # Let's check exact matches
        exact_matches = (df[col] == df['finish_position']).sum()
        match_rate = exact_matches / len(df)
        
        print(f"{col} == finish_position: {exact_matches} matches ({match_rate:.2%})")
        
        if match_rate > 0.9:
            print(f"  [ALARM] {col} is likely leaking the current finish position!")

if __name__ == "__main__":
    check_leakage()

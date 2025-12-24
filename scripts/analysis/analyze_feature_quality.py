import pandas as pd
import numpy as np

def analyze_quality():
    print("Loading training data...")
    try:
        df = pd.read_parquet('keibaai/models/mu_v2/train_data_mu_v2.parquet')
    except FileNotFoundError:
        print("Error: Training data not found.")
        return

    # Identify feature columns (same exclusion logic as before)
    exclude_cols = {
        'race_id', 'horse_id', 'race_date', 'finish_position', 'target', 
        'win_odds', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
        'race_name', 'horse_name', 'jockey_name', 'trainer_name',
        # Leakage columns
        'finish_time_seconds', 'margin_seconds', 'prize_money', 'popularity',
        'odds', 'finish_time_str', 'margin_str', 'last_3f_time', 'passing_order',
        'final_corner_to_finish', 'passing_order_1', 'passing_order_2', 
        'passing_order_3', 'passing_order_4', 'position_change_1_2', 
        'scratched', 'time_except_last3f', 'win_probability', 'pace_index'
    }
    
    feature_cols = sorted([c for c in df.columns if c not in exclude_cols])
    print(f"Analyzing {len(feature_cols)} features...")
    
    stats = []
    
    for col in feature_cols:
        total = len(df)
        
        # Count NaNs
        n_nans = df[col].isna().sum()
        nan_rate = n_nans / total
        
        # Count Zeros (only for numeric columns)
        if pd.api.types.is_numeric_dtype(df[col]):
            n_zeros = (df[col] == 0).sum()
            zero_rate = n_zeros / total
        else:
            n_zeros = 0
            zero_rate = 0.0
            
        stats.append({
            'Feature': col,
            'Null_Rate': nan_rate,
            'Zero_Rate': zero_rate,
            'Null+Zero': nan_rate + zero_rate
        })
        
    stats_df = pd.DataFrame(stats)
    
    # Sort by Null+Zero rate descending
    stats_df = stats_df.sort_values('Null+Zero', ascending=False)
    
    print("\n=== Top 20 Features with High Null/Zero Rates ===")
    print(stats_df.head(20).to_string(index=False, float_format='{:.2%}'.format))
    
    # Save full report
    stats_df.to_csv('feature_quality_stats.csv', index=False)
    print("\nFull report saved to feature_quality_stats.csv")

    # Specific check for past_ features
    print("\n=== Past Performance Features Summary ===")
    past_cols = [c for c in feature_cols if 'past_' in c]
    if past_cols:
        past_df = stats_df[stats_df['Feature'].isin(past_cols)]
        print(past_df.head(10).to_string(index=False, float_format='{:.2%}'.format))

if __name__ == "__main__":
    analyze_quality()

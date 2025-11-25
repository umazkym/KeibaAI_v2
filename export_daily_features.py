import pandas as pd
import os

def export_daily_features():
    print("Loading training data...")
    try:
        df = pd.read_parquet('keibaai/models/mu_v2/train_data_mu_v2.parquet')
    except FileNotFoundError:
        print("Error: Training data not found.")
        return

    # Find the latest date
    latest_date = df['race_date'].max()
    print(f"Latest date found: {latest_date.date()}")
    
    # Filter for that date
    daily_df = df[df['race_date'] == latest_date].copy()
    print(f"Found {len(daily_df)} rows for {latest_date.date()}")
    
    # Sort for readability
    daily_df = daily_df.sort_values(['race_id', 'horse_number'])
    
    # Save to CSV
    output_file = 'daily_features_sample.csv'
    daily_df.to_csv(output_file, index=False)
    print(f"Exported full feature data to {os.path.abspath(output_file)}")

if __name__ == "__main__":
    export_daily_features()

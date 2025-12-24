import pandas as pd
import os

def debug_horse_history():
    parquet_path = r"keibaai\data\parsed\parquet\horses_performance\horses_performance.parquet"
    if not os.path.exists(parquet_path):
        print(f"File not found: {parquet_path}")
        return

    print(f"Loading {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    
    # Search for horse by name
    # Note: The parquet might not have horse names, only IDs.
    # If so, we need to find the ID first.
    # Let's check columns first.
    print("Columns:", df.columns.tolist())
    
    target_horse_id = "2023106272" # Nishino Tomaranai
    
    # Search by horse_id
    if 'horse_id' in df.columns:
        # Ensure horse_id is string for comparison
        df['horse_id'] = df['horse_id'].astype(str)
        horse_df = df[df['horse_id'] == target_horse_id]
    else:
        print("No 'horse_id' column found.")
        return

    if horse_df.empty:
        print(f"No data found for horse ID: {target_horse_id}")
    else:
        print(f"Found {len(horse_df)} records for {target_horse_id}:")
        # Sort by date
        if 'date' in horse_df.columns:
            horse_df['date'] = pd.to_datetime(horse_df['date'])
            horse_df = horse_df.sort_values('date', ascending=False)
        
        for _, row in horse_df.iterrows():
            print(f"Date: {row.get('date')}, Race: {row.get('race_name')}, Rank: {row.get('rank')}, ID: {row.get('horse_id')}")

if __name__ == "__main__":
    debug_horse_history()

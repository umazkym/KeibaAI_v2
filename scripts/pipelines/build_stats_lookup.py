import pandas as pd
import numpy as np
import pickle
import os

# Path to races.parquet
RACES_PARQUET_PATH = r"keibaai\data\parsed\parquet\races\races.parquet"
OUTPUT_PATH = r"keibaai\data\processed\stats_lookup.pkl"

def main():
    if not os.path.exists(RACES_PARQUET_PATH):
        print(f"Error: {RACES_PARQUET_PATH} not found.")
        return

    print("Loading races.parquet...")
    df = pd.read_parquet(RACES_PARQUET_PATH)
    
    # Columns: ['finish_time_seconds', 'last_3f_time', 'distance_m', 'track_surface', 'track_condition', 'venue', ...]
    print("Columns loaded.")
    
    # Preprocessing
    print("Preprocessing...")
    
    # Use finish_time_seconds directly
    df['finish_time_sec'] = pd.to_numeric(df['finish_time_seconds'], errors='coerce')
    df['last_3f_float'] = pd.to_numeric(df['last_3f_time'], errors='coerce')

    # Filter valid times and 3F
    df = df.dropna(subset=['finish_time_sec', 'last_3f_float'])
    
    # Grouping Keys: Venue, Distance, Surface, Condition
    # Note: Track Type (Inner/Outer) is not available in current parquet columns.
    # We group by (Venue, Distance, Surface, Condition)
    
    group_cols = ['venue', 'distance_m', 'track_surface', 'track_condition']
    
    # Calculate Mean and Std for Time and 3F
    stats = df.groupby(group_cols).agg({
        'finish_time_sec': ['mean', 'std', 'count'],
        'last_3f_float': ['mean', 'std']
    }).reset_index()
    
    # Flatten MultiIndex columns
    stats.columns = ['venue', 'distance', 'surface', 'condition', 
                     'time_mean', 'time_std', 'count', 
                     'l3f_mean', 'l3f_std']
    
    # Filter groups with enough samples (e.g., count >= 3)
    stats = stats[stats['count'] >= 3]
    
    # Create Lookup Dictionary
    # Key: (Venue, Distance, Surface, Condition) -> (TimeMean, TimeStd, L3FMean, L3FStd)
    stats_lookup = {}
    for _, row in stats.iterrows():
        # Normalize keys
        venue = row['venue']
        dist = int(row['distance'])
        surf = row['surface']
        cond = row['condition']
        
        key = (venue, dist, surf, cond)
        stats_lookup[key] = {
            'time_mean': row['time_mean'],
            'time_std': row['time_std'],
            'l3f_mean': row['l3f_mean'],
            'l3f_std': row['l3f_std']
        }
        
    print(f"Generated stats for {len(stats_lookup)} conditions.")
    
    # Save
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'wb') as f:
        pickle.dump(stats_lookup, f)
    print(f"Saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()

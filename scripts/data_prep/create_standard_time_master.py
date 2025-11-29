import pandas as pd
import numpy as np
import os
from pathlib import Path

def extract_race_class(row):
    """
    Extract race class from race_class column or race_name.
    """
    cls = row['race_class']
    name = str(row['race_name']) if row['race_name'] is not None else ""
    
    # If class is already defined and valid, use it
    if cls is not None and not pd.isna(cls):
        return cls
    
    # Infer from name
    if "1勝" in name:
        return "1勝クラス"
    elif "2勝" in name:
        return "2勝クラス"
    elif "3勝" in name:
        return "3勝クラス"
    elif "未勝利" in name:
        return "未勝利"
    elif "新馬" in name:
        return "新馬"
    elif "G1" in name:
        return "G1"
    elif "G2" in name:
        return "G2"
    elif "G3" in name:
        return "G3"
    elif "OP" in name or "(L)" in name or "オープン" in name:
        return "OP"
    
    return "Unknown"

def main():
    # Paths
    base_dir = Path(__file__).resolve().parents[2]
    races_path = base_dir / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
    output_path = base_dir / "keibaai" / "data" / "master" / "standard_times.parquet"
    
    print(f"Loading races from {races_path}...")
    df = pd.read_parquet(races_path)
    
    # Filter date (Past 3 years + current, e.g., from 2020)
    # Assuming the user wants a robust master, 2020-2023 is good. 
    # But let's use all available data from 2020 onwards to be safe.
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df[df['race_date'] >= '2020-01-01'].copy()
    
    print(f"Data loaded: {len(df)} rows (from 2020-01-01)")
    
    # Fill race_class
    print("Filling missing race_class...")
    df['race_class_filled'] = df.apply(extract_race_class, axis=1)
    
    # Check coverage
    unknown_count = len(df[df['race_class_filled'] == 'Unknown'])
    print(f"Unknown classes after filling: {unknown_count}")
    
    # Grouping
    # We need standard time for each (Venue, Surface, Distance, Class)
    # We also need to handle track_condition? 
    # Usually standard time is for "Good" (良) condition or average of all?
    # Nishida method usually has a base time. 
    # For simplicity and robustness, let's calculate average for "Good" (良) condition primarily,
    # or just average of all but maybe separate by condition?
    # The requirement says "Course/Class average". 
    # Let's group by [venue, track_surface, distance_m, race_class_filled, track_condition] first,
    # then maybe aggregate.
    # But for "Standard Time" (Kijun Time), it's usually for "Ryoba" (Good).
    # Let's filter for track_condition == '良' for the standard time calculation.
    
    df_good = df[df['track_condition'] == '良'].copy()
    print(f"Rows with Good track condition: {len(df_good)}")
    
    group_cols = ['venue', 'track_surface', 'distance_m', 'race_class_filled']
    
    # Calculate stats
    # We only want the winner's time or average of all?
    # "Average winning time" is common for standard time.
    # Let's use the winner's time (finish_position == 1).
    
    df_winners = df_good[df_good['finish_position'] == 1].copy()
    print(f"Winner rows (Good condition): {len(df_winners)}")
    
    agg_funcs = {
        'finish_time_seconds': ['mean', 'std', 'count']
    }
    
    master = df_winners.groupby(group_cols).agg(agg_funcs).reset_index()
    master.columns = ['venue', 'track_surface', 'distance_m', 'race_class', 'std_time_mean', 'std_time_std', 'sample_count']
    
    # Filter low sample size
    # master = master[master['sample_count'] >= 3]
    
    print(f"Master table created: {len(master)} rows")
    print(master.head())
    
    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    master.to_parquet(output_path)
    print(f"Saved to {output_path}")

if __name__ == "__main__":
    main()

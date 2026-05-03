import pandas as pd
import numpy as np
import pickle
import os
from datetime import datetime

# Path to races.parquet
RACES_PARQUET_PATH = r"keibaai\data\parsed\parquet\races\races.parquet"
OUTPUT_PATH = r"keibaai\data\processed\stats_lookup.pkl"

ROLLING_YEARS = 3
MIN_SAMPLES = 10  # v1は3 → v2は10に引上げ


def main():
    if not os.path.exists(RACES_PARQUET_PATH):
        print(f"Error: {RACES_PARQUET_PATH} not found.")
        return

    print("Loading races.parquet...")
    df = pd.read_parquet(RACES_PARQUET_PATH)
    
    # Preprocessing
    print("Preprocessing...")
    df['finish_time_sec'] = pd.to_numeric(df['finish_time_seconds'], errors='coerce')
    df['last_3f_float'] = pd.to_numeric(df['last_3f_time'], errors='coerce')
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df.dropna(subset=['finish_time_sec', 'last_3f_float'])
    df = df[df['track_surface'].isin(['芝', 'ダート'])]
    
    # Split recent vs all
    cutoff = pd.Timestamp.now() - pd.DateOffset(years=ROLLING_YEARS)
    df_recent = df[df['race_date'] >= cutoff]
    print(f"  全期間: {len(df):,}, 直近{ROLLING_YEARS}年: {len(df_recent):,}")
    
    group_cols = ['venue', 'distance_m', 'track_surface', 'track_condition']
    
    # 全期間統計 (フォールバック)
    all_stats = df.groupby(group_cols).agg({
        'finish_time_sec': ['mean', 'std', 'count'],
        'last_3f_float': ['mean', 'std']
    })
    all_stats.columns = ['time_mean', 'time_std', 'count', 'l3f_mean', 'l3f_std']
    all_stats = all_stats.reset_index()
    all_stats = all_stats[all_stats['count'] >= MIN_SAMPLES]
    
    # 直近N年統計
    recent_stats = df_recent.groupby(group_cols).agg({
        'finish_time_sec': ['mean', 'std', 'count'],
        'last_3f_float': ['mean', 'std']
    })
    recent_stats.columns = ['time_mean', 'time_std', 'count', 'l3f_mean', 'l3f_std']
    recent_stats = recent_stats.reset_index()
    recent_stats = recent_stats[recent_stats['count'] >= MIN_SAMPLES]
    
    # Build lookup: 直近N年優先、全期間フォールバック
    stats_lookup = {}
    
    # まず全期間
    for _, row in all_stats.iterrows():
        key = (row['venue'], int(row['distance_m']), row['track_surface'], row['track_condition'])
        stats_lookup[key] = {
            'time_mean': row['time_mean'],
            'time_std': max(row['time_std'], 0.5),
            'l3f_mean': row['l3f_mean'],
            'l3f_std': max(row['l3f_std'], 0.3),
            'source': 'all_time'
        }
    
    # 直近N年で上書き
    recent_count = 0
    for _, row in recent_stats.iterrows():
        key = (row['venue'], int(row['distance_m']), row['track_surface'], row['track_condition'])
        stats_lookup[key] = {
            'time_mean': row['time_mean'],
            'time_std': max(row['time_std'], 0.5),
            'l3f_mean': row['l3f_mean'],
            'l3f_std': max(row['l3f_std'], 0.3),
            'source': f'recent_{ROLLING_YEARS}y'
        }
        recent_count += 1
    
    print(f"Generated stats: {len(stats_lookup)} conditions "
          f"(直近{ROLLING_YEARS}年: {recent_count}, 全期間FB: {len(stats_lookup) - recent_count})")
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'wb') as f:
        pickle.dump(stats_lookup, f)
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
グローバル統計と競馬場補正値を構築するスクリプト

目的:
- 競馬場を含まないグローバル統計（距離×コース×馬場状態）を構築
- 各競馬場のコース補正値（グローバル平均との差分）を算出

出力:
- global_stats_lookup.pkl: グローバル統計
- venue_adjustment.pkl: 競馬場補正値
"""

import pandas as pd
import numpy as np
import pickle
import os
from pathlib import Path

# Paths
RACES_PARQUET_PATH = Path(r"keibaai\data\parsed\parquet\races\races.parquet")
OUTPUT_DIR = Path(r"keibaai\data\processed")
GLOBAL_STATS_PATH = OUTPUT_DIR / "global_stats_lookup.pkl"
VENUE_ADJUSTMENT_PATH = OUTPUT_DIR / "venue_adjustment.pkl"


def main():
    print("=" * 60)
    print("グローバル統計・競馬場補正値構築")
    print("=" * 60)

    if not RACES_PARQUET_PATH.exists():
        print(f"Error: {RACES_PARQUET_PATH} not found.")
        return

    # Load data
    print("\n1. Loading races.parquet...")
    df = pd.read_parquet(RACES_PARQUET_PATH)
    print(f"   Loaded {len(df):,} rows")

    # Preprocessing
    print("\n2. Preprocessing...")
    df['finish_time_sec'] = pd.to_numeric(df['finish_time_seconds'], errors='coerce')
    df['last_3f_float'] = pd.to_numeric(df['last_3f_time'], errors='coerce')

    # Filter: only valid times and top 3 finishers for mean calculation
    df_valid = df.dropna(subset=['finish_time_sec', 'last_3f_float'])
    df_top3 = df_valid[df_valid['finish_position'].isin([1, 2, 3])].copy()
    print(f"   Valid rows (with time): {len(df_valid):,}")
    print(f"   Top 3 finishers: {len(df_top3):,}")

    # =========================================================================
    # Part A: Global Stats (without venue)
    # =========================================================================
    print("\n3. Building Global Stats (distance × surface × condition)...")
    
    global_group_cols = ['distance_m', 'track_surface', 'track_condition']
    global_stats_df = df_top3.groupby(global_group_cols).agg({
        'finish_time_sec': ['mean', 'std', 'count'],
        'last_3f_float': ['mean', 'std']
    }).reset_index()

    global_stats_df.columns = ['distance', 'surface', 'condition',
                               'time_mean', 'time_std', 'count',
                               'l3f_mean', 'l3f_std']

    # Filter: need at least 10 samples for reliable stats
    global_stats_df = global_stats_df[global_stats_df['count'] >= 10]
    print(f"   Generated {len(global_stats_df)} global conditions (count >= 10)")

    # Build lookup dict
    global_stats_lookup = {}
    for _, row in global_stats_df.iterrows():
        key = (int(row['distance']), row['surface'], row['condition'])
        global_stats_lookup[key] = {
            'time_mean': row['time_mean'],
            'time_std': max(row['time_std'], 0.5),  # Minimum std to avoid division issues
            'l3f_mean': row['l3f_mean'],
            'l3f_std': max(row['l3f_std'], 0.3),
            'count': int(row['count'])
        }

    # =========================================================================
    # Part B: Venue Adjustment (venue-specific mean vs global mean)
    # =========================================================================
    print("\n4. Building Venue Adjustment Table...")
    
    venue_group_cols = ['venue', 'distance_m', 'track_surface', 'track_condition']
    venue_stats_df = df_top3.groupby(venue_group_cols).agg({
        'finish_time_sec': ['mean', 'count'],
        'last_3f_float': ['mean']
    }).reset_index()

    venue_stats_df.columns = ['venue', 'distance', 'surface', 'condition',
                              'venue_time_mean', 'count', 'venue_l3f_mean']

    # Filter: need at least 5 samples
    venue_stats_df = venue_stats_df[venue_stats_df['count'] >= 5]
    print(f"   Generated {len(venue_stats_df)} venue-specific conditions (count >= 5)")

    # Calculate adjustment = venue_mean - global_mean
    # Positive adjustment means this venue is slower -> need to subtract from time
    
    venue_adjustment = {}
    
    for _, row in venue_stats_df.iterrows():
        venue = row['venue']
        dist = int(row['distance'])
        surf = row['surface']
        cond = row['condition']
        
        global_key = (dist, surf, cond)
        venue_key = (venue, dist, surf, cond)
        
        if global_key in global_stats_lookup:
            # Time Adjustment
            global_mean = global_stats_lookup[global_key]['time_mean']
            venue_mean = row['venue_time_mean']
            adjustment = venue_mean - global_mean
            
            # L3F Adjustment
            global_l3f = global_stats_lookup[global_key]['l3f_mean']
            venue_l3f = row['venue_l3f_mean']
            l3f_adjustment = venue_l3f - global_l3f
            
            venue_adjustment[venue_key] = {
                'time_adjustment': adjustment,
                'l3f_adjustment': l3f_adjustment,
                'venue_time_mean': venue_mean,
                'global_time_mean': global_mean,
                'count': int(row['count'])
            }
            
            # Also create a simplified key (venue, distance, surface) for fallback
            simple_key = (venue, dist, surf)
            if simple_key not in venue_adjustment:
                venue_adjustment[simple_key] = {
                    'time_adjustment': adjustment,
                    'l3f_adjustment': l3f_adjustment,
                    'venue_time_mean': venue_mean,
                    'global_time_mean': global_mean,
                    'count': int(row['count']),
                    'default_condition': cond
                }

    # =========================================================================
    # Part C: Summary Statistics
    # =========================================================================
    print("\n5. Summary of Venue Adjustments:")
    print("-" * 50)
    
    # Aggregate by venue for overview
    venue_summary = {}
    for key, data in venue_adjustment.items():
        if len(key) == 4:  # Full key with condition
            venue = key[0]
            if venue not in venue_summary:
                venue_summary[venue] = []
            venue_summary[venue].append(data['time_adjustment'])
    
    print(f"{'競馬場':<6} {'平均補正':<10} {'最小':<8} {'最大':<8}")
    print("-" * 40)
    for venue in ['札幌', '函館', '福島', '新潟', '東京', '中山', '中京', '京都', '阪神', '小倉']:
        if venue in venue_summary:
            adjs = venue_summary[venue]
            avg = np.mean(adjs)
            min_adj = np.min(adjs)
            max_adj = np.max(adjs)
            print(f"{venue:<6} {avg:>+8.2f}s {min_adj:>+6.2f}s {max_adj:>+6.2f}s")

    # =========================================================================
    # Part D: Save Outputs
    # =========================================================================
    print("\n6. Saving outputs...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(GLOBAL_STATS_PATH, 'wb') as f:
        pickle.dump(global_stats_lookup, f)
    print(f"   Saved: {GLOBAL_STATS_PATH}")

    with open(VENUE_ADJUSTMENT_PATH, 'wb') as f:
        pickle.dump(venue_adjustment, f)
    print(f"   Saved: {VENUE_ADJUSTMENT_PATH}")

    # =========================================================================
    # Part E: Validation
    # =========================================================================
    print("\n7. Validation Examples:")
    print("-" * 60)
    
    # Pick a common condition for validation
    test_conditions = [
        (1600, '芝', '良'),
        (2000, '芝', '良'),
        (1800, 'ダート', '良'),
    ]
    
    for dist, surf, cond in test_conditions:
        global_key = (dist, surf, cond)
        if global_key in global_stats_lookup:
            gm = global_stats_lookup[global_key]['time_mean']
            gs = global_stats_lookup[global_key]['time_std']
            print(f"\n{surf}{dist}m ({cond}): グローバル平均={gm:.2f}秒, 標準偏差={gs:.2f}")
            
            for venue in ['東京', '阪神', '中山', '京都']:
                venue_key = (venue, dist, surf, cond)
                if venue_key in venue_adjustment:
                    adj = venue_adjustment[venue_key]['time_adjustment']
                    vmean = venue_adjustment[venue_key]['venue_time_mean']
                    print(f"  {venue}: 場別平均={vmean:.2f}秒, 補正={adj:+.2f}秒")

    print("\n" + "=" * 60)
    print("完了！")
    print("=" * 60)


if __name__ == "__main__":
    main()

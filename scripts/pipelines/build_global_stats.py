#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
グローバル統計と競馬場補正値・季節補正値を構築するスクリプト (v2)

改善点 (v1からの変更):
- 直近3年のデータを主統計として使用（高速化トレンドに追従）
- 全期間統計をフォールバック用に保持
- 季節補正テーブル（月別）を新規生成
- 最小サンプル数を引き上げ

出力:
- global_stats_lookup.pkl: グローバル統計（直近3年ベース、フォールバック付き）
- venue_adjustment.pkl: 競馬場補正値（直近3年ベース、フォールバック付き）
- seasonal_adjustment.pkl: 季節補正テーブル（新規）
"""

import pandas as pd
import numpy as np
import pickle
import os
from pathlib import Path
from datetime import datetime

# Paths
RACES_PARQUET_PATH = Path(r"keibaai\data\parsed\parquet\races\races.parquet")
OUTPUT_DIR = Path(r"keibaai\data\processed")
GLOBAL_STATS_PATH = OUTPUT_DIR / "global_stats_lookup.pkl"
VENUE_ADJUSTMENT_PATH = OUTPUT_DIR / "venue_adjustment.pkl"
SEASONAL_ADJUSTMENT_PATH = OUTPUT_DIR / "seasonal_adjustment.pkl"

ROLLING_YEARS = 3  # ローリング統計の期間
MIN_SAMPLES_GLOBAL = 10
MIN_SAMPLES_VENUE = 5
MIN_SAMPLES_SEASONAL = 10
SHRINKAGE_N = 30  # ベイズ縮小の基準サンプル数 (N<30で補正値を縮小)
MAX_ADJUSTMENT = 3.0  # 補正値の上限(秒)


def main():
    print("=" * 60)
    print("グローバル統計・競馬場補正値・季節補正値構築 (v2)")
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
    df['race_date'] = pd.to_datetime(df['race_date'])
    df['month'] = df['race_date'].dt.month

    df_valid = df.dropna(subset=['finish_time_sec', 'last_3f_float'])
    df_valid = df_valid[df_valid['track_surface'].isin(['芝', 'ダート'])]
    df_top3 = df_valid[df_valid['finish_position'].isin([1, 2, 3])].copy()
    print(f"   Valid rows (with time): {len(df_valid):,}")
    print(f"   Top 3 finishers: {len(df_top3):,}")

    # Split: recent vs all
    cutoff = pd.Timestamp.now() - pd.DateOffset(years=ROLLING_YEARS)
    df_recent = df_top3[df_top3['race_date'] >= cutoff].copy()
    print(f"   直近{ROLLING_YEARS}年データ: {len(df_recent):,} (cutoff: {cutoff.strftime('%Y-%m-%d')})")

    # =========================================================================
    # Part A: Global Stats
    # =========================================================================
    print(f"\n3. Building Global Stats (直近{ROLLING_YEARS}年 + 全期間フォールバック)...")
    
    global_stats_lookup = _build_global_stats(df_recent, df_top3)
    print(f"   Generated {len(global_stats_lookup)} global conditions")

    # =========================================================================
    # Part B: Venue Adjustment
    # =========================================================================
    print(f"\n4. Building Venue Adjustment (直近{ROLLING_YEARS}年 + 全期間フォールバック)...")
    
    venue_adjustment = _build_venue_adjustment(df_recent, df_top3, global_stats_lookup)
    print(f"   Generated {len(venue_adjustment)} venue adjustments")

    # =========================================================================
    # Part C: Seasonal Adjustment (NEW)
    # =========================================================================
    print("\n5. Building Seasonal Adjustment Table...")
    
    seasonal_adjustment = _build_seasonal_adjustment(df_top3)
    print(f"   Generated {len(seasonal_adjustment)} seasonal adjustments")

    # =========================================================================
    # Part D: Summary
    # =========================================================================
    _print_summary(global_stats_lookup, venue_adjustment, seasonal_adjustment)

    # =========================================================================
    # Part E: Save
    # =========================================================================
    print("\n7. Saving outputs...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(GLOBAL_STATS_PATH, 'wb') as f:
        pickle.dump(global_stats_lookup, f)
    print(f"   Saved: {GLOBAL_STATS_PATH}")

    with open(VENUE_ADJUSTMENT_PATH, 'wb') as f:
        pickle.dump(venue_adjustment, f)
    print(f"   Saved: {VENUE_ADJUSTMENT_PATH}")

    with open(SEASONAL_ADJUSTMENT_PATH, 'wb') as f:
        pickle.dump(seasonal_adjustment, f)
    print(f"   Saved: {SEASONAL_ADJUSTMENT_PATH} (NEW)")

    # Validation
    _validate(global_stats_lookup, venue_adjustment, seasonal_adjustment)

    print("\n" + "=" * 60)
    print("完了！")
    print("=" * 60)


def _build_global_stats(df_recent, df_all):
    """直近N年ベースでグローバル統計を構築。サンプル不足時は全期間にフォールバック。"""
    global_group_cols = ['distance_m', 'track_surface', 'track_condition']
    
    # 直近N年の統計
    recent_stats = df_recent.groupby(global_group_cols).agg(
        time_mean=('finish_time_sec', 'mean'),
        time_std=('finish_time_sec', 'std'),
        count=('finish_time_sec', 'size'),
        l3f_mean=('last_3f_float', 'mean'),
        l3f_std=('last_3f_float', 'std')
    ).reset_index()
    
    # 全期間の統計 (フォールバック用)
    all_stats = df_all.groupby(global_group_cols).agg(
        time_mean=('finish_time_sec', 'mean'),
        time_std=('finish_time_sec', 'std'),
        count=('finish_time_sec', 'size'),
        l3f_mean=('last_3f_float', 'mean'),
        l3f_std=('last_3f_float', 'std')
    ).reset_index()
    
    lookup = {}
    
    # まず全期間のデータを入れる（フォールバック）
    for _, row in all_stats.iterrows():
        if row['count'] < MIN_SAMPLES_GLOBAL:
            continue
        key = (int(row['distance_m']), row['track_surface'], row['track_condition'])
        lookup[key] = {
            'time_mean': row['time_mean'],
            'time_std': max(row['time_std'], 0.5),
            'l3f_mean': row['l3f_mean'],
            'l3f_std': max(row['l3f_std'], 0.3),
            'count': int(row['count']),
            'source': 'all_time'
        }
    
    # 直近N年のデータで上書き（十分なサンプルがある場合）
    recent_count = 0
    for _, row in recent_stats.iterrows():
        if row['count'] < MIN_SAMPLES_GLOBAL:
            continue
        key = (int(row['distance_m']), row['track_surface'], row['track_condition'])
        lookup[key] = {
            'time_mean': row['time_mean'],
            'time_std': max(row['time_std'], 0.5),
            'l3f_mean': row['l3f_mean'],
            'l3f_std': max(row['l3f_std'], 0.3),
            'count': int(row['count']),
            'source': f'recent_{ROLLING_YEARS}y'
        }
        recent_count += 1
    
    print(f"   直近{ROLLING_YEARS}年ベース: {recent_count}, 全期間フォールバック: {len(lookup) - recent_count}")
    return lookup


def _build_venue_adjustment(df_recent, df_all, global_stats_lookup):
    """直近N年ベースで競馬場補正値を構築。"""
    venue_group_cols = ['venue', 'distance_m', 'track_surface', 'track_condition']
    
    # 直近N年の統計
    recent_venue = df_recent.groupby(venue_group_cols).agg(
        venue_time_mean=('finish_time_sec', 'mean'),
        count=('finish_time_sec', 'size'),
        venue_l3f_mean=('last_3f_float', 'mean')
    ).reset_index()
    
    # 全期間の統計 (フォールバック)
    all_venue = df_all.groupby(venue_group_cols).agg(
        venue_time_mean=('finish_time_sec', 'mean'),
        count=('finish_time_sec', 'size'),
        venue_l3f_mean=('last_3f_float', 'mean')
    ).reset_index()
    
    venue_adjustment = {}
    
    # 使用するデータを選択: 直近N年優先、不足時は全期間
    for source_df, source_name in [(all_venue, 'all_time'), (recent_venue, f'recent_{ROLLING_YEARS}y')]:
        for _, row in source_df.iterrows():
            if row['count'] < MIN_SAMPLES_VENUE:
                continue
            
            venue = row['venue']
            dist = int(row['distance_m'])
            surf = row['track_surface']
            cond = row['track_condition']
            
            global_key = (dist, surf, cond)
            venue_key = (venue, dist, surf, cond)
            
            if global_key in global_stats_lookup:
                global_mean = global_stats_lookup[global_key]['time_mean']
                venue_mean = row['venue_time_mean']
                raw_adjustment = venue_mean - global_mean
                
                global_l3f = global_stats_lookup[global_key]['l3f_mean']
                venue_l3f = row['venue_l3f_mean']
                raw_l3f_adj = venue_l3f - global_l3f
                
                # ベイズ縮小: 小サンプルの補正を自動抑制
                shrinkage = min(1.0, row['count'] / SHRINKAGE_N)
                adjustment = np.clip(raw_adjustment * shrinkage, -MAX_ADJUSTMENT, MAX_ADJUSTMENT)
                l3f_adjustment = np.clip(raw_l3f_adj * shrinkage, -MAX_ADJUSTMENT, MAX_ADJUSTMENT)
                
                venue_adjustment[venue_key] = {
                    'time_adjustment': adjustment,
                    'l3f_adjustment': l3f_adjustment,
                    'venue_time_mean': venue_mean,
                    'global_time_mean': global_mean,
                    'count': int(row['count']),
                    'source': source_name
                }
                
                # Simplified key fallback
                simple_key = (venue, dist, surf)
                if simple_key not in venue_adjustment:
                    venue_adjustment[simple_key] = {
                        'time_adjustment': adjustment,
                        'l3f_adjustment': l3f_adjustment,
                        'venue_time_mean': venue_mean,
                        'global_time_mean': global_mean,
                        'count': int(row['count']),
                        'default_condition': cond,
                        'source': source_name
                    }
    
    return venue_adjustment


def _build_seasonal_adjustment(df_top3):
    """
    季節補正テーブルを構築。
    各(venue, distance, surface, month) に対して、その月の平均と全期間平均の差分を計算。
    """
    group_cols = ['venue', 'distance_m', 'track_surface']
    
    # 全期間平均
    overall = df_top3.groupby(group_cols).agg(
        overall_time_mean=('finish_time_sec', 'mean'),
        overall_l3f_mean=('last_3f_float', 'mean')
    ).reset_index()
    
    # 月別平均
    monthly = df_top3.groupby(group_cols + ['month']).agg(
        month_time_mean=('finish_time_sec', 'mean'),
        month_l3f_mean=('last_3f_float', 'mean'),
        count=('finish_time_sec', 'size')
    ).reset_index()
    
    # マージ
    merged = monthly.merge(overall, on=group_cols, how='left')
    merged = merged[merged['count'] >= MIN_SAMPLES_SEASONAL]
    
    seasonal_adjustment = {}
    for _, row in merged.iterrows():
        key = (row['venue'], int(row['distance_m']), row['track_surface'], int(row['month']))
        raw_time_adj = row['month_time_mean'] - row['overall_time_mean']
        raw_l3f_adj = row['month_l3f_mean'] - row['overall_l3f_mean']
        
        # ベイズ縮小: N<30のとき補正値を縮小（小サンプルの極端な補正を抑制）
        shrinkage = min(1.0, row['count'] / SHRINKAGE_N)
        time_adj = np.clip(raw_time_adj * shrinkage, -MAX_ADJUSTMENT, MAX_ADJUSTMENT)
        l3f_adj = np.clip(raw_l3f_adj * shrinkage, -MAX_ADJUSTMENT, MAX_ADJUSTMENT)
        
        seasonal_adjustment[key] = {
            'time_adj': time_adj,
            'l3f_adj': l3f_adj,
            'raw_time_adj': raw_time_adj,
            'month_time_mean': row['month_time_mean'],
            'overall_time_mean': row['overall_time_mean'],
            'count': int(row['count']),
            'shrinkage': round(shrinkage, 2)
        }
    
    return seasonal_adjustment


def _print_summary(global_stats, venue_adj, seasonal_adj):
    """サマリー表示"""
    print("\n6. Summary:")
    print("-" * 50)
    
    # Venue adjustment summary
    venue_summary = {}
    for key, data in venue_adj.items():
        if len(key) == 4:
            venue = key[0]
            if venue not in venue_summary:
                venue_summary[venue] = []
            venue_summary[venue].append(data['time_adjustment'])
    
    print(f"{'競馬場':<6} {'平均補正':<10} {'最小':<8} {'最大':<8}")
    print("-" * 40)
    for venue in ['札幌', '函館', '福島', '新潟', '東京', '中山', '中京', '京都', '阪神', '小倉']:
        if venue in venue_summary:
            adjs = venue_summary[venue]
            print(f"{venue:<6} {np.mean(adjs):>+8.2f}s {np.min(adjs):>+6.2f}s {np.max(adjs):>+6.2f}s")
    
    # Seasonal summary (example: 芝1600m)
    print(f"\n   季節補正サンプル (中山 芝2000m):")
    for month in range(1, 13):
        key = ('中山', 2000, '芝', month)
        if key in seasonal_adj:
            adj = seasonal_adj[key]
            print(f"   {month:>2}月: 補正={adj['time_adj']:+.2f}秒 "
                  f"(月平均={adj['month_time_mean']:.2f}, 全体={adj['overall_time_mean']:.2f}, N={adj['count']})")
        else:
            print(f"   {month:>2}月: データ不足")


def _validate(global_stats, venue_adj, seasonal_adj):
    """基本検証"""
    print("\n8. Validation:")
    print("-" * 60)
    
    test_conditions = [
        (1600, '芝', '良'),
        (2000, '芝', '良'),
        (1800, 'ダート', '良'),
    ]
    
    for dist, surf, cond in test_conditions:
        global_key = (dist, surf, cond)
        if global_key in global_stats:
            gs = global_stats[global_key]
            print(f"\n{surf}{dist}m ({cond}): 平均={gs['time_mean']:.2f}秒, σ={gs['time_std']:.2f}, "
                  f"source={gs['source']}")
            
            for venue in ['東京', '阪神', '中山', '京都']:
                venue_key = (venue, dist, surf, cond)
                if venue_key in venue_adj:
                    adj = venue_adj[venue_key]
                    print(f"  {venue}: 場別平均={adj['venue_time_mean']:.2f}秒, "
                          f"補正={adj['time_adjustment']:+.2f}秒, source={adj['source']}")


if __name__ == "__main__":
    main()

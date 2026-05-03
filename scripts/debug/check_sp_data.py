#!/usr/bin/env python3
"""SP値計算に必要な既存データの構造確認スクリプト"""
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

def check():
    print("=" * 60)
    print("SP値計算用データ構造確認")
    print("=" * 60)

    # 1. race_details
    rd_path = PROJECT_ROOT / 'keibaai/data/parsed/parquet/race_details/race_details.parquet'
    if rd_path.exists():
        rd = pd.read_parquet(rd_path)
        print(f"\n[race_details] Shape: {rd.shape}")
        print(f"  Columns: {rd.columns.tolist()}")
        print(f"  first_half non-null: {rd['first_half'].notna().sum()}")
        print(f"  second_half non-null: {rd['second_half'].notna().sum()}")
        if 'lap_times' in rd.columns:
            print(f"  lap_times non-null: {rd['lap_times'].notna().sum()}")
            print(f"  lap_times sample: {rd['lap_times'].dropna().iloc[0] if rd['lap_times'].notna().any() else 'N/A'}")
        print(f"  race_id sample: {rd['race_id'].iloc[0]}")
    else:
        print(f"\n[race_details] NOT FOUND: {rd_path}")

    # 2. races
    races_path = PROJECT_ROOT / 'keibaai/data/parsed/parquet/races/races.parquet'
    if races_path.exists():
        races = pd.read_parquet(races_path)
        print(f"\n[races] Shape: {races.shape}")
        print(f"  Columns: {races.columns.tolist()}")
        print(f"  finish_time_seconds non-null: {races['finish_time_seconds'].notna().sum()}")
        print(f"  track_condition unique: {races['track_condition'].dropna().unique().tolist()}")
        print(f"  venue unique: {races['venue'].dropna().unique().tolist()}")
        print(f"  track_surface unique: {races['track_surface'].dropna().unique().tolist()}")
        print(f"  distance_m range: {races['distance_m'].min()} - {races['distance_m'].max()}")
        print(f"  race_date range: {races['race_date'].min()} - {races['race_date'].max()}")
        # 良馬場の勝ち馬サンプル数
        good_winners = races[(races['finish_position'] == 1) & (races['track_condition'] == '良') & (races['finish_time_seconds'].notna())]
        print(f"  良馬場勝ち馬(基準タイム用): {len(good_winners):,}件")
    else:
        print(f"\n[races] NOT FOUND: {races_path}")

    # 3. standard_times
    st_path = PROJECT_ROOT / 'keibaai/data/master/standard_times.parquet'
    if st_path.exists():
        st = pd.read_parquet(st_path)
        print(f"\n[standard_times] Shape: {st.shape}")
        print(f"  Columns: {st.columns.tolist()}")
        print(f"  Sample:\n{st.head(3).to_string()}")
    else:
        print(f"\n[standard_times] NOT FOUND: {st_path}")

    # 4. corner_positions
    cp_path = PROJECT_ROOT / 'keibaai/data/parsed/parquet/corners/corner_positions.parquet'
    if cp_path.exists():
        cp = pd.read_parquet(cp_path)
        print(f"\n[corner_positions] Shape: {cp.shape}")
        print(f"  Columns: {cp.columns.tolist()}")
    else:
        print(f"\n[corner_positions] NOT FOUND: {cp_path}")

    print("\n" + "=" * 60)
    print("確認完了")

if __name__ == "__main__":
    check()

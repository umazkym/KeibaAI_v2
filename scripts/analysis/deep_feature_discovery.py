"""
Deep Feature Discovery Analysis
未使用データソースとROI向上の可能性を探索
"""

import pandas as pd
import numpy as np
from pathlib import Path

def main():
    print("=" * 70)
    print("DEEP FEATURE DISCOVERY ANALYSIS")
    print("=" * 70)
    
    # Load all data
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    returns = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[races['finish_position'].notna()].copy()
    
    # Train period for analysis
    train = races[races['race_date'] <= '2024-12-31'].copy()
    train['is_win'] = (train['finish_position'] == 1).astype(int)
    
    print(f"\nTrain records: {len(train):,}")
    
    # =========================================================================
    # 1. UNUSED COLUMNS IN races.parquet
    # =========================================================================
    print("\n" + "=" * 70)
    print("1. UNUSED COLUMNS IN races.parquet")
    print("=" * 70)
    
    # Weather analysis
    print("\n--- weather ---")
    print(train['weather'].value_counts())
    
    # Weather x Surface x Win rate
    print("\n--- weather x surface x win rate ---")
    weather_stats = train.groupby(['weather', 'track_surface']).agg({
        'is_win': ['sum', 'count'],
        'win_odds': lambda x: x[train.loc[x.index, 'is_win'] == 1].sum() / len(x) * 100
    })
    print(weather_stats)
    
    # =========================================================================
    # 2. BREEDER/PRODUCING AREA ANALYSIS
    # =========================================================================
    print("\n" + "=" * 70)
    print("2. BREEDER/PRODUCING AREA ANALYSIS")
    print("=" * 70)
    
    # Merge horses data
    train_h = train.merge(horses[['horse_id', 'breeder_name', 'producing_area']], on='horse_id', how='left')
    
    # Top breeders by ROI
    print("\n--- Top Breeders by ROI (min 500 starts) ---")
    breeder_stats = train_h.groupby('breeder_name').agg({
        'is_win': ['sum', 'count', 'mean'],
        'win_odds': lambda x: x[train_h.loc[x.index, 'is_win'] == 1].sum()
    }).reset_index()
    breeder_stats.columns = ['breeder', 'wins', 'starts', 'win_rate', 'odds_sum']
    breeder_stats['roi'] = breeder_stats['odds_sum'] / breeder_stats['starts'] * 100
    breeder_stats = breeder_stats[breeder_stats['starts'] >= 500].sort_values('roi', ascending=False)
    print(breeder_stats.head(15).to_string(index=False))
    
    # Top producing areas by ROI
    print("\n--- Top Producing Areas by ROI (min 1000 starts) ---")
    area_stats = train_h.groupby('producing_area').agg({
        'is_win': ['sum', 'count', 'mean'],
        'win_odds': lambda x: x[train_h.loc[x.index, 'is_win'] == 1].sum()
    }).reset_index()
    area_stats.columns = ['area', 'wins', 'starts', 'win_rate', 'odds_sum']
    area_stats['roi'] = area_stats['odds_sum'] / area_stats['starts'] * 100
    area_stats = area_stats[area_stats['starts'] >= 1000].sort_values('roi', ascending=False)
    print(area_stats.to_string(index=False))
    
    # =========================================================================
    # 3. DAMSIRE (母父) ANALYSIS
    # =========================================================================
    print("\n" + "=" * 70)
    print("3. DAMSIRE (母父) ANALYSIS")
    print("=" * 70)
    
    # Generation 1 has sire (父) and dam (母)
    # Generation 2 position 3 is typically damsire (母父)
    # Need to find the correct ancestor
    gen1 = pedigrees[pedigrees['generation'] == 1].copy()
    
    # Get horse count per generation 1
    horses_gen1 = gen1.groupby('horse_id').agg(list).reset_index()
    
    # The second ancestor in gen1 is usually dam, and we need gen2 ancestors for damsire
    # For now, let's analyze existing sire data vs damsire
    
    # Get all gen2 ancestors (4 per horse typically)
    gen2 = pedigrees[pedigrees['generation'] == 2].copy()
    
    # Track how many ancestors per horse in gen2
    gen2_count = gen2.groupby('horse_id').size()
    
    # Create damsire mapping (assuming 3rd ancestor in gen2 is damsire)
    # This needs verification but let's check the data pattern
    gen2_sorted = gen2.sort_values(['horse_id', 'ancestor_id'])
    gen2_sorted['pos'] = gen2_sorted.groupby('horse_id').cumcount()
    
    # Position 2 (0-indexed) should be damsire in standard pedigree order
    damsire = gen2_sorted[gen2_sorted['pos'] == 2][['horse_id', 'ancestor_id', 'ancestor_name']].copy()
    damsire.columns = ['horse_id', 'damsire_id', 'damsire_name']
    
    print(f"\nDamsire mapping: {len(damsire):,} horses")
    
    # Merge with train data
    train_ds = train.merge(damsire, on='horse_id', how='left')
    
    # Top damsires by ROI
    print("\n--- Top Damsires by ROI (min 500 starts) ---")
    ds_stats = train_ds.groupby('damsire_name').agg({
        'is_win': ['sum', 'count', 'mean'],
        'win_odds': lambda x: x[train_ds.loc[x.index, 'is_win'] == 1].sum()
    }).reset_index()
    ds_stats.columns = ['damsire', 'wins', 'starts', 'win_rate', 'odds_sum']
    ds_stats['roi'] = ds_stats['odds_sum'] / ds_stats['starts'] * 100
    ds_stats = ds_stats[ds_stats['starts'] >= 500].sort_values('roi', ascending=False)
    print(ds_stats.head(20).to_string(index=False))
    
    # =========================================================================
    # 4. LAP TIME PATTERN ANALYSIS
    # =========================================================================
    print("\n" + "=" * 70)
    print("4. LAP TIME / PACE PATTERN ANALYSIS")
    print("=" * 70)
    
    # Merge race_details with train
    train_rd = train.merge(race_details[['race_id', 'first_half', 'second_half']], on='race_id', how='left')
    train_rd['first_half'] = pd.to_numeric(train_rd['first_half'], errors='coerce')
    train_rd['second_half'] = pd.to_numeric(train_rd['second_half'], errors='coerce')
    train_rd['pace_diff'] = train_rd['second_half'] - train_rd['first_half']
    
    # Pace categories
    train_rd['pace_type'] = pd.cut(train_rd['pace_diff'], 
                                    bins=[-np.inf, -2, 0, 2, np.inf],
                                    labels=['H-Pace', 'M-Pace', 'M-Slow', 'S-Pace'])
    
    print("\n--- Pace Type x Position x ROI ---")
    # Winner ROI by pace type and running style
    # First, get C4 position to determine running style
    c4 = corners[corners['corner'] == 4][['race_id', 'horse_number', 'position']].copy()
    c4.columns = ['race_id', 'horse_number', 'c4_pos']
    
    train_pace = train_rd.merge(c4, on=['race_id', 'horse_number'], how='left')
    
    # Categorize running style
    train_pace['style'] = pd.cut(train_pace['c4_pos'], 
                                  bins=[0, 2, 5, 10, np.inf],
                                  labels=['Front', 'Mid-Front', 'Mid-Back', 'Back'])
    
    # ROI by pace type x running style
    pace_style = train_pace.groupby(['pace_type', 'style']).agg({
        'is_win': ['sum', 'count'],
        'win_odds': lambda x: x[train_pace.loc[x.index, 'is_win'] == 1].sum()
    }).reset_index()
    pace_style.columns = ['pace_type', 'style', 'wins', 'starts', 'odds_sum']
    pace_style['roi'] = pace_style['odds_sum'] / pace_style['starts'] * 100
    pace_style['win_rate'] = pace_style['wins'] / pace_style['starts'] * 100
    print(pace_style.to_string(index=False))
    
    # =========================================================================
    # 5. OWNER ANALYSIS
    # =========================================================================
    print("\n" + "=" * 70)
    print("5. OWNER (馬主) ANALYSIS")
    print("=" * 70)
    
    # Top owners by ROI
    print("\n--- Top Owners by ROI (min 300 starts) ---")
    owner_stats = train.groupby('owner_name').agg({
        'is_win': ['sum', 'count', 'mean'],
        'win_odds': lambda x: x[train.loc[x.index, 'is_win'] == 1].sum()
    }).reset_index()
    owner_stats.columns = ['owner', 'wins', 'starts', 'win_rate', 'odds_sum']
    owner_stats['roi'] = owner_stats['odds_sum'] / owner_stats['starts'] * 100
    owner_stats = owner_stats[owner_stats['starts'] >= 300].sort_values('roi', ascending=False)
    print(owner_stats.head(20).to_string(index=False))
    
    # =========================================================================
    # 6. WEIGHT CHANGE PATTERNS
    # =========================================================================
    print("\n" + "=" * 70)
    print("6. WEIGHT CHANGE PATTERNS")
    print("=" * 70)
    
    train['weight_change'] = pd.to_numeric(train['horse_weight_change'], errors='coerce')
    train['weight_cat'] = pd.cut(train['weight_change'], 
                                  bins=[-np.inf, -10, -4, 4, 10, np.inf],
                                  labels=['Big Decrease', 'Small Decrease', 'Stable', 'Small Increase', 'Big Increase'])
    
    print("\n--- Weight Change x ROI ---")
    weight_stats = train.groupby('weight_cat').agg({
        'is_win': ['sum', 'count'],
        'win_odds': lambda x: x[train.loc[x.index, 'is_win'] == 1].sum()
    }).reset_index()
    weight_stats.columns = ['weight_cat', 'wins', 'starts', 'odds_sum']
    weight_stats['roi'] = weight_stats['odds_sum'] / weight_stats['starts'] * 100
    weight_stats['win_rate'] = weight_stats['wins'] / weight_stats['starts'] * 100
    print(weight_stats.to_string(index=False))
    
    # =========================================================================
    # 7. MARGIN ANALYSIS (前走着差)
    # =========================================================================
    print("\n" + "=" * 70)
    print("7. PREVIOUS MARGIN ANALYSIS")
    print("=" * 70)
    
    # Calculate previous margin for each horse
    train_sorted = train.sort_values(['horse_id', 'race_date', 'race_id'])
    train_sorted['prev_margin'] = train_sorted.groupby('horse_id')['margin_seconds'].shift(1)
    train_sorted['prev_finish'] = train_sorted.groupby('horse_id')['finish_position'].shift(1)
    
    # Winners who lost by small margin last time (potential bounce back)
    train_sorted['prev_margin_cat'] = pd.cut(train_sorted['prev_margin'], 
                                              bins=[-np.inf, 0, 0.3, 0.6, 1.0, np.inf],
                                              labels=['Winner', 'Close2nd', 'Top3', 'Mid', 'Far'])
    
    print("\n--- Previous Margin x ROI ---")
    margin_stats = train_sorted.groupby('prev_margin_cat').agg({
        'is_win': ['sum', 'count'],
        'win_odds': lambda x: x[train_sorted.loc[x.index, 'is_win'] == 1].sum()
    }).reset_index()
    margin_stats.columns = ['prev_margin_cat', 'wins', 'starts', 'odds_sum']
    margin_stats['roi'] = margin_stats['odds_sum'] / margin_stats['starts'] * 100
    margin_stats['win_rate'] = margin_stats['wins'] / margin_stats['starts'] * 100
    print(margin_stats.to_string(index=False))
    
    # =========================================================================
    # 8. JOCKEY x VENUE ANALYSIS
    # =========================================================================
    print("\n" + "=" * 70)
    print("8. JOCKEY x VENUE AFFINITY ANALYSIS")
    print("=" * 70)
    
    # Jockey venue specialization
    jv_stats = train.groupby(['jockey_id', 'venue']).agg({
        'is_win': ['sum', 'count', 'mean'],
        'win_odds': lambda x: x[train.loc[x.index, 'is_win'] == 1].sum()
    }).reset_index()
    jv_stats.columns = ['jockey_id', 'venue', 'wins', 'starts', 'win_rate', 'odds_sum']
    jv_stats['roi'] = jv_stats['odds_sum'] / jv_stats['starts'] * 100
    
    # Compare jockey's venue-specific performance vs overall
    jockey_overall = train.groupby('jockey_id').agg({
        'is_win': 'mean'
    }).reset_index()
    jockey_overall.columns = ['jockey_id', 'overall_win_rate']
    
    jv_stats = jv_stats.merge(jockey_overall, on='jockey_id', how='left')
    jv_stats['venue_bonus'] = jv_stats['win_rate'] - jv_stats['overall_win_rate']
    
    # Top venue specialists
    high_bonus = jv_stats[(jv_stats['starts'] >= 50) & (jv_stats['venue_bonus'] > 0.05)]
    print(f"\nJockey-Venue specialists (min 50 starts, bonus > 5%): {len(high_bonus)}")
    print(high_bonus.sort_values('venue_bonus', ascending=False).head(15).to_string(index=False))


if __name__ == "__main__":
    main()

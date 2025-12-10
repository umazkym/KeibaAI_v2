"""
Feature interaction analysis for V20
"""
import pandas as pd
import numpy as np

def calc_roi(g):
    wins = g[g['is_win'] == 1]
    return wins['win_odds'].sum() / len(g) * 100 if len(g) > 0 else 0

races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')

races['race_date'] = pd.to_datetime(races['race_date'])
train = races[races['race_date'] <= '2024-12-31'].copy()
train = train[train['finish_position'].notna()]
train['is_win'] = (train['finish_position'] == 1).astype(int)

print('=== INTERACTION ANALYSIS ===')
print('Checking how new features interact with EXISTING V15 features')

# Get damsire (generation 2, position 2)
gen2 = pedigrees[pedigrees['generation'] == 2].copy()
gen2_sorted = gen2.sort_values(['horse_id', 'ancestor_id'])
gen2_sorted['pos'] = gen2_sorted.groupby('horse_id').cumcount()
damsire = gen2_sorted[gen2_sorted['pos'] == 2][['horse_id', 'ancestor_id']].copy()
damsire.columns = ['horse_id', 'damsire_id']

# Get sire (generation 1, position 0)
gen1 = pedigrees[pedigrees['generation'] == 1].copy()
gen1_sorted = gen1.sort_values(['horse_id', 'ancestor_id'])
gen1_sorted['pos'] = gen1_sorted.groupby('horse_id').cumcount()
sire = gen1_sorted[gen1_sorted['pos'] == 0][['horse_id', 'ancestor_id']].copy()
sire.columns = ['horse_id', 'sire_id']

# Merge
train_p = train.merge(damsire, on='horse_id', how='left')
train_p = train_p.merge(sire, on='horse_id', how='left')
train_p = train_p.merge(horses[['horse_id', 'breeder_name']], on='horse_id', how='left')

# Calculate stats for each
sire_stats = train_p.groupby('sire_id').agg({'is_win': 'mean'}).reset_index()
sire_stats.columns = ['sire_id', 'sire_wr']

ds_stats = train_p.groupby('damsire_id').agg({'is_win': 'mean'}).reset_index()
ds_stats.columns = ['damsire_id', 'damsire_wr']

br_stats = train_p.groupby('breeder_name').agg({'is_win': 'mean'}).reset_index()
br_stats.columns = ['breeder_name', 'breeder_wr']

train_p = train_p.merge(sire_stats, on='sire_id', how='left')
train_p = train_p.merge(ds_stats, on='damsire_id', how='left')
train_p = train_p.merge(br_stats, on='breeder_name', how='left')

# Check correlation between sire and damsire win rates
valid = train_p.dropna(subset=['sire_wr', 'damsire_wr', 'breeder_wr'])
print(f"sire_wr vs damsire_wr correlation: {valid['sire_wr'].corr(valid['damsire_wr']):.3f}")
print(f"sire_wr vs breeder_wr correlation: {valid['sire_wr'].corr(valid['breeder_wr']):.3f}")
print(f"damsire_wr vs breeder_wr correlation: {valid['damsire_wr'].corr(valid['breeder_wr']):.3f}")
print(f"damsire_wr vs popularity: {valid['damsire_wr'].corr(valid['popularity']):.3f}")
print(f"breeder_wr vs popularity: {valid['breeder_wr'].corr(valid['popularity']):.3f}")

print('')
print('=== COMBINED BLOODLINE EFFECT ===')
# Combined good bloodline (both sire and damsire in top quartile)
valid['sire_q'] = pd.qcut(valid['sire_wr'], 4, labels=[1,2,3,4])
valid['damsire_q'] = pd.qcut(valid['damsire_wr'], 4, labels=[1,2,3,4])
valid['breeder_q'] = pd.qcut(valid['breeder_wr'], 4, labels=[1,2,3,4])

# Quadrant analysis
valid['blood_combo'] = valid['sire_q'].astype(str) + '_' + valid['damsire_q'].astype(str)

combo_roi = valid.groupby('blood_combo').apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100,
    'avg_pop': g['popularity'].mean()
}), include_groups=False).reset_index()

# Sort by ROI to see best combos
combo_roi = combo_roi.sort_values('roi', ascending=False)
print('Top bloodline combinations (sire_q_damsire_q):')
print(combo_roi.to_string(index=False))

print('')
print('=== BREEDER + BLOODLINE SYNERGY ===')
# Triple combination
valid['triple_combo'] = (valid['sire_q'].astype(int) + valid['damsire_q'].astype(int) + valid['breeder_q'].astype(int))

triple_roi = valid.groupby('triple_combo').apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100,
    'avg_pop': g['popularity'].mean()
}), include_groups=False).reset_index()

print('Triple combination (sire_q + damsire_q + breeder_q):')
print(triple_roi.sort_values('roi', ascending=False).to_string(index=False))

print('')
print('=== KEY INSIGHT ===')
# Best performers
best = valid[(valid['sire_q'].astype(int) >= 3) & (valid['damsire_q'].astype(int) >= 3)]
worst = valid[(valid['sire_q'].astype(int) <= 2) & (valid['damsire_q'].astype(int) <= 2)]

print(f"Best bloodline (sire+damsire Q3-4): {len(best):,} starts, ROI={calc_roi(best):.1f}%")
print(f"Worst bloodline (sire+damsire Q1-2): {len(worst):,} starts, ROI={calc_roi(worst):.1f}%")
print(f"ROI difference: {calc_roi(best) - calc_roi(worst):.1f}%")

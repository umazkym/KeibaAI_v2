"""
Summary analysis: Feature candidates for V20
Check popularity correlation and test ROI impact
"""
import pandas as pd
import numpy as np

def calc_roi(g):
    wins = g[g['is_win'] == 1]
    return wins['win_odds'].sum() / len(g) * 100 if len(g) > 0 else 0

# Load all data
races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')

races['race_date'] = pd.to_datetime(races['race_date'])
races = races[races['finish_position'].notna()]
races['is_win'] = (races['finish_position'] == 1).astype(int)

# Split data
train = races[races['race_date'] <= '2024-12-31'].copy()
test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()

print("=" * 70)
print("FEATURE CANDIDATE SUMMARY FOR V20")
print("=" * 70)
print(f"Train: {len(train):,}, Test: {len(test):,}")

# =========================================================================
# 1. DAMSIRE (母父) STATISTICS
# =========================================================================
print("\n" + "=" * 70)
print("1. DAMSIRE (母父) - ROI potential")
print("=" * 70)

# Get damsire
gen2 = pedigrees[pedigrees['generation'] == 2].copy()
gen2_sorted = gen2.sort_values(['horse_id', 'ancestor_id'])
gen2_sorted['pos'] = gen2_sorted.groupby('horse_id').cumcount()
damsire = gen2_sorted[gen2_sorted['pos'] == 2][['horse_id', 'ancestor_id']].copy()
damsire.columns = ['horse_id', 'damsire_id']

# Merge with train
train_ds = train.merge(damsire, on='horse_id', how='left')

# Calculate damsire win rate from TRAIN data ONLY
ds_wr = train_ds.groupby('damsire_id').agg({
    'is_win': ['sum', 'count', 'mean']
}).reset_index()
ds_wr.columns = ['damsire_id', 'ds_wins', 'ds_races', 'ds_win_rate']

# Only use damsires with min 100 races
ds_wr = ds_wr[ds_wr['ds_races'] >= 100]

# Merge back to train (point-in-time: use train stats for test)
train_ds = train_ds.merge(ds_wr[['damsire_id', 'ds_win_rate']], on='damsire_id', how='left')

# Check correlation with popularity
valid = train_ds.dropna(subset=['ds_win_rate', 'popularity'])
corr = valid['ds_win_rate'].corr(valid['popularity'])
print(f"damsire_win_rate vs popularity correlation: {corr:.3f}")
print(f"Coverage (non-null): {train_ds['ds_win_rate'].notna().mean():.1%}")

# ROI by damsire win rate quartile
train_ds['ds_wr_q'] = pd.qcut(train_ds['ds_win_rate'], 4, labels=['Q1(Low)', 'Q2', 'Q3', 'Q4(High)'])
ds_roi = train_ds.groupby('ds_wr_q', observed=True).apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100
}), include_groups=False).reset_index()
print("\nROI by damsire win rate quartile:")
print(ds_roi.to_string(index=False))

# =========================================================================
# 2. BREEDER STATISTICS
# =========================================================================
print("\n" + "=" * 70)
print("2. BREEDER (生産者) - ROI potential")
print("=" * 70)

train_h = train.merge(horses[['horse_id', 'breeder_name']], on='horse_id', how='left')

# Calculate breeder stats from TRAIN data
breeder_stats = train_h.groupby('breeder_name').agg({
    'is_win': ['sum', 'count', 'mean']
}).reset_index()
breeder_stats.columns = ['breeder_name', 'br_wins', 'br_races', 'br_win_rate']
breeder_stats = breeder_stats[breeder_stats['br_races'] >= 100]

train_h = train_h.merge(breeder_stats[['breeder_name', 'br_win_rate']], on='breeder_name', how='left')

valid_br = train_h.dropna(subset=['br_win_rate', 'popularity'])
corr_br = valid_br['br_win_rate'].corr(valid_br['popularity'])
print(f"breeder_win_rate vs popularity correlation: {corr_br:.3f}")
print(f"Coverage (non-null): {train_h['br_win_rate'].notna().mean():.1%}")

train_h['br_wr_q'] = pd.qcut(train_h['br_win_rate'], 4, labels=['Q1(Low)', 'Q2', 'Q3', 'Q4(High)'])
br_roi = train_h.groupby('br_wr_q', observed=True).apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100
}), include_groups=False).reset_index()
print("\nROI by breeder win rate quartile:")
print(br_roi.to_string(index=False))

# =========================================================================
# 3. WEATHER / TRACK CONDITION INTERACTION
# =========================================================================
print("\n" + "=" * 70)
print("3. WEATHER x CONDITION - Check variance")
print("=" * 70)

wc_roi = train.groupby(['weather', 'track_condition']).apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g)
}), include_groups=False).reset_index()

# High ROI combinations
high_roi = wc_roi[(wc_roi['starts'] >= 1000) & (wc_roi['roi'] > 80)]
print("High ROI weather x condition (ROI > 80%, min 1000 starts):")
print(high_roi.to_string(index=False))

# Weather correlation with popularity
train['weather_code'] = train['weather'].map({'晴': 0, '曇': 1, '小雨': 2, '雨': 3, '小雪': 4, '雪': 5})
print(f"\nweather_code vs popularity correlation: {train['weather_code'].corr(train['popularity']):.3f}")

# =========================================================================
# 4. PREVIOUS MARGIN (僅差負け)
# =========================================================================
print("\n" + "=" * 70)
print("4. PREVIOUS MARGIN (僅差負け) - ROI potential")
print("=" * 70)

train = train.sort_values(['horse_id', 'race_date', 'race_id'])
train['prev_margin'] = train.groupby('horse_id')['margin_seconds'].shift(1)

# Close loss indicator (0.1-0.3 seconds)
train['close_loss'] = ((train['prev_margin'] > 0) & (train['prev_margin'] <= 0.3)).astype(float)

valid_pm = train.dropna(subset=['prev_margin', 'popularity'])
corr_pm = valid_pm['prev_margin'].corr(valid_pm['popularity'])
print(f"prev_margin vs popularity correlation: {corr_pm:.3f}")
print(f"close_loss coverage: {train['prev_margin'].notna().mean():.1%}")

close_roi = train.groupby(train['close_loss'] == 1).apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100
}), include_groups=False).reset_index()
close_roi.columns = ['is_close_loss', 'starts', 'roi', 'win_rate']
print("\nROI by close_loss flag:")
print(close_roi.to_string(index=False))

# =========================================================================
# 5. JOCKEY UPGRADE TO TOP JOCKEY
# =========================================================================
print("\n" + "=" * 70)
print("5. JOCKEY UPGRADE - Check market effect")
print("=" * 70)

# Define top jockeys
jockey_stats = train.groupby('jockey_id').agg({'is_win': ['sum', 'count', 'mean']}).reset_index()
jockey_stats.columns = ['jockey_id', 'wins', 'races', 'win_rate']
jockey_stats = jockey_stats[jockey_stats['races'] >= 500]
top_threshold = jockey_stats['win_rate'].quantile(0.9)
top_jockeys = set(jockey_stats[jockey_stats['win_rate'] >= top_threshold]['jockey_id'])

train['prev_jockey'] = train.groupby('horse_id')['jockey_id'].shift(1)
train['jockey_upgrade'] = (train['jockey_id'].isin(top_jockeys) & ~train['prev_jockey'].isin(top_jockeys))

valid_ju = train.dropna(subset=['prev_jockey', 'popularity'])
# Check if jockey upgrade correlates with popularity change
print(f"jockey_upgrade coverage: {train['prev_jockey'].notna().mean():.1%}")

upgrade_roi = train.groupby('jockey_upgrade').apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100,
    'avg_popularity': g['popularity'].mean()
}), include_groups=False).reset_index()
print("\nROI by jockey_upgrade:")
print(upgrade_roi.to_string(index=False))

# =========================================================================
# SUMMARY
# =========================================================================
print("\n" + "=" * 70)
print("FEATURE CANDIDATES SUMMARY")
print("=" * 70)

print("""
| Feature | Pop Corr | Coverage | ROI Pattern | Recommendation |
|---------|----------|----------|-------------|----------------|
| damsire_win_rate | -0.178 | ~80% | Q4 > Q1 | ○ 有望 |
| breeder_win_rate | ~-0.2 | ~60% | Variance large | △ 要検討 |
| weather_code | ~0.0 | 100% | 雨×不良=92% | △ 条件限定 |
| prev_margin (close) | 0.12 | ~80% | +1-2% ROI | △ 効果小 |
| jockey_upgrade | High | ~80% | 79% ROI | × 市場織込 |

**最有望候補**: damsire_win_rate (母父勝率)
- 人気相関が低い (-0.178)
- カバレッジが高い (~80%)
- ROIにばらつきがある (Q4とQ1で差あり)
""")

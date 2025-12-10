"""
Deep analysis of margin and position patterns
"""
import pandas as pd
import numpy as np

def calc_roi(g):
    wins = g[g['is_win'] == 1]
    return wins['win_odds'].sum() / len(g) * 100 if len(g) > 0 else 0

races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')

races['race_date'] = pd.to_datetime(races['race_date'])
train = races[races['race_date'] <= '2024-12-31'].copy()
train = train[train['finish_position'].notna()]
train['is_win'] = (train['finish_position'] == 1).astype(int)

print('=== PREVIOUS MARGIN ANALYSIS ===')
train = train.sort_values(['horse_id', 'race_date', 'race_id'])
train['prev_margin'] = train.groupby('horse_id')['margin_seconds'].shift(1)
train['prev_finish'] = train.groupby('horse_id')['finish_position'].shift(1)

# Define categories
train['prev_margin_cat'] = pd.cut(train['prev_margin'], 
                                   bins=[-np.inf, 0, 0.3, 0.6, 1.0, np.inf],
                                   labels=['Winner', 'Close(0-0.3s)', '0.3-0.6s', '0.6-1.0s', '>1.0s'])

margin_stats = train.groupby('prev_margin_cat', observed=True).apply(lambda g: pd.Series({
    'starts': len(g),
    'wins': g['is_win'].sum(),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100,
    'avg_popularity': g['popularity'].mean()
}), include_groups=False).reset_index()

print(margin_stats.to_string(index=False))

# Correlation check
valid = train.dropna(subset=['prev_margin', 'popularity'])
corr = valid['prev_margin'].corr(valid['popularity'])
print(f'\nprev_margin vs popularity corr: {corr:.3f}')

print('')
print('=== CLOSE LOSS + ODDS RANGE ===')
# Close loss (0-0.3s) at different odds ranges
close_loss = train[(train['prev_margin'] > 0) & (train['prev_margin'] <= 0.3)].copy()
close_loss['odds_range'] = pd.cut(close_loss['win_odds'],
                                   bins=[0, 5, 10, 20, 50, 100, np.inf],
                                   labels=['1-5x', '5-10x', '10-20x', '20-50x', '50-100x', '>100x'])

odds_stats = close_loss.groupby('odds_range', observed=True).apply(lambda g: pd.Series({
    'starts': len(g),
    'wins': g['is_win'].sum(),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100
}), include_groups=False).reset_index()

print('Close Loss (0-0.3s) by Odds Range:')
print(odds_stats.to_string(index=False))

print('')
print('=== LAST 3F RANK ANALYSIS ===')
# Last 3F rank patterns
train['prev_last3f_rank'] = train.groupby('horse_id')['last3f_rank'].shift(1)

last3f_stats = train.groupby(train['prev_last3f_rank'].clip(upper=10), observed=True).apply(lambda g: pd.Series({
    'starts': len(g),
    'wins': g['is_win'].sum(),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100,
    'avg_popularity': g['popularity'].mean()
}), include_groups=False).reset_index()

print('Previous Last 3F Rank (capped at 10):')
print(last3f_stats.to_string(index=False))

# Correlation
valid_l3f = train.dropna(subset=['prev_last3f_rank', 'popularity'])
corr_l3f = valid_l3f['prev_last3f_rank'].corr(valid_l3f['popularity'])
print(f'\nprev_last3f_rank vs popularity corr: {corr_l3f:.3f}')

print('')
print('=== COMBINED: Close Loss + Good Last3F + Odds ===')
# Close loss + good last 3f rank
combo = train[(train['prev_margin'] > 0) & (train['prev_margin'] <= 0.5) & 
              (train['prev_last3f_rank'] <= 3)].copy()

combo['odds_range'] = pd.cut(combo['win_odds'],
                              bins=[0, 10, 30, 100, np.inf],
                              labels=['1-10x', '10-30x', '30-100x', '>100x'])

combo_stats = combo.groupby('odds_range', observed=True).apply(lambda g: pd.Series({
    'starts': len(g),
    'wins': g['is_win'].sum(),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100
}), include_groups=False).reset_index()

print('Close Loss (<0.5s) + Top3 Last3F:')
print(combo_stats.to_string(index=False))
print(f'\nTotal: {len(combo)} starts, ROI: {calc_roi(combo):.1f}%')

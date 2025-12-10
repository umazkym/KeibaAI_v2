"""
Basis weight and jockey change analysis
"""
import pandas as pd
import numpy as np

def calc_roi(g):
    wins = g[g['is_win'] == 1]
    return wins['win_odds'].sum() / len(g) * 100 if len(g) > 0 else 0

races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races['race_date'] = pd.to_datetime(races['race_date'])
train = races[races['race_date'] <= '2024-12-31'].copy()
train = train[train['finish_position'].notna()]
train['is_win'] = (train['finish_position'] == 1).astype(int)

print('=== BASIS WEIGHT CHANGE ANALYSIS ===')
train = train.sort_values(['horse_id', 'race_date', 'race_id'])
train['prev_weight'] = train.groupby('horse_id')['basis_weight'].shift(1)
train['weight_diff'] = train['basis_weight'] - train['prev_weight']

train['weight_change_cat'] = pd.cut(train['weight_diff'], 
                                     bins=[-np.inf, -2, -0.5, 0.5, 2, np.inf],
                                     labels=['Light -2+', 'Light -0.5~-2', 'Same', 'Heavy +0.5~2', 'Heavy +2+'])

weight_stats = train.groupby('weight_change_cat', observed=True).apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100,
    'avg_popularity': g['popularity'].mean()
}), include_groups=False).reset_index()

print(weight_stats.to_string(index=False))

valid = train.dropna(subset=['weight_diff', 'popularity'])
corr = valid['weight_diff'].corr(valid['popularity'])
print(f'\nbasis_weight_diff vs popularity corr: {corr:.3f}')

print('')
print('=== ACE JOCKEY ANALYSIS ===')
# Define top jockeys by win rate (min 500 races)
jockey_stats = train.groupby('jockey_id').agg({
    'is_win': ['sum', 'count', 'mean']
}).reset_index()
jockey_stats.columns = ['jockey_id', 'wins', 'races', 'win_rate']
jockey_stats = jockey_stats[jockey_stats['races'] >= 500]

# Top 10% jockeys
top_threshold = jockey_stats['win_rate'].quantile(0.9)
top_jockeys = set(jockey_stats[jockey_stats['win_rate'] >= top_threshold]['jockey_id'])
print(f'Top 10% jockeys (WR >= {top_threshold:.1%}): {len(top_jockeys)}')

# Check if having top jockey affects ROI
train['has_top_jockey'] = train['jockey_id'].isin(top_jockeys)

top_jockey_roi = train.groupby('has_top_jockey').apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100,
    'avg_popularity': g['popularity'].mean()
}), include_groups=False).reset_index()

print(top_jockey_roi.to_string(index=False))

# Now check jockey change to/from top jockey
train['prev_jockey'] = train.groupby('horse_id')['jockey_id'].shift(1)
train['prev_is_top'] = train['prev_jockey'].isin(top_jockeys)
train['curr_is_top'] = train['jockey_id'].isin(top_jockeys)

# Categories
def get_jockey_change_cat(row):
    if pd.isna(row['prev_jockey']):
        return 'First Race'
    if row['jockey_id'] == row['prev_jockey']:
        return 'Same Jockey'
    if row['curr_is_top'] and not row['prev_is_top']:
        return 'Upgrade to Top'
    if not row['curr_is_top'] and row['prev_is_top']:
        return 'Downgrade from Top'
    return 'Change within Same Tier'

train['jockey_change_cat'] = train.apply(get_jockey_change_cat, axis=1)

jc_stats = train.groupby('jockey_change_cat').apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100,
    'avg_popularity': g['popularity'].mean()
}), include_groups=False).reset_index()

print('\nJockey Change Categories:')
print(jc_stats.to_string(index=False))

print('')
print('=== PREVIOUS RACE CLASS ANALYSIS (クラス変動) ===')
# Already in V8+, but let's verify the pattern
train['prev_class'] = train.groupby('horse_id')['race_class'].shift(1)

# Define class hierarchy
class_order = {'新馬': 1, '未勝利': 2, '1勝クラス': 3, '2勝クラス': 4, '3勝クラス': 5, 
               'オープン': 6, 'OP': 6, 'リステッド': 7, 'G3': 8, 'G2': 9, 'G1': 10}

train['class_val'] = train['race_class'].map(class_order)
train['prev_class_val'] = train['prev_class'].map(class_order)
train['class_change'] = train['class_val'] - train['prev_class_val']

# Categorize
train['class_change_cat'] = 'Same'
train.loc[train['class_change'] > 0, 'class_change_cat'] = 'Class Up'
train.loc[train['class_change'] < 0, 'class_change_cat'] = 'Class Down'

class_stats = train.groupby('class_change_cat').apply(lambda g: pd.Series({
    'starts': len(g),
    'roi': calc_roi(g),
    'win_rate': g['is_win'].mean() * 100,
    'avg_popularity': g['popularity'].mean()
}), include_groups=False).reset_index()

print(class_stats.to_string(index=False))

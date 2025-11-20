import pandas as pd

df = pd.read_csv('keibaai/data/evaluation/evaluation_results.csv')

print('Average Recovery Rate:', f'{df["recovery_rate"].mean():.2%}')
print('Total Races:', f'{df["race_count"].sum():.0f}')
print('Total Bet:', f'{df["total_bet"].sum():.0f}円')
print('Total Return:', f'{df["total_return"].sum():.0f}円')
print('Max Recovery Rate:', f'{df["recovery_rate"].max():.2%}')
print('Min Recovery Rate:', f'{df["recovery_rate"].min():.2%}')

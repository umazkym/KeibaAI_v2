#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""V16の新特徴量テスト"""

import pandas as pd

preds = pd.read_csv('outputs/analysis/race_level_deep_analysis.csv')
print(f'データ: {len(preds):,}件')

# jw_rank_in_raceを手動計算
preds['jw_rank'] = preds.groupby('race_id')['jockey_win_rate'].rank(ascending=False, method='average')
preds['race_size'] = preds.groupby('race_id')['horse_number'].transform('count')
preds['jw_rank_in_race'] = (preds['race_size'] - preds['jw_rank'] + 1) / preds['race_size']

# team_powerを手動計算
preds['team_power'] = (preds['jockey_win_rate'].fillna(0) + preds['trainer_win_rate'].fillna(0)) / 2

print(f"\njw_rank_in_race: 平均={preds['jw_rank_in_race'].mean():.3f}")
print(f"team_power: 平均={preds['team_power'].mean():.4f}")

# 見逃しパターンA馬での効果確認
winners = preds[preds['finish_position'] == 1]
pattern_a = winners[(winners['popularity'] <= 3) & (winners['rank'] >= 6)]
correct = winners[(winners['popularity'] <= 3) & (winners['rank'] <= 3)]

print(f"\n■ JW相対順位の比較:")
print(f"  パターンA（見逃し）: {pattern_a['jw_rank_in_race'].mean():.3f}")
print(f"  正解: {correct['jw_rank_in_race'].mean():.3f}")

print(f"\n■ チーム力の比較:")
print(f"  パターンA（見逃し）: {pattern_a['team_power'].mean():.4f}")
print(f"  正解: {correct['team_power'].mean():.4f}")

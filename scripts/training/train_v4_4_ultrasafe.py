#!/usr/bin/env python3
"""V4.4 Ultra Safe: 回り方向特徴量完全除外版"""
import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import json
import yaml
from pathlib import Path
from datetime import datetime

project_root = Path('.')

LEAKY_FEATURES = [
    'form_rank', 'gap_course_fit_popularity', 'gap_jockey_popularity',
    'gap_pedigree_popularity', 'gap_trainer_popularity', 'is_overvalued',
    'turn_match', 'turn_preference', 'horse_left_turn_perf', 'horse_right_turn_perf', 'is_left_turn',
]

train_data = pd.read_parquet('keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet')
train_data['race_date'] = pd.to_datetime(train_data['race_date'])

with open('keibaai/models/mu_v4_3/feature_names.json', 'r', encoding='utf-8') as f:
    all_v43_features = json.load(f)
safe_v43_features = [f for f in all_v43_features if f not in LEAKY_FEATURES]

races_raw = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races_raw['race_date'] = pd.to_datetime(races_raw['race_date'])
races_raw = races_raw.sort_values(['horse_id', 'race_date'])

for i in range(1, 6):
    races_raw[f'prev_{i}_finish'] = races_raw.groupby('horse_id')['finish_position'].shift(i)
    races_raw[f'prev_{i}_odds'] = races_raw.groupby('horse_id')['win_odds'].shift(i)
    if 'last_3f_time' in races_raw.columns:
        races_raw[f'prev_{i}_l3f'] = races_raw.groupby('horse_id')['last_3f_time'].shift(i)

train_data['race_id'] = train_data['race_id'].astype(str)
train_data['horse_id'] = train_data['horse_id'].astype(str)
races_raw['race_id'] = races_raw['race_id'].astype(str)
races_raw['horse_id'] = races_raw['horse_id'].astype(str)

merge_cols = ['race_id', 'horse_id'] + [f'prev_{i}_finish' for i in range(1,6)] + [f'prev_{i}_odds' for i in range(1,6)]
if 'prev_1_l3f' in races_raw.columns:
    merge_cols += [f'prev_{i}_l3f' for i in range(1,6)]
merge_cols = [c for c in merge_cols if c in races_raw.columns]
train_data = train_data.merge(races_raw[merge_cols].drop_duplicates(['race_id','horse_id']), on=['race_id','horse_id'], how='left')

# 季節特徴量
train_data['month'] = train_data['race_date'].dt.month
train_data['month_sin'] = np.sin(2 * np.pi * train_data['month'] / 12)
train_data['month_cos'] = np.cos(2 * np.pi * train_data['month'] / 12)

new_features = ['month_sin', 'month_cos']
for i in range(1,6):
    new_features.extend([f'prev_{i}_finish', f'prev_{i}_odds'])
    if f'prev_{i}_l3f' in train_data.columns:
        new_features.append(f'prev_{i}_l3f')

all_features = safe_v43_features + new_features
available_features = [f for f in all_features if f in train_data.columns]
print(f'Features: {len(available_features)}')

weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
odds = train_data['win_odds'].fillna(1.0).clip(upper=90)
log_odds = np.log1p(odds)
gain = np.zeros(len(train_data))
gain[train_data['finish_position'] == 1] = log_odds[train_data['finish_position'] == 1] * weight_1st
gain[train_data['finish_position'] == 2] = log_odds[train_data['finish_position'] == 2] * weight_2nd
gain[train_data['finish_position'] == 3] = log_odds[train_data['finish_position'] == 3] * weight_3rd
train_data['target_relevance'] = gain.astype(int)
train_data['sample_weight'] = np.log1p(train_data['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))

train_mask = train_data['race_date'] < '2023-01-01'
valid_mask = (train_data['race_date'] >= '2023-01-01') & (train_data['race_date'] < '2024-01-01')
test_mask = train_data['race_date'] >= '2024-01-01'

train_df = train_data[train_mask].copy()
valid_df = train_data[valid_mask].copy()
test_df = train_data[test_mask].copy()

train_df[available_features] = train_df[available_features].fillna(0)
valid_df[available_features] = valid_df[available_features].fillna(0)
test_df[available_features] = test_df[available_features].fillna(0)

group_train = train_df.groupby('race_id').size().to_list()
group_valid = valid_df.groupby('race_id').size().to_list()

print(f'Train: {len(train_df)}, Valid: {len(valid_df)}, Test: {len(test_df)}')

params = {
    'objective': 'lambdarank',
    'metric': 'ndcg',
    'eval_at': [1,3,5],
    'boosting_type': 'gbdt',
    'num_leaves': 35,
    'max_depth': 5,
    'min_child_samples': 150,
    'learning_rate': 0.05,
    'reg_alpha': 3.0,
    'reg_lambda': 5.0,
    'feature_fraction': 0.6,
    'bagging_fraction': 0.7,
    'bagging_freq': 5,
    'verbose': -1,
    'random_state': 42,
    'label_gain': list(range(100))
}

model = lgb.LGBMRanker(**params, n_estimators=1500)
model.fit(
    train_df[available_features], train_df['target_relevance'],
    group=group_train, sample_weight=train_df['sample_weight'],
    eval_set=[(valid_df[available_features], valid_df['target_relevance'])],
    eval_group=[group_valid],
    callbacks=[lgb.early_stopping(stopping_rounds=50), lgb.log_evaluation(100)]
)

def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet_df = d[d['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    if len(bet_df) == 0: return 0, 0
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate

valid_preds = model.predict(valid_df[available_features])
test_preds = model.predict(test_df[available_features])
valid_roi, valid_hit = calc_roi(valid_df, valid_preds)
test_roi, test_hit = calc_roi(test_df, test_preds)

print('=' * 50)
print('V4.4 Ultra Safe (回り方向完全除外)')
print(f'Valid ROI: {valid_roi:.2f}%')
print(f'Test ROI:  {test_roi:.2f}%')
print(f'Gap:       {abs(valid_roi - test_roi):.2f}%')
print('=' * 50)

output_dir = Path('keibaai/models/mu_v4_4_ultrasafe')
output_dir.mkdir(parents=True, exist_ok=True)

with open(output_dir / 'model.pkl', 'wb') as f:
    pickle.dump(model, f)
with open(output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
    json.dump(available_features, f, indent=2, ensure_ascii=False)

imp = pd.DataFrame({'feature': available_features, 'importance': model.feature_importances_}).sort_values('importance', ascending=False)
imp.to_csv(output_dir / 'feature_importance.csv', index=False)

info = {
    'version': 'v4.4_ultrasafe',
    'description': '完全リークフリー版（回り方向特徴量も除外）',
    'valid_roi': valid_roi,
    'test_roi': test_roi,
    'gap': abs(valid_roi - test_roi),
    'feature_count': len(available_features),
    'created_at': datetime.now().isoformat()
}
with open(output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
    json.dump(info, f, indent=2, ensure_ascii=False)

print(f'Saved: {output_dir}')
print('Top 10 Features:')
for _, row in imp.head(10).iterrows():
    print(f"  {row['feature']}: {row['importance']:.1f}")

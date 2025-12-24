#!/usr/bin/env python3
# Feature Importance Analysis for mu v2.1 model

import joblib
import json
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set_style('whitegrid')

# Load model
model_dir = Path('models/mu_v2.1_new_features')
ranker = joblib.load(model_dir / 'ranker.pkl')
regressor = joblib.load(model_dir / 'regressor.pkl')

with open(model_dir / 'feature_names.json', 'r') as f:
    feature_names = json.load(f)

print("="*80)
print("Feature Importance Analysis - mu v2.1_new_features")
print("="*80)

# Get feature importances
ranker_importance = pd.DataFrame({
    'feature': feature_names,
    'importance': ranker.feature_importances_
}).sort_values('importance', ascending=False)

regressor_importance = pd.DataFrame({
    'feature': feature_names,
    'importance': regressor.feature_importances_
}).sort_values('importance', ascending=False)

# Define new features
new_features = {
    'stability': ['finish_std_last3', 'finish_std_last5', 'finish_std_last10',
                  'finish_cv_last3', 'finish_cv_last5', 'finish_cv_last10'],
    'condition_change': ['distance_change', 'is_distance_shortened', 'is_distance_lengthened',
                         'surface_change', 'venue_change'],
    'synergy': ['combo_win_rate'],
    'pace': ['horse_early_speed_index', 'horse_late_speed_index'],
    'rest': ['days_since_last_race', 'is_rest_return']
}

all_new_features = []
for category, features in new_features.items():
    all_new_features.extend(features)

# Check which new features are in top N
print("\n【1. Top 20 Features - Ranker (NDCG)】")
print(ranker_importance.head(20).to_string(index=False))

print("\n【2. Top 20 Features - Regressor (RMSE)】")
print(regressor_importance.head(20).to_string(index=False))

# Analyze new features
print("\n【3. New Features Performance】")
for category, features in new_features.items():
    print(f"\n{category.upper()}:")
    for feature in features:
        if feature in ranker_importance['feature'].values:
            rank_idx = ranker_importance[ranker_importance['feature'] == feature].index[0]
            rank_importance = ranker_importance.loc[rank_idx, 'importance']
            rank_rank = rank_idx + 1
            
            reg_idx = regressor_importance[regressor_importance['feature'] == feature].index[0]
            reg_importance = regressor_importance.loc[reg_idx, 'importance']
            reg_rank = reg_idx + 1
            
            print(f"  {feature}:")
            print(f"    Ranker: Rank #{rank_rank}/{len(feature_names)}, Importance: {rank_importance:.4f}")
            print(f"    Regressor: Rank #{reg_rank}/{len(feature_names)}, Importance: {reg_importance:.4f}")
        else:
            print(f"  {feature}: NOT FOUND")

# Summary statistics
print("\n【4. Summary Statistics】")
new_feature_ranker = ranker_importance[ranker_importance['feature'].isin(all_new_features)]
print(f"\nNew Features in Ranker Top 50: {(new_feature_ranker.head(50)['feature'].isin(all_new_features)).sum()}")
print(f"New Features in Ranker Top 100: {(new_feature_ranker.head(100)['feature'].isin(all_new_features)).sum()}")

new_feature_regressor = regressor_importance[regressor_importance['feature'].isin(all_new_features)]
print(f"\nNew Features in Regressor Top 50: {(new_feature_regressor.head(50)['feature'].isin(all_new_features)).sum()}")
print(f"New Features in Regressor Top 100: {(new_feature_regressor.head(100)['feature'].isin(all_new_features)).sum()}")

# Total importance contribution
total_ranker_importance = ranker_importance['importance'].sum()
new_ranker_importance = ranker_importance[ranker_importance['feature'].isin(all_new_features)]['importance'].sum()
print(f"\nNew Features Contribution (Ranker): {new_ranker_importance/total_ranker_importance*100:.2f}%")

total_regressor_importance = regressor_importance['importance'].sum()
new_regressor_importance = regressor_importance[regressor_importance['feature'].isin(all_new_features)]['importance'].sum()
print(f"New Features Contribution (Regressor): {new_regressor_importance/total_regressor_importance*100:.2f}%")

print("\n" + "="*80)
print("Conclusion:")
print("="*80)
if new_ranker_importance/total_ranker_importance > 0.1:
    print("✓ New features contribute significantly to ranking performance (>10%)")
else:
    print("⚠ New features have limited impact on ranking performance (<10%)")

if new_regressor_importance/total_regressor_importance > 0.1:
    print("✓ New features contribute significantly to regression performance (>10%)")
else:
    print("⚠ New features have limited impact on regression performance (<10%)")

# Find most valuable new feature
most_valuable_ranker = new_feature_ranker.iloc[0]
most_valuable_regressor = new_feature_regressor.iloc[0]
print(f"\nMost valuable new feature (Ranker): {most_valuable_ranker['feature']}")
print(f"Most valuable new feature (Regressor): {most_valuable_regressor['feature']}")

print("="*80)

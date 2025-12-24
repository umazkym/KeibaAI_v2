import joblib
import json
import pandas as pd
from pathlib import Path

# Load model
model_dir = Path('models/mu_v2.1_new_features')
ranker = joblib.load(model_dir / 'ranker.pkl')
regressor = joblib.load(model_dir / 'regressor.pkl')

with open(model_dir / 'feature_names.json', 'r') as f:
    feature_names = json.load(f)

print("Feature Importance Analysis")
print("="*60)

# Get importances
ranker_imp = pd.DataFrame({
    'feature': feature_names,
    'importance': ranker.feature_importances_
}).sort_values('importance', ascending=False)

regressor_imp = pd.DataFrame({
    'feature': feature_names,
    'importance': regressor.feature_importances_
}).sort_values('importance', ascending=False)

# New features
new_features = [
    'finish_std_last3', 'finish_std_last5', 'finish_std_last10',
    'finish_cv_last3', 'finish_cv_last5', 'finish_cv_last10',
    'distance_change', 'is_distance_shortened', 'is_distance_lengthened',
    'surface_change', 'venue_change',
    'combo_win_rate',
    'horse_early_speed_index', 'horse_late_speed_index',
    'days_since_last_race', 'is_rest_return'
]

# Top 10 features
print("\nTop 10 Ranker Features:")
for i, row in ranker_imp.head(10).iterrows():
    is_new = " [NEW]" if row['feature'] in new_features else ""
    print(f"  {i+1}. {row['feature']}: {row['importance']:.4f}{is_new}")

print("\nTop 10 Regressor Features:")
for i, row in regressor_imp.head(10).iterrows():
    is_new = " [NEW]" if row['feature'] in new_features else ""
    print(f"  {i+1}. {row['feature']}: {row['importance']:.4f}{is_new}")

# New features contribution
total_ranker = ranker_imp['importance'].sum()
new_ranker = ranker_imp[ranker_imp['feature'].isin(new_features)]['importance'].sum()

total_reg = regressor_imp['importance'].sum()
new_reg = regressor_imp[regressor_imp['feature'].isin(new_features)]['importance'].sum()

print(f"\nNew Features Contribution:")
print(f"  Ranker: {new_ranker/total_ranker*100:.2f}%")
print(f"  Regressor: {new_reg/total_reg*100:.2f}%")

# Count new features in top N
top_50_ranker = sum(1 for f in ranker_imp.head(50)['feature'] if f in new_features)
top_50_reg = sum(1 for f in regressor_imp.head(50)['feature'] if f in new_features)

print(f"\nNew Features in Top 50:")
print(f"  Ranker: {top_50_ranker}/50")
print(f"  Regressor: {top_50_reg}/50")

print("="*60)

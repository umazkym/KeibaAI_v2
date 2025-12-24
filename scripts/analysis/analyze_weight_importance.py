import pandas as pd
import numpy as np
from pathlib import Path

# Feature importance CSVを読み込み
importance_path = Path('keibaai/models/mu_v3_2/feature_importance.csv')
imp_df = pd.read_csv(importance_path)

print("=== Feature Importance Analysis ===\n")

# 馬体重関連の特徴量を抽出
weight_features = imp_df[imp_df['feature'].str.contains('weight', case=False)]
print(f"Weight-related features ({len(weight_features)}):")
print(weight_features[['feature', 'importance']].to_string(index=False))

total_weight_importance = weight_features['importance'].sum()
total_importance = imp_df['importance'].sum()
weight_percentage = (total_weight_importance / total_importance) * 100

print(f"\nTotal weight importance: {total_weight_importance:.0f}")
print(f"Total importance: {total_importance:.0f}")
print(f"Weight percentage: {weight_percentage:.2f}%")

print("\n=== Top 10 Features (Non-Weight) ===\n")
non_weight = imp_df[~imp_df['feature'].str.contains('weight', case=False)]
print(non_weight.head(10)[['feature', 'importance']].to_string(index=False))

# 馬体重関連の相関を確認するため、特徴量リストを出力
print("\n=== Weight Features for Correlation Check ===")
print(weight_features['feature'].tolist())

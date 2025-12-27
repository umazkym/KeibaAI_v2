#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""1着馬バイアスの原因探求"""
import pandas as pd
import numpy as np
import pickle
import json
from pathlib import Path
import sys

project_root = Path(".")
sys.path.insert(0, str(project_root))

# V4モデル
model_dir = project_root / "keibaai/models/mu_time_v4"
with open(model_dir / "mu_time_model.pkl", "rb") as f:
    model = pickle.load(f)
with open(model_dir / "feature_names.json", "r") as f:
    feature_cols = json.load(f)

df = pd.read_parquet("keibaai/data/parsed/parquet/races/races.parquet")
df["race_date"] = pd.to_datetime(df["race_date"])
df = df.dropna(subset=["finish_position", "finish_time_seconds", "venue"])
df = df.drop_duplicates(subset=["race_id", "horse_number"])

from keibaai.src.features.time_feature_engineer_v3 import TimeFeatureEngineerV3

train_mask = df["race_date"] < "2023-01-01"
time_fe = TimeFeatureEngineerV3(min_samples=30)
time_fe.fit(df[train_mask])
df = time_fe.transform(df)

for col in ["track_surface", "track_condition", "sex", "venue", "distance_category"]:
    if col in df.columns:
        df[col + "_encoded"] = df[col].astype("category").cat.codes

test_mask = (df["race_date"] >= "2023-01-01") & (df["race_date"] < "2024-01-01")
test_df = df[test_mask].copy()

available = [c for c in feature_cols if c in test_df.columns]
X = test_df[available].fillna(0)
test_df["pred"] = model.predict(X)
test_df["pred_time"] = test_df["pred"] * test_df["base_time_std"] + test_df["base_time_mean"]
test_df["error"] = test_df["pred_time"] - test_df["finish_time_seconds"]

print("=== 1着馬バイアスの原因探求 ===")
print()

# ターゲット分布の確認
winners = test_df[test_df["finish_position"] == 1]
losers = test_df[test_df["finish_position"] > 5]

print("【ターゲット分布（normalized_time）】")
print(f"1着馬: mean={winners['normalized_time'].mean():.3f}, std={winners['normalized_time'].std():.3f}")
print(f"6着以下: mean={losers['normalized_time'].mean():.3f}, std={losers['normalized_time'].std():.3f}")
print()

print("【予測値分布】")
print(f"1着馬予測: mean={winners['pred'].mean():.3f}, std={winners['pred'].std():.3f}")
print(f"6着以下予測: mean={losers['pred'].mean():.3f}, std={losers['pred'].std():.3f}")
print()

# 実際と予測のスプレッドを比較
actual_1_vs_5 = (
    test_df[test_df["finish_position"] == 1]["normalized_time"].mean()
    - test_df[test_df["finish_position"] == 5]["normalized_time"].mean()
)
pred_1_vs_5 = (
    test_df[test_df["finish_position"] == 1]["pred"].mean()
    - test_df[test_df["finish_position"] == 5]["pred"].mean()
)

print(f"1着 vs 5着の差（実際）: {actual_1_vs_5:.3f}")
print(f"1着 vs 5着の差（予測）: {pred_1_vs_5:.3f}")
print(f"→ 予測は実際の {pred_1_vs_5/actual_1_vs_5*100:.0f}% しか差を付けていない")
print()

# 特徴量の重要度
print("【重要特徴量 Top 10】")
importance = pd.DataFrame({"feature": available, "importance": model.feature_importances_}).sort_values(
    "importance", ascending=False
)
print(importance.head(10).to_string(index=False))
print()

# 仮説検証: 勝ち馬の特徴量は正しく予測に反映されているか？
print("【勝ち馬の特徴量 vs 予測】")
for feat in ["horse_last3f_avg", "horse_normalized_time_avg", "horse_last_last3f"]:
    if feat in test_df.columns:
        corr = winners[feat].corr(winners["pred"])
        print(f"  {feat} vs pred 相関: {corr:.3f}")

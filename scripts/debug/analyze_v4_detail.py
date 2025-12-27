#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""着順別MAEと詳細分析"""
import pandas as pd
import numpy as np
import pickle
import json
from pathlib import Path
import sys

project_root = Path('.')
sys.path.insert(0, str(project_root))

# V4モデル
model_dir = project_root / "keibaai/models/mu_time_v4"
with open(model_dir / "mu_time_model.pkl", "rb") as f:
    model = pickle.load(f)
with open(model_dir / "feature_names.json", "r") as f:
    feature_cols = json.load(f)

# データ
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
test_df["error_abs"] = test_df["error"].abs()

print("=== 着順別MAE ===")
for pos in range(1, 11):
    sub = test_df[test_df["finish_position"] == pos]
    if len(sub) > 100:
        mae = sub["error_abs"].mean()
        bias = sub["error"].mean()
        print(f"{pos}着: MAE={mae:.2f}秒, バイアス={bias:+.2f}秒, N={len(sub):,}")

print()
print("=== レースバイアス分析 ===")
race_mean_error = test_df.groupby("race_id")["error"].mean()
print(f"レース平均誤差: mean={race_mean_error.mean():.3f}秒, std={race_mean_error.std():.3f}秒")
print(f"レースバイアス > 1秒: {(race_mean_error.abs() > 1).sum()}レース ({(race_mean_error.abs() > 1).mean()*100:.1f}%)")
print(f"レースバイアス > 2秒: {(race_mean_error.abs() > 2).sum()}レース ({(race_mean_error.abs() > 2).mean()*100:.1f}%)")

print()
print("=== レース内タイム差の予測 ===")
actual = test_df.groupby("race_id")["finish_time_seconds"].agg(["max", "min"])
actual["spread"] = actual["max"] - actual["min"]
pred = test_df.groupby("race_id")["pred_time"].agg(["max", "min"])
pred["spread"] = pred["max"] - pred["min"]
print(f"実際: mean={actual['spread'].mean():.2f}秒, median={actual['spread'].median():.2f}秒")
print(f"予測: mean={pred['spread'].mean():.2f}秒, median={pred['spread'].median():.2f}秒")
print(f"差: {pred['spread'].mean() - actual['spread'].mean():+.2f}秒（予測がより狭い）")

print()
print("=== パーセンタイル分析 ===")
for p in [50, 75, 90, 95, 99]:
    val = np.percentile(test_df["error_abs"], p)
    print(f"  {p}%: {val:.2f}秒以内")

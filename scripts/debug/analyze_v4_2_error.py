#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""V4.2 誤差分解"""
import pandas as pd
import numpy as np
import pickle
import json
from pathlib import Path
import sys

project_root = Path(".")
sys.path.insert(0, str(project_root))

# V4.2モデル
model_dir = project_root / "keibaai/models/mu_time_v4_2"
with open(model_dir / "mu_time_model.pkl", "rb") as f:
    model = pickle.load(f)
with open(model_dir / "feature_names.json", "r") as f:
    feature_cols = json.load(f)

df = pd.read_parquet("keibaai/data/parsed/parquet/races/races.parquet")
df["race_date"] = pd.to_datetime(df["race_date"])
df = df.dropna(subset=["finish_position", "finish_time_seconds", "venue"])
df = df.drop_duplicates(subset=["race_id", "horse_number"])

from keibaai.src.features.time_feature_engineer_v4 import TimeFeatureEngineerV4

train_mask = df["race_date"] < "2023-01-01"
time_fe = TimeFeatureEngineerV4(min_samples=30)
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

print("=== V4.2 誤差分解 ===")
race_pace_error = test_df.groupby("race_id")["error"].mean()
test_df["race_pace_error"] = test_df["race_id"].map(race_pace_error)
test_df["individual_error"] = test_df["error"] - test_df["race_pace_error"]

all_mae = test_df["error"].abs().mean()
pace_mae = race_pace_error.abs().mean()
ind_mae = test_df["individual_error"].abs().mean()

print(f"全馬誤差: MAE = {all_mae:.3f}秒")
print(f"レースペース誤差: MAE = {pace_mae:.3f}秒 (V4.1: 0.816秒)")
print(f"個馬誤差: MAE = {ind_mae:.3f}秒 (V4.1: 0.796秒)")

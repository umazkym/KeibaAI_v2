#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""V4.1の予測結果を具体的なレースで確認"""
import pandas as pd
import numpy as np
import pickle
import json
from pathlib import Path
import sys

project_root = Path(".")
sys.path.insert(0, str(project_root))

# V4.1モデル
model_dir = project_root / "keibaai/models/mu_time_v4_1"
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

# Test期間
test_mask = (df["race_date"] >= "2023-01-01") & (df["race_date"] < "2024-01-01")
test_df = df[test_mask].copy()

available = [c for c in feature_cols if c in test_df.columns]
X = test_df[available].fillna(0)
test_df["pred"] = model.predict(X)
test_df["pred_time"] = test_df["pred"] * test_df["base_time_std"] + test_df["base_time_mean"]
test_df["error"] = test_df["pred_time"] - test_df["finish_time_seconds"]

# いくつかのレースをサンプル
sample_races = test_df.groupby("race_id").apply(lambda x: len(x) >= 10).reset_index()
sample_races = sample_races[sample_races[0] == True]["race_id"].sample(5, random_state=42).tolist()

print("=" * 80)
print("V4.1 予測結果サンプル（5レース）")
print("=" * 80)

for race_id in sample_races:
    race = test_df[test_df["race_id"] == race_id].sort_values("finish_position")
    
    race_info = race.iloc[0]
    print(f"\n【レース】{race_info['race_date'].strftime('%Y-%m-%d')} {race_info['venue']} {int(race_info['distance_m'])}m {race_info['track_surface']} {race_info['track_condition']}")
    print("-" * 80)
    print(f"{'着':>3} {'馬番':>4} {'予測タイム':>10} {'実タイム':>10} {'誤差':>8} {'予測順位':>8}")
    print("-" * 80)
    
    race = race.copy()
    race["pred_rank"] = race["pred_time"].rank()
    
    for _, row in race.head(10).iterrows():
        pos = int(row["finish_position"])
        num = int(row["horse_number"])
        pred_t = row["pred_time"]
        actual_t = row["finish_time_seconds"]
        err = row["error"]
        pred_r = int(row["pred_rank"])
        
        print(f"{pos:>3}着 {num:>4}番 {pred_t:>10.2f}秒 {actual_t:>10.2f}秒 {err:>+7.2f}秒 {pred_r:>5}位予測")
    
    # サマリー
    top3_correct = (race.head(3)["pred_rank"] <= 3).sum()
    winner_pred_rank = int(race[race["finish_position"] == 1]["pred_rank"].values[0])
    print(f"\n  1着馬の予測順位: {winner_pred_rank}位 / 上位3着中{top3_correct}頭を3位以内予測")

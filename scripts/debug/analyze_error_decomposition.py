#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
誤差を「レースペース誤差」と「個馬誤差」に分解して分析

全馬のタイム誤差 = レースペース誤差（レース全体のズレ）+ 個馬誤差（馬個別のズレ）
"""
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

print("=" * 70)
print("誤差分解分析: レースペース誤差 vs 個馬誤差")
print("=" * 70)

# レースごとの平均誤差（= レースペース誤差）
race_pace_error = test_df.groupby("race_id")["error"].mean()

# 個馬誤差 = 全体誤差 - レースペース誤差
test_df["race_pace_error"] = test_df["race_id"].map(race_pace_error)
test_df["individual_error"] = test_df["error"] - test_df["race_pace_error"]

print(f"\n全馬誤差: MAE = {test_df['error'].abs().mean():.3f}秒")
print(f"レースペース誤差: MAE = {race_pace_error.abs().mean():.3f}秒")
print(f"個馬誤差: MAE = {test_df['individual_error'].abs().mean():.3f}秒")
print()

# 分散で見る
total_var = test_df["error"].var()
pace_var = race_pace_error.var()
# 個馬誤差の分散（レース内のばらつき）
individual_var = test_df.groupby("race_id")["error"].var().mean()

print(f"全体誤差の分散: {total_var:.3f}")
print(f"レースペース誤差の分散: {pace_var:.3f} ({pace_var/total_var*100:.1f}%)")
print(f"個馬誤差の分散: {individual_var:.3f} ({individual_var/total_var*100:.1f}%)")
print()

print("=" * 70)
print("レースペース誤差の原因分析")
print("=" * 70)

# 基準タイムとの関係
test_df["base_time_error"] = test_df["base_time_mean"] - test_df.groupby("race_id")["finish_time_seconds"].transform("mean")
base_time_mae = test_df.groupby("race_id")["base_time_error"].first().abs().mean()
print(f"\n基準タイムの誤差: MAE = {base_time_mae:.3f}秒")
print("→ 基準タイムが正確なら、レースペース誤差はもっと小さいはず")

# レースペース誤差が大きいレースの特徴
high_pace_error = race_pace_error[race_pace_error.abs() > 2.0]
print(f"\nレースペース誤差 > 2秒のレース: {len(high_pace_error)}レース ({len(high_pace_error)/len(race_pace_error)*100:.1f}%)")

# 馬場別
print("\n馬場別のレースペース誤差:")
for surface in ["芝", "ダート"]:
    for cond in ["良", "稍重", "重", "不良"]:
        sub = test_df[(test_df["track_surface"] == surface) & (test_df["track_condition"] == cond)]
        if len(sub) > 100:
            race_ids = sub["race_id"].unique()
            sub_pace_error = race_pace_error[race_ids].abs().mean()
            print(f"  {surface}・{cond}: レースペースMAE = {sub_pace_error:.2f}秒")

print()
print("=" * 70)
print("改善の方向性")
print("=" * 70)

print("""
【問題の構造】
全馬誤差 (MAE {:.2f}秒) = レースペース誤差 ({:.2f}秒) + 個馬誤差 ({:.2f}秒)

【解決策】
1. レースペース予測を改善する
   - 当日の馬場状態（既走レースから推定）
   - ペース（逃げ馬の有無など）
   - 天候・含水率

2. 個馬予測を改善する
   - 馬の過去成績の詳細化
   - 上がり3Fの精緻化
""".format(
    test_df["error"].abs().mean(),
    race_pace_error.abs().mean(),
    test_df["individual_error"].abs().mean()
))

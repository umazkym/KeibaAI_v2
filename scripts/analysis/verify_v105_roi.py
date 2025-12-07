#!/usr/bin/env python3
"""
v10.5モデルのROI独立検証
学習時とは別のコードでROIを再計算し、本当にこの性能があるか確認
"""

import pandas as pd
import numpy as np
import pickle
import json
from pathlib import Path

print("=" * 60)
print("v10.5 ROI 独立検証")
print("=" * 60)
print()

# モデルとデータを読み込み
model_dir = Path("keibaai/models/mu_v10_5")
model = pickle.load(open(model_dir / "mu_v10_5_ranker.pkl", "rb"))
features = json.load(open(model_dir / "feature_names.json", encoding="utf-8"))
model_info = json.load(open(model_dir / "model_info.json", encoding="utf-8"))

print(f"モデル: v10.5")
print(f"特徴量数: {len(features)}")
print(f"記録されたTest ROI: {model_info['test_roi']:.2%}")
print()

# 完全に新しくデータを準備
print("データを新しく準備中...")
train_data = pd.read_parquet("keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet")
train_data["race_date"] = pd.to_datetime(train_data["race_date"])
train_data["horse_id"] = train_data["horse_id"].astype(str)

races = pd.read_parquet("keibaai/data/parsed/parquet/races/races.parquet")
corners = pd.read_parquet("keibaai/data/parsed/parquet/corners/corner_positions.parquet")

races["race_date"] = pd.to_datetime(races["race_date"])
races = races.sort_values(["race_date", "race_id"])
races["finish_position"] = pd.to_numeric(races["finish_position"], errors="coerce")
races["popularity"] = pd.to_numeric(races["popularity"], errors="coerce")
races["margin_seconds"] = pd.to_numeric(races["margin_seconds"], errors="coerce")
races["horse_id"] = races["horse_id"].astype(str)
races["jockey_id"] = races["jockey_id"].astype(str)
races["trainer_id"] = races["trainer_id"].astype(str)
races["is_win"] = (races["finish_position"] == 1).fillna(False).astype(int)

# リークフリー特徴量を再生成
print("リークフリー特徴量を再生成中...")
races["jockey_win_rate_lf"] = races.groupby("jockey_id")["is_win"].transform(
    lambda x: x.expanding().mean().shift(1)
)
races["trainer_win_rate_lf"] = races.groupby("trainer_id")["is_win"].transform(
    lambda x: x.expanding().mean().shift(1)
)
races["jockey_rank_lf"] = races.groupby("race_id")["jockey_win_rate_lf"].rank(ascending=False)
races["gap_jockey_pop_lf"] = races["popularity"] - races["jockey_rank_lf"]
races["trainer_rank_lf"] = races.groupby("race_id")["trainer_win_rate_lf"].rank(ascending=False)
races["gap_trainer_pop_lf"] = races["popularity"] - races["trainer_rank_lf"]
races["avg_margin_lf"] = races.groupby("horse_id")["margin_seconds"].transform(
    lambda x: x.expanding().mean().shift(1)
)

c4 = corners[corners["corner"] == 4][["race_id", "horse_number", "position", "gap_from_leader"]].copy()
c4.columns = ["race_id", "horse_number", "c4_position", "c4_gap"]
races["horse_number"] = pd.to_numeric(races["horse_number"], errors="coerce")
races = races.merge(c4, on=["race_id", "horse_number"], how="left")
races["avg_c4_position_lf"] = races.groupby("horse_id")["c4_position"].transform(
    lambda x: x.expanding().mean().shift(1)
)
races["avg_c4_gap_lf"] = races.groupby("horse_id")["c4_gap"].transform(
    lambda x: x.expanding().mean().shift(1)
)

# マージ
merge_cols = [
    "horse_id", "race_date",
    "jockey_win_rate_lf", "trainer_win_rate_lf",
    "gap_jockey_pop_lf", "gap_trainer_pop_lf",
    "avg_margin_lf", "avg_c4_position_lf", "avg_c4_gap_lf"
]
merge_df = races[merge_cols].drop_duplicates(subset=["horse_id", "race_date"], keep="last")
train_data = train_data.merge(merge_df, on=["horse_id", "race_date"], how="left")

train_data["win_odds"] = pd.to_numeric(train_data["win_odds"], errors="coerce")
train_data["finish_position"] = pd.to_numeric(train_data["finish_position"], errors="coerce")

# Test期間のみ
test_df = train_data[train_data["race_date"] >= "2024-01-01"].copy()
print(f"Test期間: {test_df['race_date'].min()} - {test_df['race_date'].max()}")
print(f"Testレコード数: {len(test_df):,}")
print(f"Testレース数: {test_df['race_id'].nunique():,}")
print()

# 特徴量の欠損を埋める
for col in features:
    if col in test_df.columns:
        test_df[col] = test_df[col].fillna(0)
    else:
        print(f"  警告: {col} がデータにない")
        test_df[col] = 0

# 予測
available_features = [f for f in features if f in test_df.columns]
print(f"利用可能特徴量: {len(available_features)}/{len(features)}")

preds = model.predict(test_df[available_features])
test_df["score"] = preds

# ROI計算（複数の方法で確認）
print()
print("=" * 60)
print("ROI検証（複数の方法で確認）")
print("=" * 60)
print()

# 方法1: 各レースで最高スコアの馬に賭ける
test_df["rank_pred"] = test_df.groupby("race_id")["score"].rank(ascending=False, method="first")
bet = test_df[test_df["rank_pred"] == 1]
hits = bet[bet["finish_position"] == 1]
roi1 = hits["win_odds"].sum() / len(bet) if len(bet) > 0 else 0

print(f"【方法1】全レースTop1に賭ける")
print(f"  賭け数: {len(bet):,}")
print(f"  的中数: {len(hits):,}")
print(f"  的中率: {len(hits)/len(bet):.1%}")
print(f"  回収額: {hits['win_odds'].sum():,.0f}倍")
print(f"  ROI: {roi1:.2%}")
print()

# 方法2: 確認のため手計算
total_bet = len(bet)
total_return = hits["win_odds"].sum()
roi2 = total_return / total_bet

print(f"【方法2】手計算確認")
print(f"  投資額: {total_bet}（各100円なら{total_bet*100:,}円）")
print(f"  回収額: {total_return:.1f}倍（各100円なら{total_return*100:,.0f}円）")
print(f"  ROI: {roi2:.2%}")
print()

# 人気別の的中率
print("【人気別分析】")
for pop in [1, 2, 3, 5, 8, 10]:
    pop_hits = hits[hits["popularity"] == pop]
    pop_bets = bet[bet["popularity"] == pop]
    if len(pop_bets) > 0:
        print(f"  {pop}番人気を選択: {len(pop_bets):4}回, 的中{len(pop_hits):3}回, 的中率{len(pop_hits)/len(pop_bets):.1%}")

print()
print("=" * 60)
print("【結論】")
print("=" * 60)
print(f"  記録されたROI: {model_info['test_roi']:.2%}")
print(f"  再検証ROI: {roi1:.2%}")
print(f"  差: {abs(roi1 - model_info['test_roi']):.2%}")

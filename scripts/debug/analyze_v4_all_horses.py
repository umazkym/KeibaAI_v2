#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μ_time_v4 全馬予測精度の深層分析

1着だけでなく、全馬の予測精度を詳細に分析
"""
import sys
import json
import pickle
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

def main():
    print("=" * 70)
    print("μ_time_v4 全馬予測精度の深層分析")
    print("=" * 70)
    
    # V4モデルロード
    model_dir = project_root / "keibaai" / "models" / "mu_time_v4"
    with open(model_dir / "mu_time_model.pkl", "rb") as f:
        model = pickle.load(f)
    with open(model_dir / "feature_names.json", "r") as f:
        feature_cols = json.load(f)
    
    # データ読み込み
    df = pd.read_parquet(project_root / "keibaai/data/parsed/parquet/races/races.parquet")
    df["race_date"] = pd.to_datetime(df["race_date"])
    df = df.dropna(subset=["finish_position", "finish_time_seconds", "venue"])
    df = df.drop_duplicates(subset=["race_id", "horse_number"], keep="first")
    
    # 特徴量生成
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
    
    # 予測
    available = [c for c in feature_cols if c in test_df.columns]
    X = test_df[available].fillna(0)
    test_df["pred"] = model.predict(X)
    test_df["pred_time"] = test_df["pred"] * test_df["base_time_std"] + test_df["base_time_mean"]
    test_df["error"] = test_df["pred_time"] - test_df["finish_time_seconds"]
    test_df["error_abs"] = test_df["error"].abs()
    
    print(f"\nTest期間: {len(test_df):,}頭")
    print(f"レース数: {test_df['race_id'].nunique():,}")
    
    # ===== 1. 全体の誤差分布 =====
    print("\n" + "=" * 70)
    print("1. 全馬の誤差分布")
    print("=" * 70)
    
    print(f"誤差(秒): mean={test_df['error'].mean():.3f}, std={test_df['error'].std():.3f}")
    print(f"MAE: {test_df['error_abs'].mean():.3f}秒")
    print(f"Median Error: {test_df['error_abs'].median():.3f}秒")
    print()
    
    print("パーセンタイル:")
    for p in [10, 25, 50, 75, 90, 95, 99]:
        val = np.percentile(test_df["error_abs"], p)
        print(f"  {p}%: {val:.2f}秒以内")
    
    # ===== 2. 順位別の誤差 =====
    print("\n" + "=" * 70)
    print("2. 着順別の予測誤差")
    print("=" * 70)
    
    for pos in range(1, 11):
        sub = test_df[test_df["finish_position"] == pos]
        if len(sub) > 100:
            mae = sub["error_abs"].mean()
            bias = sub["error"].mean()
            print(f"  {pos}着: MAE={mae:.2f}秒, バイアス={bias:+.2f}秒, N={len(sub):,}")
    
    # ===== 3. レース内での誤差の一貫性 =====
    print("\n" + "=" * 70)
    print("3. レース内での予測誤差の分析")
    print("=" * 70)
    
    def analyze_race(group):
        errors = group["error"].values
        return pd.Series({
            "mean_error": errors.mean(),
            "std_error": errors.std(),
            "max_error": errors.max(),
            "min_error": errors.min(),
            "spread": errors.max() - errors.min(),
        })
    
    race_analysis = test_df.groupby("race_id").apply(analyze_race, include_groups=False)
    
    print(f"レース平均バイアス: mean={race_analysis['mean_error'].mean():.3f}秒, std={race_analysis['mean_error'].std():.3f}秒")
    print(f"レース内誤差のばらつき(std): mean={race_analysis['std_error'].mean():.3f}秒")
    print(f"レース内誤差のspread: mean={race_analysis['spread'].mean():.3f}秒")
    
    # レースバイアスが大きいケース
    high_bias_races = race_analysis[race_analysis["mean_error"].abs() > 2.0]
    print(f"\nレースバイアス > 2秒のレース: {len(high_bias_races):,}レース ({len(high_bias_races)/len(race_analysis)*100:.1f}%)")
    
    # ===== 4. 予測が「ずれる」パターンの分析 =====
    print("\n" + "=" * 70)
    print("4. 予測がずれるパターン分析")
    print("=" * 70)
    
    # レースのペース（レース全体の速さ）との関係
    race_speed = test_df.groupby("race_id")["finish_time_seconds"].mean()
    race_pred = test_df.groupby("race_id")["pred_time"].mean()
    race_diff = (race_pred - race_speed).rename("pace_diff")
    
    print(f"レース平均タイムの予測誤差: mean={race_diff.mean():.3f}秒, std={race_diff.std():.3f}秒")
    
    # ===== 5. 理想値との比較 =====
    print("\n" + "=" * 70)
    print("5. 理想値との比較（μモデルの限界点）")
    print("=" * 70)
    
    # レース内の実際のタイム差（1着-最下位）
    actual_spread = test_df.groupby("race_id").apply(
        lambda g: g["finish_time_seconds"].max() - g["finish_time_seconds"].min()
    )
    pred_spread = test_df.groupby("race_id").apply(
        lambda g: g["pred_time"].max() - g["pred_time"].min()
    )
    
    print(f"実際のレース内タイム差: mean={actual_spread.mean():.2f}秒, median={actual_spread.median():.2f}秒")
    print(f"予測のレース内タイム差: mean={pred_spread.mean():.2f}秒, median={pred_spread.median():.2f}秒")
    print(f"→ 予測は実際より{'広い' if pred_spread.mean() > actual_spread.mean() else '狭い'}")
    
    # ===== 6. 馬の過去成績との関係 =====
    print("\n" + "=" * 70)
    print("6. 馬の経験と予測精度")
    print("=" * 70)
    
    if "horse_last_time" in test_df.columns:
        # 過去走データがある馬 vs ない馬
        has_history = test_df[test_df["horse_last_time"].notna()]
        no_history = test_df[test_df["horse_last_time"].isna()]
        
        print(f"過去走あり: MAE={has_history['error_abs'].mean():.2f}秒, N={len(has_history):,}")
        print(f"過去走なし(新馬等): MAE={no_history['error_abs'].mean():.2f}秒, N={len(no_history):,}")
    
    # ===== 7. 改善の余地 =====
    print("\n" + "=" * 70)
    print("7. 改善の余地")
    print("=" * 70)
    
    # もし「レースのペース」を完璧に予測できたら？
    # = 各馬の「レース平均からの偏差」を予測すればよい
    test_df["race_mean_time"] = test_df.groupby("race_id")["finish_time_seconds"].transform("mean")
    test_df["time_from_mean"] = test_df["finish_time_seconds"] - test_df["race_mean_time"]
    
    print("もしレースの平均タイムが完璧に分かっていたら...")
    print(f"  予測すべき: 各馬のレース平均からの偏差")
    print(f"  偏差の分布: mean={test_df['time_from_mean'].mean():.3f}秒, std={test_df['time_from_mean'].std():.3f}秒")
    
    # 現在の予測の「偏差成分」の精度
    test_df["pred_race_mean"] = test_df.groupby("race_id")["pred_time"].transform("mean")
    test_df["pred_from_mean"] = test_df["pred_time"] - test_df["pred_race_mean"]
    
    corr = test_df["time_from_mean"].corr(test_df["pred_from_mean"])
    print(f"  偏差成分の相関: {corr:.4f}")
    
    print()
    print("=" * 70)
    print("分析完了")
    print("=" * 70)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SP値改善 検証スクリプト

改善前後のSP値を比較し、以下の指標を評価:
1. 馬別SP一貫性（標準偏差）
2. SP-着順 Spearman相関
3. Top3的中率
4. ケーススタディ（モズロックンロール）

Usage:
  python scripts/analysis/sp_analysis/validate_sp_improvements.py
"""

import sys
import logging
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd
from scipy import stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def compute_metrics(sp_df: pd.DataFrame, sp_col: str, label: str) -> dict:
    """SP値の精度メトリクスを計算。"""
    df = sp_df.dropna(subset=[sp_col, "finish_position"]).copy()
    if len(df) == 0:
        return {}

    df["sp_rank"] = df.groupby("race_id")[sp_col].rank(ascending=False)

    # 1. 馬別SP一貫性（5走以上の馬のSP標準偏差）
    horse_stats = (
        df.groupby("horse_id")[sp_col]
        .agg(sp_std="std", sp_count="count")
        .reset_index()
    )
    horse_5plus = horse_stats[horse_stats["sp_count"] >= 5]
    mean_std = horse_5plus["sp_std"].mean() if len(horse_5plus) > 0 else np.nan

    # 2. Spearman相関（レースごと）
    corrs = []
    for _, group in df.groupby("race_id"):
        if len(group) >= 4:
            c, _ = stats.spearmanr(group[sp_col], group["finish_position"])
            corrs.append(c)
    mean_corr = np.mean(corrs) if corrs else np.nan

    # 3. Top3的中率
    n_races = df["race_id"].nunique()
    winner_hits = 0
    place_hits = 0
    for rid in df["race_id"].unique():
        r = df[df["race_id"] == rid]
        top = r[r["sp_rank"] <= 3]
        if (top["finish_position"] == 1).any():
            winner_hits += 1
        place_hits += (top["finish_position"] <= 3).sum()

    return {
        "label": label,
        "n_records": len(df),
        "n_races": n_races,
        "horse_sp_std": round(mean_std, 3),
        "spearman": round(mean_corr, 4),
        "winner_hit_rate": round(winner_hits / n_races * 100, 1) if n_races > 0 else 0,
        "place_hit_rate": round(place_hits / (n_races * 3) * 100, 1) if n_races > 0 else 0,
    }


def main():
    print("=" * 70)
    print("  SP値改善 検証スクリプト")
    print("=" * 70)

    from keibaai.src.analysis.speed_index.calculator import SpeedIndexCalculator, get_best_sp_col
    from keibaai.src.analysis.speed_index.pace_correction import PaceCorrectionEngine

    # --- データ読み込み ---
    logger.info("データ読み込み中...")
    data_root = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"
    races = pd.read_parquet(data_root / "races" / "races.parquet")
    rd = pd.read_parquet(data_root / "race_details" / "race_details.parquet")

    races["race_date"] = pd.to_datetime(races["race_date"])
    races["race_id"] = races["race_id"].astype(str)

    # 検証対象: 2024年のレース（2023年以前でfit）
    train = races[races["race_date"].dt.year <= 2023]
    test = races[races["race_date"].dt.year == 2024]
    logger.info(f"  学習: {len(train):,} rows, テスト: {len(test):,} rows (2024年)")

    # =============================================
    # A. 改善前（従来手法）
    # =============================================
    print("\n" + "-" * 70)
    print("A. 改善前（良馬場基準 + mean馬場指数）")
    print("-" * 70)

    start = time.time()
    calc_old = SpeedIndexCalculator(use_condition_base=False)
    calc_old.fit(train)
    sp_old = calc_old.calculate(test)

    # ペース補正
    pace_engine = PaceCorrectionEngine()
    pace_engine.fit(train, rd)
    test_rd = rd[rd["race_id"].astype(str).isin(sp_old["race_id"])]
    if len(test_rd) > 0:
        sp_old = pace_engine.apply(sp_old, test, test_rd)
    sp_col_old = "sp_extended" if "sp_extended" in sp_old.columns else "sp_raw"

    metrics_old = compute_metrics(sp_old, sp_col_old, "改善前")
    elapsed_old = time.time() - start

    # =============================================
    # B. 改善後（条件別基準 + median馬場指数）
    # =============================================
    print("\n" + "-" * 70)
    print("B. 改善後（条件別基準 + median馬場指数）")
    print("-" * 70)

    start = time.time()
    calc_new = SpeedIndexCalculator(use_condition_base=True)
    calc_new.fit(train)
    sp_new = calc_new.calculate(test)

    # ペース補正
    if len(test_rd) > 0:
        sp_new = pace_engine.apply(sp_new, test, test_rd)
    sp_col_new = "sp_extended" if "sp_extended" in sp_new.columns else "sp_raw"

    metrics_new_base = compute_metrics(sp_new, sp_col_new, "改善後(基準+馬場)")
    elapsed_new = time.time() - start

    # =============================================
    # C. 改善後 + レースレベル補正
    # =============================================
    print("\n" + "-" * 70)
    print("C. 改善後 + レースレベル補正")
    print("-" * 70)

    start = time.time()
    sp_refined = calc_new.refine_with_race_quality(sp_new)
    metrics_refined = compute_metrics(sp_refined, "sp_refined", "改善後+レベル補正")
    elapsed_refined = time.time() - start

    # =============================================
    # 結果比較
    # =============================================
    print("\n" + "=" * 70)
    print("  結果比較")
    print("=" * 70)

    results = pd.DataFrame([metrics_old, metrics_new_base, metrics_refined])
    print("\n" + results.to_string(index=False))

    print(f"\n  処理時間: 改善前={elapsed_old:.1f}s, 改善後={elapsed_new:.1f}s, レベル補正={elapsed_refined:.1f}s")

    # =============================================
    # ケーススタディ: モズロックンロール
    # =============================================
    print("\n" + "=" * 70)
    print("  ケーススタディ: モズロックンロール 2024/11/02 東京芝1800m")
    print("=" * 70)

    target_race_id = "202405050110"
    for label, sp_df, sp_col in [
        ("改善前", sp_old, sp_col_old),
        ("改善後", sp_new, sp_col_new),
        ("レベル補正後", sp_refined, "sp_refined"),
    ]:
        race = sp_df[sp_df["race_id"] == target_race_id].sort_values("finish_position")
        if len(race) == 0:
            print(f"\n  {label}: データなし")
            continue

        moz = race[race["horse_name"] == "モズロックンロール"]
        winner = race.iloc[0]
        print(f"\n  【{label}】")
        print(f"    勝ち馬SP: {winner[sp_col]:.1f}")
        if len(moz) > 0:
            moz_row = moz.iloc[0]
            adj = moz_row.get("race_quality_adj", 0)
            adj_str = f" (補正: {adj:+.1f})" if adj != 0 else ""
            print(f"    モズSP:   {moz_row[sp_col]:.1f}{adj_str}")
            print(f"    SP差:     {winner[sp_col] - moz_row[sp_col]:.1f}")

    # =============================================
    # レースレベル補正の分布
    # =============================================
    if "race_quality_adj" in sp_refined.columns:
        adj = sp_refined.drop_duplicates("race_id")["race_quality_adj"]
        adj_valid = adj[adj.abs() > 0.01]
        print(f"\n\n  【レースレベル補正値の分布】")
        print(f"    適用レース数: {len(adj_valid):,} / {len(adj):,}")
        if len(adj_valid) > 0:
            print(f"    平均: {adj_valid.mean():+.2f}")
            print(f"    標準偏差: {adj_valid.std():.2f}")
            print(f"    最小: {adj_valid.min():+.2f}")
            print(f"    最大: {adj_valid.max():+.2f}")

    print("\n" + "=" * 70)
    print("  検証完了")
    print("=" * 70)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SP値計算システム — 動作確認スクリプト

Phase 1-2 の基本動作を検証し、サンプル結果を表示する。

実行:
  python scripts/analysis/sp_analysis/run_sp_analysis.py
"""

import sys
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import numpy as np

from keibaai.src.analysis.speed_index.calculator import SpeedIndexCalculator, get_best_sp_col
from keibaai.src.analysis.speed_index.pace_correction import PaceCorrectionEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    print("=" * 70)
    print("  SP値（スピード指数）計算システム — 動作確認")
    print("=" * 70)

    # --- データ読み込み ---
    logger.info("データ読み込み中...")
    data_root = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"

    races = pd.read_parquet(data_root / "races" / "races.parquet")
    rd = pd.read_parquet(data_root / "race_details" / "race_details.parquet")

    logger.info(f"  races: {len(races):,} rows")
    logger.info(f"  race_details: {len(rd):,} rows")

    # --- Phase 1: SP値算出 ---
    print("\n" + "-" * 70)
    print("Phase 1: 基本SP値算出")
    print("-" * 70)

    calc = SpeedIndexCalculator(weight_coeff=1.0)
    calc.fit(races)

    # 直近のレース（2025年）のSP値を算出
    races["race_date"] = pd.to_datetime(races["race_date"])
    test_races = races[races["race_date"].dt.year == 2025]
    logger.info(f"  2025年のレース数: {len(test_races):,}")

    sp_result = calc.calculate(test_races)
    logger.info(f"  SP値有効件数: {sp_result['sp_raw'].notna().sum():,}")

    # 基準タイムのサンプル表示
    print("\n【基準タイム（一部）】")
    bt = calc.base_times
    sample_bt = bt[bt["base_time_sufficient"]].sort_values("base_time_sample_count", ascending=False).head(10)
    print(sample_bt[["venue", "track_surface", "distance_m", "base_time", "base_time_sample_count"]].to_string(index=False))

    # SP値分布
    print("\n【SP値（sp_raw）分布】")
    desc = sp_result["sp_raw"].describe()
    for k, v in desc.items():
        print(f"  {k}: {v:.2f}")

    # 信頼度別
    print("\n【信頼度別SP値】")
    for rel in ["A", "B", "C"]:
        sub = sp_result[sp_result["reliability"] == rel]
        if len(sub) > 0:
            print(f"  {rel}: {len(sub):,} 件, mean={sub['sp_raw'].mean():.2f}, std={sub['sp_raw'].std():.2f}")

    # --- Phase 2: ペース補正 ---
    print("\n" + "-" * 70)
    print("Phase 2: ペース補正")
    print("-" * 70)

    pace_engine = PaceCorrectionEngine(leading_coeff=0.5, closing_coeff=0.3)
    pace_engine.fit(races, rd)

    # ペースデータのあるレースのみ
    test_rd = rd[rd["race_id"].astype(str).isin(sp_result["race_id"])]
    logger.info(f"  ペースデータ有効レース数: {len(test_rd):,}")

    if len(test_rd) > 0:
        sp_extended = pace_engine.apply(sp_result, test_races, test_rd)

        print("\n【ペース補正後SP値（sp_extended）分布】")
        desc_ext = sp_extended["sp_extended"].describe()
        for k, v in desc_ext.items():
            print(f"  {k}: {v:.2f}")

        # 脚質別
        print("\n【脚質別ペース補正】")
        for style in ["逃げ", "先行", "差し", "追込"]:
            sub = sp_extended[sp_extended["running_style"] == style]
            if len(sub) > 0:
                print(
                    f"  {style}: {len(sub):,} 件, "
                    f"補正平均={sub['pace_correction'].mean():+.2f}, "
                    f"sp_ext平均={sub['sp_extended'].mean():.2f}"
                )

        # ペースカテゴリ別
        print("\n【ペースカテゴリ別】")
        for cat in ["H", "M", "S"]:
            sub = sp_extended[sp_extended["pace_category"] == cat]
            if len(sub) > 0:
                print(f"  {cat}: {len(sub):,} レース件")

        # サンプルレース表示（SP値上位5頭）
        sample_race_id = sp_extended.dropna(subset=["sp_extended"]).iloc[0]["race_id"]
        sample_race = sp_extended[sp_extended["race_id"] == sample_race_id].sort_values(
            "sp_extended", ascending=False
        )
        print(f"\n【サンプルレース: {sample_race_id}】")
        show_cols = [
            "horse_name", "finish_position", "sp_raw", "sp_extended",
            "pace_correction", "running_style", "reliability",
        ]
        show_cols = [c for c in show_cols if c in sample_race.columns]
        print(sample_race[show_cols].head(10).to_string(index=False))
    else:
        print("  ※ 2025年のペースデータがありません")

    # --- Phase 3: レースレベル補正 ---
    print("\n" + "-" * 70)
    print("Phase 3: レースレベル補正")
    print("-" * 70)

    target = sp_extended if len(test_rd) > 0 else sp_result
    target = calc.refine_with_race_quality(target)

    # --- 簡易精度検証 ---
    print("\n" + "-" * 70)
    print("簡易精度検証（2025年）")
    print("-" * 70)

    sp_col = get_best_sp_col(target)
    valid = target.dropna(subset=[sp_col, "finish_position"]).copy()
    valid["sp_rank"] = valid.groupby("race_id")[sp_col].rank(ascending=False)

    # SP値上位3頭の複勝率
    from scipy import stats as sp_stats

    n_races = valid["race_id"].nunique()
    top3 = valid[valid["sp_rank"] <= 3]
    place_hits = (top3["finish_position"] <= 3).sum()
    winner_hits = 0
    for rid in valid["race_id"].unique():
        r = valid[valid["race_id"] == rid]
        t = r[r["sp_rank"] <= 3]
        if (t["finish_position"] == 1).any():
            winner_hits += 1

    print(f"  レース数: {n_races:,}")
    print(f"  SP上位3頭の複勝率: {place_hits / (n_races * 3) * 100:.1f}%")
    print(f"  SP上位3頭に1着含む率: {winner_hits / n_races * 100:.1f}%")

    # Spearman相関
    corrs = []
    for rid in valid["race_id"].unique():
        r = valid[valid["race_id"] == rid]
        if len(r) >= 4:
            c, _ = sp_stats.spearmanr(r[sp_col], r["finish_position"])
            corrs.append(c)
    print(f"  Spearman相関（平均）: {np.mean(corrs):.4f}")

    print("\n" + "=" * 70)
    print("動作確認完了")
    print("=" * 70)


if __name__ == "__main__":
    main()

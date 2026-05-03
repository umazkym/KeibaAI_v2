#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SP値 係数キャリブレーション実行スクリプト

グリッドサーチで最適なペース補正係数・斤量補正係数を探索する。
結果はCSVとコンソールに出力。

実行:
  python scripts/analysis/sp_analysis/run_calibration.py
"""

import sys
import logging
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    print("=" * 70)
    print("  SP値 係数キャリブレーション")
    print("=" * 70)

    from keibaai.src.analysis.speed_index.calibration import SPCalibrator

    # --- データ読み込み ---
    logger.info("データ読み込み中...")
    data_root = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"
    races = pd.read_parquet(data_root / "races" / "races.parquet")
    rd = pd.read_parquet(data_root / "race_details" / "race_details.parquet")

    # --- グリッドサーチ設定 ---
    param_grid = {
        "weight_coeff": [0.3, 0.5, 0.8, 1.0, 1.2, 1.5],
        "leading_coeff": [0.3, 0.5, 0.7],
        "closing_coeff": [0.2, 0.3, 0.5],
    }

    # 検証年度: 2020-2025（6年間）
    validation_years = list(range(2020, 2026))

    total_combos = 1
    for v in param_grid.values():
        total_combos *= len(v)
    total_evals = total_combos * len(validation_years)

    print(f"\nパラメータグリッド:")
    for k, v in param_grid.items():
        print(f"  {k}: {v}")
    print(f"\n組み合わせ数: {total_combos}")
    print(f"検証年度: {validation_years}")
    print(f"総評価回数: {total_evals}")
    print(f"推定所要時間: {total_evals * 3 // 60}〜{total_evals * 5 // 60} 分")
    print()

    # --- 実行 ---
    start = time.time()

    calibrator = SPCalibrator(
        param_grid=param_grid,
        validation_years=validation_years,
    )
    results = calibrator.run(races, rd, top_n=3)

    elapsed = time.time() - start

    # --- 結果出力 ---
    print("\n" + "=" * 70)
    print("  キャリブレーション結果")
    print("=" * 70)

    summary = calibrator.get_summary()
    print("\n【上位10パラメータ】")
    print(summary.head(10).to_string(index=False))

    print(f"\n【最適パラメータ】")
    print(f"  {calibrator.best_params}")

    # CSV保存
    output_dir = PROJECT_ROOT / "outputs" / "reports" / "sp_analysis"
    output_dir.mkdir(parents=True, exist_ok=True)

    results_path = output_dir / "calibration_results.csv"
    results.to_csv(results_path, index=False, encoding="utf-8-sig")
    print(f"\n  詳細結果: {results_path}")

    summary_path = output_dir / "calibration_summary.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    print(f"  サマリー: {summary_path}")

    print(f"\n  所要時間: {elapsed:.0f} 秒 ({elapsed/60:.1f} 分)")


if __name__ == "__main__":
    main()

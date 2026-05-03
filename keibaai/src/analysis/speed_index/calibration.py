#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ペース補正係数のグリッドサーチ・キャリブレーション

【概要】
複数の係数パターンを年度Walk-forwardで検証し、
最適なペース補正係数を探索する。

【評価指標】
1. SP値上位3頭の複勝率（Top3 Hit Rate）
2. SP値と着順のSpearman相関
3. 年度別の安定性（標準偏差）
"""

import logging
import itertools
from typing import List, Dict, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats

from .calculator import SpeedIndexCalculator
from .pace_correction import PaceCorrectionEngine

logger = logging.getLogger(__name__)


class SPCalibrator:
    """
    グリッドサーチによるSP値係数の最適化。

    Usage:
        calibrator = SPCalibrator()
        results = calibrator.run(races_df, race_details_df)
        print(calibrator.best_params)
    """

    DEFAULT_PARAM_GRID = {
        "leading_coeff": [0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
        "closing_coeff": [0.1, 0.2, 0.3, 0.4, 0.5],
        "weight_coeff": [0.5, 0.8, 1.0, 1.2, 1.5],
    }

    def __init__(
        self,
        param_grid: Optional[Dict[str, List[float]]] = None,
        validation_years: Optional[List[int]] = None,
    ):
        """
        Args:
            param_grid: 検証するパラメータグリッド
            validation_years: 検証に使う年度リスト（デフォルト: 2018-2025）
        """
        self.param_grid = param_grid or self.DEFAULT_PARAM_GRID
        self.validation_years = validation_years or list(range(2018, 2026))

        self.results: Optional[pd.DataFrame] = None
        self.best_params: Optional[Dict] = None

    def run(
        self,
        races_df: pd.DataFrame,
        race_details_df: pd.DataFrame,
        top_n: int = 3,
    ) -> pd.DataFrame:
        """
        グリッドサーチを実行し、全パラメータ組み合わせの結果を返す。

        Args:
            races_df: races.parquet
            race_details_df: race_details.parquet
            top_n: 上位N頭の的中率を評価

        Returns:
            各パラメータ組み合わせ × 年度の結果DataFrame
        """
        logger.info("=" * 60)
        logger.info("SPCalibrator: グリッドサーチ開始")
        logger.info(f"  パラメータグリッド: {self.param_grid}")
        logger.info(f"  検証年度: {self.validation_years}")
        logger.info("=" * 60)

        # 前処理
        races = races_df.copy()
        races["race_date"] = pd.to_datetime(races["race_date"])
        races["year"] = races["race_date"].dt.year
        races["race_id"] = races["race_id"].astype(str)

        rd = race_details_df.copy()
        rd["race_id"] = rd["race_id"].astype(str)

        # パラメータの全組み合わせ
        keys = list(self.param_grid.keys())
        values = list(self.param_grid.values())
        combinations = list(itertools.product(*values))

        total = len(combinations)
        logger.info(f"  組み合わせ数: {total}")

        all_results = []

        for idx, combo in enumerate(combinations):
            params = dict(zip(keys, combo))
            logger.info(
                f"  [{idx+1}/{total}] α={params['leading_coeff']}, "
                f"β={params['closing_coeff']}, w={params['weight_coeff']}"
            )

            # 年度ごとのWalk-forward検証
            for year in self.validation_years:
                try:
                    metrics = self._evaluate_year(
                        races, rd, year, params, top_n
                    )
                    metrics.update(params)
                    metrics["year"] = year
                    all_results.append(metrics)
                except Exception as e:
                    logger.warning(f"    year={year} 失敗: {e}")

        self.results = pd.DataFrame(all_results)

        if len(self.results) > 0:
            # 最適パラメータ: 全年度平均の top3_hit_rate が最大
            summary = (
                self.results.groupby(keys)
                .agg(
                    mean_hit_rate=("top3_hit_rate", "mean"),
                    std_hit_rate=("top3_hit_rate", "std"),
                    mean_spearman=("spearman_corr", "mean"),
                )
                .reset_index()
                .sort_values("mean_hit_rate", ascending=False)
            )
            best_row = summary.iloc[0]
            self.best_params = {k: best_row[k] for k in keys}

            logger.info(f"\n  最適パラメータ: {self.best_params}")
            logger.info(f"  平均Hit Rate: {best_row['mean_hit_rate']:.4f}")
            logger.info(f"  平均Spearman: {best_row['mean_spearman']:.4f}")

        return self.results

    def _evaluate_year(
        self,
        races: pd.DataFrame,
        rd: pd.DataFrame,
        target_year: int,
        params: Dict[str, float],
        top_n: int,
    ) -> Dict:
        """1年度分の検証を実行。"""
        # Train: target_year より前の全データ
        train_races = races[races["year"] < target_year]
        # Test: target_year のデータ
        test_races = races[races["year"] == target_year]

        if len(test_races) == 0:
            return {"top3_hit_rate": np.nan, "spearman_corr": np.nan, "n_races": 0}

        # SP値計算（Trainデータでfit）
        calc = SpeedIndexCalculator(weight_coeff=params["weight_coeff"])
        calc.fit(train_races)

        # Test期間のSP値算出
        sp_result = calc.calculate(test_races)

        # ペース補正適用
        # race_detailsをtest期間でフィルタ
        test_race_ids = set(test_races["race_id"].unique())
        test_rd = rd[rd["race_id"].isin(test_race_ids)]

        if len(test_rd) > 0:
            pace_engine = PaceCorrectionEngine(
                leading_coeff=params["leading_coeff"],
                closing_coeff=params["closing_coeff"],
            )
            pace_engine.fit(train_races, rd)
            sp_result = pace_engine.apply(sp_result, test_races, test_rd)
            sp_col = "sp_extended"
        else:
            sp_col = "sp_raw"

        # 評価
        return self._compute_metrics(sp_result, sp_col, top_n)

    @staticmethod
    def _compute_metrics(
        sp_df: pd.DataFrame, sp_col: str, top_n: int
    ) -> Dict:
        """SP値の予測精度メトリクスを計算。"""
        df = sp_df.dropna(subset=[sp_col, "finish_position"]).copy()
        if len(df) == 0:
            return {"top3_hit_rate": np.nan, "spearman_corr": np.nan, "n_races": 0}

        # レースごとにSP値順位を付与
        df["sp_rank"] = df.groupby("race_id")[sp_col].rank(ascending=False)

        # 1. Top N Hit Rate: SP値上位N頭に1着馬が含まれる割合
        race_results = []
        for race_id, group in df.groupby("race_id"):
            top_horses = group[group["sp_rank"] <= top_n]
            winner_in_top = (top_horses["finish_position"] == 1).any()
            place_in_top = (top_horses["finish_position"] <= 3).any()
            race_results.append({
                "race_id": race_id,
                "winner_hit": winner_in_top,
                "place_hit": place_in_top,
            })
        race_res_df = pd.DataFrame(race_results)
        n_races = len(race_res_df)

        # 2. Spearman相関（レースごとに計算して平均）
        spearman_list = []
        for _, group in df.groupby("race_id"):
            if len(group) >= 4:
                corr, _ = stats.spearmanr(
                    group[sp_col], group["finish_position"]
                )
                spearman_list.append(corr)

        return {
            "top3_hit_rate": race_res_df["place_hit"].mean() if n_races > 0 else np.nan,
            "winner_hit_rate": race_res_df["winner_hit"].mean() if n_races > 0 else np.nan,
            "spearman_corr": np.mean(spearman_list) if spearman_list else np.nan,
            "n_races": n_races,
        }

    def get_summary(self) -> pd.DataFrame:
        """パラメータ別の集計サマリーを返す。"""
        if self.results is None:
            raise RuntimeError("run() を先に実行してください。")

        keys = list(self.param_grid.keys())
        return (
            self.results.groupby(keys)
            .agg(
                mean_hit_rate=("top3_hit_rate", "mean"),
                std_hit_rate=("top3_hit_rate", "std"),
                mean_winner_hit=("winner_hit_rate", "mean"),
                mean_spearman=("spearman_corr", "mean"),
                total_races=("n_races", "sum"),
            )
            .reset_index()
            .sort_values("mean_hit_rate", ascending=False)
        )

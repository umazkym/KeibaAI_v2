#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
競馬場別多年度分析

【概要】
競馬場×コース×距離ごとに基準タイムの推移、馬場指数の分布、
SP値の予測精度、ペース傾向を分析する。
"""

import logging
from typing import Optional, Dict, List

import numpy as np
import pandas as pd
from scipy import stats

from .calculator import SpeedIndexCalculator
from .pace_correction import PaceCorrectionEngine

logger = logging.getLogger(__name__)


class VenueAnalyzer:
    """
    競馬場別の多年度分析を行うクラス。

    Usage:
        analyzer = VenueAnalyzer()
        analyzer.fit(races_df, race_details_df)
        report = analyzer.generate_report()
    """

    def __init__(self):
        self._races: Optional[pd.DataFrame] = None
        self._race_details: Optional[pd.DataFrame] = None
        self._sp_result: Optional[pd.DataFrame] = None

    def fit(
        self,
        races_df: pd.DataFrame,
        race_details_df: pd.DataFrame,
        sp_calculator: Optional[SpeedIndexCalculator] = None,
        pace_engine: Optional[PaceCorrectionEngine] = None,
    ) -> "VenueAnalyzer":
        """
        分析データを準備する。

        Args:
            races_df: races.parquet
            race_details_df: race_details.parquet
            sp_calculator: 事前にfitされたSP値計算器（Noneなら自動作成）
            pace_engine: 事前にfitされたペース補正器（Noneなら自動作成）
        """
        logger.info("VenueAnalyzer: データ準備開始")
        self._races = races_df.copy()
        self._races["race_date"] = pd.to_datetime(self._races["race_date"])
        self._races["year"] = self._races["race_date"].dt.year
        self._races["race_id"] = self._races["race_id"].astype(str)

        self._race_details = race_details_df.copy()
        self._race_details["race_id"] = self._race_details["race_id"].astype(str)

        # SP値算出
        if sp_calculator is None:
            sp_calculator = SpeedIndexCalculator()
            sp_calculator.fit(self._races)

        self._sp_result = sp_calculator.calculate(self._races)

        # ペース補正
        if pace_engine is None:
            pace_engine = PaceCorrectionEngine()
            pace_engine.fit(self._races, self._race_details)

        rd_ids = set(self._race_details["race_id"].unique())
        sp_with_rd = self._sp_result[self._sp_result["race_id"].isin(rd_ids)]
        if len(sp_with_rd) > 0:
            self._sp_result = pace_engine.apply(
                self._sp_result, self._races, self._race_details
            )

        self._sp_calculator = sp_calculator
        self._pace_engine = pace_engine

        logger.info("VenueAnalyzer: データ準備完了")
        return self

    # ------------------------------------------------------------------
    # 分析メソッド
    # ------------------------------------------------------------------
    def analyze_base_time_trends(self) -> pd.DataFrame:
        """
        基準タイムの年次推移を分析。
        路盤改修等による変化を検出する。
        """
        races = self._races.copy()
        mask = (
            (races["finish_position"] == 1)
            & (races["track_condition"] == "良")
            & races["finish_time_seconds"].notna()
            & (races["track_surface"] != "障害")
        )
        winners = races[mask]

        trends = (
            winners.groupby(["venue", "track_surface", "distance_m", "year"])
            ["finish_time_seconds"]
            .agg(mean_time="mean", std_time="std", count="count")
            .reset_index()
        )

        # 全期間平均との差
        overall = (
            winners.groupby(["venue", "track_surface", "distance_m"])
            ["finish_time_seconds"]
            .mean()
            .reset_index()
            .rename(columns={"finish_time_seconds": "overall_mean"})
        )
        trends = trends.merge(
            overall, on=["venue", "track_surface", "distance_m"], how="left"
        )
        trends["deviation_from_overall"] = trends["mean_time"] - trends["overall_mean"]

        return trends

    def analyze_track_variants(self) -> pd.DataFrame:
        """馬場指数の分布を競馬場×年度別で分析。"""
        tv = self._sp_calculator.track_variants
        if tv is None:
            return pd.DataFrame()

        tv = tv.copy()
        tv["year"] = tv["race_date"].dt.year

        dist = (
            tv.groupby(["venue", "track_surface", "year"])
            ["track_variant"]
            .agg(
                mean_variant="mean",
                std_variant="std",
                min_variant="min",
                max_variant="max",
                count="count",
            )
            .reset_index()
        )
        return dist

    def analyze_sp_accuracy(self, top_n: int = 3) -> pd.DataFrame:
        """SP値の予測精度を競馬場×距離帯×年度別で分析。"""
        df = self._sp_result.copy()
        if df is None:
            return pd.DataFrame()

        sp_col = "sp_extended" if "sp_extended" in df.columns else "sp_raw"
        df = df.dropna(subset=[sp_col, "finish_position"])
        df["race_date"] = pd.to_datetime(df["race_date"])
        df["year"] = df["race_date"].dt.year

        # 距離帯
        df["dist_band"] = pd.cut(
            df["distance_m"],
            bins=[0, 1400, 1800, 2200, 5000],
            labels=["〜1400m", "1600-1800m", "2000-2200m", "2400m〜"],
        )

        df["sp_rank"] = df.groupby("race_id")[sp_col].rank(ascending=False)

        results = []
        for (venue, dist_band, year), grp in df.groupby(["venue", "dist_band", "year"], observed=False):
            race_ids = grp["race_id"].unique()
            n_races = len(race_ids)
            if n_races < 5:
                continue

            # 複勝率
            top_horses = grp[grp["sp_rank"] <= top_n]
            place_hits = (top_horses["finish_position"] <= 3).sum()
            place_rate = place_hits / (n_races * top_n) if n_races > 0 else np.nan

            # 勝率
            winner_hits = 0
            for rid in race_ids:
                race_grp = grp[grp["race_id"] == rid]
                top = race_grp[race_grp["sp_rank"] <= top_n]
                if (top["finish_position"] == 1).any():
                    winner_hits += 1
            winner_rate = winner_hits / n_races if n_races > 0 else np.nan

            # Spearman相関
            corrs = []
            for rid in race_ids:
                race_grp = grp[grp["race_id"] == rid]
                if len(race_grp) >= 4:
                    c, _ = stats.spearmanr(race_grp[sp_col], race_grp["finish_position"])
                    corrs.append(c)

            results.append({
                "venue": venue,
                "dist_band": dist_band,
                "year": year,
                "n_races": n_races,
                "top3_place_rate": place_rate,
                "top3_winner_rate": winner_rate,
                "mean_spearman": np.mean(corrs) if corrs else np.nan,
            })

        return pd.DataFrame(results)

    def analyze_pace_trends(self) -> pd.DataFrame:
        """ペース傾向を競馬場×コース×距離別で分析。"""
        rd = self._race_details.copy()
        rd["pace_index_rd"] = rd["first_half"] - rd["second_half"]

        races = self._races[["race_id", "venue", "track_surface", "distance_m", "year"]].drop_duplicates("race_id")
        rd = rd.merge(races, on="race_id", how="left")
        rd = rd.dropna(subset=["pace_index_rd"])
        rd = rd[rd["track_surface"] != "障害"]

        trends = (
            rd.groupby(["venue", "track_surface", "distance_m", "year"])
            ["pace_index_rd"]
            .agg(mean_pace="mean", std_pace="std", count="count")
            .reset_index()
        )

        # ペースカテゴリ分布
        rd["pace_cat"] = "M"
        rd.loc[rd["pace_index_rd"] < -2.0, "pace_cat"] = "H"
        rd.loc[rd["pace_index_rd"] > 1.0, "pace_cat"] = "S"

        cat_dist = (
            rd.groupby(["venue", "track_surface", "distance_m", "year", "pace_cat"])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )

        return trends, cat_dist

    def generate_report(self) -> Dict[str, pd.DataFrame]:
        """全分析結果をまとめて返す。"""
        logger.info("VenueAnalyzer: レポート生成開始")

        report = {
            "base_time_trends": self.analyze_base_time_trends(),
            "track_variants": self.analyze_track_variants(),
            "sp_accuracy": self.analyze_sp_accuracy(),
        }

        try:
            pace_trends, pace_cats = self.analyze_pace_trends()
            report["pace_trends"] = pace_trends
            report["pace_categories"] = pace_cats
        except Exception as e:
            logger.warning(f"ペース分析スキップ: {e}")

        for key, df in report.items():
            if isinstance(df, pd.DataFrame):
                logger.info(f"  {key}: {len(df)} rows")

        logger.info("VenueAnalyzer: レポート生成完了")
        return report

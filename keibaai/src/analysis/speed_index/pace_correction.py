#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ペース補正エンジン

【概要】
race_details.parquet のラップタイムと races.parquet の通過順を使い、
展開（ペース）の有利不利を定量化してSP値を補正する。

【武田優駿氏の知見】
- SP値の最大の弱点は「展開・ペースの概念がない」こと
- Hペースで先行して沈んだ馬 → 能力は実際より高い（上方修正）
- Sペースで逃げて好走した馬 → 能力は実際より低い（下方修正）

【補正公式】
  ペース補正 = ペース偏差 × 脚質係数
  ペース偏差 = 当日ペース指数 - 基準ペース指数
  脚質係数  = { 逃げ・先行: -α,  差し・追込: +β }
"""

import logging
from typing import Optional, Dict, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class PaceCorrectionEngine:
    """
    ペース補正の算出を担うクラス。

    Usage:
        engine = PaceCorrectionEngine()
        engine.fit(races_df, race_details_df)
        result = engine.apply(sp_df, races_df, race_details_df)
    """

    # 脚質判定の閾値（通過順位 / 出走頭数）
    STYLE_THRESHOLDS = {
        "逃げ": 0.15,
        "先行": 0.40,
        "差し": 0.70,
        # 0.70超 → 追込
    }

    def __init__(
        self,
        leading_coeff: float = 0.3,
        closing_coeff: float = 0.2,
    ):
        """
        Args:
            leading_coeff: 先行馬のペース補正係数 α（キャリブレーション最適値: 0.3）
            closing_coeff: 差し馬のペース補正係数 β（キャリブレーション最適値: 0.2）
        """
        self.leading_coeff = leading_coeff
        self.closing_coeff = closing_coeff

        self._base_paces: Optional[pd.DataFrame] = None
        self._is_fitted = False

    # ------------------------------------------------------------------
    # fit: 基準ペースの算出
    # ------------------------------------------------------------------
    def fit(
        self,
        races_df: pd.DataFrame,
        race_details_df: pd.DataFrame,
    ) -> "PaceCorrectionEngine":
        """
        過去データから基準ペース指数を算出する。

        Args:
            races_df: races.parquet
            race_details_df: race_details.parquet
        """
        logger.info("PaceCorrectionEngine: fit 開始")

        # race_details からペース指数を算出
        rd = race_details_df.copy()
        rd["race_id"] = rd["race_id"].astype(str)
        rd["pace_index_rd"] = rd["first_half"] - rd["second_half"]

        # races からレース情報を結合
        races = races_df.copy()
        races["race_id"] = races["race_id"].astype(str)
        race_info = races.drop_duplicates("race_id")[
            ["race_id", "venue", "track_surface", "distance_m"]
        ]
        rd = rd.merge(race_info, on="race_id", how="left")

        # 会場×コース×距離ごとの基準ペース指数
        valid = rd.dropna(subset=["pace_index_rd", "venue", "track_surface", "distance_m"])
        valid = valid[valid["track_surface"] != "障害"]

        self._base_paces = (
            valid.groupby(["venue", "track_surface", "distance_m"])
            ["pace_index_rd"]
            .agg(base_pace="mean", pace_std="std", pace_count="count")
            .reset_index()
        )

        logger.info(f"  基準ペース: {len(self._base_paces)} パターン")
        self._is_fitted = True
        logger.info("PaceCorrectionEngine: fit 完了")
        return self

    # ------------------------------------------------------------------
    # apply: ペース補正をSP値DataFrameに適用
    # ------------------------------------------------------------------
    def apply(
        self,
        sp_df: pd.DataFrame,
        races_df: pd.DataFrame,
        race_details_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        SP値にペース補正を適用する。

        Args:
            sp_df: SpeedIndexCalculator.calculate() の出力
            races_df: races.parquet（通過順位の取得用）
            race_details_df: race_details.parquet

        Returns:
            ペース補正済みのDataFrame。追加カラム:
              - sp_extended: ペース補正後のSP値
              - pace_correction: 適用されたペース補正値
              - running_style: 判定された脚質
              - pace_category: ペースカテゴリ (H/M/S)
        """
        if not self._is_fitted:
            raise RuntimeError("fit() を先に実行してください。")

        logger.info("PaceCorrectionEngine: ペース補正 適用開始")
        df = sp_df.copy()

        # --- レースのペース指数を算出 ---
        rd = race_details_df.copy()
        rd["race_id"] = rd["race_id"].astype(str)
        rd["pace_index_rd"] = rd["first_half"] - rd["second_half"]

        race_pace = rd[["race_id", "pace_index_rd", "first_half", "second_half"]].copy()
        df = df.merge(race_pace, on="race_id", how="left")

        # --- 基準ペースを結合 ---
        df = df.merge(
            self._base_paces[["venue", "track_surface", "distance_m", "base_pace"]],
            on=["venue", "track_surface", "distance_m"],
            how="left",
        )

        # ペース偏差 = 当日ペース - 基準ペース
        # 正: 前傾(H)寄り、負: 後傾(S)寄り
        df["pace_deviation"] = df["pace_index_rd"] - df["base_pace"].fillna(0)

        # --- ペースカテゴリ ---
        df["pace_category"] = self._classify_pace(df["pace_index_rd"])

        # --- 脚質判定 ---
        races = races_df.copy()
        races["race_id"] = races["race_id"].astype(str)
        if "horse_id" in races.columns:
            races["horse_id"] = races["horse_id"].astype(str)

        # 出走頭数を算出
        head_counts = (
            races.groupby("race_id")["horse_number"]
            .count()
            .reset_index()
            .rename(columns={"horse_number": "total_horses"})
        )

        # 通過順位を取得
        style_cols = ["race_id", "horse_id", "passing_order_1"]
        if "passing_order_1" in races.columns:
            style_info = races[
                [c for c in style_cols if c in races.columns]
            ].drop_duplicates(subset=["race_id", "horse_id"])
            style_info = style_info.merge(head_counts, on="race_id", how="left")

            style_info["passing_order_1"] = pd.to_numeric(
                style_info["passing_order_1"], errors="coerce"
            )
            style_info["running_style"] = self._classify_style(
                style_info["passing_order_1"], style_info["total_horses"]
            )

            if "horse_id" in df.columns:
                df["horse_id"] = df["horse_id"].astype(str)
            df = df.merge(
                style_info[["race_id", "horse_id", "running_style", "total_horses"]],
                on=["race_id", "horse_id"],
                how="left",
            )
        else:
            df["running_style"] = "不明"

        df["running_style"] = df["running_style"].fillna("不明")

        # --- ペース補正値の算出 ---
        df["pace_correction"] = self._compute_correction(
            df["pace_deviation"], df["running_style"]
        )

        # --- 拡張SP値 ---
        df["sp_extended"] = df["sp_raw"] + df["pace_correction"]

        # NaN伝播: sp_rawがNaNならsp_extendedもNaN
        df.loc[df["sp_raw"].isna(), "sp_extended"] = np.nan

        logger.info(
            f"  ペース補正完了: sp_extended 有効={df['sp_extended'].notna().sum():,}"
        )

        return df

    # ------------------------------------------------------------------
    # 内部メソッド
    # ------------------------------------------------------------------
    def _classify_pace(self, pace_index: pd.Series) -> pd.Series:
        """ペースカテゴリを判定。"""
        cats = pd.Series("M", index=pace_index.index)
        cats[pace_index < -2.0] = "H"  # ハイペース（前傾）
        cats[pace_index > 1.0] = "S"   # スローペース（後傾）
        cats[pace_index.isna()] = "不明"
        return cats

    def _classify_style(
        self, passing_order: pd.Series, total_horses: pd.Series
    ) -> pd.Series:
        """通過順位と出走頭数から脚質を判定。"""
        ratio = passing_order / total_horses.replace(0, np.nan)
        style = pd.Series("不明", index=passing_order.index)

        style[ratio <= self.STYLE_THRESHOLDS["逃げ"]] = "逃げ"
        style[
            (ratio > self.STYLE_THRESHOLDS["逃げ"])
            & (ratio <= self.STYLE_THRESHOLDS["先行"])
        ] = "先行"
        style[
            (ratio > self.STYLE_THRESHOLDS["先行"])
            & (ratio <= self.STYLE_THRESHOLDS["差し"])
        ] = "差し"
        style[ratio > self.STYLE_THRESHOLDS["差し"]] = "追込"
        style[passing_order.isna()] = "不明"

        return style

    def _compute_correction(
        self, pace_deviation: pd.Series, running_style: pd.Series
    ) -> pd.Series:
        """ペース補正値を計算。"""
        correction = pd.Series(0.0, index=pace_deviation.index)

        is_leading = running_style.isin(["逃げ", "先行"])
        is_closing = running_style.isin(["差し", "追込"])

        # Hペース先行 → 上方修正（厳しいペースを先行した能力を評価）
        # Sペース先行 → 下方修正（展開利の分を割引）
        correction[is_leading] = pace_deviation[is_leading] * (-self.leading_coeff)

        # Hペース差し → やや下方修正（展開利の分を割引）
        # Sペース差し → やや上方修正
        correction[is_closing] = pace_deviation[is_closing] * self.closing_coeff

        correction[pace_deviation.isna()] = 0.0

        return correction

    # ------------------------------------------------------------------
    # プロパティ
    # ------------------------------------------------------------------
    @property
    def base_paces(self) -> Optional[pd.DataFrame]:
        """基準ペースのDataFrame。"""
        return self._base_paces

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
スピード指数（SP値）計算エンジン

【概要】
走破タイムを基準タイム・馬場指数・斤量補正・クラス補正で正規化し、
馬の絶対能力を数値化する。

【公式】
  SP値 = (基準タイム - 走破タイム) × 距離係数
       - 馬場指数  ※速い馬場(+)なら下方修正、遅い馬場(-)なら上方修正
       + 斤量補正
       + クラス補正  ※上級クラスほど加点
       + ベース値(50)

  上がり3F指数 = (レース上がり3F平均 - 自身の上がり3F) × 距離係数

【データソース】
  - races.parquet: 走破タイム, 馬場状態, 斤量, 着順, 上がり3F等
"""

import logging
from typing import Optional, Dict, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class SpeedIndexCalculator:
    """
    スピード指数の算出を担うメインクラス。

    Usage:
        calc = SpeedIndexCalculator()
        calc.fit(races_df)
        result = calc.calculate(races_df)
    """

    # 斤量補正の基準値（kg）
    WEIGHT_BASE = 57.0
    # SP値のベース値
    SP_BASE = 50.0
    # 基準タイム算出に必要な最小サンプル数
    MIN_SAMPLE_COUNT = 10

    # クラス補正オフセット（データ駆動で算出）
    # 値 = 全クラス混合基準タイムに対するクラス別タイム差（SP換算）
    # 負 = クラスレベルが低い（タイムが遅い）、正 = クラスレベルが高い（タイムが速い）
    # SP値ではこの値を「減算」してクラス差を吸収する
    CLASS_OFFSETS = {
        "新馬": -10.6,
        "未勝利": -3.9,
        "1勝": 1.0,
        "2勝": 1.8,
        "3勝": 4.0,
        "OP": 6.6,
        "L": 6.6,      # リステッド ≈ OP
        "GIII": 9.6,
        "GII": 9.6,
        "GI": 9.6,
        # 旧クラス名（2019年以前）
        "500": 1.0,    # ≈ 1勝
        "1000": 1.8,   # ≈ 2勝
        "1600": 4.0,   # ≈ 3勝
    }

    # 馬場指数クリッピング範囲
    TRACK_VARIANT_CLIP = 15.0
    # 馬場指数算出に必要な最低レース数（日単位）
    MIN_RACES_FOR_VARIANT = 3

    def __init__(
        self,
        weight_coeff: float = 0.3,
        use_class_specific_base: bool = False,
        use_class_offset: bool = False,  # 分析用: std改善。予測用: 逆効果のためOFF
        use_last3f: bool = True,
        use_condition_base: bool = True,
        condition_prior_weight: float = 5.0,
    ):
        """
        Args:
            weight_coeff: 斤量補正の係数（1kg当たりのSP値変化）
                          キャリブレーション最適値: 0.3
            use_class_specific_base: Trueならクラス別基準タイムを使用
            use_class_offset: Trueならクラス補正オフセットを適用
            use_last3f: Trueなら上がり3F指数を算出
            use_condition_base: Trueなら馬場条件別基準タイムを使用
                                ベイズ縮小推定で「良」基準とブレンド
            condition_prior_weight: ベイズ縮小の事前重み。
                                    大きいほど「良」寄り。5.0推奨。
        """
        self.weight_coeff = weight_coeff
        self.use_class_specific_base = use_class_specific_base
        self.use_class_offset = use_class_offset
        self.use_last3f = use_last3f
        self.use_condition_base = use_condition_base
        self.condition_prior_weight = condition_prior_weight

        # fit()で構築されるデータ
        self._base_times: Optional[pd.DataFrame] = None
        self._base_times_good: Optional[pd.DataFrame] = None  # 良馬場の基準タイム（縮小推定用）
        self._track_variants: Optional[pd.DataFrame] = None
        self._is_fitted = False

    # ------------------------------------------------------------------
    # fit: 基準タイムと馬場指数を事前計算
    # ------------------------------------------------------------------
    def fit(self, races_df: pd.DataFrame) -> "SpeedIndexCalculator":
        """
        過去のレース結果から基準タイムと馬場指数を算出する。

        Args:
            races_df: races.parquet の全データ
        """
        logger.info("SpeedIndexCalculator: fit 開始")
        df = self._preprocess(races_df)

        # 1. 基準タイム算出
        self._base_times = self._compute_base_times(df)
        logger.info(f"  基準タイム: {len(self._base_times)} パターン")

        # 2. 馬場指数算出（レース日 × 会場 × コース別）
        self._track_variants = self._compute_track_variants(df)
        logger.info(f"  馬場指数: {len(self._track_variants)} 日×会場")

        self._is_fitted = True
        logger.info("SpeedIndexCalculator: fit 完了")
        return self

    # ------------------------------------------------------------------
    # calculate: SP値算出
    # ------------------------------------------------------------------
    def calculate(self, races_df: pd.DataFrame) -> pd.DataFrame:
        """
        全出走馬のSP値を算出する。

        Args:
            races_df: 対象レースのDataFrame

        Returns:
            SP値を含むDataFrame。カラム:
              - sp_raw: 基本SP値（馬場指数 + 斤量補正のみ）
              - track_variant: 適用された馬場指数
              - weight_correction: 斤量補正値
              - distance_coeff: 距離係数
              - reliability: 信頼度ラベル (A/B/C)
        """
        if not self._is_fitted:
            raise RuntimeError("fit() を先に実行してください。")

        logger.info("SpeedIndexCalculator: SP値算出 開始")
        df = self._preprocess(races_df)

        # --- 基準タイムを結合 ---
        bt_keys = ["venue", "track_surface", "distance_m"]
        if self.use_class_specific_base and "race_class" in df.columns:
            bt_keys.append("race_class")
        if self.use_condition_base and "track_condition" in self._base_times.columns:
            bt_keys.append("track_condition")

        df = df.merge(self._base_times, on=bt_keys, how="left")

        # 条件別基準タイムでマッチしない場合、良馬場基準にフォールバック
        if self.use_condition_base and self._base_times_good is not None:
            missing_bt = df["base_time"].isna()
            if missing_bt.any():
                fb_keys = ["venue", "track_surface", "distance_m"]
                if self.use_class_specific_base and "race_class" in df.columns:
                    fb_keys.append("race_class")
                fb = self._base_times_good[fb_keys + ["base_time", "base_time_sample_count", "base_time_sufficient"]]
                df_missing = df.loc[missing_bt, fb_keys].merge(fb, on=fb_keys, how="left")
                for col in ["base_time", "base_time_sample_count", "base_time_sufficient"]:
                    df.loc[missing_bt, col] = df_missing[col].values

        # --- 馬場指数を結合 ---
        tv_keys = ["race_date", "venue", "track_surface"]
        if self.use_condition_base and "track_condition" in self._track_variants.columns:
            tv_keys.append("track_condition")
        df = df.merge(self._track_variants, on=tv_keys, how="left")
        df["track_variant"] = df["track_variant"].fillna(0.0)

        # --- 距離係数 ---
        df["distance_coeff"] = 1000.0 / df["distance_m"] * 10.0

        # --- 斤量補正 ---
        df["weight_correction"] = (
            (self.WEIGHT_BASE - df["basis_weight"].fillna(self.WEIGHT_BASE))
            * self.weight_coeff
        )

        # --- SP値算出 ---
        # 馬場指数は「減算」: 速い馬場(+)→SP下方修正、遅い馬場(-)→SP上方修正
        has_base = df["base_time"].notna() & df["finish_time_seconds"].notna()
        df["sp_raw"] = np.nan
        df.loc[has_base, "sp_raw"] = (
            (df.loc[has_base, "base_time"] - df.loc[has_base, "finish_time_seconds"])
            * df.loc[has_base, "distance_coeff"]
            - df.loc[has_base, "track_variant"]
            + df.loc[has_base, "weight_correction"]
            + self.SP_BASE
        )

        # --- クラス補正 ---
        # race_nameから正確なグレードを解析し、クラス差を吸収
        df["class_offset"] = 0.0
        df["parsed_grade"] = ""
        if self.use_class_offset:
            df["parsed_grade"] = self._parse_race_grade(df)
            df["class_offset"] = df["parsed_grade"].map(self.CLASS_OFFSETS).fillna(0.0)
            # 減算: 上級クラス(+)→SP下方修正、下級クラス(-)→SP上方修正
            df.loc[has_base, "sp_raw"] = (
                df.loc[has_base, "sp_raw"] - df.loc[has_base, "class_offset"]
            )

        # --- 外れ値クリッピング ---
        df["sp_raw"] = df["sp_raw"].clip(lower=0, upper=120)

        # --- 上がり3F指数 ---
        df["last3f_index"] = np.nan
        if self.use_last3f and "last_3f_time" in df.columns:
            df["last_3f_time"] = pd.to_numeric(df["last_3f_time"], errors="coerce")
            # レース内平均上がり3Fとの差
            race_avg_3f = df.groupby("race_id")["last_3f_time"].transform("mean")
            has_3f = df["last_3f_time"].notna() & race_avg_3f.notna()
            df.loc[has_3f, "last3f_index"] = (
                (race_avg_3f[has_3f] - df.loc[has_3f, "last_3f_time"])
                * df.loc[has_3f, "distance_coeff"]
            )

        # --- 信頼度ラベル ---
        df["reliability"] = self._assign_reliability(df)

        logger.info(
            f"  SP値算出完了: {df['sp_raw'].notna().sum():,} / {len(df):,} 件"
        )

        # 出力カラムを絞る
        out_cols = [
            "race_id", "horse_id", "horse_number", "horse_name",
            "race_date", "venue", "track_surface", "distance_m",
            "track_condition", "finish_position", "race_class", "race_name",
            "finish_time_seconds", "basis_weight",
            "base_time", "base_time_sample_count",
            "sp_raw", "track_variant", "weight_correction",
            "class_offset", "parsed_grade", "last3f_index", "last_3f_time",
            "distance_coeff", "reliability",
        ]
        out_cols = [c for c in out_cols if c in df.columns]
        return df[out_cols].copy()

    # ------------------------------------------------------------------
    # 基準タイム: 良馬場 × 勝ち馬の平均走破タイム
    # ------------------------------------------------------------------
    def _compute_base_times(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        会場×コース×距離ごとの基準タイムを算出。

        use_condition_base=True の場合:
          - 全馬場条件（良/稍重/重/不良）で基準タイムを算出
          - ベイズ縮小推定: 条件別タイムを「良」基準とブレンド
          - N_cond < 3 → 「良」の値をそのまま使用
        """
        # まず良馬場の基準タイムを算出（常に必要）
        mask_good = (
            (df["finish_position"] == 1)
            & (df["track_condition"] == "良")
            & df["finish_time_seconds"].notna()
            & (df["track_surface"] != "障害")
        )
        winners_good = df.loc[mask_good].copy()

        base_keys = ["venue", "track_surface", "distance_m"]
        if self.use_class_specific_base and "race_class" in winners_good.columns:
            base_keys.append("race_class")

        bt_good = (
            winners_good.groupby(base_keys)["finish_time_seconds"]
            .agg(base_time="mean", base_time_std="std", base_time_sample_count="count")
            .reset_index()
        )
        bt_good["base_time_sufficient"] = bt_good["base_time_sample_count"] >= self.MIN_SAMPLE_COUNT
        self._base_times_good = bt_good.copy()

        if not self.use_condition_base:
            logger.info(
                f"  基準タイム（良のみ）: 十分={bt_good['base_time_sufficient'].sum()}, "
                f"不足={(~bt_good['base_time_sufficient']).sum()}"
            )
            return bt_good

        # --- 馬場条件別基準タイムを算出 ---
        mask_all = (
            (df["finish_position"] == 1)
            & df["finish_time_seconds"].notna()
            & (df["track_surface"] != "障害")
        )
        winners_all = df.loc[mask_all].copy()

        cond_keys = base_keys + ["track_condition"]
        bt_cond = (
            winners_all.groupby(cond_keys)["finish_time_seconds"]
            .agg(cond_mean="mean", cond_std="std", cond_count="count")
            .reset_index()
        )

        # 良馬場の基準タイムを結合（ベイズ縮小の事前分布として使用）
        bt_cond = bt_cond.merge(
            bt_good[base_keys + ["base_time"]].rename(columns={"base_time": "good_base_time"}),
            on=base_keys,
            how="left",
        )

        # ベイズ縮小推定
        prior = self.condition_prior_weight
        has_good = bt_cond["good_base_time"].notna()
        has_min = bt_cond["cond_count"] >= 3  # ハード最低

        # ケース1: 条件別サンプル≥3 かつ 良基準あり → ブレンド
        blend_mask = has_good & has_min
        bt_cond.loc[blend_mask, "base_time"] = (
            (bt_cond.loc[blend_mask, "cond_count"] * bt_cond.loc[blend_mask, "cond_mean"]
             + prior * bt_cond.loc[blend_mask, "good_base_time"])
            / (bt_cond.loc[blend_mask, "cond_count"] + prior)
        )

        # ケース2: 条件別サンプル<3 かつ 良基準あり → 良をそのまま使用
        fallback_mask = has_good & (~has_min)
        bt_cond.loc[fallback_mask, "base_time"] = bt_cond.loc[fallback_mask, "good_base_time"]

        # ケース3: 良基準なし → 条件別の値をそのまま使用
        no_good_mask = (~has_good) & has_min
        bt_cond.loc[no_good_mask, "base_time"] = bt_cond.loc[no_good_mask, "cond_mean"]

        # サンプル数は条件別+事前で合算（信頼度判定用）
        bt_cond["base_time_sample_count"] = bt_cond["cond_count"]
        bt_cond.loc[blend_mask, "base_time_sample_count"] = (
            bt_cond.loc[blend_mask, "cond_count"] + prior
        ).astype(int)
        bt_cond["base_time_sufficient"] = bt_cond["base_time_sample_count"] >= self.MIN_SAMPLE_COUNT

        # 出力カラムを整理
        out_cols = cond_keys + ["base_time", "base_time_sample_count", "base_time_sufficient"]
        bt_final = bt_cond[out_cols].dropna(subset=["base_time"]).copy()

        n_good = len(bt_final[bt_final["track_condition"] == "良"])
        n_other = len(bt_final[bt_final["track_condition"] != "良"])
        logger.info(
            f"  基準タイム（条件別）: 良={n_good}, 稍重/重/不良={n_other}, "
            f"十分={bt_final['base_time_sufficient'].sum()}"
        )
        return bt_final

    # ------------------------------------------------------------------
    # 馬場指数: 当日上位3頭平均 vs 基準タイム
    # ------------------------------------------------------------------
    def _compute_track_variants(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        日別×会場×コースの馬場指数を算出。

        改善点:
          - medianで集約（外れ値に強い）
          - クリッピング（±TRACK_VARIANT_CLIP）
          - 日のレース数 < MIN_RACES_FOR_VARIANT → 馬場指数=0
          - use_condition_base=True時はtrack_condition別に算出
        """
        if self._base_times is None:
            raise RuntimeError("基準タイムが未計算です。")

        # 障害除外 & 走破タイム有効 & 上位3着以内
        mask = (
            (df["finish_position"] <= 3)
            & df["finish_time_seconds"].notna()
            & (df["track_surface"] != "障害")
        )
        top3 = df.loc[mask].copy()

        # レースごとの上位3頭平均タイム
        race_group_keys = ["race_id", "race_date", "venue", "track_surface", "distance_m"]
        if self.use_condition_base and "track_condition" in top3.columns:
            race_group_keys.append("track_condition")

        race_top3 = (
            top3.groupby(race_group_keys)
            ["finish_time_seconds"]
            .mean()
            .reset_index()
            .rename(columns={"finish_time_seconds": "top3_avg"})
        )

        # 基準タイムとの差を算出
        bt_keys = ["venue", "track_surface", "distance_m"]
        if self.use_condition_base and "track_condition" in self._base_times.columns:
            bt_keys.append("track_condition")

        race_top3 = race_top3.merge(
            self._base_times[bt_keys + ["base_time"]],
            on=bt_keys,
            how="left",
        )

        # 距離係数を掛けた差分
        race_top3["distance_coeff"] = 1000.0 / race_top3["distance_m"] * 10.0
        race_top3["variant_raw"] = (
            (race_top3["base_time"] - race_top3["top3_avg"])
            * race_top3["distance_coeff"]
        )

        # 当日×会場×コース×(条件)で中央値
        tv_group_keys = ["race_date", "venue", "track_surface"]
        if self.use_condition_base and "track_condition" in race_top3.columns:
            tv_group_keys.append("track_condition")

        tv = (
            race_top3.dropna(subset=["variant_raw"])
            .groupby(tv_group_keys)
            .agg(
                track_variant=("variant_raw", "median"),
                race_count=("variant_raw", "count"),
            )
            .reset_index()
        )

        # レース数が少ない日は馬場指数=0（信頼度不足）
        tv.loc[tv["race_count"] < self.MIN_RACES_FOR_VARIANT, "track_variant"] = 0.0

        # クリッピング
        tv["track_variant"] = tv["track_variant"].clip(
            lower=-self.TRACK_VARIANT_CLIP,
            upper=self.TRACK_VARIANT_CLIP,
        )

        # race_countは内部用なので削除
        tv = tv.drop(columns=["race_count"])

        return tv

    # ------------------------------------------------------------------
    # グレード解析: race_name + race_class から正確なクラスを判定
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_race_grade(df: pd.DataFrame) -> pd.Series:
        """
        race_nameとrace_classから正確なグレードを解析する。

        race_classはG1/G2/G3が混在、1勝/2勝/3勝がNaN等の品質問題があるため、
        race_nameの文字列パターンを優先して判定する。

        Returns:
            "GI", "GII", "GIII", "L", "OP", "3勝", "2勝", "1勝", "未勝利", "新馬", ""
        """
        grade = pd.Series("", index=df.index)
        name = df["race_name"].fillna("") if "race_name" in df.columns else pd.Series("", index=df.index)
        cls = df["race_class"].fillna("") if "race_class" in df.columns else pd.Series("", index=df.index)

        # race_name から判定（優先度順）
        grade[name.str.contains(r"\(GI\)", na=False, regex=True)] = "GI"
        grade[name.str.contains(r"\(GII\)", na=False, regex=True)] = "GII"
        grade[name.str.contains(r"\(GIII\)", na=False, regex=True)] = "GIII"
        grade[name.str.contains(r"\(L\)", na=False, regex=True)] = "L"
        grade[name.str.contains(r"\(OP\)", na=False, regex=True)] = "OP"
        grade[name.str.contains("3勝", na=False)] = "3勝"
        grade[name.str.contains("2勝", na=False)] = "2勝"
        grade[name.str.contains("1勝", na=False)] = "1勝"
        grade[name.str.contains("未勝利", na=False)] = "未勝利"

        # race_class からフォールバック（race_nameで判定できなかった行）
        unmatched = grade == ""
        grade[unmatched & (cls == "新馬")] = "新馬"
        grade[unmatched & (cls == "未勝利")] = "未勝利"
        grade[unmatched & (cls == "500")] = "1勝"      # 旧500万下
        grade[unmatched & (cls == "1000")] = "2勝"     # 旧1000万下
        grade[unmatched & (cls == "1600")] = "3勝"     # 旧1600万下
        grade[unmatched & (cls == "OP")] = "OP"
        grade[unmatched & (cls == "G1")] = "GI"        # フォールバック

        # 障害レースは空のまま（補正なし）
        grade[name.str.contains("障害", na=False)] = ""

        return grade

    # ------------------------------------------------------------------
    # 信頼度ラベル
    # ------------------------------------------------------------------
    def _assign_reliability(self, df: pd.DataFrame) -> pd.Series:
        """
        信頼度ラベルを付与。

        A: 良馬場 & サンプル≥20 & 距離≤1600m
        B: 良馬場 & サンプル≥10
        C: それ以外（重/不良, サンプル不足, 長距離）
        """
        cond_good = df["track_condition"] == "良"
        cond_sample20 = df.get("base_time_sample_count", pd.Series(0)) >= 20
        cond_sample10 = df.get("base_time_sample_count", pd.Series(0)) >= 10
        cond_short = df["distance_m"] <= 1600

        reliability = pd.Series("C", index=df.index)
        reliability[cond_good & cond_sample10] = "B"
        reliability[cond_good & cond_sample20 & cond_short] = "A"

        return reliability

    # ------------------------------------------------------------------
    # 前処理
    # ------------------------------------------------------------------
    @staticmethod
    def _preprocess(df: pd.DataFrame) -> pd.DataFrame:
        """型変換等の前処理。"""
        df = df.copy()
        df["race_id"] = df["race_id"].astype(str)
        df["race_date"] = pd.to_datetime(df["race_date"])

        for col in ["finish_time_seconds", "basis_weight", "distance_m"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in ["finish_position"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 障害レース除外
        if "track_surface" in df.columns:
            df = df[df["track_surface"] != "障害"].copy()

        return df

    # ------------------------------------------------------------------
    # プロパティ
    # ------------------------------------------------------------------
    @property
    def base_times(self) -> Optional[pd.DataFrame]:
        """基準タイムのDataFrame。"""
        return self._base_times

    @property
    def track_variants(self) -> Optional[pd.DataFrame]:
        """馬場指数のDataFrame。"""
        return self._track_variants

    # ------------------------------------------------------------------
    # レースレベル補正: 反復アルゴリズム
    # ------------------------------------------------------------------
    def refine_with_race_quality(
        self,
        sp_df: pd.DataFrame,
        max_iterations: int = 5,
        damping: float = 0.5,
        min_races_per_horse: int = 3,
        min_experienced_per_race: int = 3,
    ) -> pd.DataFrame:
        """
        レースレベル補正を適用し、field strengthとtrack speedを分離する。

        反復アルゴリズム:
          1. 各馬の基準能力(horse_par)を算出（過去SP中央値）
          2. 各レースの期待レベルと実際のレベルを比較
          3. 差分をdampingして補正適用
          4. 収束するまで反復

        Args:
            sp_df: calculate() or pace_engine.apply() の出力DataFrame
            max_iterations: 最大反復回数
            damping: 収束速度制御（0〜1、0.5推奨）
            min_races_per_horse: 馬の基準能力算出に必要な最低レース数
            min_experienced_per_race: 補正適用に必要な経験馬の最低数

        Returns:
            sp_refined, race_quality_adj カラムが追加されたDataFrame
        """
        logger.info("SpeedIndexCalculator: レースレベル補正 開始")
        df = sp_df.copy()

        # SP値カラムの決定
        sp_col = get_best_sp_col(df)
        if sp_col is None or df[sp_col].notna().sum() == 0:
            logger.warning("  有効なSP値がありません。補正をスキップします。")
            df["sp_refined"] = df.get(sp_col, np.nan)
            df["race_quality_adj"] = 0.0
            return df

        # --- 時系列考慮の経験走数を計算 ---
        df["race_id"] = df["race_id"].astype(str)
        if "horse_id" in df.columns:
            df["horse_id"] = df["horse_id"].astype(str)
        df["race_date"] = pd.to_datetime(df["race_date"])

        df = df.sort_values(["horse_id", "race_date", "race_id"])
        df["career_race_num"] = df.groupby("horse_id").cumcount()

        # 作業用SP値
        df["sp_work"] = df[sp_col].copy()
        df["race_quality_adj"] = 0.0

        for iteration in range(max_iterations):
            # --- Step 2: 馬別基準能力（horse_par） ---
            qualified = df[
                (df["career_race_num"] >= min_races_per_horse)
                & df["sp_work"].notna()
            ]
            horse_par = (
                qualified.groupby("horse_id")["sp_work"]
                .median()
                .reset_index()
                .rename(columns={"sp_work": "horse_par"})
            )

            # --- Step 3: レース別補正値 ---
            df_with_par = df.merge(horse_par, on="horse_id", how="left")

            # レースごとに経験馬が十分いるか確認
            race_stats = (
                df_with_par[df_with_par["horse_par"].notna()]
                .groupby("race_id")
                .agg(
                    expected_level=("horse_par", "median"),
                    actual_level=("sp_work", "median"),
                    n_experienced=("horse_par", "count"),
                )
                .reset_index()
            )

            # 経験馬が十分なレースのみ補正
            race_stats = race_stats[
                race_stats["n_experienced"] >= min_experienced_per_race
            ].copy()
            race_stats["adjustment"] = (
                (race_stats["actual_level"] - race_stats["expected_level"])
                * damping
            )

            # 収束判定
            adj_std = race_stats["adjustment"].std()
            adj_mean = race_stats["adjustment"].abs().mean()
            logger.info(
                f"  反復{iteration + 1}: 補正対象={len(race_stats)}R, "
                f"補正値std={adj_std:.3f}, 補正値abs平均={adj_mean:.3f}"
            )

            if adj_std < 0.1 and iteration > 0:
                logger.info("  → 収束しました。")
                break

            # --- Step 4: 補正適用 ---
            adj_map = race_stats.set_index("race_id")["adjustment"]
            race_adj = df["race_id"].map(adj_map).fillna(0.0)
            df["sp_work"] = df["sp_work"] - race_adj
            df["race_quality_adj"] = df["race_quality_adj"] + race_adj

        # 補正値のクリッピング（極端な補正を抑制）
        df["race_quality_adj"] = df["race_quality_adj"].clip(
            lower=-self.TRACK_VARIANT_CLIP,
            upper=self.TRACK_VARIANT_CLIP,
        )

        # 最終結果: 元のSP値 - クリッピング済み補正値
        df["sp_refined"] = df[sp_col] - df["race_quality_adj"]
        # sp_rawがNaNならsp_refinedもNaN
        df.loc[df[sp_col].isna(), "sp_refined"] = np.nan

        # 作業カラムの削除
        df = df.drop(columns=["sp_work", "career_race_num"], errors="ignore")

        n_adjusted = (df["race_quality_adj"].abs() > 0.01).sum()
        n_clipped = ((df["race_quality_adj"].abs() - self.TRACK_VARIANT_CLIP).abs() < 0.01).sum()
        logger.info(
            f"  レースレベル補正完了: "
            f"補正適用={n_adjusted:,} / {len(df):,} 件, "
            f"クリッピング={n_clipped:,} 件"
        )

        return df


def get_best_sp_col(df: pd.DataFrame) -> Optional[str]:
    """
    DataFrameから最適なSP値カラムを選択する。

    優先順: sp_refined > sp_extended > sp_raw

    Args:
        df: SP値を含むDataFrame

    Returns:
        カラム名。見つからない場合はNone。
    """
    for col in ["sp_refined", "sp_extended", "sp_raw"]:
        if col in df.columns and df[col].notna().any():
            return col
    return None


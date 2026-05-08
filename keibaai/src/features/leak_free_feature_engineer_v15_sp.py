"""
リークフリー特徴量エンジニア V15+SP

V15（本番ベースモデル）をベースに、SP値（スピード指数）由来の特徴量を追加する拡張。

【検証根拠】 5年Walk-Forward検証 (2021-2025)
- V15単体: 平均ROI +1.5pt改善 (76.9% → 78.4%), 3/5年で改善
- Gap（過学習）: 5年中4年で改善 (平均 -2.3pt)
- horse_sp_race_rank_avg: 全72特徴量中8位を5年連続キープ
- V4.4には逆効果(-1.4pt)のため、V15専用として設計

【追加特徴量 (6個)】
1. horse_sp_avg: 過去SP値の累積平均（条件正規化された絶対能力値）
2. horse_sp_trend: 直近3走SP値 - 全期間平均（成長/衰え方向）
3. horse_sp_std: SP値の標準偏差（パフォーマンス安定性）
4. horse_sp_best: 過去最高SP値（ポテンシャル上限）
5. horse_last3f_index_avg: 上がり3F指数の累積平均（末脚能力正規化版）
6. horse_sp_race_rank_avg: レース内SP順位の累積平均（★最重要★）

【リーク対策（厳格）】
1. SP値計算の基準タイム・馬場指数: fit()でTrain期間のみから算出
2. 全特徴量: shift(1)で当該レースを除外
3. 最低3走フィルタ（初走・2走の馬はNaN）

Author: KeibaAI Development Team
Date: 2026-05-06
"""

import logging
from typing import Optional, List

import numpy as np
import pandas as pd
from dataclasses import dataclass

from .leak_free_feature_engineer_v15 import (
    LeakFreeFeatureEngineerV15,
    FeatureConfigV15,
)

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV15SP(FeatureConfigV15):
    """V15+SP用設定"""
    sp_weight_coeff: float = 0.3
    sp_use_class_offset: bool = False
    sp_use_last3f: bool = True
    sp_use_condition_base: bool = True
    sp_leading_coeff: float = 0.3
    sp_closing_coeff: float = 0.2
    sp_min_races: int = 3


class LeakFreeFeatureEngineerV15SP(LeakFreeFeatureEngineerV15):
    """
    V15 + SP値特徴量6個 = 72特徴量

    V15の66個 + SP値由来の6個。
    V4.4(LambdaRank)には使用しない。V15 Binary専用。
    """

    SP_FEATURE_COLS = [
        'horse_sp_avg',
        'horse_sp_trend',
        'horse_sp_std',
        'horse_sp_best',
        'horse_last3f_index_avg',
        'horse_sp_race_rank_avg',
    ]

    FEATURE_COLS = LeakFreeFeatureEngineerV15.FEATURE_COLS + SP_FEATURE_COLS

    def __init__(self, config: Optional[FeatureConfigV15SP] = None):
        super().__init__(config or FeatureConfigV15SP())
        self._sp_calculator = None
        self._pace_engine = None
        self._sp_history = None
        self._race_details_ref = None
        logger.info("LeakFreeFeatureEngineerV15SP: 初期化完了")

    def fit(self, races_df, pedigrees_df=None, corners_df=None,
            race_details_df=None, returns_df=None, horses_df=None):
        """fit: 親V15 + SP値エンジンのfit + Train期間のSP値履歴構築"""
        logger.info("LeakFreeFeatureEngineerV15SP: fit開始")

        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)

        # race_detailsの参照を保持（transform時のペース補正に使用）
        if race_details_df is not None:
            self._race_details_ref = race_details_df.copy()

        self._fit_sp(races_df, race_details_df)
        self._build_sp_history(races_df, race_details_df)

        logger.info("LeakFreeFeatureEngineerV15SP: fit完了")
        return self

    def _fit_sp(self, races_df, race_details_df):
        """SP値計算エンジンをfit"""
        from ..analysis.speed_index.calculator import SpeedIndexCalculator
        from ..analysis.speed_index.pace_correction import PaceCorrectionEngine

        cfg = self.config
        self._sp_calculator = SpeedIndexCalculator(
            weight_coeff=cfg.sp_weight_coeff,
            use_class_offset=cfg.sp_use_class_offset,
            use_last3f=cfg.sp_use_last3f,
            use_condition_base=cfg.sp_use_condition_base,
        )
        self._sp_calculator.fit(races_df)
        logger.info("  SP値計算エンジンfit完了")

        if race_details_df is not None and len(race_details_df) > 0:
            rd = race_details_df.copy()
            rd['race_id'] = rd['race_id'].astype(str)
            self._pace_engine = PaceCorrectionEngine(
                leading_coeff=cfg.sp_leading_coeff,
                closing_coeff=cfg.sp_closing_coeff,
            )
            self._pace_engine.fit(races_df, rd)
            logger.info("  ペース補正エンジンfit完了")

    def _compute_sp_values(self, races_df, race_details_df=None):
        """SP値を算出（ペース補正込み）"""
        sp_result = self._sp_calculator.calculate(races_df)

        if self._pace_engine is not None and race_details_df is not None:
            rd = race_details_df.copy()
            rd['race_id'] = rd['race_id'].astype(str)
            all_rd_ids = set(rd['race_id'].unique())
            if sp_result['race_id'].isin(all_rd_ids).any():
                sp_result = self._pace_engine.apply(sp_result, races_df, rd)

        sp_col = 'sp_extended' if 'sp_extended' in sp_result.columns and sp_result['sp_extended'].notna().any() else 'sp_raw'
        sp_df = sp_result[['race_id', 'horse_id', 'race_date', sp_col, 'last3f_index']].copy()
        sp_df = sp_df.rename(columns={sp_col: 'sp_value'})
        sp_df['horse_id'] = sp_df['horse_id'].astype(str)
        sp_df['race_id'] = sp_df['race_id'].astype(str)
        sp_df['race_date'] = pd.to_datetime(sp_df['race_date'])
        sp_df = sp_df.dropna(subset=['sp_value'])

        sp_df['sp_rank_in_race'] = sp_df.groupby('race_id')['sp_value'].rank(ascending=False)
        sp_df['sp_field_size'] = sp_df.groupby('race_id')['sp_value'].transform('count')
        sp_df['sp_relative_rank'] = sp_df['sp_rank_in_race'] / sp_df['sp_field_size']
        return sp_df

    def _build_sp_history(self, races_df, race_details_df):
        """Train期間のSP値履歴を構築"""
        logger.info("  SP値履歴を構築中...")
        self._sp_history = self._compute_sp_values(races_df, race_details_df)
        logger.info(f"  SP値履歴: {len(self._sp_history):,}件")

    def _generate_sp_features(self, sp_df):
        """SP値DataFrameから6つの累積特徴量を生成（全てshift(1)）"""
        sp_df = sp_df.sort_values(['horse_id', 'race_date', 'race_id'])
        min_races = self.config.sp_min_races

        sp_df['horse_sp_avg'] = sp_df.groupby('horse_id')['sp_value'].transform(
            lambda x: x.expanding().mean().shift(1))
        sp_df['sp_last3_avg'] = sp_df.groupby('horse_id')['sp_value'].transform(
            lambda x: x.rolling(3, min_periods=3).mean().shift(1))
        sp_df['horse_sp_trend'] = sp_df['sp_last3_avg'] - sp_df['horse_sp_avg']
        sp_df['horse_sp_std'] = sp_df.groupby('horse_id')['sp_value'].transform(
            lambda x: x.expanding().std().shift(1))
        sp_df['horse_sp_best'] = sp_df.groupby('horse_id')['sp_value'].transform(
            lambda x: x.expanding().max().shift(1))
        sp_df['horse_last3f_index_avg'] = sp_df.groupby('horse_id')['last3f_index'].transform(
            lambda x: x.expanding().mean().shift(1))
        sp_df['horse_sp_race_rank_avg'] = sp_df.groupby('horse_id')['sp_relative_rank'].transform(
            lambda x: x.expanding().mean().shift(1))

        # 最低出走数フィルタ
        sp_df['cum_count'] = sp_df.groupby('horse_id')['sp_value'].transform(
            lambda x: x.expanding().count().shift(1))
        mask = sp_df['cum_count'] < min_races
        for col in self.SP_FEATURE_COLS:
            sp_df.loc[mask, col] = np.nan

        return sp_df[['race_id', 'horse_id'] + self.SP_FEATURE_COLS].drop_duplicates(['race_id', 'horse_id'])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V15のtransform + SP特徴量のマージ"""
        result = super().transform(df)

        logger.info("LeakFreeFeatureEngineerV15SP: SP特徴量を追加中...")

        if self._sp_calculator is None or self._sp_history is None:
            logger.warning("  SP未初期化。NaNで埋めます。")
            for col in self.SP_FEATURE_COLS:
                result[col] = np.nan
            return result

        # transform対象のSP値も算出（走破タイムが既知の場合）
        has_time = 'finish_time_seconds' in df.columns and df['finish_time_seconds'].notna().any()
        if has_time:
            try:
                sp_new = self._compute_sp_values(df, self._race_details_ref)
                all_sp = pd.concat([self._sp_history, sp_new], ignore_index=True)
                all_sp = all_sp.drop_duplicates(['race_id', 'horse_id'])
            except Exception as e:
                logger.warning(f"  SP値算出失敗: {e}")
                all_sp = self._sp_history.copy()
        else:
            all_sp = self._sp_history.copy()

        sp_features = self._generate_sp_features(all_sp)

        result['race_id'] = result['race_id'].astype(str)
        result['horse_id'] = result['horse_id'].astype(str)
        result = result.merge(sp_features, on=['race_id', 'horse_id'], how='left')

        for col in self.SP_FEATURE_COLS:
            n = result[col].notna().sum()
            logger.info(f"    {col}: {n:,}/{len(result):,} ({n/len(result)*100:.1f}%)")

        logger.info("LeakFreeFeatureEngineerV15SP: transform完了")
        return result

    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

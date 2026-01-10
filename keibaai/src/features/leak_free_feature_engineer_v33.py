# -*- coding: utf-8 -*-
"""
リークフリー特徴量エンジニア V33

V32をベースに、脚質×ペース適合度特徴量を追加。

【設計根拠（分析結果）】
- ハイペース時: Q1(前)15.9%, Q5(後)2.2%
- スローペース時: Q1(前)15.4%, Q5(後)1.5%
- 脚質によってペースへの適性が異なる
- 馬ごとの過去ペース別成績から適性を算出

【追加特徴量 (8個)】

1. ペース適性スコア (3個)
   - horse_hi_pace_top3_rate: ハイペース時のTop3率
   - horse_slow_pace_top3_rate: スローペース時のTop3率
   - horse_pace_preference: ペース嗜好スコア（ハイ好き→正、スロー好き→負）

2. ペース×脚質適合度 (3個)
   - pace_style_fit_score: ペース×脚質の適合度
   - front_hi_pace_advantage: 前方馬のハイペース耐性
   - back_slow_pace_disadvantage: 後方馬のスローペース不利度

3. レース内ペース予測 (2個)
   - race_predicted_pace: 出走馬の脚質構成からペース予測
   - pace_match_advantage: 予測ペースと馬の適性の適合度

【リーク対策】
- ペース判定は過去レースのみ使用（shift(1)）
- ペース予測は馬の過去脚質傾向のみ使用
- min_pace_races=3
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v32 import LeakFreeFeatureEngineerV32, FeatureConfigV32

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV33(FeatureConfigV32):
    """V33用設定"""
    min_pace_races: int = 3
    hi_pace_threshold: float = -1.0  # second_half - first_half < -1 がハイペース
    slow_pace_threshold: float = 1.0  # second_half - first_half > 1 がスローペース


class LeakFreeFeatureEngineerV33(LeakFreeFeatureEngineerV32):
    """
    リークフリー特徴量エンジニア V33
    
    V32に脚質×ペース適合度特徴量を追加
    """
    
    FEATURE_COLS = LeakFreeFeatureEngineerV32.FEATURE_COLS + [
        # ペース適性スコア
        'horse_hi_pace_top3_rate',      # ハイペースTop3率
        'horse_slow_pace_top3_rate',    # スローペースTop3率
        'horse_pace_preference',        # ペース嗜好スコア
        # ペース×脚質適合度
        'pace_style_fit_score',       # ペース×脚質適合度
        'front_hi_pace_advantage',      # 前方ハイペース耐性
        'back_slow_pace_disadvantage',  # 後方スローペース不利度
        # レース内ペース予測
        'race_predicted_pace',          # 予測ペース
        'pace_match_advantage',         # ペース適合有利度
    ]
    
    def __init__(self, config: Optional[FeatureConfigV33] = None):
        super().__init__(config or FeatureConfigV33())
        self._horse_pace_preference: Dict = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """fitメソッドの拡張"""
        logger.info("LeakFreeFeatureEngineerV33: fit開始")
        
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # ペース適性統計を計算
        self._calc_pace_preference_stats(races_df, race_details_df)
        
        logger.info("LeakFreeFeatureEngineerV33: fit完了")
        return self
    
    def _calc_pace_preference_stats(self, races_df: pd.DataFrame, race_details_df: Optional[pd.DataFrame]):
        """ペース適性統計を計算"""
        logger.info("  ペース適性統計を計算中...")
        
        if race_details_df is None or self._full_history is None:
            logger.warning("  必要なデータがありません")
            return
        
        # ペースデータ
        pace = race_details_df[['race_id', 'first_half', 'second_half']].copy()
        pace = pace.dropna()
        pace['pace_diff'] = pace['second_half'] - pace['first_half']
        
        hi_thresh = self.config.hi_pace_threshold
        slow_thresh = self.config.slow_pace_threshold
        
        pace['is_hi_pace'] = (pace['pace_diff'] < hi_thresh).astype(float)
        pace['is_slow_pace'] = (pace['pace_diff'] > slow_thresh).astype(float)
        
        # 履歴データ
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date', 'finish_position']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        hist = hist.merge(pace, on='race_id', how='left')
        hist = hist.dropna(subset=['horse_id', 'pace_diff'])
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # Top3フラグ
        hist['is_top3'] = (hist['finish_position'] <= 3).astype(float)
        
        # ハイペース時Top3
        hist['hi_pace_top3'] = hist['is_hi_pace'] * hist['is_top3']
        hist['slow_pace_top3'] = hist['is_slow_pace'] * hist['is_top3']
        
        # 累積統計（shift(1)）
        hist['cum_hi_pace_count'] = hist.groupby('horse_id')['is_hi_pace'].transform(
            lambda x: x.shift(1).expanding().sum()
        )
        hist['cum_hi_pace_top3'] = hist.groupby('horse_id')['hi_pace_top3'].transform(
            lambda x: x.shift(1).expanding().sum()
        )
        hist['cum_slow_pace_count'] = hist.groupby('horse_id')['is_slow_pace'].transform(
            lambda x: x.shift(1).expanding().sum()
        )
        hist['cum_slow_pace_top3'] = hist.groupby('horse_id')['slow_pace_top3'].transform(
            lambda x: x.shift(1).expanding().sum()
        )
        
        # Top3率計算
        min_races = self.config.min_pace_races
        hist['hi_pace_top3_rate'] = np.where(
            hist['cum_hi_pace_count'] >= min_races,
            hist['cum_hi_pace_top3'] / hist['cum_hi_pace_count'],
            np.nan
        )
        hist['slow_pace_top3_rate'] = np.where(
            hist['cum_slow_pace_count'] >= min_races,
            hist['cum_slow_pace_top3'] / hist['cum_slow_pace_count'],
            np.nan
        )
        
        # ペース嗜好スコア（ハイペースでの好走率 - スローペースでの好走率）
        hist['pace_preference'] = hist['hi_pace_top3_rate'] - hist['slow_pace_top3_rate']
        
        # 最新統計を保存
        latest = hist.groupby('horse_id').last()[
            ['hi_pace_top3_rate', 'slow_pace_top3_rate', 'pace_preference']
        ].reset_index()
        
        self._horse_pace_preference = {}
        for _, row in latest.iterrows():
            self._horse_pace_preference[row['horse_id']] = {
                'hi_pace_top3_rate': row['hi_pace_top3_rate'],
                'slow_pace_top3_rate': row['slow_pace_top3_rate'],
                'pace_preference': row['pace_preference'],
            }
        
        logger.info(f"    ペース適性統計: {len(self._horse_pace_preference):,}頭")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V33のtransform"""
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV33: 追加transform開始")
        
        result = self._apply_pace_preference_features(result)
        result = self._calc_race_pace_prediction(result)
        
        logger.info("LeakFreeFeatureEngineerV33: transform完了")
        return result
    
    def _apply_pace_preference_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """ペース適性特徴量を付与"""
        logger.info("  ペース適性特徴量を付与中...")
        
        df = df.copy()
        
        # ペース適性スコア
        df['horse_hi_pace_top3_rate'] = df['horse_id'].map(
            lambda x: self._horse_pace_preference.get(x, {}).get('hi_pace_top3_rate', np.nan)
        )
        df['horse_slow_pace_top3_rate'] = df['horse_id'].map(
            lambda x: self._horse_pace_preference.get(x, {}).get('slow_pace_top3_rate', np.nan)
        )
        df['horse_pace_preference'] = df['horse_id'].map(
            lambda x: self._horse_pace_preference.get(x, {}).get('pace_preference', np.nan)
        )
        
        # ペース×脚質適合度
        style_score = df['horse_style_score_avg'].fillna(0.5)
        pace_pref = df['horse_pace_preference'].fillna(0)
        
        # 前方馬のハイペース耐性（前方脚質かつハイペース好き）
        is_front = (style_score < self.config.front_threshold).astype(float)
        df['front_hi_pace_advantage'] = is_front * df['horse_hi_pace_top3_rate'].fillna(0)
        
        # 後方馬のスローペース不利度
        is_back = (style_score > 0.66).astype(float)
        df['back_slow_pace_disadvantage'] = is_back * (1 - df['horse_slow_pace_top3_rate'].fillna(0))
        
        # 総合ペース×脚質適合度
        df['pace_style_fit_score'] = (
            df['front_hi_pace_advantage'].fillna(0) * 0.5 +
            (1 - df['back_slow_pace_disadvantage'].fillna(0)) * 0.5
        )
        
        non_null = df['horse_pace_preference'].notna().sum()
        logger.info(f"    horse_pace_preference非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_race_pace_prediction(self, df: pd.DataFrame) -> pd.DataFrame:
        """レース内ペース予測を計算"""
        logger.info("  レース内ペース予測を計算中...")
        
        df = df.copy()
        
        style_score = df['horse_style_score_avg'].fillna(0.5)
        
        # レース内の前方脚質馬の割合からペースを予測
        # 前方馬が多い → ハイペース予測
        is_front = (style_score < self.config.front_threshold).astype(float)
        df['race_predicted_pace'] = df.groupby('race_id')[is_front.name].transform('mean')
        
        # 予測ペースと馬の適性の適合度
        pace_pref = df['horse_pace_preference'].fillna(0)
        predicted_pace = df['race_predicted_pace'].fillna(0.5)
        
        # ペースが高い（ハイペース予測）時にハイペース好きなら有利
        df['pace_match_advantage'] = pace_pref * (predicted_pace - 0.5) * 2
        
        logger.info(f"    race_predicted_pace平均: {df['race_predicted_pace'].mean():.3f}")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

"""
リークフリー特徴量エンジニア V10

V8.1をベースに、多角的データ探索で発見された有望特徴量を追加：

Phase 1: 馬体重変動（リークフリー）
- weight_trend: 減量/維持/増量カテゴリ（0/1/2）
- 根拠: 減量ROI 85% vs 増量58%で27%差

Phase 2: 過去上がり3F順位（リークフリー変換）
- horse_last3f_rank_avg: 過去レースの上がり3F順位平均
- cumsum().shift(1)で未来情報を排除

Phase 3: 過去4C位置（リークフリー変換）
- horse_c4_leader_rate: 4C先頭率（逃げ馬傾向）
- horse_avg_c4_gap: 4C先頭からの平均馬身差

リーク対策:
- 全ての累積統計はshift(1)で当該レースを除外
- Train固定統計は使用しない（V9で効果なしと判明）
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v8 import LeakFreeFeatureEngineerV8, FeatureConfigV8

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV10(FeatureConfigV8):
    """V10用設定"""
    pass


class LeakFreeFeatureEngineerV10(LeakFreeFeatureEngineerV8):
    """
    リークフリー特徴量エンジニア V10
    
    V8.1の全特徴量に加えて、以下を追加：
    
    Phase 1: 馬体重変動
    - weight_trend: 減量(-1)/維持(0)/増量(1)
    
    Phase 2: 過去上がり3F順位
    - horse_last3f_rank_avg: 過去レースの上がり3F順位平均
    
    Phase 3: 過去4C位置
    - horse_c4_leader_rate: 4C先頭率
    - horse_avg_c4_gap: 4C先頭からの平均馬身差
    """
    
    # V8.1の特徴量 + V10の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV8.FEATURE_COLS + [
        # Phase 1: 馬体重変動
        'weight_trend',
        # Phase 2: 過去上がり3F順位
        'horse_last3f_rank_avg',
        # Phase 3: 過去4C位置（horse_avg_c4_gapはV7で既に存在するため除外）
        'horse_c4_leader_rate',
    ]
    
    def __init__(self, config: Optional[FeatureConfigV10] = None):
        super().__init__(config or FeatureConfigV10())
        self._last3f_rank_stats = None
        self._c4_stats = None
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV10':
        """V8のfitを拡張"""
        # V8のfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df)
        
        logger.info("LeakFreeFeatureEngineerV10: 追加fit処理開始")
        
        # Phase 2: 上がり3F順位の累積統計を構築
        self._build_last3f_rank_stats()
        
        # Phase 3: 4C位置の累積統計を構築
        if corners_df is not None:
            self._build_c4_stats(corners_df)
        
        logger.info("LeakFreeFeatureEngineerV10: fit完了")
        return self
    
    def _build_last3f_rank_stats(self):
        """上がり3F順位の累積統計を構築（リークフリー）"""
        hist = self._full_history.copy()
        
        if 'last3f_rank' not in hist.columns:
            logger.warning("last3f_rankカラムが存在しません")
            return
        
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 累積統計をshift(1)で計算（当該レースを除外）
        hist['_last3f_rank_cumsum'] = hist.groupby('horse_id')['last3f_rank'].transform(
            lambda x: x.cumsum().shift(1)
        )
        hist['_last3f_rank_count'] = hist.groupby('horse_id').cumcount()
        
        # 平均を計算
        hist['horse_last3f_rank_avg'] = hist['_last3f_rank_cumsum'] / hist['_last3f_rank_count']
        
        # 各馬×レースの統計を保存
        self._last3f_rank_stats = hist[['race_id', 'horse_number', 'horse_last3f_rank_avg']].copy()
        
        logger.info(f"  上がり3F順位累積統計構築完了: {len(self._last3f_rank_stats):,}件")
    
    def _build_c4_stats(self, corners_df: pd.DataFrame):
        """4C位置の累積統計を構築（リークフリー）"""
        hist = self._full_history.copy()
        
        # 4コーナーデータを取得
        c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
        c4.columns = ['race_id', 'horse_number', 'c4_position', 'c4_gap']
        
        # 履歴とマージ
        hist = hist.merge(c4, on=['race_id', 'horse_number'], how='left')
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 4C先頭フラグ
        hist['_is_c4_leader'] = (hist['c4_position'] == 1).astype(float)
        hist.loc[hist['c4_position'].isna(), '_is_c4_leader'] = np.nan
        
        # 累積統計をshift(1)で計算
        # 4C先頭率
        hist['_c4_leader_cumsum'] = hist.groupby('horse_id')['_is_c4_leader'].transform(
            lambda x: x.cumsum().shift(1)
        )
        hist['_c4_count'] = hist.groupby('horse_id')['_is_c4_leader'].transform(
            lambda x: x.notna().cumsum().shift(1)
        )
        hist['horse_c4_leader_rate'] = hist['_c4_leader_cumsum'] / hist['_c4_count']
        
        # 4C平均gap
        hist['_c4_gap_cumsum'] = hist.groupby('horse_id')['c4_gap'].transform(
            lambda x: x.cumsum().shift(1)
        )
        # horse_avg_c4_gapはV7で既に存在するため、horse_c4_leader_rateのみ保存
        
        # 各馬×レースの統計を保存
        self._c4_stats = hist[['race_id', 'horse_number', 'horse_c4_leader_rate']].copy()
        
        logger.info(f"  4C位置累積統計構築完了: {len(self._c4_stats):,}件")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V8のtransformを拡張"""
        # V8のtransform
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV10: 追加transform開始")
        
        # Phase 1: 馬体重変動
        result = self._add_weight_trend(result)
        
        # Phase 2: 上がり3F順位
        result = self._merge_last3f_rank_stats(result)
        
        # Phase 3: 4C位置
        result = self._merge_c4_stats(result)
        
        logger.info("LeakFreeFeatureEngineerV10: transform完了")
        return result
    
    def _add_weight_trend(self, df: pd.DataFrame) -> pd.DataFrame:
        """馬体重変動カテゴリを追加"""
        if 'horse_weight_change' not in df.columns:
            df['weight_trend'] = np.nan
            return df
        
        # 減量=-1, 維持=0, 増量=1
        conditions = [
            df['horse_weight_change'] < -4,  # 減量
            df['horse_weight_change'] > 4,   # 増量
        ]
        choices = [-1, 1]
        df['weight_trend'] = np.select(conditions, choices, default=0)
        
        # NaN処理
        df.loc[df['horse_weight_change'].isna(), 'weight_trend'] = np.nan
        
        return df
    
    def _merge_last3f_rank_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """上がり3F順位累積統計をマージ"""
        if self._last3f_rank_stats is None:
            df['horse_last3f_rank_avg'] = np.nan
            return df
        
        # race_id + horse_numberでマージ
        key_cols = ['race_id', 'horse_number']
        stats = self._last3f_rank_stats[key_cols + ['horse_last3f_rank_avg']].drop_duplicates(subset=key_cols)
        
        df = df.merge(stats, on=key_cols, how='left', suffixes=('', '_v10'))
        
        return df
    
    def _merge_c4_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """4C位置累積統計をマージ"""
        if self._c4_stats is None:
            df['horse_c4_leader_rate'] = np.nan
            return df
        
        # race_id + horse_numberでマージ
        key_cols = ['race_id', 'horse_number']
        stats = self._c4_stats[key_cols + ['horse_c4_leader_rate']].drop_duplicates(subset=key_cols)
        
        df = df.merge(stats, on=key_cols, how='left', suffixes=('', '_v10'))
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """使用特徴量"""
        return self.FEATURE_COLS

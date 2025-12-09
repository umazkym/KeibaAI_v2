"""
リークフリー特徴量エンジニア V13

V12 (Leak Fix) をベースに、V11で開発した5つの新特徴量を追加した完全版モデル。

ベース: LeakFreeFeatureEngineerV12
- class_change のリークが修正されている

追加特徴量 (V11由来):
1. prev_finish_category: 前走着順カテゴリ (巻き返し検出)
2. distance_change: 距離変化
3. distance_extend_flag: 距離延長フラグ
4. surface_switch_flag: 芝ダート転向フラグ
5. basis_weight_light_flag: 軽斤量フラグ

実装方針:
- V12を継承してリーク修正を維持
- V11の動的計算ロジック (_calc_prev_race_info 等) を移植
- 全ての特徴量はPoint-in-Timeで正確に計算される
"""

import pandas as pd
import numpy as np
from typing import Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v12 import LeakFreeFeatureEngineerV12, FeatureConfigV12
from .leak_free_feature_engineer_v11 import FeatureConfigV11

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV13(FeatureConfigV12, FeatureConfigV11):
    """V13用設定 (V12とV11の設定をマージ)"""
    pass


class LeakFreeFeatureEngineerV13(LeakFreeFeatureEngineerV12):
    """
    リークフリー特徴量エンジニア V13
    
    V12（リーク修正版）にV11の新特徴量を追加。
    """
    
    # V12の特徴量 + V11の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV12.FEATURE_COLS + [
        'prev_finish_category',      # 前走着順カテゴリ
        'distance_change',           # 距離変化
        'distance_extend_flag',      # 距離延長フラグ
        'surface_switch_flag',       # 芝ダート転向フラグ
        'basis_weight_light_flag',   # 軽斤量フラグ
    ]
    
    def __init__(self, config: Optional[FeatureConfigV13] = None):
        super().__init__(config or FeatureConfigV13())
        self._prev_race_info = None
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, race_details_df=None, returns_df=None, horses_df=None):
        logger.info("LeakFreeFeatureEngineerV13: fit開始")
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        logger.info("LeakFreeFeatureEngineerV13: fit完了")
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V12のtransform（クラス変動計算済み）の後にV11特徴量を追加"""
        
        # 1. Base transform (V12 -> V10.2 -> ...)
        # ここで class_change 等は V12 の修正ロジックで計算される
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV13: 追加transform開始")
        
        # 2. V11特徴量の計算 logic (V11から移植)
        
        # 前走情報を動的に計算（リークフリー）
        # V11と同じロジックを使用
        result = self._calc_prev_race_info(result)
        
        # 特徴量追加
        result = self._add_prev_finish_category(result)
        result = self._add_distance_features(result)
        result = self._add_surface_switch_flag(result)
        result = self._add_basis_weight_light_flag(result)
        
        logger.info("LeakFreeFeatureEngineerV13: transform完了")
        return result

    # --- 以下、V11から移植したメソッド群 ---

    def _calc_prev_race_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """前走情報を動的に計算（リークフリー）"""
        if not hasattr(self, '_full_history') or self._full_history is None:
            df['prev_finish'] = np.nan
            df['prev_distance'] = np.nan
            df['prev_surface'] = np.nan
            return df
        
        hist = self._full_history.copy()
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 各馬の最終レースの情報
        last_race = hist.groupby('horse_id').agg({
            'finish_position': 'last',
            'distance_m': 'last',
            'track_surface': 'last'
        }).reset_index()
        last_race.columns = ['horse_id', 'prev_finish', 'prev_distance', 'prev_surface']
        
        df = df.merge(last_race, on='horse_id', how='left', suffixes=('', '_prev'))
        
        for col in ['prev_finish', 'prev_distance', 'prev_surface']:
            if f'{col}_prev' in df.columns:
                df[col] = df[f'{col}_prev']
                df = df.drop(columns=[f'{col}_prev'])
        
        return df
    
    def _add_prev_finish_category(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'prev_finish' not in df.columns:
            df['prev_finish_category'] = np.nan
            return df
        
        prev_finish = pd.to_numeric(df['prev_finish'], errors='coerce')
        df['prev_finish_category'] = pd.cut(
            prev_finish,
            bins=[0, 1, 3, 9, float('inf')],
            labels=[0, 1, 2, 3],
            include_lowest=True
        ).astype(float)
        return df
    
    def _add_distance_features(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'prev_distance' in df.columns and 'distance_m' in df.columns:
            dist_m = pd.to_numeric(df['distance_m'], errors='coerce')
            prev_dist = pd.to_numeric(df['prev_distance'], errors='coerce')
            df['distance_change'] = dist_m - prev_dist
        else:
            df['distance_change'] = np.nan
        
        threshold = self.config.distance_extend_threshold
        dist_change = pd.to_numeric(df['distance_change'], errors='coerce')
        is_na = dist_change.isna()
        df['distance_extend_flag'] = (dist_change.fillna(0) >= threshold).astype(float)
        df.loc[is_na, 'distance_extend_flag'] = np.nan
        return df
    
    def _add_surface_switch_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'prev_surface' not in df.columns or 'track_surface' not in df.columns:
            df['surface_switch_flag'] = np.nan
            return df
        
        prev_surf = df['prev_surface'].astype(str)
        curr_surf = df['track_surface'].astype(str)
        is_na = df['prev_surface'].isna()
        
        df['surface_switch_flag'] = (prev_surf != curr_surf).astype(float)
        df.loc[is_na, 'surface_switch_flag'] = np.nan
        return df
    
    def _add_basis_weight_light_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'basis_weight' not in df.columns:
            df['basis_weight_light_flag'] = np.nan
            return df
        
        threshold = self.config.basis_weight_light_threshold
        basis_wt = pd.to_numeric(df['basis_weight'], errors='coerce')
        is_na = basis_wt.isna()
        
        df['basis_weight_light_flag'] = (basis_wt.fillna(999) < threshold).astype(float)
        df.loc[is_na, 'basis_weight_light_flag'] = np.nan
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

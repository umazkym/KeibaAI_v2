"""
リークフリー特徴量エンジニア V21

V15_Fixedをベースに、μモデル（タイム予測）開発で得た知見を統合。

【追加特徴量】
1. horse_normalized_time_avg: 正規化タイムの過去平均
2. horse_normalized_time_last: 直近の正規化タイム
3. horse_normalized_time_std: 正規化タイムの過去標準偏差

【μモデルからの還元ポイント】
- 競馬場×距離×馬場×状態で基準タイムを計算（階層的フォールバック）
- 「タイム」を条件間で比較可能な正規化値に変換
- 馬の「時間的な強さ」を直接的に特徴量化

【リーク対策】
- 基準タイム統計はTrain期間のデータのみで計算
- 馬の過去統計は shift(1) + expanding() で過去データのみ使用
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed, FeatureConfigV15Fixed

logger = logging.getLogger(__name__)

MIN_SAMPLES = 30


@dataclass
class FeatureConfigV21(FeatureConfigV15Fixed):
    """V21用設定"""
    min_samples_for_base_time: int = MIN_SAMPLES



class LeakFreeFeatureEngineerV21(LeakFreeFeatureEngineerV15Fixed):
    """
    リークフリー特徴量エンジニア V21
    
    V15_Fixedにμモデル開発で得た正規化タイム特徴量を追加:
    - horse_normalized_time_avg: 馬の過去正規化タイム平均
    - horse_normalized_time_last: 直近の正規化タイム
    - horse_normalized_time_std: 正規化タイムの標準偏差（安定度）
    """
    
    V21_FEATURES = [
        'horse_normalized_time_avg',
        'horse_normalized_time_last',
        'horse_normalized_time_std',
    ]
    
    def __init__(self, config: Optional[FeatureConfigV21] = None):
        super().__init__(config or FeatureConfigV21())
        self._base_time_stats: Dict = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        fitメソッドの拡張
        
        - μモデル由来の基準タイム統計を計算
        """
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        logger.info("LeakFreeFeatureEngineerV21: 基準タイム統計計算開始")
        
        # 基準タイム統計を計算
        self._calc_base_time_stats(races_df)
        
        logger.info("LeakFreeFeatureEngineerV21: fit完了")
        return self
    
    def _calc_base_time_stats(self, races_df: pd.DataFrame):
        """
        基準タイム統計を計算（Train期間のみ）
        
        μモデルと同じ階層的アプローチ:
        Level1: venue × distance × surface × condition
        Level2: venue × distance × surface
        Level3: distance × surface × condition
        """
        df = races_df.copy()
        df = df.dropna(subset=['finish_time_seconds', 'distance_m', 'track_surface', 'track_condition', 'venue'])
        
        # Level1
        level1 = df.groupby(
            ['venue', 'distance_m', 'track_surface', 'track_condition']
        )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        level1.columns = ['venue', 'distance_m', 'track_surface', 'track_condition', 'bt_l1_mean', 'bt_l1_std', 'bt_l1_count']
        level1['bt_l1_std'] = level1['bt_l1_std'].fillna(2.0).clip(lower=0.5)
        
        # Level2
        level2 = df.groupby(
            ['venue', 'distance_m', 'track_surface']
        )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        level2.columns = ['venue', 'distance_m', 'track_surface', 'bt_l2_mean', 'bt_l2_std', 'bt_l2_count']
        level2['bt_l2_std'] = level2['bt_l2_std'].fillna(2.0).clip(lower=0.5)
        
        # Level3
        level3 = df.groupby(
            ['distance_m', 'track_surface', 'track_condition']
        )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        level3.columns = ['distance_m', 'track_surface', 'track_condition', 'bt_l3_mean', 'bt_l3_std', 'bt_l3_count']
        level3['bt_l3_std'] = level3['bt_l3_std'].fillna(2.0).clip(lower=0.5)
        
        self._base_time_stats = {
            'level1': level1,
            'level2': level2,
            'level3': level3,
        }
        
        logger.info(f"  Base time stats: L1={len(level1)}, L2={len(level2)}, L3={len(level3)}")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V21のtransform"""
        # 親クラスのtransform
        df = super().transform(df)
        
        # μモデル由来特徴量を追加
        df = self._calc_normalized_time_features(df)
        
        return df
    
    def _calc_normalized_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        正規化タイム特徴量を計算
        """
        if 'finish_time_seconds' not in df.columns:
            return df
        
        logger.info("LeakFreeFeatureEngineerV21: 正規化タイム特徴量計算中")
        
        df = df.copy()
        min_samples = self.config.min_samples_for_base_time
        
        # 基準タイム統計をマージ
        level1 = self._base_time_stats.get('level1')
        level2 = self._base_time_stats.get('level2')
        level3 = self._base_time_stats.get('level3')
        
        if level1 is None:
            return df
        
        df = df.merge(level1, on=['venue', 'distance_m', 'track_surface', 'track_condition'], how='left')
        df = df.merge(level2, on=['venue', 'distance_m', 'track_surface'], how='left')
        df = df.merge(level3, on=['distance_m', 'track_surface', 'track_condition'], how='left')
        
        # 階層的フォールバック
        def select_base_time(row):
            if pd.notna(row.get('bt_l1_count')) and row['bt_l1_count'] >= min_samples:
                return row['bt_l1_mean'], row['bt_l1_std']
            if pd.notna(row.get('bt_l2_count')) and row['bt_l2_count'] >= min_samples:
                return row['bt_l2_mean'], row['bt_l2_std']
            if pd.notna(row.get('bt_l3_count')):
                return row['bt_l3_mean'], row['bt_l3_std']
            return np.nan, 2.0
        
        result = df.apply(select_base_time, axis=1, result_type='expand')
        df['_base_mean'] = result[0]
        df['_base_std'] = result[1]
        
        # 正規化タイム
        df['_normalized_time'] = (df['finish_time_seconds'] - df['_base_mean']) / df['_base_std']
        
        # 馬ごとの過去統計（リーク対策: shift + expanding）
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        df['horse_normalized_time_avg'] = (
            df.groupby('horse_id')['_normalized_time']
            .transform(lambda x: x.shift(1).expanding().mean())
        )
        df['horse_normalized_time_last'] = df.groupby('horse_id')['_normalized_time'].shift(1)
        df['horse_normalized_time_std'] = (
            df.groupby('horse_id')['_normalized_time']
            .transform(lambda x: x.shift(1).expanding().std())
        )
        
        # 有効率をログ
        for name in self.V21_FEATURES:
            valid = df[name].notna().sum()
            logger.info(f"    {name}有効: {valid:,}/{len(df):,} ({valid/len(df)*100:.1f}%)")
        
        # 作業用カラムを削除
        df = df.drop(columns=[
            '_base_mean', '_base_std', '_normalized_time',
            'bt_l1_mean', 'bt_l1_std', 'bt_l1_count',
            'bt_l2_mean', 'bt_l2_std', 'bt_l2_count',
            'bt_l3_mean', 'bt_l3_std', 'bt_l3_count',
        ], errors='ignore')
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """V21の特徴量カラムを返す"""
        base_features = super().get_feature_columns()
        v21_features = [f for f in self.V21_FEATURES if f not in base_features]
        return base_features + v21_features

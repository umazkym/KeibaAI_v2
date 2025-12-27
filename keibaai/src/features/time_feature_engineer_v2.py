#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
時間予測特化特徴量エンジン V2

【V1からの改善】
1. 芝・不良馬場のバイアス補正
2. 上がり3F特徴量の強化（距離別、馬場別）
3. レースラップ特徴量（過去の競馬場・距離・馬場でのペース傾向）

【リーク対策】
- 全ての特徴量は shift(1) + expanding() で計算
- ラップ特徴量も過去レースの累積統計のみ使用
"""

import logging
import numpy as np
import pandas as pd
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)


class TimeFeatureEngineerV2:
    """
    時間予測特化の特徴量エンジン V2
    """
    
    def __init__(self):
        self.base_time_stats: Optional[pd.DataFrame] = None
        self.venue_time_stats: Optional[pd.DataFrame] = None
        self.track_condition_bias: Optional[Dict] = None
        self.pace_stats: Optional[pd.DataFrame] = None
        self._feature_columns: List[str] = []
        self._is_fitted = False
    
    def fit(self, races_df: pd.DataFrame, race_details_df: Optional[pd.DataFrame] = None) -> 'TimeFeatureEngineerV2':
        """
        学習データから統計を計算
        """
        logger.info("TimeFeatureEngineerV2: fit開始")
        
        df = races_df.copy()
        df = df.dropna(subset=['finish_time_seconds', 'distance_m', 'track_surface', 'track_condition'])
        
        # 1. 距離×馬場×状態の基準タイム
        self.base_time_stats = df.groupby(
            ['distance_m', 'track_surface', 'track_condition']
        )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        self.base_time_stats.columns = [
            'distance_m', 'track_surface', 'track_condition',
            'base_time_mean', 'base_time_std', 'base_time_count'
        ]
        self.base_time_stats['base_time_std'] = self.base_time_stats['base_time_std'].fillna(2.0).clip(lower=0.5)
        
        logger.info(f"  基準タイム統計: {len(self.base_time_stats)}パターン")
        
        # 2. 馬場状態×馬場種別のバイアス（芝不良対策）
        self.track_condition_bias = {}
        for surface in ['芝', 'ダート']:
            for condition in ['良', '稍重', '重', '不良']:
                key = (surface, condition)
                sub = df[(df['track_surface'] == surface) & (df['track_condition'] == condition)]
                if len(sub) > 100:
                    # 基準（良馬場）との差
                    good_sub = df[(df['track_surface'] == surface) & (df['track_condition'] == '良')]
                    if len(good_sub) > 100:
                        # 距離ごとの差を計算（距離の影響を排除）
                        bias_list = []
                        for dist in sub['distance_m'].unique():
                            sub_dist = sub[sub['distance_m'] == dist]
                            good_dist = good_sub[good_sub['distance_m'] == dist]
                            if len(sub_dist) > 10 and len(good_dist) > 10:
                                bias = sub_dist['finish_time_seconds'].mean() - good_dist['finish_time_seconds'].mean()
                                bias_list.append(bias)
                        if bias_list:
                            self.track_condition_bias[key] = np.mean(bias_list)
                        else:
                            self.track_condition_bias[key] = 0.0
                    else:
                        self.track_condition_bias[key] = 0.0
                else:
                    self.track_condition_bias[key] = 0.0
        
        logger.info(f"  馬場バイアス: {self.track_condition_bias}")
        
        # 3. 競馬場×距離×馬場のペース統計（レースラップから）
        if race_details_df is not None and len(race_details_df) > 0:
            # race_detailsとracesをマージ
            detail_merged = race_details_df.merge(
                df[['race_id', 'distance_m', 'track_surface', 'track_condition', 'venue']].drop_duplicates(),
                on='race_id',
                how='left'
            )
            
            # 前半3Fペースの統計
            valid_pace = detail_merged[detail_merged['first_half'].notna()]
            if len(valid_pace) > 0:
                self.pace_stats = valid_pace.groupby(
                    ['venue', 'distance_m', 'track_surface']
                ).agg({
                    'first_half': ['mean', 'std'],
                    'second_half': ['mean', 'std']
                }).reset_index()
                self.pace_stats.columns = [
                    'venue', 'distance_m', 'track_surface',
                    'first_half_mean', 'first_half_std',
                    'second_half_mean', 'second_half_std'
                ]
                logger.info(f"  ペース統計: {len(self.pace_stats)}パターン")
        
        # 4. 競馬場×距離の基準タイム
        self.venue_time_stats = df.groupby(
            ['venue', 'distance_m', 'track_surface']
        )['finish_time_seconds'].agg(['mean', 'std']).reset_index()
        self.venue_time_stats.columns = [
            'venue', 'distance_m', 'track_surface',
            'venue_base_time_mean', 'venue_base_time_std'
        ]
        
        self._is_fitted = True
        logger.info("TimeFeatureEngineerV2: fit完了")
        
        return self
    
    def transform(self, races_df: pd.DataFrame) -> pd.DataFrame:
        """
        特徴量変換
        """
        if not self._is_fitted:
            raise RuntimeError("fitを先に実行してください")
        
        logger.info("TimeFeatureEngineerV2: transform開始")
        
        df = races_df.copy()
        
        # --- 1. 基準タイムをマージ ---
        df = df.merge(
            self.base_time_stats,
            on=['distance_m', 'track_surface', 'track_condition'],
            how='left'
        )
        df['base_time_mean'] = df['base_time_mean'].fillna(100)
        df['base_time_std'] = df['base_time_std'].fillna(2.0)
        
        # --- 2. 馬場バイアス補正 ---
        def get_bias(row):
            key = (row['track_surface'], row['track_condition'])
            return self.track_condition_bias.get(key, 0.0)
        
        df['track_condition_bias'] = df.apply(get_bias, axis=1)
        
        # バイアス補正版の基準タイム
        df['base_time_mean_corrected'] = df['base_time_mean'] - df['track_condition_bias']
        
        # --- 3. 標準化タイム（補正版） ---
        if 'finish_time_seconds' in df.columns:
            df['normalized_time'] = (df['finish_time_seconds'] - df['base_time_mean']) / df['base_time_std']
            df['normalized_time_corrected'] = (df['finish_time_seconds'] - df['base_time_mean_corrected']) / df['base_time_std']
        
        # --- 4. 競馬場×距離の基準タイム ---
        df = df.merge(
            self.venue_time_stats,
            on=['venue', 'distance_m', 'track_surface'],
            how='left'
        )
        df['venue_base_time_mean'] = df['venue_base_time_mean'].fillna(df['base_time_mean'])
        
        # --- 5. ペース統計 ---
        if self.pace_stats is not None:
            df = df.merge(
                self.pace_stats,
                on=['venue', 'distance_m', 'track_surface'],
                how='left'
            )
        
        # --- 6. 馬の過去タイム特徴量（リーク対策: shift + expanding）---
        logger.info("  馬の過去タイム特徴量を計算中...")
        
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 6.1 馬の標準化タイム
        if 'normalized_time' in df.columns:
            df['horse_normalized_time_avg'] = (
                df.groupby('horse_id')['normalized_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
            df['horse_normalized_time_std'] = (
                df.groupby('horse_id')['normalized_time']
                .transform(lambda x: x.shift(1).expanding().std())
            )
            df['horse_last_normalized_time'] = df.groupby('horse_id')['normalized_time'].shift(1)
        
        # 6.2 馬の生タイム
        if 'finish_time_seconds' in df.columns:
            df['horse_time_avg'] = (
                df.groupby('horse_id')['finish_time_seconds']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
            df['horse_time_best'] = (
                df.groupby('horse_id')['finish_time_seconds']
                .transform(lambda x: x.shift(1).expanding().min())
            )
            df['horse_last_time'] = df.groupby('horse_id')['finish_time_seconds'].shift(1)
        
        # --- 7. 上がり3F特徴量の強化 ---
        logger.info("  上がり3F特徴量を強化中...")
        
        if 'last_3f_time' in df.columns:
            # 基本
            df['horse_last3f_avg'] = (
                df.groupby('horse_id')['last_3f_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
            df['horse_last3f_best'] = (
                df.groupby('horse_id')['last_3f_time']
                .transform(lambda x: x.shift(1).expanding().min())
            )
            df['horse_last_last3f'] = df.groupby('horse_id')['last_3f_time'].shift(1)
            
            # 距離カテゴリ別の上がり3F
            df['distance_category'] = pd.cut(
                df['distance_m'],
                bins=[0, 1400, 1800, 5000],
                labels=['sprint', 'mile', 'long']
            )
            df['horse_last3f_avg_by_dist'] = (
                df.groupby(['horse_id', 'distance_category'])['last_3f_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
            
            # 馬場別の上がり3F
            df['horse_last3f_avg_by_surface'] = (
                df.groupby(['horse_id', 'track_surface'])['last_3f_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
            
            # 上がり3Fの改善傾向（直近3走の傾向）
            df['horse_last3f_trend'] = (
                df.groupby('horse_id')['last_3f_time']
                .transform(lambda x: x.shift(1).rolling(3, min_periods=2).apply(
                    lambda s: s.iloc[-1] - s.iloc[0] if len(s) >= 2 else 0, raw=False
                ))
            )
            
            # 上がり3Fのベストとの差（安定性指標）
            df['horse_last3f_consistency'] = df['horse_last3f_avg'] - df['horse_last3f_best']
        
        # --- 8. 同距離での過去タイム ---
        logger.info("  同距離での過去タイム特徴量...")
        
        if 'normalized_time' in df.columns:
            df['horse_same_dist_norm_time'] = (
                df.groupby(['horse_id', 'distance_category'])['normalized_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
        
        # --- 9. 騎手・調教師の平均タイム偏差 ---
        if 'normalized_time' in df.columns:
            if 'jockey_id' in df.columns:
                df['jockey_normalized_time_avg'] = (
                    df.groupby('jockey_id')['normalized_time']
                    .transform(lambda x: x.shift(1).expanding().mean())
                )
            if 'trainer_id' in df.columns:
                df['trainer_normalized_time_avg'] = (
                    df.groupby('trainer_id')['normalized_time']
                    .transform(lambda x: x.shift(1).expanding().mean())
                )
        
        # --- 特徴量リスト更新 ---
        self._feature_columns = [
            # バイアス補正関連
            'track_condition_bias',
            
            # ペース関連
            'first_half_mean',
            'first_half_std',
            'second_half_mean',
            
            # 馬の標準化タイム
            'horse_normalized_time_avg',
            'horse_normalized_time_std',
            'horse_last_normalized_time',
            
            # 馬の生タイム
            'horse_time_avg',
            'horse_time_best',
            'horse_last_time',
            
            # 上がり3F（強化版）
            'horse_last3f_avg',
            'horse_last3f_best',
            'horse_last_last3f',
            'horse_last3f_avg_by_dist',
            'horse_last3f_avg_by_surface',
            'horse_last3f_trend',
            'horse_last3f_consistency',
            
            # 同距離
            'horse_same_dist_norm_time',
            
            # 騎手・調教師
            'jockey_normalized_time_avg',
            'trainer_normalized_time_avg',
        ]
        
        self._feature_columns = [c for c in self._feature_columns if c in df.columns]
        
        logger.info(f"  時間予測特徴量数: {len(self._feature_columns)}")
        logger.info("TimeFeatureEngineerV2: transform完了")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self._feature_columns
    
    def get_base_time_stats(self) -> pd.DataFrame:
        return self.base_time_stats
    
    def get_track_condition_bias(self) -> Dict:
        return self.track_condition_bias

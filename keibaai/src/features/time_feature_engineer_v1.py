#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
時間予測特化特徴量エンジン V1

目的: finish_time_secondsを予測するための特徴量を生成

【リーク対策】
1. 全ての過去タイム特徴量は shift(1) + expanding() で計算
2. 同一レースの情報は絶対に使用しない
3. 基準タイムは学習期間のデータのみで計算

【過学習対策】
1. 特徴量数を必要最小限に抑制
2. expanding()による累積統計で外れ値の影響を軽減
"""

import logging
import numpy as np
import pandas as pd
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)


class TimeFeatureEngineerV1:
    """
    時間予測特化の特徴量エンジン
    
    V15特徴量に加えて、過去タイム特徴量を追加
    """
    
    def __init__(self):
        self.base_time_stats: Optional[pd.DataFrame] = None
        self.horse_time_stats: Optional[pd.DataFrame] = None
        self.venue_time_stats: Optional[pd.DataFrame] = None
        self._feature_columns: List[str] = []
        self._is_fitted = False
    
    def fit(self, races_df: pd.DataFrame) -> 'TimeFeatureEngineerV1':
        """
        学習データから基準タイム統計を計算
        
        【リーク対策】
        - fit時点までのデータのみで統計を計算
        - これにより、未来の情報が統計に含まれない
        """
        logger.info("TimeFeatureEngineerV1: fit開始")
        
        df = races_df.copy()
        df = df.dropna(subset=['finish_time_seconds', 'distance_m', 'track_surface', 'track_condition'])
        
        # 1. 距離×馬場×馬場状態の基準タイム
        self.base_time_stats = df.groupby(
            ['distance_m', 'track_surface', 'track_condition']
        )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        self.base_time_stats.columns = [
            'distance_m', 'track_surface', 'track_condition',
            'base_time_mean', 'base_time_std', 'base_time_count'
        ]
        # std=0 or NaN対策
        self.base_time_stats['base_time_std'] = self.base_time_stats['base_time_std'].fillna(2.0).clip(lower=0.5)
        
        logger.info(f"  基準タイム統計: {len(self.base_time_stats)}パターン")
        
        # 2. 競馬場×距離の基準タイム
        self.venue_time_stats = df.groupby(
            ['venue', 'distance_m', 'track_surface']
        )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        self.venue_time_stats.columns = [
            'venue', 'distance_m', 'track_surface',
            'venue_base_time_mean', 'venue_base_time_std', 'venue_base_time_count'
        ]
        self.venue_time_stats['venue_base_time_std'] = self.venue_time_stats['venue_base_time_std'].fillna(2.0).clip(lower=0.5)
        
        logger.info(f"  競馬場別タイム統計: {len(self.venue_time_stats)}パターン")
        
        self._is_fitted = True
        logger.info("TimeFeatureEngineerV1: fit完了")
        
        return self
    
    def transform(self, races_df: pd.DataFrame) -> pd.DataFrame:
        """
        特徴量変換
        
        【リーク対策】
        - 馬の過去タイムは shift(1) + expanding() で計算
        - race_dateでソートしてから計算
        """
        if not self._is_fitted:
            raise RuntimeError("fitを先に実行してください")
        
        logger.info("TimeFeatureEngineerV1: transform開始")
        
        df = races_df.copy()
        
        # --- 1. 基準タイムをマージ ---
        df = df.merge(
            self.base_time_stats,
            on=['distance_m', 'track_surface', 'track_condition'],
            how='left'
        )
        
        # 欠損の場合はデフォルト値
        df['base_time_mean'] = df['base_time_mean'].fillna(df['finish_time_seconds'].mean() if 'finish_time_seconds' in df.columns else 100)
        df['base_time_std'] = df['base_time_std'].fillna(2.0)
        
        # --- 2. 標準化タイム（ターゲット用） ---
        if 'finish_time_seconds' in df.columns:
            df['normalized_time'] = (df['finish_time_seconds'] - df['base_time_mean']) / df['base_time_std']
        
        # --- 3. 競馬場別基準タイム ---
        df = df.merge(
            self.venue_time_stats,
            on=['venue', 'distance_m', 'track_surface'],
            how='left'
        )
        df['venue_base_time_mean'] = df['venue_base_time_mean'].fillna(df['base_time_mean'])
        
        # --- 4. 馬の過去タイム特徴量（リーク対策: shift + expanding）---
        logger.info("  馬の過去タイム特徴量を計算中...")
        
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 4.1 馬の標準化タイム（全距離・馬場）
        if 'normalized_time' in df.columns:
            df['horse_normalized_time_cum'] = (
                df.groupby('horse_id')['normalized_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
            df['horse_normalized_time_std'] = (
                df.groupby('horse_id')['normalized_time']
                .transform(lambda x: x.shift(1).expanding().std())
            )
            # 直近の標準化タイム
            df['horse_last_normalized_time'] = df.groupby('horse_id')['normalized_time'].shift(1)
        
        # 4.2 馬の生タイム累積平均
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
        
        # 4.3 馬の上がり3F
        if 'last_3f_time' in df.columns:
            df['horse_last3f_avg'] = (
                df.groupby('horse_id')['last_3f_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
            df['horse_last3f_best'] = (
                df.groupby('horse_id')['last_3f_time']
                .transform(lambda x: x.shift(1).expanding().min())
            )
            df['horse_last_last3f'] = df.groupby('horse_id')['last_3f_time'].shift(1)
        
        # --- 5. 同距離での過去タイム ---
        logger.info("  同距離での過去タイム特徴量を計算中...")
        
        # 距離カテゴリを作成（1000-1400: Sprint, 1600-1800: Mile, 2000+: Long）
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 5000],
            labels=['sprint', 'mile', 'long']
        )
        
        # 同距離カテゴリでの標準化タイム
        if 'normalized_time' in df.columns:
            df['horse_same_dist_cat_norm_time'] = (
                df.groupby(['horse_id', 'distance_category'])['normalized_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
        
        # --- 6. 騎手の平均タイム偏差 ---
        if 'normalized_time' in df.columns and 'jockey_id' in df.columns:
            df['jockey_normalized_time_avg'] = (
                df.groupby('jockey_id')['normalized_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
        
        # --- 7. 調教師の平均タイム偏差 ---
        if 'normalized_time' in df.columns and 'trainer_id' in df.columns:
            df['trainer_normalized_time_avg'] = (
                df.groupby('trainer_id')['normalized_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
        
        # --- 8. 特徴量リストを更新 ---
        self._feature_columns = [
            # 基準タイム関連
            'base_time_mean',
            'base_time_std',
            'venue_base_time_mean',
            
            # 馬の標準化タイム
            'horse_normalized_time_cum',
            'horse_normalized_time_std',
            'horse_last_normalized_time',
            
            # 馬の生タイム
            'horse_time_avg',
            'horse_time_best',
            'horse_last_time',
            
            # 上がり3F
            'horse_last3f_avg',
            'horse_last3f_best',
            'horse_last_last3f',
            
            # 同距離カテゴリ
            'horse_same_dist_cat_norm_time',
            
            # 騎手・調教師
            'jockey_normalized_time_avg',
            'trainer_normalized_time_avg',
        ]
        
        # 実際に存在する列のみ
        self._feature_columns = [c for c in self._feature_columns if c in df.columns]
        
        logger.info(f"  時間予測特徴量数: {len(self._feature_columns)}")
        logger.info("TimeFeatureEngineerV1: transform完了")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """時間予測特徴量の列名を返す"""
        return self._feature_columns
    
    def get_base_time_stats(self) -> pd.DataFrame:
        """基準タイム統計を返す（逆変換用）"""
        return self.base_time_stats

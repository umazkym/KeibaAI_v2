#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
時間予測特化特徴量エンジン V3

【V2からの改善】
競馬場×距離×馬場×状態で基準タイムを計算し、階層的フォールバックを実装

【階層的フォールバック】
Level1: venue × distance × surface × condition （N>=30で使用）
Level2: venue × distance × surface             （N>=30で使用）
Level3: distance × surface × condition         （常に十分なサンプル）

【リーク対策】
- 全統計はTrain期間のデータのみで計算
- 馬の過去タイムは shift(1) + expanding()

【過学習対策】
- 組み合わせのサンプル数チェック（N<30でフォールバック）
- 階層的な平滑化で外れ値の影響を軽減
"""

import logging
import numpy as np
import pandas as pd
from typing import Optional, List, Dict, Tuple

logger = logging.getLogger(__name__)

MIN_SAMPLES = 30  # フォールバック閾値


class TimeFeatureEngineerV3:
    """
    時間予測特化の特徴量エンジン V3（競馬場×距離別基準タイム）
    """
    
    def __init__(self, min_samples: int = MIN_SAMPLES):
        self.min_samples = min_samples
        
        # 各レベルの統計
        self.stats_level1: Optional[pd.DataFrame] = None  # venue×dist×surf×cond
        self.stats_level2: Optional[pd.DataFrame] = None  # venue×dist×surf
        self.stats_level3: Optional[pd.DataFrame] = None  # dist×surf×cond
        
        self._feature_columns: List[str] = []
        self._is_fitted = False
    
    def fit(self, races_df: pd.DataFrame) -> 'TimeFeatureEngineerV3':
        """
        学習データから階層的な基準タイム統計を計算
        """
        logger.info("TimeFeatureEngineerV3: fit開始")
        logger.info(f"  フォールバック閾値: N >= {self.min_samples}")
        
        df = races_df.copy()
        df = df.dropna(subset=['finish_time_seconds', 'distance_m', 'track_surface', 'track_condition', 'venue'])
        
        logger.info(f"  有効データ: {len(df):,}件")
        
        # Level1: venue × distance × surface × condition
        self.stats_level1 = df.groupby(
            ['venue', 'distance_m', 'track_surface', 'track_condition']
        )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        self.stats_level1.columns = [
            'venue', 'distance_m', 'track_surface', 'track_condition',
            'l1_mean', 'l1_std', 'l1_count'
        ]
        self.stats_level1['l1_std'] = self.stats_level1['l1_std'].fillna(2.0).clip(lower=0.5)
        
        level1_valid = (self.stats_level1['l1_count'] >= self.min_samples).sum()
        logger.info(f"  Level1 (venue×dist×surf×cond): {len(self.stats_level1)}パターン, N>={self.min_samples}: {level1_valid}")
        
        # Level2: venue × distance × surface
        self.stats_level2 = df.groupby(
            ['venue', 'distance_m', 'track_surface']
        )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        self.stats_level2.columns = [
            'venue', 'distance_m', 'track_surface',
            'l2_mean', 'l2_std', 'l2_count'
        ]
        self.stats_level2['l2_std'] = self.stats_level2['l2_std'].fillna(2.0).clip(lower=0.5)
        
        level2_valid = (self.stats_level2['l2_count'] >= self.min_samples).sum()
        logger.info(f"  Level2 (venue×dist×surf): {len(self.stats_level2)}パターン, N>={self.min_samples}: {level2_valid}")
        
        # Level3: distance × surface × condition
        self.stats_level3 = df.groupby(
            ['distance_m', 'track_surface', 'track_condition']
        )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        self.stats_level3.columns = [
            'distance_m', 'track_surface', 'track_condition',
            'l3_mean', 'l3_std', 'l3_count'
        ]
        self.stats_level3['l3_std'] = self.stats_level3['l3_std'].fillna(2.0).clip(lower=0.5)
        
        level3_valid = (self.stats_level3['l3_count'] >= self.min_samples).sum()
        logger.info(f"  Level3 (dist×surf×cond): {len(self.stats_level3)}パターン, N>={self.min_samples}: {level3_valid}")
        
        self._is_fitted = True
        logger.info("TimeFeatureEngineerV3: fit完了")
        
        return self
    
    def transform(self, races_df: pd.DataFrame) -> pd.DataFrame:
        """
        特徴量変換（階層的フォールバック付き）
        """
        if not self._is_fitted:
            raise RuntimeError("fitを先に実行してください")
        
        logger.info("TimeFeatureEngineerV3: transform開始")
        
        df = races_df.copy()
        
        # --- 各レベルの統計をマージ ---
        df = df.merge(
            self.stats_level1,
            on=['venue', 'distance_m', 'track_surface', 'track_condition'],
            how='left'
        )
        
        df = df.merge(
            self.stats_level2,
            on=['venue', 'distance_m', 'track_surface'],
            how='left'
        )
        
        df = df.merge(
            self.stats_level3,
            on=['distance_m', 'track_surface', 'track_condition'],
            how='left'
        )
        
        # --- 階層的フォールバックで基準タイムを決定 ---
        def select_base_time(row):
            # Level1が十分なら使用
            if pd.notna(row['l1_count']) and row['l1_count'] >= self.min_samples:
                return row['l1_mean'], row['l1_std'], 1
            # Level2が十分なら使用
            if pd.notna(row['l2_count']) and row['l2_count'] >= self.min_samples:
                return row['l2_mean'], row['l2_std'], 2
            # Level3を使用
            if pd.notna(row['l3_count']):
                return row['l3_mean'], row['l3_std'], 3
            # 最終フォールバック
            return 100.0, 2.0, 0
        
        logger.info("  階層的フォールバックで基準タイムを選択中...")
        result = df.apply(select_base_time, axis=1, result_type='expand')
        df['base_time_mean'] = result[0]
        df['base_time_std'] = result[1]
        df['base_time_level'] = result[2]
        
        # 使用レベルの分布
        level_dist = df['base_time_level'].value_counts().sort_index()
        for level, count in level_dist.items():
            pct = count / len(df) * 100
            logger.info(f"    Level{level}: {count:,}件 ({pct:.1f}%)")
        
        # --- 標準化タイム ---
        if 'finish_time_seconds' in df.columns:
            df['normalized_time'] = (df['finish_time_seconds'] - df['base_time_mean']) / df['base_time_std']
        
        # --- 馬の過去タイム特徴量（リーク対策: shift + expanding）---
        logger.info("  馬の過去タイム特徴量を計算中...")
        
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
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
        
        # --- 上がり3F特徴量 ---
        logger.info("  上がり3F特徴量を計算中...")
        
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
            
            # 距離カテゴリ
            df['distance_category'] = pd.cut(
                df['distance_m'],
                bins=[0, 1400, 1800, 5000],
                labels=['sprint', 'mile', 'long']
            )
            df['horse_last3f_avg_by_dist'] = (
                df.groupby(['horse_id', 'distance_category'])['last_3f_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
        
        # --- 同条件（venue×dist×surf）での過去成績 ---
        logger.info("  同条件での過去成績を計算中...")
        
        if 'normalized_time' in df.columns:
            df['horse_same_venue_dist_time'] = (
                df.groupby(['horse_id', 'venue', 'distance_m', 'track_surface'])['normalized_time']
                .transform(lambda x: x.shift(1).expanding().mean())
            )
        
        # --- 騎手・調教師 ---
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
        
        # --- 特徴量リスト ---
        self._feature_columns = [
            # 基準タイム関連（予測には使わないが参照用）
            'base_time_level',
            
            # 馬の標準化タイム
            'horse_normalized_time_avg',
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
            'horse_last3f_avg_by_dist',
            
            # 同条件成績
            'horse_same_venue_dist_time',
            
            # 騎手・調教師
            'jockey_normalized_time_avg',
            'trainer_normalized_time_avg',
        ]
        
        self._feature_columns = [c for c in self._feature_columns if c in df.columns]
        
        logger.info(f"  時間予測特徴量数: {len(self._feature_columns)}")
        logger.info("TimeFeatureEngineerV3: transform完了")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self._feature_columns
    
    def get_base_time_stats(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """基準タイム統計を返す（逆変換用）"""
        return self.stats_level1, self.stats_level2, self.stats_level3

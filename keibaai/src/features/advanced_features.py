# src/features/advanced_features.py (新規)

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging

class AdvancedFeatureEngine:
    """モデル精度向上のための高度な特徴量生成"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def generate_performance_trend_features(
        self, 
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """パフォーマンストレンド特徴量の生成"""
        
        # 1. 直近N走の成績トレンド
        windows = [3, 5, 10]
        
        for horse_id in df['horse_id'].unique():
            horse_perf = performance_df[
                performance_df['horse_id'] == horse_id
            ].sort_values('race_date')
            
            if len(horse_perf) == 0:
                continue
            
            # 各ウィンドウでの集計
            for w in windows:
                recent_races = horse_perf.tail(w)
                
                # 平均着順のトレンド
                df.loc[df['horse_id'] == horse_id, f'avg_finish_last{w}'] = \
                    recent_races['finish_position'].mean()
                
                # 着順の改善率
                if len(recent_races) >= 2:
                    first_half = recent_races.head(w//2)['finish_position'].mean()
                    second_half = recent_races.tail(w//2)['finish_position'].mean()
                    improvement = (first_half - second_half) / first_half
                    df.loc[df['horse_id'] == horse_id, f'improvement_rate_{w}'] = improvement
                
                # 勝率
                win_rate = (recent_races['finish_position'] == 1).mean()
                df.loc[df['horse_id'] == horse_id, f'win_rate_last{w}'] = win_rate
                
                # 連対率（2着以内）
                place_rate = (recent_races['finish_position'] <= 2).mean()
                df.loc[df['horse_id'] == horse_id, f'place_rate_last{w}'] = place_rate
        
        return df
    
    def generate_course_affinity_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """コース適性特徴量の生成"""
        
        # 競馬場別成績
        venue_stats = performance_df.groupby(['horse_id', 'place']).agg({
            'finish_position': ['mean', 'count'],
            'win_odds': 'mean'
        }).reset_index()
        
        venue_stats.columns = ['horse_id', 'place', 'venue_avg_finish', 
                              'venue_races', 'venue_avg_odds']
        
        # 距離別成績
        performance_df['distance_category'] = pd.cut(
            performance_df['distance_m'],
            bins=[0, 1400, 1800, 2200, 3000, 4000],
            labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
        )
        
        distance_stats = performance_df.groupby(['horse_id', 'distance_category']).agg({
            'finish_position': ['mean', 'count'],
            'finish_time_sec': 'mean'
        }).reset_index()
        
        distance_stats.columns = ['horse_id', 'distance_category', 
                                 'dist_avg_finish', 'dist_races', 'dist_avg_time']
        
        # 馬場別成績
        surface_stats = performance_df.groupby(['horse_id', 'track_surface']).agg({
            'finish_position': ['mean', 'count'],
            'last_3f_time': 'mean'
        }).reset_index()
        
        surface_stats.columns = ['horse_id', 'track_surface', 
                                'surface_avg_finish', 'surface_races', 'surface_avg_last3f']
        
        # メインデータフレームにマージ
        df = df.merge(venue_stats, on=['horse_id', 'place'], how='left')
        df = df.merge(distance_stats, on=['horse_id', 'distance_category'], how='left')
        df = df.merge(surface_stats, on=['horse_id', 'track_surface'], how='left')
        
        return df
    
    def generate_jockey_trainer_synergy(
        self,
        df: pd.DataFrame,
        historical_df: pd.DataFrame
    ) -> pd.DataFrame:
        """騎手・調教師の相性特徴量"""
        
        # 騎手×調教師のコンビ成績
        combo_stats = historical_df.groupby(['jockey_id', 'trainer_id']).agg({
            'finish_position': ['mean', 'count'],
            'win_odds': 'mean',
            'popularity': 'mean'
        }).reset_index()
        
        combo_stats.columns = ['jockey_id', 'trainer_id', 'combo_avg_finish',
                              'combo_races', 'combo_avg_odds', 'combo_avg_popularity']
        
        # 期待値を上回る度合い
        combo_stats['combo_overperform'] = \
            combo_stats['combo_avg_popularity'] - combo_stats['combo_avg_finish']
        
        df = df.merge(combo_stats, on=['jockey_id', 'trainer_id'], how='left')
        
        return df
    
    def generate_bloodline_features(
        self,
        df: pd.DataFrame,
        pedigree_df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """血統特徴量の生成"""
        
        # 父系の成績集計
        sire_stats = performance_df.merge(
            pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']],
            on='horse_id'
        ).groupby('ancestor_id').agg({
            'finish_position': ['mean', 'std'],
            'distance_m': 'mean',
            'win_odds': 'mean'
        }).reset_index()
        
        sire_stats.columns = ['sire_id', 'sire_avg_finish', 'sire_std_finish',
                             'sire_avg_distance', 'sire_avg_odds']
        
        # 母父系の成績集計
        damsire_df = pedigree_df[
            (pedigree_df['generation'] == 2) & 
            (pedigree_df['ancestor_name'].str.contains('母'))
        ]
        
        # ニックス（相性の良い配合）
        # 父×母父の組み合わせでの成績
        
        return df
    
    def generate_race_condition_features(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """レース条件に関する特徴量"""
        
        # 1. フィールドサイズの影響
        df['field_size_category'] = pd.cut(
            df['head_count'],
            bins=[0, 10, 14, 18, 24],
            labels=['small', 'medium', 'large', 'extra_large']
        )
        
        # 2. 季節性
        df['race_month'] = pd.to_datetime(df['race_date']).dt.month
        df['race_season'] = df['race_month'].map({
            12: 'winter', 1: 'winter', 2: 'winter',
            3: 'spring', 4: 'spring', 5: 'spring',
            6: 'summer', 7: 'summer', 8: 'summer',
            9: 'autumn', 10: 'autumn', 11: 'autumn'
        })
        
        # 3. レースの重要度（賞金ベース）
        df['race_importance'] = df['prize_1st'].fillna(500).apply(
            lambda x: 'high' if x >= 2000 else ('medium' if x >= 1000 else 'low')
        )
        
        # 4. 休養明けフラグ
        # （過去成績データと結合後に計算）
        
        return df
    
    def calculate_relative_metrics(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """レース内での相対的な指標"""
        
        # グループごとの処理
        for race_id in df['race_id'].unique():
            race_df = df[df['race_id'] == race_id]
            race_indices = race_df.index
            
            # 1. タイムの偏差値
            if 'finish_time_seconds' in df.columns:
                mean_time = race_df['finish_time_seconds'].mean()
                std_time = race_df['finish_time_seconds'].std()
                if std_time > 0:
                    df.loc[race_indices, 'time_deviation'] = \
                        50 + 10 * (race_df['finish_time_seconds'] - mean_time) / std_time
            
            # 2. 上がり3Fの相対値
            if 'last_3f_time' in df.columns:
                min_last3f = race_df['last_3f_time'].min()
                df.loc[race_indices, 'last3f_diff_from_best'] = \
                    race_df['last_3f_time'] - min_last3f
            
            # 3. オッズの順位
            if 'win_odds' in df.columns:
                df.loc[race_indices, 'odds_rank'] = \
                    race_df['win_odds'].rank(method='min')
            
            # 4. 斤量の相対値
            if 'basis_weight' in df.columns:
                avg_weight = race_df['basis_weight'].mean()
                df.loc[race_indices, 'weight_diff_from_avg'] = \
                    race_df['basis_weight'] - avg_weight
        
        return df
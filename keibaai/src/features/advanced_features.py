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
        
        df = df.merge(sire_stats, left_on='sire_id', right_on='sire_id', how='left')
        
        return df

    def generate_pace_features(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """展開・ペース特徴量"""
        # 通過順位から脚質を判定する簡易ロジック
        # 1-1-1-1 のような形式を解析
        
        def estimate_running_style(passing_order_str):
            if not isinstance(passing_order_str, str):
                return 'unknown'
            try:
                # 最初の通過順を取得
                first_pos = int(passing_order_str.split('-')[0])
                if first_pos == 1:
                    return 'nige' # 逃げ
                elif first_pos <= 4:
                    return 'senko' # 先行
                elif first_pos <= 10:
                    return 'sashi' # 差し
                else:
                    return 'oikomi' # 追込
            except:
                return 'unknown'

        if 'passing_order' in df.columns:
            df['running_style'] = df['passing_order'].apply(estimate_running_style)
            
            # レースごとの脚質構成比率
            race_styles = df.groupby('race_id')['running_style'].value_counts(normalize=True).unstack(fill_value=0)
            
            # 逃げ馬の割合（ペース予想の指標）
            if 'nige' in race_styles.columns:
                df = df.merge(race_styles[['nige']].rename(columns={'nige': 'nige_ratio'}), on='race_id', how='left')
            else:
                df['nige_ratio'] = 0.0
                
            # 先行馬の割合
            if 'senko' in race_styles.columns:
                df = df.merge(race_styles[['senko']].rename(columns={'senko': 'senko_ratio'}), on='race_id', how='left')
            else:
                df['senko_ratio'] = 0.0

        return df

    def generate_deep_pedigree_features(
        self,
        df: pd.DataFrame,
        pedigree_df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """詳細な血統特徴量（ニックス、コース適性）"""
        
        # 1. ニックス（父×母父）
        # まず、各馬の父と母父を特定
        sires = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].rename(columns={'ancestor_id': 'sire_id'})
        damsires = pedigree_df[(pedigree_df['generation'] == 2) & (pedigree_df['ancestor_name'].str.contains('母'))][['horse_id', 'ancestor_id']].rename(columns={'ancestor_id': 'damsire_id'})
        
        horse_pedigree = sires.merge(damsires, on='horse_id', how='inner')
        
        # パフォーマンスデータに血統情報を結合
        perf_ped = performance_df.merge(horse_pedigree, on='horse_id', how='inner')
        
        # ニックスごとの成績集計
        nicks_stats = perf_ped.groupby(['sire_id', 'damsire_id']).agg({
            'finish_position': ['mean', 'count', 'std'],
            'win_odds': 'mean'
        }).reset_index()
        nicks_stats.columns = ['sire_id', 'damsire_id', 'nicks_avg_finish', 'nicks_count', 'nicks_std_finish', 'nicks_avg_odds']
        
        # 信頼度のため、ある程度の出走数があるもののみ採用
        nicks_stats = nicks_stats[nicks_stats['nicks_count'] >= 5]
        
        # メインデータフレームに結合（父・母父が必要）
        # dfにsire_id, damsire_idがある前提、なければ結合してから
        if 'sire_id' not in df.columns or 'damsire_id' not in df.columns:
             df = df.merge(horse_pedigree, on='horse_id', how='left')
             
        df = df.merge(nicks_stats, on=['sire_id', 'damsire_id'], how='left')
        
        # 2. 種牡馬×コース適性
        # 種牡馬ごとの、競馬場・距離カテゴリ・芝ダート別の成績
        perf_ped['distance_category'] = pd.cut(
            perf_ped['distance_m'],
            bins=[0, 1400, 1800, 2200, 3000, 4000],
            labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
        )
        
        sire_course_stats = perf_ped.groupby(['sire_id', 'place', 'distance_category', 'track_surface']).agg({
            'finish_position': 'mean',
            'win_odds': 'mean'
        }).reset_index()
        sire_course_stats.columns = ['sire_id', 'place', 'distance_category', 'track_surface', 'sire_course_avg_finish', 'sire_course_avg_odds']
        
        # メインデータフレームに結合
        # dfにdistance_categoryなどが必要
        if 'distance_category' not in df.columns:
             df['distance_category'] = pd.cut(
                df['distance_m'],
                bins=[0, 1400, 1800, 2200, 3000, 4000],
                labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
            )
            
        df = df.merge(sire_course_stats, on=['sire_id', 'place', 'distance_category', 'track_surface'], how='left')

        return df

    def generate_course_bias_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """コースバイアス（枠順など）"""
        
        # 枠順バイアス
        # コース（競馬場、距離、芝ダート）ごとの枠番別成績
        
        performance_df['distance_category'] = pd.cut(
            performance_df['distance_m'],
            bins=[0, 1400, 1800, 2200, 3000, 4000],
            labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
        )
        
        bracket_stats = performance_df.groupby(['place', 'distance_category', 'track_surface', 'bracket_number']).agg({
            'finish_position': 'mean'
        }).reset_index()
        bracket_stats.columns = ['place', 'distance_category', 'track_surface', 'bracket_number', 'bracket_avg_finish']
        
        # メインデータフレームに結合
        if 'distance_category' not in df.columns:
             df['distance_category'] = pd.cut(
                df['distance_m'],
                bins=[0, 1400, 1800, 2200, 3000, 4000],
                labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
            )
            
        df = df.merge(bracket_stats, on=['place', 'distance_category', 'track_surface', 'bracket_number'], how='left')
        
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
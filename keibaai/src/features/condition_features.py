"""
条件別特徴量生成モジュール（ROI改善v3.6用）

【施策1】騎手の距離別・馬場別勝率
【施策2】着差情報（秒）の活用
【施策3】血統の条件別産駒成績
"""
import pandas as pd
import numpy as np
import logging


class ConditionSpecificFeatureEngine:
    """条件別特徴量生成エンジン"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def generate_all(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame,
        pedigree_df: pd.DataFrame = None
    ) -> pd.DataFrame:
        """全ての条件別特徴量を生成"""
        self.logger.info("=" * 60)
        self.logger.info("条件別特徴量の生成開始（v3.6 ROI改善）")
        self.logger.info("=" * 60)
        
        # ID型変換
        for col in ['horse_id', 'jockey_id', 'trainer_id']:
            if col in df.columns:
                df[col] = df[col].astype(str)
            if col in performance_df.columns:
                performance_df = performance_df.copy()
                performance_df[col] = performance_df[col].astype(str)
        
        # 距離カテゴリの作成
        if 'distance_category' not in performance_df.columns:
            performance_df['distance_category'] = pd.cut(
                performance_df['distance_m'],
                bins=[0, 1400, 1800, 2200, 3000, 4000],
                labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
            )
        if 'distance_category' not in df.columns:
            df['distance_category'] = pd.cut(
                df['distance_m'],
                bins=[0, 1400, 1800, 2200, 3000, 4000],
                labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
            )
        
        # is_win作成
        if 'is_win' not in performance_df.columns:
            performance_df['is_win'] = (performance_df['finish_position'] == 1).fillna(False).astype(int)
        
        # 各施策を実行
        df = self._generate_jockey_condition_features(df, performance_df)
        df = self._generate_margin_features(df, performance_df)
        if pedigree_df is not None:
            df = self._generate_sire_condition_features(df, performance_df, pedigree_df)
        
        self.logger.info("=" * 60)
        self.logger.info("条件別特徴量の生成完了")
        self.logger.info("=" * 60)
        
        return df
    
    def _generate_jockey_condition_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """【施策1】騎手の条件別成績"""
        self.logger.info("【施策1】騎手条件別特徴量を生成中...")
        
        # 騎手×距離カテゴリ
        jockey_dist_stats = performance_df.groupby(
            ['jockey_id', 'distance_category'], observed=True
        ).agg({
            'finish_position': 'mean',
            'is_win': ['mean', 'count']
        }).reset_index()
        jockey_dist_stats.columns = [
            'jockey_id', 'distance_category',
            'jockey_dist_avg_finish', 'jockey_dist_win_rate', 'jockey_dist_races'
        ]
        jockey_dist_stats.loc[
            jockey_dist_stats['jockey_dist_races'] < 10,
            ['jockey_dist_avg_finish', 'jockey_dist_win_rate']
        ] = np.nan
        
        df = df.merge(
            jockey_dist_stats[['jockey_id', 'distance_category', 'jockey_dist_avg_finish', 'jockey_dist_win_rate']],
            on=['jockey_id', 'distance_category'],
            how='left'
        )
        self.logger.info(f"  騎手×距離: {len(jockey_dist_stats):,}パターン")
        
        # 騎手×馬場（芝/ダート）
        jockey_surface_stats = performance_df.groupby(
            ['jockey_id', 'track_surface'], observed=True
        ).agg({
            'finish_position': 'mean',
            'is_win': ['mean', 'count']
        }).reset_index()
        jockey_surface_stats.columns = [
            'jockey_id', 'track_surface',
            'jockey_surface_avg_finish', 'jockey_surface_win_rate', 'jockey_surface_races'
        ]
        jockey_surface_stats.loc[
            jockey_surface_stats['jockey_surface_races'] < 10,
            ['jockey_surface_avg_finish', 'jockey_surface_win_rate']
        ] = np.nan
        
        df = df.merge(
            jockey_surface_stats[['jockey_id', 'track_surface', 'jockey_surface_avg_finish', 'jockey_surface_win_rate']],
            on=['jockey_id', 'track_surface'],
            how='left'
        )
        self.logger.info(f"  騎手×馬場: {len(jockey_surface_stats):,}パターン")
        
        return df
    
    def _generate_margin_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """【施策2】着差情報"""
        self.logger.info("【施策2】着差情報特徴量を生成中...")
        
        if 'finish_time_seconds' in performance_df.columns:
            perf_sorted = performance_df.sort_values(['race_id', 'finish_position'])
            winner_times = perf_sorted.groupby('race_id')['finish_time_seconds'].first().reset_index()
            winner_times.columns = ['race_id', 'winner_time']
            
            perf_with_margin = performance_df.merge(winner_times, on='race_id', how='left')
            perf_with_margin['margin_to_winner'] = perf_with_margin['finish_time_seconds'] - perf_with_margin['winner_time']
            
            perf_sorted_by_horse = perf_with_margin.sort_values(['horse_id', 'race_date'])
            margin_stats = perf_sorted_by_horse.groupby('horse_id')['margin_to_winner'].apply(
                lambda x: x.shift(1).rolling(5, min_periods=1).mean()
            ).reset_index(level=0, drop=True)
            
            perf_with_margin['past_5_margin_mean'] = margin_stats
            margin_merge_data = perf_with_margin.groupby('horse_id').last()[['past_5_margin_mean']].reset_index()
            df = df.merge(margin_merge_data, on='horse_id', how='left')
            
            self.logger.info("  past_5_margin_mean 生成完了")
        else:
            self.logger.warning("  finish_time_seconds がないためスキップ")
        
        # 上がり3F順位
        if 'last_3f_time' in performance_df.columns:
            perf_with_3f = performance_df.copy()
            perf_with_3f['last_3f_rank'] = perf_with_3f.groupby('race_id')['last_3f_time'].rank(method='first')
            
            perf_sorted_3f = perf_with_3f.sort_values(['horse_id', 'race_date'])
            last3f_stats = perf_sorted_3f.groupby('horse_id')['last_3f_rank'].apply(
                lambda x: x.shift(1).rolling(5, min_periods=1).mean()
            ).reset_index(level=0, drop=True)
            perf_with_3f['past_5_last_3f_rank_mean'] = last3f_stats
            
            last3f_merge = perf_with_3f.groupby('horse_id').last()[['past_5_last_3f_rank_mean']].reset_index()
            df = df.merge(last3f_merge, on='horse_id', how='left')
            
            self.logger.info("  past_5_last_3f_rank_mean 生成完了")
        else:
            self.logger.warning("  last_3f_time がないためスキップ")
        
        return df
    
    def _generate_sire_condition_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame,
        pedigree_df: pd.DataFrame
    ) -> pd.DataFrame:
        """【施策3】血統条件別産駒成績"""
        self.logger.info("【施策3】血統条件別特徴量を生成中...")
        
        # sire_idを取得
        if 'sire_id' not in df.columns:
            sire_df = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates(subset=['horse_id'])
            sire_df.columns = ['horse_id', 'sire_id']
            sire_df['horse_id'] = sire_df['horse_id'].astype(str)
            sire_df['sire_id'] = sire_df['sire_id'].astype(str)
            df = df.merge(sire_df, on='horse_id', how='left')
        
        if 'sire_id' not in performance_df.columns:
            sire_df = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates(subset=['horse_id'])
            sire_df.columns = ['horse_id', 'sire_id']
            sire_df['horse_id'] = sire_df['horse_id'].astype(str)
            sire_df['sire_id'] = sire_df['sire_id'].astype(str)
            performance_df = performance_df.merge(sire_df, on='horse_id', how='left')
        
        # 父×距離カテゴリ
        sire_dist_stats = performance_df.groupby(
            ['sire_id', 'distance_category'], observed=True
        ).agg({
            'finish_position': 'mean',
            'is_win': ['mean', 'count']
        }).reset_index()
        sire_dist_stats.columns = [
            'sire_id', 'distance_category',
            'sire_dist_avg_finish', 'sire_dist_win_rate', 'sire_dist_races'
        ]
        sire_dist_stats.loc[
            sire_dist_stats['sire_dist_races'] < 20,
            ['sire_dist_avg_finish', 'sire_dist_win_rate']
        ] = np.nan
        
        df = df.merge(
            sire_dist_stats[['sire_id', 'distance_category', 'sire_dist_avg_finish', 'sire_dist_win_rate']],
            on=['sire_id', 'distance_category'],
            how='left'
        )
        self.logger.info(f"  父×距離: {len(sire_dist_stats):,}パターン")
        
        return df

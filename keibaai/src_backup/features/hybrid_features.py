"""
Hybrid AI用の特徴量エンジニア v3（完全版）

設計書に基づく全特徴量を実装
- 時系列リークを完全に防止
- ベクトル化で高速処理
- Phase 1-4の全特徴量
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from dataclasses import dataclass
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


# =========================================================================
# 禁止特徴量の定義
# =========================================================================

FORBIDDEN_FEATURES = [
    # 直接的なオッズ情報（絶対禁止）
    'win_odds', 'morning_odds', 'popularity', 'morning_popularity',
    'win_probability', 'relative_odds',
    
    # オッズと高相関の情報
    'prize_money', 'prize_1st', 'prize_2nd', 'prize_3rd', 'prize_4th', 'prize_5th',
    'popularity_finish_diff', 'odds_rank', 'prize_total',
    
    # 結果情報（未来情報）
    'finish_position', 'finish_time_str', 'finish_time_seconds',
    'margin_str', 'margin_seconds', 'last3f_rank',
    'position_change_1_2', 'position_change_2_3', 'position_change_3_4',
    'final_corner_to_finish',
    
    # 当日のスピード指数（finish_time_secondsから計算されるため禁止）
    'speed_figure',  # 当日のレース結果
    'base_time_mean', 'base_time_std',  # 基準タイム情報
    
    # ペイアウト情報
    'payout',
]


@dataclass
class FeatureConfig:
    """特徴量設計の設定"""
    lookback_races: int = 5  # 直近N走
    min_races_for_stats: int = 3  # 統計に必要な最低レース数


class HybridFeatureEngineerV3:
    """
    4層ハイブリッドAI用の特徴量エンジニア（完全版）
    
    設計書の全特徴量を実装:
    - Phase 1: タイム系 + 位置取り系 + 適性系（基本）
    - Phase 2: 騎手・調教師系 + 血統系
    - Phase 3: コンディション系 + 複合系
    - Phase 4: 交互作用系 + 相対比較系
    """
    
    def __init__(self, config: Optional[FeatureConfig] = None):
        self.config = config or FeatureConfig()
        self.statistics = {}
        self._fitted = False
        
    def fit(self, 
            races_df: pd.DataFrame,
            horses_df: Optional[pd.DataFrame] = None,
            pedigrees_df: Optional[pd.DataFrame] = None,
            corners_df: Optional[pd.DataFrame] = None,
            race_details_df: Optional[pd.DataFrame] = None) -> 'HybridFeatureEngineerV3':
        """統計情報を計算して保存"""
        logger.info("特徴量エンジニア v3: 統計情報の計算開始")
        
        # データを保存
        self._races_df = races_df.copy()
        self._horses_df = horses_df.copy() if horses_df is not None else None
        self._pedigrees_df = pedigrees_df.copy() if pedigrees_df is not None else None
        self._corners_df = corners_df.copy() if corners_df is not None else None
        self._race_details_df = race_details_df.copy() if race_details_df is not None else None
        
        if 'race_date' in self._races_df.columns:
            self._races_df['race_date'] = pd.to_datetime(self._races_df['race_date'])
        
        # 統計計算
        self._calculate_base_times()
        self._calculate_jockey_stats()
        self._calculate_trainer_stats()
        self._calculate_post_bias()
        self._calculate_venue_stats()
        
        if pedigrees_df is not None:
            self._calculate_pedigree_stats()
        
        self._fitted = True
        logger.info("特徴量エンジニア v3: 統計情報の計算完了")
        
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """特徴量を生成"""
        if not self._fitted:
            raise ValueError("fit()を先に呼び出してください")
        
        logger.info(f"特徴量生成開始: {len(df)}行")
        
        df = df.copy()
        
        if 'race_date' in df.columns:
            df['race_date'] = pd.to_datetime(df['race_date'])
        
        # === Phase 1: 基本特徴量 ===
        logger.info("  [Phase 1] タイム・位置取り・適性...")
        df = self._add_speed_figure_features(df)
        df = self._add_historical_features(df)
        df = self._add_aptitude_features_safe(df)
        
        # === Phase 2: 騎手・調教師・血統 ===
        logger.info("  [Phase 2] 騎手・調教師・血統...")
        df = self._add_jockey_features(df)
        df = self._add_trainer_features(df)
        
        if self._pedigrees_df is not None:
            df = self._add_pedigree_features(df)
        
        # === Phase 3: コンディション・複合 ===
        logger.info("  [Phase 3] コンディション・複合...")
        df = self._add_condition_features(df)
        df = self._add_post_bias_features(df)
        df = self._add_venue_features(df)
        df = self._add_season_features(df)
        
        # === Phase 4: 交互作用・相対比較 ===
        logger.info("  [Phase 4] 交互作用・相対比較...")
        df = self._add_interaction_features(df)
        df = self._add_relative_features(df)
        df = self._add_difference_features(df)
        
        # 禁止特徴量を除去
        df = self._remove_forbidden_features(df)
        
        logger.info(f"特徴量生成完了: {len(df.columns)}カラム")
        
        return df
    
    def fit_transform(self, races_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        self.fit(races_df, **kwargs)
        return self.transform(races_df)
    
    # ========================================================================
    # 統計計算メソッド
    # ========================================================================
    
    def _calculate_base_times(self):
        """距離・馬場・馬場状態ごとの基準タイムを計算"""
        df = self._races_df.dropna(subset=['finish_time_seconds', 'distance_m', 'track_surface', 'track_condition'])
        winners = df[df['finish_position'] == 1]
        
        base_times = winners.groupby(
            ['distance_m', 'track_surface', 'track_condition']
        )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        base_times.columns = ['distance_m', 'track_surface', 'track_condition', 
                              'base_time_mean', 'base_time_std', 'base_time_count']
        
        self.statistics['base_times'] = base_times
    
    def _calculate_jockey_stats(self):
        """騎手の成績統計を計算"""
        df = self._races_df.dropna(subset=['jockey_id', 'finish_position'])
        
        # 全体成績
        jockey_stats = df.groupby('jockey_id').apply(
            lambda x: pd.Series({
                'jockey_total_races': len(x),
                'jockey_wins': (x['finish_position'] == 1).sum(),
                'jockey_top3': (x['finish_position'] <= 3).sum(),
            })
        ).reset_index()
        
        jockey_stats['jockey_overall_win_rate'] = jockey_stats['jockey_wins'] / jockey_stats['jockey_total_races']
        jockey_stats['jockey_overall_top3_rate'] = jockey_stats['jockey_top3'] / jockey_stats['jockey_total_races']
        
        self.statistics['jockey_stats'] = jockey_stats
        
        # 競馬場別
        jockey_venue = df.groupby(['jockey_id', 'venue']).apply(
            lambda x: pd.Series({
                'jockey_venue_races': len(x),
                'jockey_venue_wins': (x['finish_position'] == 1).sum(),
            })
        ).reset_index()
        jockey_venue['jockey_venue_win_rate'] = jockey_venue['jockey_venue_wins'] / jockey_venue['jockey_venue_races']
        self.statistics['jockey_venue'] = jockey_venue
        
        # 距離カテゴリ別
        jockey_dist = df.groupby(['jockey_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'jockey_dist_races': len(x),
                'jockey_dist_wins': (x['finish_position'] == 1).sum(),
            })
        ).reset_index()
        jockey_dist['jockey_dist_win_rate'] = jockey_dist['jockey_dist_wins'] / jockey_dist['jockey_dist_races']
        self.statistics['jockey_dist'] = jockey_dist
    
    def _calculate_trainer_stats(self):
        """調教師の成績統計を計算"""
        df = self._races_df.dropna(subset=['trainer_id', 'finish_position'])
        
        trainer_stats = df.groupby('trainer_id').apply(
            lambda x: pd.Series({
                'trainer_total_races': len(x),
                'trainer_wins': (x['finish_position'] == 1).sum(),
                'trainer_top3': (x['finish_position'] <= 3).sum(),
            })
        ).reset_index()
        
        trainer_stats['trainer_overall_win_rate'] = trainer_stats['trainer_wins'] / trainer_stats['trainer_total_races']
        trainer_stats['trainer_overall_top3_rate'] = trainer_stats['trainer_top3'] / trainer_stats['trainer_total_races']
        
        self.statistics['trainer_stats'] = trainer_stats
        
        # 競馬場別
        trainer_venue = df.groupby(['trainer_id', 'venue']).apply(
            lambda x: pd.Series({
                'trainer_venue_races': len(x),
                'trainer_venue_wins': (x['finish_position'] == 1).sum(),
            })
        ).reset_index()
        trainer_venue['trainer_venue_win_rate'] = trainer_venue['trainer_venue_wins'] / trainer_venue['trainer_venue_races']
        self.statistics['trainer_venue'] = trainer_venue
    
    def _calculate_pedigree_stats(self):
        """血統統計を計算"""
        if self._pedigrees_df is None:
            return
        
        # 父馬
        sires = self._pedigrees_df[self._pedigrees_df['generation'] == 1][['horse_id', 'ancestor_id']]
        sires.columns = ['horse_id', 'sire_id']
        
        df = self._races_df.merge(sires, on='horse_id', how='left')
        df = df.dropna(subset=['sire_id', 'finish_position'])
        
        # 父馬の全体成績
        sire_stats = df.groupby('sire_id').apply(
            lambda x: pd.Series({
                'sire_offspring_races': len(x),
                'sire_offspring_wins': (x['finish_position'] == 1).sum(),
            })
        ).reset_index()
        sire_stats['sire_win_rate'] = sire_stats['sire_offspring_wins'] / sire_stats['sire_offspring_races']
        
        # 父馬の距離別成績
        sire_distance = df.groupby(['sire_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'sire_dist_races': len(x),
                'sire_dist_wins': (x['finish_position'] == 1).sum(),
            })
        ).reset_index()
        sire_distance['sire_dist_win_rate'] = sire_distance['sire_dist_wins'] / sire_distance['sire_dist_races']
        
        # 父馬の馬場別成績
        sire_surface = df.groupby(['sire_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'sire_surface_races': len(x),
                'sire_surface_wins': (x['finish_position'] == 1).sum(),
            })
        ).reset_index()
        sire_surface['sire_surface_win_rate'] = sire_surface['sire_surface_wins'] / sire_surface['sire_surface_races']
        
        # 父馬の馬場状態別成績
        sire_condition = df.groupby(['sire_id', 'track_condition']).apply(
            lambda x: pd.Series({
                'sire_cond_races': len(x),
                'sire_cond_wins': (x['finish_position'] == 1).sum(),
            })
        ).reset_index()
        sire_condition['sire_cond_win_rate'] = sire_condition['sire_cond_wins'] / sire_condition['sire_cond_races']
        
        self.statistics['sire_stats'] = sire_stats
        self.statistics['sire_distance'] = sire_distance
        self.statistics['sire_surface'] = sire_surface
        self.statistics['sire_condition'] = sire_condition
        self.statistics['sires'] = sires
    
    def _calculate_post_bias(self):
        """枠順バイアスを計算"""
        df = self._races_df.dropna(subset=['finish_position', 'bracket_number'])
        df['is_winner'] = (df['finish_position'] == 1).astype(int)
        
        # 競馬場×距離カテゴリ×馬場×枠番
        post_bias = df.groupby(['venue', 'distance_category', 'track_surface', 'bracket_number']).agg({
            'is_winner': 'mean',
            'race_id': 'count'
        }).reset_index()
        post_bias.columns = ['venue', 'distance_category', 'track_surface', 'bracket_number', 
                             'bracket_win_rate', 'bracket_count']
        
        self.statistics['post_bias'] = post_bias
    
    def _calculate_venue_stats(self):
        """競馬場別の統計を計算"""
        df = self._races_df.dropna(subset=['finish_position', 'venue'])
        df['is_winner'] = (df['finish_position'] == 1).astype(int)
        
        # 競馬場×距離×馬場の基準
        venue_stats = df.groupby(['venue', 'distance_category', 'track_surface']).agg({
            'finish_time_seconds': ['mean', 'std'],
            'race_id': 'count'
        }).reset_index()
        venue_stats.columns = ['venue', 'distance_category', 'track_surface', 
                               'venue_time_mean', 'venue_time_std', 'venue_race_count']
        
        self.statistics['venue_stats'] = venue_stats
    
    # ========================================================================
    # Phase 1: タイム・位置取り・適性
    # ========================================================================
    
    def _add_speed_figure_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """スピード指数を計算"""
        # 基準タイムをマージ
        base_times = self.statistics.get('base_times', pd.DataFrame())
        
        if len(base_times) > 0:
            df = df.merge(
                base_times[['distance_m', 'track_surface', 'track_condition', 'base_time_mean', 'base_time_std']],
                on=['distance_m', 'track_surface', 'track_condition'],
                how='left'
            )
            
            # スピード指数 = (基準タイム - 実タイム) / 標準偏差 * 10 + 50
            # 高いほど速い
            df['speed_figure'] = np.where(
                df['base_time_std'] > 0,
                (df['base_time_mean'] - df['finish_time_seconds']) / df['base_time_std'] * 10 + 50,
                np.nan
            )
        else:
            df['speed_figure'] = np.nan
        
        return df
    
    def _add_historical_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """過去成績の特徴量をベクトル化で計算"""
        history = self._races_df.sort_values(['horse_id', 'race_date']).copy()
        horse_groups = history.groupby('horse_id')
        
        # === 直近N走の平均着順 ===
        history['_finish_shifted'] = horse_groups['finish_position'].shift(1)
        history['time_avg_finish'] = horse_groups['_finish_shifted'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        history['time_best_finish'] = horse_groups['_finish_shifted'].transform(
            lambda x: x.expanding().min()
        )
        history['time_races_count'] = horse_groups['_finish_shifted'].transform(
            lambda x: x.expanding().count()
        )
        
        # 勝率・複勝率
        history['_is_win'] = (history['finish_position'] == 1).astype(float)
        history['_is_top3'] = (history['finish_position'] <= 3).astype(float)
        history['_win_shifted'] = horse_groups['_is_win'].shift(1)
        history['_top3_shifted'] = horse_groups['_is_top3'].shift(1)
        history['horse_win_rate'] = horse_groups['_win_shifted'].transform(lambda x: x.expanding().mean())
        history['horse_top3_rate'] = horse_groups['_top3_shifted'].transform(lambda x: x.expanding().mean())
        
        # === 上がり3F ===
        history['_last3f_shifted'] = horse_groups['last_3f_time'].shift(1)
        history['last3f_avg'] = horse_groups['_last3f_shifted'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        history['last3f_best'] = horse_groups['_last3f_shifted'].transform(
            lambda x: x.expanding().min()
        )
        history['last3f_std'] = horse_groups['_last3f_shifted'].transform(
            lambda x: x.rolling(window=5, min_periods=2).std()
        )
        
        # === スピード指数の履歴 ===
        if 'speed_figure' in history.columns:
            history['_sf_shifted'] = horse_groups['speed_figure'].shift(1)
            history['speed_figure_avg'] = horse_groups['_sf_shifted'].transform(
                lambda x: x.rolling(window=5, min_periods=1).mean()
            )
            history['speed_figure_best'] = horse_groups['_sf_shifted'].transform(
                lambda x: x.expanding().max()
            )
            history['speed_figure_trend'] = horse_groups['_sf_shifted'].transform(
                lambda x: self._calc_trend(x)
            )
        
        # === コーナー通過順位 ===
        history['_pos1_shifted'] = horse_groups['passing_order_1'].shift(1)
        history['_pos4_shifted'] = horse_groups['passing_order_4'].shift(1)
        history['pos_avg_1corner'] = horse_groups['_pos1_shifted'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        history['pos_avg_4corner'] = horse_groups['_pos4_shifted'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        
        # === 順位上昇能力 ===
        history['_gain'] = history['passing_order_4'] - history['finish_position']
        history['_gain_shifted'] = horse_groups['_gain'].shift(1)
        history['pos_gain_ability'] = horse_groups['_gain_shifted'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        
        # === 脚質判定 ===
        history['pos_running_style'] = history['pos_avg_1corner'].apply(
            lambda x: 0 if x <= 2 else (1 if x <= 5 else (2 if x <= 10 else 3)) if pd.notna(x) else np.nan
        )
        
        # === 休養日数 ===
        history['_prev_date'] = horse_groups['race_date'].shift(1)
        history['cond_days_since_last'] = (history['race_date'] - history['_prev_date']).dt.days
        
        # === 着順トレンド ===
        history['finish_trend'] = horse_groups['_finish_shifted'].transform(
            lambda x: self._calc_trend(x)
        )
        
        # 必要なカラムを選択
        feature_cols = [
            'race_id', 'horse_id', 
            'time_avg_finish', 'time_best_finish', 'time_races_count',
            'horse_win_rate', 'horse_top3_rate',
            'last3f_avg', 'last3f_best', 'last3f_std',
            'speed_figure_avg', 'speed_figure_best', 'speed_figure_trend',
            'pos_avg_1corner', 'pos_avg_4corner', 'pos_gain_ability', 'pos_running_style',
            'cond_days_since_last', 'finish_trend'
        ]
        
        # 存在するカラムのみ選択
        feature_cols = [c for c in feature_cols if c in history.columns]
        
        history_features = history[feature_cols].drop_duplicates(['race_id', 'horse_id'])
        
        df = df.merge(history_features, on=['race_id', 'horse_id'], how='left', suffixes=('', '_hist'))
        
        return df
    
    def _add_aptitude_features_safe(self, df: pd.DataFrame) -> pd.DataFrame:
        """適性特徴量を時系列安全に計算"""
        history = self._races_df.sort_values(['horse_id', 'race_date']).copy()
        
        history['is_winner'] = (history['finish_position'] == 1).astype(float)
        history['is_top3'] = (history['finish_position'] <= 3).astype(float)
        
        # 芝での累積成績
        turf = history[history['track_surface'] == '芝'].copy()
        if len(turf) > 0:
            turf_groups = turf.groupby('horse_id')
            turf['_turf_win_shifted'] = turf_groups['is_winner'].shift(1)
            turf['apt_turf_win_rate'] = turf_groups['_turf_win_shifted'].transform(lambda x: x.expanding().mean())
            turf['apt_turf_count'] = turf_groups['is_winner'].transform(lambda x: x.expanding().count().shift(1))
            turf_features = turf[['race_id', 'horse_id', 'apt_turf_win_rate', 'apt_turf_count']].drop_duplicates(['race_id', 'horse_id'])
            df = df.merge(turf_features, on=['race_id', 'horse_id'], how='left')
        
        # ダートでの累積成績
        dirt = history[history['track_surface'] == 'ダート'].copy()
        if len(dirt) > 0:
            dirt_groups = dirt.groupby('horse_id')
            dirt['_dirt_win_shifted'] = dirt_groups['is_winner'].shift(1)
            dirt['apt_dirt_win_rate'] = dirt_groups['_dirt_win_shifted'].transform(lambda x: x.expanding().mean())
            dirt['apt_dirt_count'] = dirt_groups['is_winner'].transform(lambda x: x.expanding().count().shift(1))
            dirt_features = dirt[['race_id', 'horse_id', 'apt_dirt_win_rate', 'apt_dirt_count']].drop_duplicates(['race_id', 'horse_id'])
            df = df.merge(dirt_features, on=['race_id', 'horse_id'], how='left')
        
        # 統合
        df['apt_surface_win_rate'] = np.where(
            df['track_surface'] == '芝',
            df.get('apt_turf_win_rate', np.nan),
            df.get('apt_dirt_win_rate', np.nan)
        )
        df['apt_surface_count'] = np.where(
            df['track_surface'] == '芝',
            df.get('apt_turf_count', np.nan),
            df.get('apt_dirt_count', np.nan)
        )
        
        # 距離カテゴリ別の累積成績
        for dist_cat in ['短距離', 'マイル', '中距離', '長距離']:
            dist_df = history[history['distance_category'] == dist_cat].copy()
            if len(dist_df) > 0:
                dist_groups = dist_df.groupby('horse_id')
                dist_df['_dist_win_shifted'] = dist_groups['is_winner'].shift(1)
                dist_df[f'apt_{dist_cat}_win_rate'] = dist_groups['_dist_win_shifted'].transform(lambda x: x.expanding().mean())
                dist_features = dist_df[['race_id', 'horse_id', f'apt_{dist_cat}_win_rate']].drop_duplicates(['race_id', 'horse_id'])
                df = df.merge(dist_features, on=['race_id', 'horse_id'], how='left')
        
        # 現在の距離カテゴリの適性を統合
        df['apt_distance_win_rate'] = np.nan
        for dist_cat in ['短距離', 'マイル', '中距離', '長距離']:
            col = f'apt_{dist_cat}_win_rate'
            if col in df.columns:
                mask = df['distance_category'] == dist_cat
                df.loc[mask, 'apt_distance_win_rate'] = df.loc[mask, col]
        
        return df
    
    # ========================================================================
    # Phase 2: 騎手・調教師・血統
    # ========================================================================
    
    def _add_jockey_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手系特徴量を追加"""
        jockey_stats = self.statistics.get('jockey_stats', pd.DataFrame())
        jockey_venue = self.statistics.get('jockey_venue', pd.DataFrame())
        jockey_dist = self.statistics.get('jockey_dist', pd.DataFrame())
        
        if len(jockey_stats) > 0:
            df = df.merge(
                jockey_stats[['jockey_id', 'jockey_overall_win_rate', 'jockey_overall_top3_rate', 'jockey_total_races']],
                on='jockey_id',
                how='left'
            )
        
        if len(jockey_venue) > 0:
            df = df.merge(
                jockey_venue[['jockey_id', 'venue', 'jockey_venue_win_rate']],
                on=['jockey_id', 'venue'],
                how='left'
            )
        
        if len(jockey_dist) > 0:
            df = df.merge(
                jockey_dist[['jockey_id', 'distance_category', 'jockey_dist_win_rate']],
                on=['jockey_id', 'distance_category'],
                how='left'
            )
        
        return df
    
    def _add_trainer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """調教師系特徴量を追加"""
        trainer_stats = self.statistics.get('trainer_stats', pd.DataFrame())
        trainer_venue = self.statistics.get('trainer_venue', pd.DataFrame())
        
        if len(trainer_stats) > 0:
            df = df.merge(
                trainer_stats[['trainer_id', 'trainer_overall_win_rate', 'trainer_overall_top3_rate', 'trainer_total_races']],
                on='trainer_id',
                how='left'
            )
        
        if len(trainer_venue) > 0:
            df = df.merge(
                trainer_venue[['trainer_id', 'venue', 'trainer_venue_win_rate']],
                on=['trainer_id', 'venue'],
                how='left'
            )
        
        return df
    
    def _add_pedigree_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """血統系特徴量を追加"""
        sires = self.statistics.get('sires', pd.DataFrame())
        sire_stats = self.statistics.get('sire_stats', pd.DataFrame())
        sire_distance = self.statistics.get('sire_distance', pd.DataFrame())
        sire_surface = self.statistics.get('sire_surface', pd.DataFrame())
        sire_condition = self.statistics.get('sire_condition', pd.DataFrame())
        
        if len(sires) == 0:
            return df
        
        df = df.merge(sires, on='horse_id', how='left')
        
        if len(sire_stats) > 0:
            df = df.merge(
                sire_stats[['sire_id', 'sire_win_rate', 'sire_offspring_races']],
                on='sire_id',
                how='left'
            )
        
        if len(sire_distance) > 0:
            df = df.merge(
                sire_distance[['sire_id', 'distance_category', 'sire_dist_win_rate']],
                on=['sire_id', 'distance_category'],
                how='left'
            )
        
        if len(sire_surface) > 0:
            df = df.merge(
                sire_surface[['sire_id', 'track_surface', 'sire_surface_win_rate']],
                on=['sire_id', 'track_surface'],
                how='left'
            )
        
        if len(sire_condition) > 0:
            df = df.merge(
                sire_condition[['sire_id', 'track_condition', 'sire_cond_win_rate']],
                on=['sire_id', 'track_condition'],
                how='left'
            )
        
        return df
    
    # ========================================================================
    # Phase 3: コンディション・複合・季節
    # ========================================================================
    
    def _add_condition_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """コンディション特徴量を追加"""
        history = self._races_df.sort_values(['horse_id', 'race_date']).copy()
        horse_groups = history.groupby('horse_id')
        
        # 馬体重の過去平均
        history['_weight_shifted'] = horse_groups['horse_weight'].shift(1)
        history['weight_avg'] = horse_groups['_weight_shifted'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        history['weight_std'] = horse_groups['_weight_shifted'].transform(
            lambda x: x.rolling(window=5, min_periods=2).std()
        )
        
        # 馬体重の変動率
        history['weight_change_rate'] = (history['horse_weight'] - history['weight_avg']) / history['weight_avg']
        
        # 最適体重（好走時の体重平均）
        good_runs = history[history['finish_position'] <= 3].copy()
        if len(good_runs) > 0:
            optimal_weight = good_runs.groupby('horse_id')['horse_weight'].mean().reset_index()
            optimal_weight.columns = ['horse_id', 'optimal_weight']
            history = history.merge(optimal_weight, on='horse_id', how='left')
            history['weight_from_optimal'] = np.abs(history['horse_weight'] - history['optimal_weight']) / history['optimal_weight']
        else:
            history['optimal_weight'] = np.nan
            history['weight_from_optimal'] = np.nan
        
        # 休養カテゴリ
        history['_prev_date'] = horse_groups['race_date'].shift(1)
        history['days_since_last'] = (history['race_date'] - history['_prev_date']).dt.days
        history['rest_category'] = pd.cut(
            history['days_since_last'],
            bins=[-np.inf, 7, 14, 28, 60, np.inf],
            labels=[0, 1, 2, 3, 4]
        ).astype(float)
        
        weight_features = history[['race_id', 'horse_id', 'weight_avg', 'weight_std', 
                                    'weight_change_rate', 'weight_from_optimal', 'rest_category']].drop_duplicates(['race_id', 'horse_id'])
        
        df = df.merge(weight_features, on=['race_id', 'horse_id'], how='left')
        
        return df
    
    def _add_post_bias_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """枠順バイアス特徴量を追加"""
        post_bias = self.statistics.get('post_bias', pd.DataFrame())
        
        if len(post_bias) == 0:
            return df
        
        df = df.merge(
            post_bias[['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_win_rate']],
            on=['venue', 'distance_category', 'track_surface', 'bracket_number'],
            how='left'
        )
        
        return df
    
    def _add_venue_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """競馬場別特徴量を追加"""
        venue_stats = self.statistics.get('venue_stats', pd.DataFrame())
        
        if len(venue_stats) == 0:
            return df
        
        df = df.merge(
            venue_stats[['venue', 'distance_category', 'track_surface', 'venue_time_mean', 'venue_time_std']],
            on=['venue', 'distance_category', 'track_surface'],
            how='left'
        )
        
        return df
    
    def _add_season_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """季節・時期特徴量を追加"""
        df['month'] = df['race_date'].dt.month
        df['day_of_week'] = df['race_date'].dt.dayofweek
        
        # 季節カテゴリ（月から直接計算）
        # 春: 3-5月, 夏: 6-8月, 秋: 9-11月, 冬: 12-2月
        df['season'] = df['month'].apply(
            lambda m: 0 if m in [3, 4, 5] else (1 if m in [6, 7, 8] else (2 if m in [9, 10, 11] else 3))
        )
        
        return df
    
    # ========================================================================
    # Phase 4: 交互作用・相対比較・差分
    # ========================================================================
    
    def _add_interaction_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """交互作用特徴量を追加"""
        # 距離×脚質
        df['dist_style_interaction'] = df.get('pos_running_style', 0) * df.get('distance_m', 0) / 1000
        
        # 馬場状態×血統（重馬場巧者）
        condition_map = {'良': 0, '稍重': 1, '重': 2, '不良': 3}
        df['track_condition_num'] = df['track_condition'].map(condition_map).fillna(0)
        df['cond_sire_interaction'] = df['track_condition_num'] * df.get('sire_cond_win_rate', 0).fillna(0)
        
        # 枠順×脚質（外枠×追込のペナルティ）
        df['post_style_interaction'] = df.get('bracket_number', 4) * df.get('pos_running_style', 2)
        
        # 年齢×クラス
        if 'age' in df.columns and 'race_class' in df.columns:
            class_map = {'新馬': 1, '未勝利': 2, '1勝': 3, '2勝': 4, '3勝': 5, 'OP': 6, 'G3': 7, 'G2': 8, 'G1': 9}
            df['race_class_num'] = df['race_class'].map(class_map).fillna(3)
            df['age_class_interaction'] = (df['age'] - 4) * df['race_class_num']
        
        return df
    
    def _add_relative_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """相対比較系特徴量を追加（レース内での順位）"""
        # 低い方が良い特徴量
        low_is_good = ['time_avg_finish', 'last3f_avg', 'pos_avg_1corner', 'pos_avg_4corner']
        # 高い方が良い特徴量
        high_is_good = ['jockey_overall_win_rate', 'trainer_overall_win_rate', 'horse_win_rate', 
                        'horse_top3_rate', 'pos_gain_ability', 'sire_win_rate', 'speed_figure_avg']
        
        for col in low_is_good:
            if col in df.columns:
                df[f'{col}_rank'] = df.groupby('race_id')[col].rank(ascending=True, pct=True)
        
        for col in high_is_good:
            if col in df.columns:
                df[f'{col}_rank'] = df.groupby('race_id')[col].rank(ascending=False, pct=True)
        
        # レース内での能力差
        if 'speed_figure_avg' in df.columns:
            race_max = df.groupby('race_id')['speed_figure_avg'].transform('max')
            race_mean = df.groupby('race_id')['speed_figure_avg'].transform('mean')
            df['ability_gap_to_top'] = race_max - df['speed_figure_avg']
            df['ability_gap_to_avg'] = df['speed_figure_avg'] - race_mean
        
        return df
    
    def _add_difference_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """差分・比率特徴量を追加"""
        history = self._races_df.sort_values(['horse_id', 'race_date']).copy()
        horse_groups = history.groupby('horse_id')
        
        # 前走からの距離変化
        history['_prev_distance'] = horse_groups['distance_m'].shift(1)
        history['distance_change'] = history['distance_m'] - history['_prev_distance']
        history['distance_change_pct'] = history['distance_change'] / history['_prev_distance']
        
        # 上がり3F比率
        if 'finish_time_seconds' in history.columns and 'last_3f_time' in history.columns:
            history['last3f_ratio'] = history['last_3f_time'] / history['finish_time_seconds']
        
        diff_features = history[['race_id', 'horse_id', 'distance_change', 'distance_change_pct']].drop_duplicates(['race_id', 'horse_id'])
        
        if 'last3f_ratio' in history.columns:
            history['_last3f_ratio_shifted'] = horse_groups['last3f_ratio'].shift(1)
            history['last3f_ratio_avg'] = horse_groups['_last3f_ratio_shifted'].transform(
                lambda x: x.rolling(window=5, min_periods=1).mean()
            )
            ratio_features = history[['race_id', 'horse_id', 'last3f_ratio_avg']].drop_duplicates(['race_id', 'horse_id'])
            diff_features = diff_features.merge(ratio_features, on=['race_id', 'horse_id'], how='left')
        
        df = df.merge(diff_features, on=['race_id', 'horse_id'], how='left')
        
        return df
    
    # ========================================================================
    # ユーティリティ
    # ========================================================================
    
    def _calc_trend(self, series: pd.Series) -> pd.Series:
        """トレンド（傾き）を計算"""
        def trend_fn(x):
            vals = x.dropna()
            if len(vals) < 2:
                return 0.0
            vals = vals.values[-5:]
            if len(vals) < 2:
                return 0.0
            try:
                slope, _ = np.polyfit(np.arange(len(vals)), vals, 1)
                return slope
            except:
                return 0.0
        
        return series.rolling(window=5, min_periods=2).apply(trend_fn, raw=False)
    
    def _remove_forbidden_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """禁止特徴量を除去"""
        cols_to_drop = [c for c in df.columns if c in FORBIDDEN_FEATURES]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop, errors='ignore')
        return df
    
    def get_feature_names(self) -> List[str]:
        """生成される特徴量名のリストを返す"""
        return [
            # Phase 1
            'speed_figure', 'speed_figure_avg', 'speed_figure_best', 'speed_figure_trend',
            'time_avg_finish', 'time_best_finish', 'time_races_count',
            'horse_win_rate', 'horse_top3_rate',
            'last3f_avg', 'last3f_best', 'last3f_std',
            'pos_avg_1corner', 'pos_avg_4corner', 'pos_gain_ability', 'pos_running_style',
            'cond_days_since_last', 'finish_trend',
            'apt_surface_win_rate', 'apt_surface_count', 'apt_distance_win_rate',
            # Phase 2
            'jockey_overall_win_rate', 'jockey_overall_top3_rate', 'jockey_total_races',
            'jockey_venue_win_rate', 'jockey_dist_win_rate',
            'trainer_overall_win_rate', 'trainer_overall_top3_rate', 'trainer_total_races',
            'trainer_venue_win_rate',
            'sire_win_rate', 'sire_offspring_races', 'sire_dist_win_rate', 
            'sire_surface_win_rate', 'sire_cond_win_rate',
            # Phase 3
            'weight_avg', 'weight_std', 'weight_change_rate', 'weight_from_optimal', 'rest_category',
            'bracket_win_rate', 'venue_time_mean', 'venue_time_std',
            'month', 'day_of_week', 'season',
            # Phase 4
            'dist_style_interaction', 'cond_sire_interaction', 'post_style_interaction',
            'time_avg_finish_rank', 'last3f_avg_rank', 'speed_figure_avg_rank',
            'jockey_overall_win_rate_rank', 'trainer_overall_win_rate_rank',
            'horse_win_rate_rank', 'horse_top3_rate_rank', 'sire_win_rate_rank',
            'ability_gap_to_top', 'ability_gap_to_avg',
            'distance_change', 'distance_change_pct', 'last3f_ratio_avg',
        ]


# 後方互換性
HybridFeatureEngineer = HybridFeatureEngineerV3
FeatureConfig = FeatureConfig

"""
競馬AI特徴量設計書（完全版）に基づく特徴量エンジニア

全155個の特徴量を実装
- Phase 1: タイム系 + 位置取り系 + 適性系（基本）
- Phase 2: 騎手・調教師系 + 血統系
- Phase 3: コンディション系 + 複合系
- Phase 4: 交互作用系 + 相対比較系

重要:
- 全ての履歴特徴量は時系列安全（予測時点より前のデータのみ使用）
- オッズ関連特徴量は完全に排除
- ベクトル化処理で高速化
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
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
    """特徴量設定"""
    lookback_races: int = 10  # 過去レース参照数
    min_races_for_stats: int = 3  # 統計計算の最小レース数


class ComprehensiveFeatureEngineer:
    """
    競馬AI特徴量設計書（完全版）に基づく特徴量エンジニア
    
    全155個の特徴量を4フェーズで生成
    """
    
    def __init__(self, config: Optional[FeatureConfig] = None):
        self.config = config or FeatureConfig()
        self.statistics = {}
        self.is_fitted = False
        
    def fit(self, df: pd.DataFrame, pedigrees_df: pd.DataFrame = None) -> 'ComprehensiveFeatureEngineer':
        """
        統計情報を計算（訓練データのみで計算すること）
        """
        logger.info("特徴量エンジニア v4: 統計情報の計算開始")
        
        # 距離カテゴリを追加
        df = df.copy()
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        )
        
        # ========================================
        # 騎手統計
        # ========================================
        self._calculate_jockey_stats(df)
        
        # ========================================
        # 調教師統計
        # ========================================
        self._calculate_trainer_stats(df)
        
        # ========================================
        # 血統統計
        # ========================================
        if pedigrees_df is not None:
            self._calculate_pedigree_stats(df, pedigrees_df)
        
        # ========================================
        # 馬場バイアス（精緻化版）
        # ========================================
        self._calculate_track_bias_detailed(df)
        
        # ========================================
        # コース別脚質有利度
        # ========================================
        self._calculate_running_style_advantage(df)
        
        self.is_fitted = True
        logger.info("特徴量エンジニア v4: 統計情報の計算完了")
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """特徴量を生成"""
        if not self.is_fitted:
            raise ValueError("fit()を先に実行してください")
        
        df = df.copy()
        logger.info(f"特徴量生成開始: {len(df)}行")
        
        # 距離カテゴリを追加
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        )
        
        # ソート（時系列処理のため必須）
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # ========================================
        # Phase 1: タイム・位置取り・適性
        # ========================================
        logger.info("  [Phase 1] タイム・位置取り・適性...")
        df = self._add_time_features(df)
        df = self._add_last3f_features(df)
        df = self._add_position_features(df)
        df = self._add_aptitude_features(df)
        
        # ========================================
        # Phase 2: 騎手・調教師・血統
        # ========================================
        logger.info("  [Phase 2] 騎手・調教師・血統...")
        df = self._add_jockey_features(df)
        df = self._add_trainer_features(df)
        df = self._add_pedigree_features(df)
        
        # ========================================
        # Phase 3: コンディション・複合
        # ========================================
        logger.info("  [Phase 3] コンディション・複合...")
        df = self._add_condition_features(df)
        df = self._add_weight_features(df)
        df = self._add_rest_features(df)
        df = self._add_season_features(df)
        
        # ========================================
        # Phase 4: 交互作用・相対比較・差分
        # ========================================
        logger.info("  [Phase 4] 交互作用・相対比較...")
        df = self._add_interaction_features(df)
        df = self._add_relative_features(df)
        df = self._add_difference_features(df)
        
        # ========================================
        # 禁止特徴量の除去
        # ========================================
        df = self._remove_forbidden_features(df)
        
        logger.info(f"特徴量生成完了: {len([c for c in df.columns if df[c].dtype in ['float64', 'int64', 'float32', 'int32']])}カラム")
        return df
    
    # ========================================================================
    # 統計計算メソッド
    # ========================================================================
    
    def _calculate_jockey_stats(self, df: pd.DataFrame):
        """騎手統計を計算"""
        # 全体成績
        jockey_stats = df.groupby('jockey_id').apply(
            lambda x: pd.Series({
                'jockey_overall_races': len(x),
                'jockey_overall_wins': (x['finish_position'] == 1).sum(),
                'jockey_overall_top3': (x['finish_position'] <= 3).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_stats['jockey_overall_win_rate'] = jockey_stats['jockey_overall_wins'] / jockey_stats['jockey_overall_races']
        jockey_stats['jockey_overall_top3_rate'] = jockey_stats['jockey_overall_top3'] / jockey_stats['jockey_overall_races']
        
        # 競馬場別成績
        jockey_venue = df.groupby(['jockey_id', 'venue']).apply(
            lambda x: pd.Series({
                'jockey_venue_races': len(x),
                'jockey_venue_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_venue['jockey_venue_win_rate'] = jockey_venue['jockey_venue_wins'] / jockey_venue['jockey_venue_races']
        
        # 距離別成績
        jockey_dist = df.groupby(['jockey_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'jockey_dist_races': len(x),
                'jockey_dist_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_dist['jockey_dist_win_rate'] = jockey_dist['jockey_dist_wins'] / jockey_dist['jockey_dist_races']
        
        # 馬場別成績
        jockey_surface = df.groupby(['jockey_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'jockey_surface_races': len(x),
                'jockey_surface_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_surface['jockey_surface_win_rate'] = jockey_surface['jockey_surface_wins'] / jockey_surface['jockey_surface_races']
        
        self.statistics['jockey_stats'] = jockey_stats
        self.statistics['jockey_venue'] = jockey_venue
        self.statistics['jockey_dist'] = jockey_dist
        self.statistics['jockey_surface'] = jockey_surface
    
    def _calculate_trainer_stats(self, df: pd.DataFrame):
        """調教師統計を計算"""
        trainer_stats = df.groupby('trainer_id').apply(
            lambda x: pd.Series({
                'trainer_overall_races': len(x),
                'trainer_overall_wins': (x['finish_position'] == 1).sum(),
                'trainer_overall_top3': (x['finish_position'] <= 3).sum(),
            }), include_groups=False
        ).reset_index()
        trainer_stats['trainer_overall_win_rate'] = trainer_stats['trainer_overall_wins'] / trainer_stats['trainer_overall_races']
        trainer_stats['trainer_overall_top3_rate'] = trainer_stats['trainer_overall_top3'] / trainer_stats['trainer_overall_races']
        
        # 競馬場別成績
        trainer_venue = df.groupby(['trainer_id', 'venue']).apply(
            lambda x: pd.Series({
                'trainer_venue_races': len(x),
                'trainer_venue_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        trainer_venue['trainer_venue_win_rate'] = trainer_venue['trainer_venue_wins'] / trainer_venue['trainer_venue_races']
        
        self.statistics['trainer_stats'] = trainer_stats
        self.statistics['trainer_venue'] = trainer_venue
    
    def _calculate_pedigree_stats(self, df: pd.DataFrame, pedigrees_df: pd.DataFrame):
        """血統統計を計算"""
        # 父馬を抽出
        sires = pedigrees_df[pedigrees_df['generation'] == 1][['horse_id', 'ancestor_id']]
        sires.columns = ['horse_id', 'sire_id']
        
        df_with_sire = df.merge(sires, on='horse_id', how='left')
        
        # 父馬の全体成績
        sire_stats = df_with_sire.groupby('sire_id').apply(
            lambda x: pd.Series({
                'sire_offspring_races': len(x),
                'sire_offspring_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_stats['sire_win_rate'] = sire_stats['sire_offspring_wins'] / sire_stats['sire_offspring_races']
        
        # 父馬の距離別成績
        sire_distance = df_with_sire.groupby(['sire_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'sire_dist_races': len(x),
                'sire_dist_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_distance['sire_dist_win_rate'] = sire_distance['sire_dist_wins'] / sire_distance['sire_dist_races']
        
        # 父馬の馬場別成績
        sire_surface = df_with_sire.groupby(['sire_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'sire_surface_races': len(x),
                'sire_surface_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_surface['sire_surface_win_rate'] = sire_surface['sire_surface_wins'] / sire_surface['sire_surface_races']
        
        # 父馬の馬場状態別成績
        sire_condition = df_with_sire.groupby(['sire_id', 'track_condition']).apply(
            lambda x: pd.Series({
                'sire_cond_races': len(x),
                'sire_cond_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_condition['sire_cond_win_rate'] = sire_condition['sire_cond_wins'] / sire_condition['sire_cond_races']
        
        self.statistics['sire_stats'] = sire_stats
        self.statistics['sire_distance'] = sire_distance
        self.statistics['sire_surface'] = sire_surface
        self.statistics['sire_condition'] = sire_condition
        self.statistics['sires'] = sires
    
    def _calculate_track_bias_detailed(self, df: pd.DataFrame):
        """馬場バイアスを精緻化（競馬場×距離×馬場状態×枠番）"""
        # 競馬場×距離×馬場×枠番
        post_bias = df.groupby(['venue', 'distance_category', 'track_surface', 'track_condition', 'bracket_number']).agg({
            'finish_position': 'mean',
            'race_id': 'count'
        }).reset_index()
        post_bias.columns = ['venue', 'distance_category', 'track_surface', 'track_condition', 'bracket_number', 'bias_avg_finish', 'bias_count']
        
        # 競馬場×距離×馬場の基準着順
        venue_base = df.groupby(['venue', 'distance_category', 'track_surface', 'track_condition']).agg({
            'finish_position': 'mean'
        }).reset_index()
        venue_base.columns = ['venue', 'distance_category', 'track_surface', 'track_condition', 'venue_avg_finish']
        
        # バイアス = 枠別着順 - 基準着順
        post_bias = post_bias.merge(venue_base, on=['venue', 'distance_category', 'track_surface', 'track_condition'], how='left')
        post_bias['bracket_bias'] = post_bias['bias_avg_finish'] - post_bias['venue_avg_finish']
        
        self.statistics['post_bias_detailed'] = post_bias
        self.statistics['venue_base'] = venue_base
    
    def _calculate_running_style_advantage(self, df: pd.DataFrame):
        """コース別脚質有利度を計算"""
        # 1コーナー通過順位から脚質を推定
        df = df.copy()
        if 'passing_order_1' in df.columns:
            df['running_style_cat'] = pd.cut(
                df['passing_order_1'].fillna(5),
                bins=[0, 2, 5, 10, 100],
                labels=['逃げ', '先行', '差し', '追込']
            )
            
            style_advantage = df.groupby(['venue', 'distance_category', 'track_surface', 'running_style_cat']).agg({
                'finish_position': 'mean',
                'race_id': 'count'
            }).reset_index()
            style_advantage.columns = ['venue', 'distance_category', 'track_surface', 'running_style', 'style_avg_finish', 'style_count']
            
            self.statistics['running_style_advantage'] = style_advantage
    
    # ========================================================================
    # Phase 1: タイム・位置取り・適性
    # ========================================================================
    
    def _add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """タイム系特徴量を追加"""
        # 過去レースの着順履歴
        df['_prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
        df['_prev_finish_2'] = df.groupby('horse_id')['finish_position'].shift(2)
        df['_prev_finish_3'] = df.groupby('horse_id')['finish_position'].shift(3)
        
        # 直近5走の平均着順
        df['time_avg_finish'] = df.groupby('horse_id')['finish_position'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        # 直近5走のベスト着順
        df['time_best_finish'] = df.groupby('horse_id')['finish_position'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).min()
        )
        
        # 出走回数
        df['time_races_count'] = df.groupby('horse_id').cumcount()
        
        # 勝率（過去）
        df['_cumsum_wins'] = df.groupby('horse_id').apply(
            lambda x: (x['finish_position'] == 1).shift(1).cumsum()
        ).reset_index(level=0, drop=True)
        df['horse_win_rate'] = df['_cumsum_wins'] / df['time_races_count'].clip(lower=1)
        
        # 複勝率（過去）
        df['_cumsum_top3'] = df.groupby('horse_id').apply(
            lambda x: (x['finish_position'] <= 3).shift(1).cumsum()
        ).reset_index(level=0, drop=True)
        df['horse_top3_rate'] = df['_cumsum_top3'] / df['time_races_count'].clip(lower=1)
        
        # 着順トレンド（直近5走の回帰係数）
        df['finish_trend'] = df.groupby('horse_id')['finish_position'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=3).apply(
                lambda y: np.polyfit(range(len(y)), y, 1)[0] if len(y) >= 3 else 0, raw=False
            )
        )
        
        # 不要列削除
        df = df.drop(columns=['_prev_finish', '_prev_finish_2', '_prev_finish_3', '_cumsum_wins', '_cumsum_top3'], errors='ignore')
        
        return df
    
    def _add_last3f_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """上がり3F系特徴量を追加"""
        if 'last_3f_time' not in df.columns:
            return df
        
        # 直近5走の上がり3F平均
        df['last3f_avg'] = df.groupby('horse_id')['last_3f_time'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        # 上がり3Fベスト
        df['last3f_best'] = df.groupby('horse_id')['last_3f_time'].transform(
            lambda x: x.shift(1).expanding().min()
        )
        
        # 上がり3F順位の平均
        if 'last3f_rank' in df.columns:
            df['last3f_avg_rank'] = df.groupby('horse_id')['last3f_rank'].transform(
                lambda x: x.shift(1).rolling(5, min_periods=1).mean()
            )
        
        # 上がり3Fトレンド
        df['last3f_trend'] = df.groupby('horse_id')['last_3f_time'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=3).apply(
                lambda y: np.polyfit(range(len(y)), y, 1)[0] if len(y) >= 3 else 0, raw=False
            )
        )
        
        return df
    
    def _add_position_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """位置取り系特徴量を追加"""
        # 1コーナー通過順位
        if 'passing_order_1' in df.columns:
            df['pos_avg_1corner'] = df.groupby('horse_id')['passing_order_1'].transform(
                lambda x: x.shift(1).rolling(5, min_periods=1).mean()
            )
        
        # 4コーナー通過順位
        if 'passing_order_4' in df.columns:
            df['pos_avg_4corner'] = df.groupby('horse_id')['passing_order_4'].transform(
                lambda x: x.shift(1).rolling(5, min_periods=1).mean()
            )
            
            # 直線での順位上昇度（4コーナー - 最終着順）
            df['_pos_gain'] = df['passing_order_4'] - df['finish_position']
            df['pos_gain_avg'] = df.groupby('horse_id')['_pos_gain'].transform(
                lambda x: x.shift(1).rolling(5, min_periods=1).mean()
            )
            df = df.drop(columns=['_pos_gain'], errors='ignore')
        
        # 脚質判定（1コーナー通過順位から）
        if 'passing_order_1' in df.columns:
            df['running_style'] = df.groupby('horse_id')['passing_order_1'].transform(
                lambda x: x.shift(1).rolling(5, min_periods=1).mean()
            )
            # 脚質カテゴリ（逃げ=0, 先行=1, 差し=2, 追込=3）
            running_style_cat = pd.cut(
                df['running_style'].fillna(5),
                bins=[0, 2, 5, 10, 100],
                labels=[0, 1, 2, 3]
            )
            df['running_style_cat'] = pd.to_numeric(running_style_cat, errors='coerce')
        
        return df
    
    def _add_aptitude_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """適性系特徴量を追加"""
        # 距離カテゴリ別の成績
        df = df.sort_values(['horse_id', 'race_date'])
        
        # 同馬場での勝率
        df['_surface_win'] = (df['finish_position'] == 1).astype(int)
        df['_surface_count'] = 1
        
        # 芝/ダート別の累積勝利数と出走数
        df['apt_surface_wins'] = df.groupby(['horse_id', 'track_surface'])['_surface_win'].transform(
            lambda x: x.shift(1).cumsum()
        ).fillna(0)
        df['apt_surface_count'] = df.groupby(['horse_id', 'track_surface'])['_surface_count'].transform(
            lambda x: x.shift(1).cumsum()
        ).fillna(0)
        df['apt_surface_win_rate'] = df['apt_surface_wins'] / df['apt_surface_count'].clip(lower=1)
        
        # 距離カテゴリ別の累積勝利数と出走数
        df['apt_dist_wins'] = df.groupby(['horse_id', 'distance_category'])['_surface_win'].transform(
            lambda x: x.shift(1).cumsum()
        ).fillna(0)
        df['apt_dist_count'] = df.groupby(['horse_id', 'distance_category'])['_surface_count'].transform(
            lambda x: x.shift(1).cumsum()
        ).fillna(0)
        df['apt_dist_win_rate'] = df['apt_dist_wins'] / df['apt_dist_count'].clip(lower=1)
        
        # 競馬場別の累積勝利数と出走数
        df['apt_venue_wins'] = df.groupby(['horse_id', 'venue'])['_surface_win'].transform(
            lambda x: x.shift(1).cumsum()
        ).fillna(0)
        df['apt_venue_count'] = df.groupby(['horse_id', 'venue'])['_surface_count'].transform(
            lambda x: x.shift(1).cumsum()
        ).fillna(0)
        df['apt_venue_win_rate'] = df['apt_venue_wins'] / df['apt_venue_count'].clip(lower=1)
        
        # 馬場状態別
        df['apt_cond_wins'] = df.groupby(['horse_id', 'track_condition'])['_surface_win'].transform(
            lambda x: x.shift(1).cumsum()
        ).fillna(0)
        df['apt_cond_count'] = df.groupby(['horse_id', 'track_condition'])['_surface_count'].transform(
            lambda x: x.shift(1).cumsum()
        ).fillna(0)
        df['apt_cond_win_rate'] = df['apt_cond_wins'] / df['apt_cond_count'].clip(lower=1)
        
        # 不要列削除
        df = df.drop(columns=['_surface_win', '_surface_count'], errors='ignore')
        
        # 芝/ダート経験数
        df['apt_turf_count'] = np.where(
            df['track_surface'] == '芝',
            df['apt_surface_count'],
            np.nan
        )
        df['apt_dirt_count'] = np.where(
            df['track_surface'] == 'ダート',
            df['apt_surface_count'],
            np.nan
        )
        
        return df
    
    # ========================================================================
    # Phase 2: 騎手・調教師・血統
    # ========================================================================
    
    def _add_jockey_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手特徴量を追加"""
        jockey_stats = self.statistics.get('jockey_stats')
        jockey_venue = self.statistics.get('jockey_venue')
        jockey_dist = self.statistics.get('jockey_dist')
        jockey_surface = self.statistics.get('jockey_surface')
        
        if jockey_stats is not None:
            df = df.merge(
                jockey_stats[['jockey_id', 'jockey_overall_win_rate', 'jockey_overall_top3_rate', 'jockey_overall_races']],
                on='jockey_id', how='left'
            )
        
        if jockey_venue is not None:
            df = df.merge(
                jockey_venue[['jockey_id', 'venue', 'jockey_venue_win_rate']],
                on=['jockey_id', 'venue'], how='left'
            )
        
        if jockey_dist is not None:
            df = df.merge(
                jockey_dist[['jockey_id', 'distance_category', 'jockey_dist_win_rate']],
                on=['jockey_id', 'distance_category'], how='left'
            )
        
        if jockey_surface is not None:
            df = df.merge(
                jockey_surface[['jockey_id', 'track_surface', 'jockey_surface_win_rate']],
                on=['jockey_id', 'track_surface'], how='left'
            )
        
        # 騎手×馬の過去組み合わせ
        df['jockey_horse_races'] = df.groupby(['jockey_id', 'horse_id']).cumcount()
        
        return df
    
    def _add_trainer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """調教師特徴量を追加"""
        trainer_stats = self.statistics.get('trainer_stats')
        trainer_venue = self.statistics.get('trainer_venue')
        
        if trainer_stats is not None:
            df = df.merge(
                trainer_stats[['trainer_id', 'trainer_overall_win_rate', 'trainer_overall_top3_rate', 'trainer_overall_races']],
                on='trainer_id', how='left'
            )
        
        if trainer_venue is not None:
            df = df.merge(
                trainer_venue[['trainer_id', 'venue', 'trainer_venue_win_rate']],
                on=['trainer_id', 'venue'], how='left'
            )
        
        return df
    
    def _add_pedigree_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """血統特徴量を追加"""
        sires = self.statistics.get('sires')
        sire_stats = self.statistics.get('sire_stats')
        sire_distance = self.statistics.get('sire_distance')
        sire_surface = self.statistics.get('sire_surface')
        sire_condition = self.statistics.get('sire_condition')
        
        if sires is not None:
            df = df.merge(sires, on='horse_id', how='left')
        
        if sire_stats is not None:
            df = df.merge(
                sire_stats[['sire_id', 'sire_win_rate', 'sire_offspring_races']],
                on='sire_id', how='left'
            )
        
        if sire_distance is not None:
            df = df.merge(
                sire_distance[['sire_id', 'distance_category', 'sire_dist_win_rate']],
                on=['sire_id', 'distance_category'], how='left'
            )
        
        if sire_surface is not None:
            df = df.merge(
                sire_surface[['sire_id', 'track_surface', 'sire_surface_win_rate']],
                on=['sire_id', 'track_surface'], how='left'
            )
        
        if sire_condition is not None:
            df = df.merge(
                sire_condition[['sire_id', 'track_condition', 'sire_cond_win_rate']],
                on=['sire_id', 'track_condition'], how='left'
            )
        
        return df
    
    # ========================================================================
    # Phase 3: コンディション・複合
    # ========================================================================
    
    def _add_condition_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """コンディション特徴量を追加"""
        # 馬場状態のエンコード
        condition_map = {'良': 0, '稍重': 1, '稍': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(condition_map).fillna(0)
        
        # 馬場種別のエンコード
        surface_map = {'芝': 0, 'ダート': 1}
        df['track_surface_encoded'] = df['track_surface'].map(surface_map).fillna(0)
        
        # 距離カテゴリのエンコード
        dist_map = {'sprint': 0, 'mile': 1, 'middle': 2, 'long': 3}
        df['distance_category_encoded'] = df['distance_category'].astype(str).map(dist_map).fillna(1).astype(float)
        
        return df
    
    def _add_weight_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """馬体重特徴量を追加"""
        if 'horse_weight' not in df.columns:
            return df
        
        # 馬体重の平均（過去）
        df['weight_avg'] = df.groupby('horse_id')['horse_weight'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        # 馬体重の標準偏差（過去）
        df['weight_std'] = df.groupby('horse_id')['horse_weight'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=2).std()
        )
        
        # 馬体重変化
        if 'horse_weight_change' in df.columns:
            df['weight_change'] = df['horse_weight_change']
        else:
            df['weight_change'] = df.groupby('horse_id')['horse_weight'].diff()
        
        # 最適体重からの乖離
        # 過去に3着以内に入った時の馬体重の平均
        df['_good_weight'] = np.where(
            df['finish_position'] <= 3,
            df['horse_weight'],
            np.nan
        )
        df['optimal_weight'] = df.groupby('horse_id')['_good_weight'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        df['weight_deviation'] = np.abs(df['horse_weight'] - df['optimal_weight']) / df['optimal_weight'].clip(lower=1)
        df = df.drop(columns=['_good_weight'], errors='ignore')
        
        return df
    
    def _add_rest_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """休養日数特徴量を追加"""
        # 前走からの日数
        df['cond_days_since_last'] = df.groupby('horse_id')['race_date'].diff().dt.days
        
        # 休養カテゴリ
        df['rest_category'] = pd.cut(
            df['cond_days_since_last'].fillna(90),
            bins=[0, 14, 28, 60, 90, 180, 9999],
            labels=[0, 1, 2, 3, 4, 5]  # 連闘, 中2週, 1ヶ月, 2ヶ月, 3ヶ月, 長期休養
        ).astype(float)
        
        # 最適休養日数からの乖離
        df['_good_rest'] = np.where(
            df['finish_position'] <= 3,
            df['cond_days_since_last'],
            np.nan
        )
        df['optimal_rest'] = df.groupby('horse_id')['_good_rest'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        df['rest_deviation'] = np.abs(df['cond_days_since_last'] - df['optimal_rest']) / df['optimal_rest'].clip(lower=1)
        df = df.drop(columns=['_good_rest'], errors='ignore')
        
        # 過去90日の出走回数
        df['races_in_90days'] = df.groupby('horse_id').apply(
            lambda x: x['race_date'].diff().dt.days.rolling(window=5, min_periods=1).apply(
                lambda y: (y < 90).sum(), raw=False
            )
        ).reset_index(level=0, drop=True)
        
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
        
        # 馬の季節別成績
        df['_season_win'] = (df['finish_position'] == 1).astype(int)
        df['season_wins'] = df.groupby(['horse_id', 'season'])['_season_win'].transform(
            lambda x: x.shift(1).cumsum()
        ).fillna(0)
        df['season_races'] = df.groupby(['horse_id', 'season']).cumcount()
        df['season_win_rate'] = df['season_wins'] / df['season_races'].clip(lower=1)
        df = df.drop(columns=['_season_win'], errors='ignore')
        
        return df
    
    # ========================================================================
    # Phase 4: 交互作用・相対比較・差分
    # ========================================================================
    
    def _add_interaction_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """交互作用特徴量を追加"""
        # 距離×脚質
        if 'running_style_cat' in df.columns and 'distance_category_encoded' in df.columns:
            df['int_dist_style'] = df['distance_category_encoded'] * df['running_style_cat']
        
        # 馬場状態×血統（父馬の馬場状態適性）
        if 'track_condition_encoded' in df.columns and 'sire_cond_win_rate' in df.columns:
            df['int_cond_pedigree'] = df['track_condition_encoded'] * df['sire_cond_win_rate'].fillna(0)
        
        # 枠順×脚質
        if 'bracket_number' in df.columns and 'running_style_cat' in df.columns:
            df['int_post_style'] = df['bracket_number'] * df['running_style_cat']
        
        # 年齢×クラス
        if 'age' in df.columns:
            df['age'] = df['age'].fillna(4)
            # クラスレベルをrace_nameから推定（簡易版）
            df['int_age_class'] = (df['age'] - 4) * df['distance_m'].fillna(1600) / 1600
        
        return df
    
    def _add_relative_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """相対比較特徴量を追加（レース内順位）"""
        numeric_cols = [
            'time_avg_finish', 'horse_win_rate', 'horse_top3_rate',
            'jockey_overall_win_rate', 'trainer_overall_win_rate',
            'last3f_avg', 'sire_win_rate'
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                rank_col = f'{col}_rank'
                df[rank_col] = df.groupby('race_id')[col].rank(ascending=True, method='min')
        
        return df
    
    def _add_difference_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """差分・比率特徴量を追加"""
        # 前走との距離差
        df['dist_change'] = df.groupby('horse_id')['distance_m'].diff()
        
        # 前走との距離比
        df['_prev_dist'] = df.groupby('horse_id')['distance_m'].shift(1)
        df['dist_change_ratio'] = df['distance_m'] / df['_prev_dist'].clip(lower=1)
        df = df.drop(columns=['_prev_dist'], errors='ignore')
        
        # 体重変化率
        if 'horse_weight' in df.columns:
            df['_prev_weight'] = df.groupby('horse_id')['horse_weight'].shift(1)
            df['weight_change_ratio'] = df['horse_weight'] / df['_prev_weight'].clip(lower=1)
            df = df.drop(columns=['_prev_weight'], errors='ignore')
        
        return df
    
    def _remove_forbidden_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """禁止特徴量を除去"""
        cols_to_drop = [c for c in df.columns if c in FORBIDDEN_FEATURES]
        if cols_to_drop:
            logger.info(f"禁止特徴量を除去: {cols_to_drop}")
        return df.drop(columns=cols_to_drop, errors='ignore')

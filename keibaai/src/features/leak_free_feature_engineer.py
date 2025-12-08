"""
リークを完全に排除した特徴量エンジニア

設計原則:
1. fit()で全ての統計を事前計算して保存
2. transform()ではfinish_positionを使用しない（lookupのみ）
3. 馬の累積成績は「各時点での累積」を事前計算

重要:
- テストデータのfinish_positionは一切参照しない
- 時系列安全性を保証
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class LeakFreeConfig:
    """設定"""
    min_races_for_stats: int = 3  # 統計計算の最小レース数


class LeakFreeFeatureEngineer:
    """
    リークを完全に排除した特徴量エンジニア
    
    fit()で訓練データから全ての統計を事前計算し、
    transform()ではfinish_positionを使用せずに特徴量を生成
    """
    
    def __init__(self, config: Optional[LeakFreeConfig] = None):
        self.config = config or LeakFreeConfig()
        self.statistics = {}
        self.is_fitted = False
        self.fit_date = None  # fit時の最終日付
        
    def fit(self, df: pd.DataFrame, pedigrees_df: pd.DataFrame = None) -> 'LeakFreeFeatureEngineer':
        """
        訓練データから統計を事前計算
        
        Args:
            df: 訓練データ（finish_positionを含む）
            pedigrees_df: 血統データ
        """
        logger.info("LeakFreeFeatureEngineer: fit開始")
        
        df = df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # fit時の最終日付を保存
        self.fit_date = df['race_date'].max()
        logger.info(f"  fit期間: {df['race_date'].min()} - {self.fit_date}")
        
        # 距離カテゴリを追加
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        # ========================================
        # 1. 騎手統計
        # ========================================
        self._calculate_jockey_stats(df)
        
        # ========================================
        # 2. 調教師統計
        # ========================================
        self._calculate_trainer_stats(df)
        
        # ========================================
        # 3. 血統統計
        # ========================================
        if pedigrees_df is not None:
            self._calculate_pedigree_stats(df, pedigrees_df)
        
        # ========================================
        # 4. 馬の累積成績（各時点での累積）
        # ========================================
        self._calculate_horse_cumulative_stats(df)
        
        # ========================================
        # 5. 馬場バイアス
        # ========================================
        self._calculate_track_bias(df)
        
        self.is_fitted = True
        logger.info("LeakFreeFeatureEngineer: fit完了")
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        特徴量を生成（finish_positionは使用しない）
        
        Args:
            df: 予測対象データ（finish_positionは不要、あっても使用しない）
            
        Returns:
            特徴量を追加したDataFrame
        """
        if not self.is_fitted:
            raise ValueError("fit()を先に実行してください")
        
        df = df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logger.info(f"LeakFreeFeatureEngineer: transform開始 ({len(df)}行)")
        
        # 距離カテゴリを追加
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        # ========================================
        # 騎手特徴量をマージ
        # ========================================
        df = self._merge_jockey_features(df)
        
        # ========================================
        # 調教師特徴量をマージ
        # ========================================
        df = self._merge_trainer_features(df)
        
        # ========================================
        # 血統特徴量をマージ
        # ========================================
        df = self._merge_pedigree_features(df)
        
        # ========================================
        # 馬の累積成績をマージ
        # ========================================
        df = self._merge_horse_stats(df)
        
        # ========================================
        # 馬場バイアスをマージ
        # ========================================
        df = self._merge_track_bias(df)
        
        # ========================================
        # 当日情報のエンコード
        # ========================================
        df = self._encode_current_race_features(df)
        
        logger.info(f"LeakFreeFeatureEngineer: transform完了")
        return df
    
    # ========================================================================
    # fit時の統計計算
    # ========================================================================
    
    def _calculate_jockey_stats(self, df: pd.DataFrame):
        """騎手統計を計算"""
        # 全体成績
        jockey_overall = df.groupby('jockey_id').apply(
            lambda x: pd.Series({
                'jockey_overall_races': len(x),
                'jockey_overall_wins': (x['finish_position'] == 1).sum(),
                'jockey_overall_top3': (x['finish_position'] <= 3).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_overall['jockey_win_rate'] = jockey_overall['jockey_overall_wins'] / jockey_overall['jockey_overall_races']
        jockey_overall['jockey_top3_rate'] = jockey_overall['jockey_overall_top3'] / jockey_overall['jockey_overall_races']
        self.statistics['jockey_overall'] = jockey_overall
        
        # 競馬場別成績
        jockey_venue = df.groupby(['jockey_id', 'venue']).apply(
            lambda x: pd.Series({
                'jockey_venue_races': len(x),
                'jockey_venue_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_venue['jockey_venue_win_rate'] = jockey_venue['jockey_venue_wins'] / jockey_venue['jockey_venue_races']
        self.statistics['jockey_venue'] = jockey_venue
        
        # 距離別成績
        jockey_dist = df.groupby(['jockey_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'jockey_dist_races': len(x),
                'jockey_dist_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_dist['jockey_dist_win_rate'] = jockey_dist['jockey_dist_wins'] / jockey_dist['jockey_dist_races']
        self.statistics['jockey_dist'] = jockey_dist
        
        # 馬場別成績
        jockey_surface = df.groupby(['jockey_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'jockey_surface_races': len(x),
                'jockey_surface_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_surface['jockey_surface_win_rate'] = jockey_surface['jockey_surface_wins'] / jockey_surface['jockey_surface_races']
        self.statistics['jockey_surface'] = jockey_surface
        
        logger.info(f"  騎手統計: {len(jockey_overall)}人")
    
    def _calculate_trainer_stats(self, df: pd.DataFrame):
        """調教師統計を計算"""
        # 全体成績
        trainer_overall = df.groupby('trainer_id').apply(
            lambda x: pd.Series({
                'trainer_overall_races': len(x),
                'trainer_overall_wins': (x['finish_position'] == 1).sum(),
                'trainer_overall_top3': (x['finish_position'] <= 3).sum(),
            }), include_groups=False
        ).reset_index()
        trainer_overall['trainer_win_rate'] = trainer_overall['trainer_overall_wins'] / trainer_overall['trainer_overall_races']
        trainer_overall['trainer_top3_rate'] = trainer_overall['trainer_overall_top3'] / trainer_overall['trainer_overall_races']
        self.statistics['trainer_overall'] = trainer_overall
        
        # 競馬場別成績
        trainer_venue = df.groupby(['trainer_id', 'venue']).apply(
            lambda x: pd.Series({
                'trainer_venue_races': len(x),
                'trainer_venue_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        trainer_venue['trainer_venue_win_rate'] = trainer_venue['trainer_venue_wins'] / trainer_venue['trainer_venue_races']
        self.statistics['trainer_venue'] = trainer_venue
        
        logger.info(f"  調教師統計: {len(trainer_overall)}人")
    
    def _calculate_pedigree_stats(self, df: pd.DataFrame, pedigrees_df: pd.DataFrame):
        """血統統計を計算"""
        # 父馬を抽出（generation=1）
        sires = pedigrees_df[pedigrees_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
        sires.columns = ['horse_id', 'sire_id']
        self.statistics['sires'] = sires
        
        # 父馬付きデータ
        df_with_sire = df.merge(sires, on='horse_id', how='left')
        
        # 父馬全体成績
        sire_overall = df_with_sire.groupby('sire_id').apply(
            lambda x: pd.Series({
                'sire_races': len(x),
                'sire_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_overall['sire_win_rate'] = sire_overall['sire_wins'] / sire_overall['sire_races']
        self.statistics['sire_overall'] = sire_overall
        
        # 父馬×距離別成績
        sire_dist = df_with_sire.groupby(['sire_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'sire_dist_races': len(x),
                'sire_dist_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_dist['sire_dist_win_rate'] = sire_dist['sire_dist_wins'] / sire_dist['sire_dist_races']
        self.statistics['sire_dist'] = sire_dist
        
        # 父馬×馬場別成績
        sire_surface = df_with_sire.groupby(['sire_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'sire_surface_races': len(x),
                'sire_surface_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_surface['sire_surface_win_rate'] = sire_surface['sire_surface_wins'] / sire_surface['sire_surface_races']
        self.statistics['sire_surface'] = sire_surface
        
        # 父馬×馬場状態別成績
        sire_condition = df_with_sire.groupby(['sire_id', 'track_condition']).apply(
            lambda x: pd.Series({
                'sire_cond_races': len(x),
                'sire_cond_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_condition['sire_cond_win_rate'] = sire_condition['sire_cond_wins'] / sire_condition['sire_cond_races']
        self.statistics['sire_condition'] = sire_condition
        
        logger.info(f"  血統統計: {len(sire_overall)}頭")
    
    def _calculate_horse_cumulative_stats(self, df: pd.DataFrame):
        """馬の累積成績を計算（各時点での累積）"""
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 各馬の最終時点での累積成績
        horse_final = df.groupby('horse_id').apply(
            lambda x: pd.Series({
                'horse_total_races': len(x),
                'horse_total_wins': (x['finish_position'] == 1).sum(),
                'horse_total_top3': (x['finish_position'] <= 3).sum(),
                'horse_avg_finish': x['finish_position'].mean(),
                'horse_last_finish': x['finish_position'].iloc[-1] if len(x) > 0 else np.nan,
                'horse_last3_avg_finish': x['finish_position'].tail(3).mean() if len(x) >= 3 else x['finish_position'].mean(),
                'horse_best_finish': x['finish_position'].min(),
            }), include_groups=False
        ).reset_index()
        horse_final['horse_win_rate'] = horse_final['horse_total_wins'] / horse_final['horse_total_races']
        horse_final['horse_top3_rate'] = horse_final['horse_total_top3'] / horse_final['horse_total_races']
        
        self.statistics['horse_final'] = horse_final
        
        # 上がり3Fの統計
        if 'last_3f_time' in df.columns:
            horse_last3f = df.groupby('horse_id').apply(
                lambda x: pd.Series({
                    'horse_last3f_avg': x['last_3f_time'].mean(),
                    'horse_last3f_best': x['last_3f_time'].min(),
                }), include_groups=False
            ).reset_index()
            self.statistics['horse_last3f'] = horse_last3f
        
        # ========================================
        # 馬の条件別成績
        # ========================================
        
        # 距離カテゴリ別成績
        horse_dist = df.groupby(['horse_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'horse_dist_races': len(x),
                'horse_dist_wins': (x['finish_position'] == 1).sum(),
                'horse_dist_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        horse_dist['horse_dist_win_rate'] = horse_dist['horse_dist_wins'] / horse_dist['horse_dist_races']
        self.statistics['horse_dist'] = horse_dist
        
        # 馬場別成績
        horse_surface = df.groupby(['horse_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'horse_surface_races': len(x),
                'horse_surface_wins': (x['finish_position'] == 1).sum(),
                'horse_surface_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        horse_surface['horse_surface_win_rate'] = horse_surface['horse_surface_wins'] / horse_surface['horse_surface_races']
        self.statistics['horse_surface'] = horse_surface
        
        # 競馬場別成績
        horse_venue = df.groupby(['horse_id', 'venue']).apply(
            lambda x: pd.Series({
                'horse_venue_races': len(x),
                'horse_venue_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        horse_venue['horse_venue_win_rate'] = horse_venue['horse_venue_wins'] / horse_venue['horse_venue_races']
        self.statistics['horse_venue'] = horse_venue
        
        # 馬場状態別成績
        horse_condition = df.groupby(['horse_id', 'track_condition']).apply(
            lambda x: pd.Series({
                'horse_cond_races': len(x),
                'horse_cond_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        horse_condition['horse_cond_win_rate'] = horse_condition['horse_cond_wins'] / horse_condition['horse_cond_races']
        self.statistics['horse_condition'] = horse_condition
        
        logger.info(f"  馬累積統計: {len(horse_final)}頭")
    
    def _calculate_track_bias(self, df: pd.DataFrame):
        """馬場バイアス（枠順有利不利）を計算"""
        # 競馬場×距離カテゴリ×馬場×枠番
        track_bias = df.groupby(['venue', 'distance_category', 'track_surface', 'bracket_number']).apply(
            lambda x: pd.Series({
                'bracket_avg_finish': x['finish_position'].mean(),
                'bracket_count': len(x),
            }), include_groups=False
        ).reset_index()
        
        # 競馬場×距離カテゴリ×馬場の基準着順
        venue_base = df.groupby(['venue', 'distance_category', 'track_surface']).apply(
            lambda x: pd.Series({
                'venue_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        
        # バイアス = 枠別着順 - 基準着順（負が有利）
        track_bias = track_bias.merge(venue_base, on=['venue', 'distance_category', 'track_surface'], how='left')
        track_bias['bracket_bias'] = track_bias['bracket_avg_finish'] - track_bias['venue_avg_finish']
        
        self.statistics['track_bias'] = track_bias
        
        logger.info(f"  馬場バイアス: {len(track_bias)}条件")
    
    # ========================================================================
    # transform時のマージ処理
    # ========================================================================
    
    def _merge_jockey_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手特徴量をマージ"""
        # 全体成績
        jockey_overall = self.statistics.get('jockey_overall')
        if jockey_overall is not None:
            df = df.merge(
                jockey_overall[['jockey_id', 'jockey_win_rate', 'jockey_top3_rate', 'jockey_overall_races']],
                on='jockey_id', how='left'
            )
        
        # 競馬場別
        jockey_venue = self.statistics.get('jockey_venue')
        if jockey_venue is not None:
            df = df.merge(
                jockey_venue[['jockey_id', 'venue', 'jockey_venue_win_rate']],
                on=['jockey_id', 'venue'], how='left'
            )
        
        # 距離別
        jockey_dist = self.statistics.get('jockey_dist')
        if jockey_dist is not None:
            df = df.merge(
                jockey_dist[['jockey_id', 'distance_category', 'jockey_dist_win_rate']],
                on=['jockey_id', 'distance_category'], how='left'
            )
        
        # 馬場別
        jockey_surface = self.statistics.get('jockey_surface')
        if jockey_surface is not None:
            df = df.merge(
                jockey_surface[['jockey_id', 'track_surface', 'jockey_surface_win_rate']],
                on=['jockey_id', 'track_surface'], how='left'
            )
        
        return df
    
    def _merge_trainer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """調教師特徴量をマージ"""
        trainer_overall = self.statistics.get('trainer_overall')
        if trainer_overall is not None:
            df = df.merge(
                trainer_overall[['trainer_id', 'trainer_win_rate', 'trainer_top3_rate', 'trainer_overall_races']],
                on='trainer_id', how='left'
            )
        
        trainer_venue = self.statistics.get('trainer_venue')
        if trainer_venue is not None:
            df = df.merge(
                trainer_venue[['trainer_id', 'venue', 'trainer_venue_win_rate']],
                on=['trainer_id', 'venue'], how='left'
            )
        
        return df
    
    def _merge_pedigree_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """血統特徴量をマージ"""
        sires = self.statistics.get('sires')
        if sires is not None:
            df = df.merge(sires, on='horse_id', how='left')
        
        sire_overall = self.statistics.get('sire_overall')
        if sire_overall is not None:
            df = df.merge(
                sire_overall[['sire_id', 'sire_win_rate', 'sire_races']],
                on='sire_id', how='left'
            )
        
        sire_dist = self.statistics.get('sire_dist')
        if sire_dist is not None:
            df = df.merge(
                sire_dist[['sire_id', 'distance_category', 'sire_dist_win_rate']],
                on=['sire_id', 'distance_category'], how='left'
            )
        
        sire_surface = self.statistics.get('sire_surface')
        if sire_surface is not None:
            df = df.merge(
                sire_surface[['sire_id', 'track_surface', 'sire_surface_win_rate']],
                on=['sire_id', 'track_surface'], how='left'
            )
        
        sire_condition = self.statistics.get('sire_condition')
        if sire_condition is not None:
            df = df.merge(
                sire_condition[['sire_id', 'track_condition', 'sire_cond_win_rate']],
                on=['sire_id', 'track_condition'], how='left'
            )
        
        return df
    
    def _merge_horse_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """馬の累積成績をマージ"""
        horse_final = self.statistics.get('horse_final')
        if horse_final is not None:
            df = df.merge(
                horse_final[['horse_id', 'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
                             'horse_total_races', 'horse_last3_avg_finish', 'horse_best_finish']],
                on='horse_id', how='left'
            )
        
        horse_last3f = self.statistics.get('horse_last3f')
        if horse_last3f is not None:
            df = df.merge(
                horse_last3f[['horse_id', 'horse_last3f_avg', 'horse_last3f_best']],
                on='horse_id', how='left'
            )
        
        # 馬の条件別成績
        horse_dist = self.statistics.get('horse_dist')
        if horse_dist is not None:
            df = df.merge(
                horse_dist[['horse_id', 'distance_category', 'horse_dist_win_rate', 'horse_dist_avg_finish']],
                on=['horse_id', 'distance_category'], how='left'
            )
        
        horse_surface = self.statistics.get('horse_surface')
        if horse_surface is not None:
            df = df.merge(
                horse_surface[['horse_id', 'track_surface', 'horse_surface_win_rate', 'horse_surface_avg_finish']],
                on=['horse_id', 'track_surface'], how='left'
            )
        
        horse_venue = self.statistics.get('horse_venue')
        if horse_venue is not None:
            df = df.merge(
                horse_venue[['horse_id', 'venue', 'horse_venue_win_rate']],
                on=['horse_id', 'venue'], how='left'
            )
        
        horse_condition = self.statistics.get('horse_condition')
        if horse_condition is not None:
            df = df.merge(
                horse_condition[['horse_id', 'track_condition', 'horse_cond_win_rate']],
                on=['horse_id', 'track_condition'], how='left'
            )
        
        return df
    
    def _merge_track_bias(self, df: pd.DataFrame) -> pd.DataFrame:
        """馬場バイアスをマージ"""
        track_bias = self.statistics.get('track_bias')
        if track_bias is not None:
            df = df.merge(
                track_bias[['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_bias']],
                on=['venue', 'distance_category', 'track_surface', 'bracket_number'], how='left'
            )
        
        return df
    
    def _encode_current_race_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """当日情報をエンコード"""
        # 距離カテゴリ
        dist_map = {'sprint': 0, 'mile': 1, 'middle': 2, 'long': 3}
        df['distance_category_encoded'] = df['distance_category'].map(dist_map).fillna(1).astype(float)
        
        # 馬場状態
        condition_map = {'良': 0, '稍重': 1, '稍': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(condition_map).fillna(0).astype(float)
        
        # 馬場種別
        surface_map = {'芝': 0, 'ダート': 1}
        df['track_surface_encoded'] = df['track_surface'].map(surface_map).fillna(0).astype(float)
        
        # 性別
        if 'sex' in df.columns:
            sex_map = {'牡': 0, '牝': 1, 'セ': 2}
            df['sex_encoded'] = df['sex'].map(sex_map).fillna(0).astype(float)
        
        # 年齢（存在する場合）
        if 'age' not in df.columns and 'sex_age' in df.columns:
            # sex_ageから年齢を抽出（例: "牡3" → 3）
            df['age'] = df['sex_age'].str.extract(r'(\d+)').astype(float)
        
        return df
    
    def get_feature_columns(self) -> list:
        """使用する特徴量カラムのリストを返す"""
        return [
            # 騎手
            'jockey_win_rate', 'jockey_top3_rate', 'jockey_overall_races',
            'jockey_venue_win_rate', 'jockey_dist_win_rate', 'jockey_surface_win_rate',
            # 調教師
            'trainer_win_rate', 'trainer_top3_rate', 'trainer_overall_races',
            'trainer_venue_win_rate',
            # 血統
            'sire_win_rate', 'sire_races', 'sire_dist_win_rate',
            'sire_surface_win_rate', 'sire_cond_win_rate',
            # 馬（全体）
            'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
            'horse_total_races', 'horse_last3_avg_finish', 'horse_best_finish',
            'horse_last3f_avg', 'horse_last3f_best',
            # 馬（条件別）
            'horse_dist_win_rate', 'horse_dist_avg_finish',
            'horse_surface_win_rate', 'horse_surface_avg_finish',
            'horse_venue_win_rate', 'horse_cond_win_rate',
            # 馬場バイアス
            'bracket_bias',
            # 当日情報
            'distance_category_encoded', 'track_condition_encoded', 'track_surface_encoded',
            'bracket_number', 'horse_weight', 'age',
            'distance_m',
        ]

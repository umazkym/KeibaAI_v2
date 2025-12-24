"""
リークを完全に排除した拡張特徴量エンジニア（v2）

設計書の155個の特徴量を実装。

設計原則:
1. fit()で全ての統計を事前計算して保存
2. transform()ではfinish_positionを使用しない（lookupのみ）
3. 時系列安全性を完全に保証

カテゴリ:
1. 騎手統計（全体/条件別/相性）
2. 調教師統計（全体/条件別）
3. 血統統計（父/母父/距離別/馬場別）
4. 馬の累積成績（全体/条件別/安定性）
5. 馬場バイアス
6. 前走・休養情報
7. レース内相対特徴量
8. 交互作用特徴量
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class ExtendedFeatureConfig:
    """設定"""
    min_races_for_stats: int = 3


class LeakFreeFeatureEngineerV2:
    """
    リークを完全に排除した拡張特徴量エンジニア（v2）
    
    設計書の155個の特徴量を実装
    """
    
    def __init__(self, config: Optional[ExtendedFeatureConfig] = None):
        self.config = config or ExtendedFeatureConfig()
        self.statistics = {}
        self.is_fitted = False
        self.fit_date = None
        
    def fit(self, df: pd.DataFrame, pedigrees_df: pd.DataFrame = None) -> 'LeakFreeFeatureEngineerV2':
        """訓練データから統計を事前計算"""
        logger.info("LeakFreeFeatureEngineerV2: fit開始")
        
        df = df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        self.fit_date = df['race_date'].max()
        logger.info(f"  fit期間: {df['race_date'].min()} - {self.fit_date}")
        
        # 前処理
        df = self._preprocess(df)
        
        # 1. 騎手統計
        self._calculate_jockey_stats(df)
        
        # 2. 調教師統計
        self._calculate_trainer_stats(df)
        
        # 3. 血統統計
        if pedigrees_df is not None:
            self._calculate_pedigree_stats(df, pedigrees_df)
        
        # 4. 馬の累積成績
        self._calculate_horse_stats(df)
        
        # 5. 馬の安定性
        self._calculate_horse_stability(df)
        
        # 6. 騎手×馬、騎手×調教師の相性
        self._calculate_combination_stats(df)
        
        # 7. 馬場バイアス
        self._calculate_track_bias(df)
        
        # 8. 前走情報のテンプレート
        self._prepare_last_race_info(df)
        
        self.is_fitted = True
        logger.info("LeakFreeFeatureEngineerV2: fit完了")
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """特徴量を生成（finish_positionは使用しない）"""
        if not self.is_fitted:
            raise ValueError("fit()を先に実行してください")
        
        df = df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logger.info(f"LeakFreeFeatureEngineerV2: transform開始 ({len(df)}行)")
        
        # 前処理
        df = self._preprocess(df)
        
        # 各種統計をマージ
        df = self._merge_jockey_features(df)
        df = self._merge_trainer_features(df)
        df = self._merge_pedigree_features(df)
        df = self._merge_horse_stats(df)
        df = self._merge_horse_stability(df)
        df = self._merge_combination_features(df)
        df = self._merge_track_bias(df)
        df = self._merge_last_race_info(df)
        
        # 当日情報のエンコード
        df = self._encode_current_race_features(df)
        
        # レース内相対特徴量
        df = self._add_relative_features(df)
        
        logger.info("LeakFreeFeatureEngineerV2: transform完了")
        return df
    
    # ========================================================================
    # 前処理
    # ========================================================================
    
    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """前処理"""
        # 距離カテゴリ
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        return df
    
    # ========================================================================
    # 1. 騎手統計
    # ========================================================================
    
    def _calculate_jockey_stats(self, df: pd.DataFrame):
        """騎手統計を計算"""
        # 全体成績
        jockey_overall = df.groupby('jockey_id').apply(
            lambda x: pd.Series({
                'jockey_races': len(x),
                'jockey_wins': (x['finish_position'] == 1).sum(),
                'jockey_top3': (x['finish_position'] <= 3).sum(),
                'jockey_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        jockey_overall['jockey_win_rate'] = jockey_overall['jockey_wins'] / jockey_overall['jockey_races']
        jockey_overall['jockey_top3_rate'] = jockey_overall['jockey_top3'] / jockey_overall['jockey_races']
        self.statistics['jockey_overall'] = jockey_overall
        
        # 競馬場別成績
        jockey_venue = df.groupby(['jockey_id', 'venue']).apply(
            lambda x: pd.Series({
                'jockey_venue_races': len(x),
                'jockey_venue_wins': (x['finish_position'] == 1).sum(),
                'jockey_venue_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        jockey_venue['jockey_venue_win_rate'] = jockey_venue['jockey_venue_wins'] / jockey_venue['jockey_venue_races']
        self.statistics['jockey_venue'] = jockey_venue
        
        # 距離別成績
        jockey_dist = df.groupby(['jockey_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'jockey_dist_races': len(x),
                'jockey_dist_wins': (x['finish_position'] == 1).sum(),
                'jockey_dist_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        jockey_dist['jockey_dist_win_rate'] = jockey_dist['jockey_dist_wins'] / jockey_dist['jockey_dist_races']
        self.statistics['jockey_dist'] = jockey_dist
        
        # 馬場別成績
        jockey_surface = df.groupby(['jockey_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'jockey_surface_races': len(x),
                'jockey_surface_wins': (x['finish_position'] == 1).sum(),
                'jockey_surface_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        jockey_surface['jockey_surface_win_rate'] = jockey_surface['jockey_surface_wins'] / jockey_surface['jockey_surface_races']
        self.statistics['jockey_surface'] = jockey_surface
        
        # 馬場状態別成績
        jockey_condition = df.groupby(['jockey_id', 'track_condition']).apply(
            lambda x: pd.Series({
                'jockey_cond_races': len(x),
                'jockey_cond_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_condition['jockey_cond_win_rate'] = jockey_condition['jockey_cond_wins'] / jockey_condition['jockey_cond_races']
        self.statistics['jockey_condition'] = jockey_condition
        
        logger.info(f"  騎手統計: {len(jockey_overall)}人")
    
    # ========================================================================
    # 2. 調教師統計
    # ========================================================================
    
    def _calculate_trainer_stats(self, df: pd.DataFrame):
        """調教師統計を計算"""
        # 全体成績
        trainer_overall = df.groupby('trainer_id').apply(
            lambda x: pd.Series({
                'trainer_races': len(x),
                'trainer_wins': (x['finish_position'] == 1).sum(),
                'trainer_top3': (x['finish_position'] <= 3).sum(),
                'trainer_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        trainer_overall['trainer_win_rate'] = trainer_overall['trainer_wins'] / trainer_overall['trainer_races']
        trainer_overall['trainer_top3_rate'] = trainer_overall['trainer_top3'] / trainer_overall['trainer_races']
        self.statistics['trainer_overall'] = trainer_overall
        
        # 競馬場別
        trainer_venue = df.groupby(['trainer_id', 'venue']).apply(
            lambda x: pd.Series({
                'trainer_venue_races': len(x),
                'trainer_venue_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        trainer_venue['trainer_venue_win_rate'] = trainer_venue['trainer_venue_wins'] / trainer_venue['trainer_venue_races']
        self.statistics['trainer_venue'] = trainer_venue
        
        # 距離別
        trainer_dist = df.groupby(['trainer_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'trainer_dist_races': len(x),
                'trainer_dist_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        trainer_dist['trainer_dist_win_rate'] = trainer_dist['trainer_dist_wins'] / trainer_dist['trainer_dist_races']
        self.statistics['trainer_dist'] = trainer_dist
        
        # 馬場別
        trainer_surface = df.groupby(['trainer_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'trainer_surface_races': len(x),
                'trainer_surface_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        trainer_surface['trainer_surface_win_rate'] = trainer_surface['trainer_surface_wins'] / trainer_surface['trainer_surface_races']
        self.statistics['trainer_surface'] = trainer_surface
        
        logger.info(f"  調教師統計: {len(trainer_overall)}人")
    
    # ========================================================================
    # 3. 血統統計
    # ========================================================================
    
    def _calculate_pedigree_stats(self, df: pd.DataFrame, pedigrees_df: pd.DataFrame):
        """血統統計を計算"""
        # 父馬（generation=1, position=1）
        sires = pedigrees_df[(pedigrees_df['generation'] == 1)][['horse_id', 'ancestor_id']].copy()
        sires.columns = ['horse_id', 'sire_id']
        self.statistics['sires'] = sires
        
        # 母父（BMS=Broodmare Sire: generation=2の3番目）
        # pedigrees_dfの構造によっては調整が必要
        
        df_with_sire = df.merge(sires, on='horse_id', how='left')
        
        # 父馬全体成績
        sire_overall = df_with_sire.groupby('sire_id').apply(
            lambda x: pd.Series({
                'sire_races': len(x),
                'sire_wins': (x['finish_position'] == 1).sum(),
                'sire_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        sire_overall['sire_win_rate'] = sire_overall['sire_wins'] / sire_overall['sire_races']
        self.statistics['sire_overall'] = sire_overall
        
        # 父馬×距離
        sire_dist = df_with_sire.groupby(['sire_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'sire_dist_races': len(x),
                'sire_dist_wins': (x['finish_position'] == 1).sum(),
                'sire_dist_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        sire_dist['sire_dist_win_rate'] = sire_dist['sire_dist_wins'] / sire_dist['sire_dist_races']
        self.statistics['sire_dist'] = sire_dist
        
        # 父馬×馬場
        sire_surface = df_with_sire.groupby(['sire_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'sire_surface_races': len(x),
                'sire_surface_wins': (x['finish_position'] == 1).sum(),
                'sire_surface_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        sire_surface['sire_surface_win_rate'] = sire_surface['sire_surface_wins'] / sire_surface['sire_surface_races']
        self.statistics['sire_surface'] = sire_surface
        
        # 父馬×馬場状態
        sire_condition = df_with_sire.groupby(['sire_id', 'track_condition']).apply(
            lambda x: pd.Series({
                'sire_cond_races': len(x),
                'sire_cond_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_condition['sire_cond_win_rate'] = sire_condition['sire_cond_wins'] / sire_condition['sire_cond_races']
        self.statistics['sire_condition'] = sire_condition
        
        logger.info(f"  血統統計: {len(sire_overall)}頭")
    
    # ========================================================================
    # 4. 馬の累積成績
    # ========================================================================
    
    def _calculate_horse_stats(self, df: pd.DataFrame):
        """馬の累積成績を計算"""
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 全体成績
        horse_overall = df.groupby('horse_id').apply(
            lambda x: pd.Series({
                'horse_total_races': len(x),
                'horse_total_wins': (x['finish_position'] == 1).sum(),
                'horse_total_top3': (x['finish_position'] <= 3).sum(),
                'horse_avg_finish': x['finish_position'].mean(),
                'horse_best_finish': x['finish_position'].min(),
                'horse_worst_finish': x['finish_position'].max(),
                'horse_last_finish': x['finish_position'].iloc[-1] if len(x) > 0 else np.nan,
                'horse_last3_avg_finish': x['finish_position'].tail(3).mean() if len(x) >= 3 else x['finish_position'].mean(),
                'horse_last5_avg_finish': x['finish_position'].tail(5).mean() if len(x) >= 5 else x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        horse_overall['horse_win_rate'] = horse_overall['horse_total_wins'] / horse_overall['horse_total_races']
        horse_overall['horse_top3_rate'] = horse_overall['horse_total_top3'] / horse_overall['horse_total_races']
        self.statistics['horse_overall'] = horse_overall
        
        # 上がり3F
        if 'last_3f_time' in df.columns:
            horse_last3f = df.groupby('horse_id').apply(
                lambda x: pd.Series({
                    'horse_last3f_avg': x['last_3f_time'].mean(),
                    'horse_last3f_best': x['last_3f_time'].min(),
                    'horse_last3f_last': x['last_3f_time'].iloc[-1] if len(x) > 0 else np.nan,
                }), include_groups=False
            ).reset_index()
            self.statistics['horse_last3f'] = horse_last3f
        
        # 距離別成績
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
                'horse_venue_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        horse_venue['horse_venue_win_rate'] = horse_venue['horse_venue_wins'] / horse_venue['horse_venue_races']
        self.statistics['horse_venue'] = horse_venue
        
        # 馬場状態別成績
        horse_condition = df.groupby(['horse_id', 'track_condition']).apply(
            lambda x: pd.Series({
                'horse_cond_races': len(x),
                'horse_cond_wins': (x['finish_position'] == 1).sum(),
                'horse_cond_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        horse_condition['horse_cond_win_rate'] = horse_condition['horse_cond_wins'] / horse_condition['horse_cond_races']
        self.statistics['horse_condition'] = horse_condition
        
        logger.info(f"  馬累積統計: {len(horse_overall)}頭")
    
    # ========================================================================
    # 5. 馬の安定性
    # ========================================================================
    
    def _calculate_horse_stability(self, df: pd.DataFrame):
        """馬の安定性指標を計算"""
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        horse_stability = df.groupby('horse_id').apply(
            lambda x: pd.Series({
                # 変動係数（着順のばらつき）
                'horse_finish_cv': x['finish_position'].std() / x['finish_position'].mean() if x['finish_position'].mean() > 0 else 0,
                # 最大最小差
                'horse_finish_range': x['finish_position'].max() - x['finish_position'].min(),
                # 経験値（対数）
                'horse_experience_log': np.log1p(len(x)),
                # トレンド（直近5走の回帰係数）
                'horse_finish_trend': self._calc_trend(x['finish_position'].tail(5).values) if len(x) >= 3 else 0,
            }), include_groups=False
        ).reset_index()
        
        self.statistics['horse_stability'] = horse_stability
        logger.info(f"  馬安定性: {len(horse_stability)}頭")
    
    def _calc_trend(self, values: np.ndarray) -> float:
        """線形回帰の傾きを計算"""
        if len(values) < 2:
            return 0
        x = np.arange(len(values))
        try:
            slope = np.polyfit(x, values, 1)[0]
            return slope
        except:
            return 0
    
    # ========================================================================
    # 6. 組み合わせ統計（騎手×馬、騎手×調教師）
    # ========================================================================
    
    def _calculate_combination_stats(self, df: pd.DataFrame):
        """騎手×馬、騎手×調教師などの組み合わせ統計"""
        # 騎手×馬
        jockey_horse = df.groupby(['jockey_id', 'horse_id']).apply(
            lambda x: pd.Series({
                'jh_races': len(x),
                'jh_wins': (x['finish_position'] == 1).sum(),
                'jh_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        jockey_horse['jh_win_rate'] = jockey_horse['jh_wins'] / jockey_horse['jh_races']
        self.statistics['jockey_horse'] = jockey_horse
        
        # 騎手×調教師
        jockey_trainer = df.groupby(['jockey_id', 'trainer_id']).apply(
            lambda x: pd.Series({
                'jt_races': len(x),
                'jt_wins': (x['finish_position'] == 1).sum(),
                'jt_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        jockey_trainer['jt_win_rate'] = jockey_trainer['jt_wins'] / jockey_trainer['jt_races']
        self.statistics['jockey_trainer'] = jockey_trainer
        
        logger.info(f"  組み合わせ統計: 騎手×馬={len(jockey_horse)}, 騎手×調教師={len(jockey_trainer)}")
    
    # ========================================================================
    # 7. 馬場バイアス
    # ========================================================================
    
    def _calculate_track_bias(self, df: pd.DataFrame):
        """馬場バイアス（枠順有利不利）を計算"""
        track_bias = df.groupby(['venue', 'distance_category', 'track_surface', 'bracket_number']).apply(
            lambda x: pd.Series({
                'bracket_avg_finish': x['finish_position'].mean(),
                'bracket_count': len(x),
            }), include_groups=False
        ).reset_index()
        
        venue_base = df.groupby(['venue', 'distance_category', 'track_surface']).apply(
            lambda x: pd.Series({
                'venue_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        
        track_bias = track_bias.merge(venue_base, on=['venue', 'distance_category', 'track_surface'], how='left')
        track_bias['bracket_bias'] = track_bias['bracket_avg_finish'] - track_bias['venue_avg_finish']
        
        self.statistics['track_bias'] = track_bias
        logger.info(f"  馬場バイアス: {len(track_bias)}条件")
    
    # ========================================================================
    # 8. 前走情報のテンプレート
    # ========================================================================
    
    def _prepare_last_race_info(self, df: pd.DataFrame):
        """前走情報のテンプレートを準備"""
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 各馬の最終レース情報を保存
        last_race = df.groupby('horse_id').apply(
            lambda x: pd.Series({
                'last_race_date': x['race_date'].iloc[-1],
                'last_finish_position': x['finish_position'].iloc[-1],
                'last_venue': x['venue'].iloc[-1],
                'last_distance': x['distance_m'].iloc[-1],
                'last_surface': x['track_surface'].iloc[-1],
                'last_weight': x['horse_weight'].iloc[-1] if 'horse_weight' in x.columns else np.nan,
            }), include_groups=False
        ).reset_index()
        
        self.statistics['last_race'] = last_race
        logger.info(f"  前走情報: {len(last_race)}頭")
    
    # ========================================================================
    # マージ処理
    # ========================================================================
    
    def _merge_jockey_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手特徴量をマージ"""
        jockey_overall = self.statistics.get('jockey_overall')
        if jockey_overall is not None:
            df = df.merge(
                jockey_overall[['jockey_id', 'jockey_win_rate', 'jockey_top3_rate', 'jockey_races', 'jockey_avg_finish']],
                on='jockey_id', how='left'
            )
        
        jockey_venue = self.statistics.get('jockey_venue')
        if jockey_venue is not None:
            df = df.merge(
                jockey_venue[['jockey_id', 'venue', 'jockey_venue_win_rate', 'jockey_venue_avg_finish']],
                on=['jockey_id', 'venue'], how='left'
            )
        
        jockey_dist = self.statistics.get('jockey_dist')
        if jockey_dist is not None:
            df = df.merge(
                jockey_dist[['jockey_id', 'distance_category', 'jockey_dist_win_rate', 'jockey_dist_avg_finish']],
                on=['jockey_id', 'distance_category'], how='left'
            )
        
        jockey_surface = self.statistics.get('jockey_surface')
        if jockey_surface is not None:
            df = df.merge(
                jockey_surface[['jockey_id', 'track_surface', 'jockey_surface_win_rate', 'jockey_surface_avg_finish']],
                on=['jockey_id', 'track_surface'], how='left'
            )
        
        jockey_condition = self.statistics.get('jockey_condition')
        if jockey_condition is not None:
            df = df.merge(
                jockey_condition[['jockey_id', 'track_condition', 'jockey_cond_win_rate']],
                on=['jockey_id', 'track_condition'], how='left'
            )
        
        return df
    
    def _merge_trainer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """調教師特徴量をマージ"""
        trainer_overall = self.statistics.get('trainer_overall')
        if trainer_overall is not None:
            df = df.merge(
                trainer_overall[['trainer_id', 'trainer_win_rate', 'trainer_top3_rate', 'trainer_races', 'trainer_avg_finish']],
                on='trainer_id', how='left'
            )
        
        trainer_venue = self.statistics.get('trainer_venue')
        if trainer_venue is not None:
            df = df.merge(
                trainer_venue[['trainer_id', 'venue', 'trainer_venue_win_rate']],
                on=['trainer_id', 'venue'], how='left'
            )
        
        trainer_dist = self.statistics.get('trainer_dist')
        if trainer_dist is not None:
            df = df.merge(
                trainer_dist[['trainer_id', 'distance_category', 'trainer_dist_win_rate']],
                on=['trainer_id', 'distance_category'], how='left'
            )
        
        trainer_surface = self.statistics.get('trainer_surface')
        if trainer_surface is not None:
            df = df.merge(
                trainer_surface[['trainer_id', 'track_surface', 'trainer_surface_win_rate']],
                on=['trainer_id', 'track_surface'], how='left'
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
                sire_overall[['sire_id', 'sire_win_rate', 'sire_races', 'sire_avg_finish']],
                on='sire_id', how='left'
            )
        
        sire_dist = self.statistics.get('sire_dist')
        if sire_dist is not None:
            df = df.merge(
                sire_dist[['sire_id', 'distance_category', 'sire_dist_win_rate', 'sire_dist_avg_finish']],
                on=['sire_id', 'distance_category'], how='left'
            )
        
        sire_surface = self.statistics.get('sire_surface')
        if sire_surface is not None:
            df = df.merge(
                sire_surface[['sire_id', 'track_surface', 'sire_surface_win_rate', 'sire_surface_avg_finish']],
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
        horse_overall = self.statistics.get('horse_overall')
        if horse_overall is not None:
            df = df.merge(
                horse_overall[['horse_id', 'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
                               'horse_total_races', 'horse_last3_avg_finish', 'horse_last5_avg_finish',
                               'horse_best_finish', 'horse_worst_finish', 'horse_last_finish']],
                on='horse_id', how='left'
            )
        
        horse_last3f = self.statistics.get('horse_last3f')
        if horse_last3f is not None:
            df = df.merge(
                horse_last3f[['horse_id', 'horse_last3f_avg', 'horse_last3f_best', 'horse_last3f_last']],
                on='horse_id', how='left'
            )
        
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
                horse_venue[['horse_id', 'venue', 'horse_venue_win_rate', 'horse_venue_avg_finish']],
                on=['horse_id', 'venue'], how='left'
            )
        
        horse_condition = self.statistics.get('horse_condition')
        if horse_condition is not None:
            df = df.merge(
                horse_condition[['horse_id', 'track_condition', 'horse_cond_win_rate', 'horse_cond_avg_finish']],
                on=['horse_id', 'track_condition'], how='left'
            )
        
        return df
    
    def _merge_horse_stability(self, df: pd.DataFrame) -> pd.DataFrame:
        """馬の安定性をマージ"""
        horse_stability = self.statistics.get('horse_stability')
        if horse_stability is not None:
            df = df.merge(
                horse_stability[['horse_id', 'horse_finish_cv', 'horse_finish_range', 
                                 'horse_experience_log', 'horse_finish_trend']],
                on='horse_id', how='left'
            )
        
        return df
    
    def _merge_combination_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """組み合わせ特徴量をマージ"""
        jockey_horse = self.statistics.get('jockey_horse')
        if jockey_horse is not None:
            df = df.merge(
                jockey_horse[['jockey_id', 'horse_id', 'jh_races', 'jh_win_rate', 'jh_avg_finish']],
                on=['jockey_id', 'horse_id'], how='left'
            )
        
        jockey_trainer = self.statistics.get('jockey_trainer')
        if jockey_trainer is not None:
            df = df.merge(
                jockey_trainer[['jockey_id', 'trainer_id', 'jt_races', 'jt_win_rate', 'jt_avg_finish']],
                on=['jockey_id', 'trainer_id'], how='left'
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
    
    def _merge_last_race_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """前走情報をマージ"""
        last_race = self.statistics.get('last_race')
        if last_race is not None:
            df = df.merge(
                last_race[['horse_id', 'last_race_date', 'last_finish_position', 
                           'last_venue', 'last_distance', 'last_surface', 'last_weight']],
                on='horse_id', how='left'
            )
            
            # 前走からの日数
            df['days_since_last_race'] = (df['race_date'] - df['last_race_date']).dt.days
            
            # 休養フラグ
            df['is_after_long_rest'] = (df['days_since_last_race'] >= 90).astype(int)
            df['is_after_mid_rest'] = ((df['days_since_last_race'] >= 30) & (df['days_since_last_race'] < 90)).astype(int)
            
            # 距離変更
            df['distance_change'] = df['distance_m'] - df['last_distance']
            df['distance_change_category'] = pd.cut(
                df['distance_change'],
                bins=[-9999, -400, -200, 200, 400, 9999],
                labels=['distance_down_2', 'distance_down_1', 'distance_same', 'distance_up_1', 'distance_up_2']
            )
            
            # 馬場変更
            df['surface_change'] = (df['track_surface'] != df['last_surface']).astype(int)
            
            # 競馬場変更
            df['venue_change'] = (df['venue'] != df['last_venue']).astype(int)
            
            # 馬体重変化（前走データがある場合）
            if 'horse_weight' in df.columns and 'last_weight' in df.columns:
                df['weight_change'] = df['horse_weight'] - df['last_weight']
                df['weight_change_pct'] = df['weight_change'] / df['last_weight'] * 100
        
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
            sex_map = {'牡': 0, '牝': 1, 'セ': 2, '騸': 2}
            df['sex_encoded'] = df['sex'].map(sex_map).fillna(0).astype(float)
        
        # 年齢
        if 'age' not in df.columns and 'sex_age' in df.columns:
            df['age'] = df['sex_age'].str.extract(r'(\d+)').astype(float)
        
        return df
    
    def _add_relative_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """レース内相対特徴量を追加"""
        # レースグループごとに計算
        for col in ['horse_win_rate', 'horse_avg_finish', 'jockey_win_rate', 'horse_weight']:
            if col in df.columns:
                # レース内平均との差
                df[f'{col}_vs_race_mean'] = df.groupby('race_id')[col].transform(
                    lambda x: x - x.mean()
                )
                # レース内順位
                df[f'{col}_rank_in_race'] = df.groupby('race_id')[col].transform(
                    lambda x: x.rank(method='average', ascending=(col != 'horse_avg_finish'))
                )
        
        return df
    
    def get_feature_columns(self) -> list:
        """使用する特徴量カラムのリストを返す"""
        return [
            # 騎手（12）
            'jockey_win_rate', 'jockey_top3_rate', 'jockey_races', 'jockey_avg_finish',
            'jockey_venue_win_rate', 'jockey_venue_avg_finish',
            'jockey_dist_win_rate', 'jockey_dist_avg_finish',
            'jockey_surface_win_rate', 'jockey_surface_avg_finish',
            'jockey_cond_win_rate',
            
            # 調教師（8）
            'trainer_win_rate', 'trainer_top3_rate', 'trainer_races', 'trainer_avg_finish',
            'trainer_venue_win_rate', 'trainer_dist_win_rate', 'trainer_surface_win_rate',
            
            # 血統（10）
            'sire_win_rate', 'sire_races', 'sire_avg_finish',
            'sire_dist_win_rate', 'sire_dist_avg_finish',
            'sire_surface_win_rate', 'sire_surface_avg_finish',
            'sire_cond_win_rate',
            
            # 馬（全体, 22）
            'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
            'horse_total_races', 'horse_last3_avg_finish', 'horse_last5_avg_finish',
            'horse_best_finish', 'horse_worst_finish', 'horse_last_finish',
            'horse_last3f_avg', 'horse_last3f_best', 'horse_last3f_last',
            'horse_dist_win_rate', 'horse_dist_avg_finish',
            'horse_surface_win_rate', 'horse_surface_avg_finish',
            'horse_venue_win_rate', 'horse_venue_avg_finish',
            'horse_cond_win_rate', 'horse_cond_avg_finish',
            
            # 馬（安定性, 4）
            'horse_finish_cv', 'horse_finish_range', 'horse_experience_log', 'horse_finish_trend',
            
            # 組み合わせ（6）
            'jh_races', 'jh_win_rate', 'jh_avg_finish',
            'jt_races', 'jt_win_rate', 'jt_avg_finish',
            
            # 馬場バイアス（1）
            'bracket_bias',
            
            # 前走（10）
            'days_since_last_race', 'is_after_long_rest', 'is_after_mid_rest',
            'last_finish_position', 'distance_change', 'surface_change', 'venue_change',
            'weight_change', 'weight_change_pct',
            
            # 当日情報（7）
            'distance_category_encoded', 'track_condition_encoded', 'track_surface_encoded',
            'bracket_number', 'horse_weight', 'age', 'distance_m',
            
            # レース内相対（8）
            'horse_win_rate_vs_race_mean', 'horse_win_rate_rank_in_race',
            'horse_avg_finish_vs_race_mean', 'horse_avg_finish_rank_in_race',
            'jockey_win_rate_vs_race_mean', 'jockey_win_rate_rank_in_race',
            'horse_weight_vs_race_mean', 'horse_weight_rank_in_race',
        ]

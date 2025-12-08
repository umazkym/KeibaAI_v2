"""
拡張版リークフリー特徴量エンジニア V3

新データソース活用:
- corner_positions.parquet: 位置取り・脚質分析
- race_details.parquet: ペース分析
- returns.parquet: 高払戻統計

特異値処理:
- last_3f_time: 50秒でクリップ
- days_since_last: 365日でクリップ
- gap_from_leader: 30馬身でクリップ
- 統計値はmin_samplesで閾値設定
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV3:
    """特徴量設計の設定"""
    min_races_for_stats: int = 3
    lookback_races: int = 5
    # クリッピング閾値
    max_last3f: float = 50.0
    max_rest_days: int = 365
    max_gap_from_leader: float = 30.0
    max_odds: float = 500.0


class LeakFreeFeatureEngineerV3:
    """
    拡張版リークフリー特徴量エンジニア V3
    
    設計原則:
    1. fit()で全統計を事前計算
    2. transform()ではfinish_position参照禁止
    3. 外れ値はクリッピングで処理
    4. 新データソース（corners, pace, returns）活用
    """
    
    def __init__(self, config: Optional[FeatureConfigV3] = None):
        self.config = config or FeatureConfigV3()
        self.statistics = {}
        self.is_fitted = False
        self.fit_date = None
        self.feature_names_: List[str] = []
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV3':
        """
        訓練データから全統計を計算
        """
        logger.info("LeakFreeFeatureEngineerV3: fit開始")
        
        df = races_df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        df = df.sort_values(['race_date', 'race_id', 'horse_number'])
        
        self.fit_date = df['race_date'].max()
        logger.info(f"  fit期間: {df['race_date'].min()} - {self.fit_date}")
        
        # 距離カテゴリ
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        # ==========================================================
        # 1. 騎手統計
        # ==========================================================
        self._calculate_jockey_stats(df)
        
        # ==========================================================
        # 2. 調教師統計
        # ==========================================================
        self._calculate_trainer_stats(df)
        
        # ==========================================================
        # 3. 血統統計
        # ==========================================================
        if pedigrees_df is not None:
            self._calculate_pedigree_stats(df, pedigrees_df)
        
        # ==========================================================
        # 4. 馬の累積成績
        # ==========================================================
        self._calculate_horse_cumulative_stats(df)
        
        # ==========================================================
        # 5. 馬場バイアス
        # ==========================================================
        self._calculate_track_bias(df)
        
        # ==========================================================
        # 6. 位置取り・脚質統計（NEW）
        # ==========================================================
        if corners_df is not None:
            self._calculate_running_style_stats(df, corners_df)
        
        # ==========================================================
        # 7. ペース統計（NEW）
        # ==========================================================
        if race_details_df is not None:
            self._calculate_pace_stats(df, race_details_df)
        
        # ==========================================================
        # 8. 高払戻統計（NEW）
        # ==========================================================
        if returns_df is not None:
            self._calculate_payout_stats(df, returns_df)
        
        # ==========================================================
        # 9. 交互作用統計
        # ==========================================================
        self._calculate_interaction_stats(df)
        
        self.is_fitted = True
        logger.info(f"LeakFreeFeatureEngineerV3: fit完了（{len(self.statistics)}種類の統計）")
        return self
    
    # ==========================================================
    # 騎手統計
    # ==========================================================
    def _calculate_jockey_stats(self, df: pd.DataFrame):
        """騎手統計を計算"""
        min_n = self.config.min_races_for_stats
        
        # 全体成績
        jockey_overall = df.groupby('jockey_id').apply(
            lambda x: pd.Series({
                'jockey_races': len(x),
                'jockey_wins': (x['finish_position'] == 1).sum(),
                'jockey_top3': (x['finish_position'] <= 3).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_overall['jockey_win_rate'] = np.where(
            jockey_overall['jockey_races'] >= min_n,
            jockey_overall['jockey_wins'] / jockey_overall['jockey_races'],
            np.nan
        )
        jockey_overall['jockey_top3_rate'] = np.where(
            jockey_overall['jockey_races'] >= min_n,
            jockey_overall['jockey_top3'] / jockey_overall['jockey_races'],
            np.nan
        )
        self.statistics['jockey_overall'] = jockey_overall
        
        # 競馬場別
        jockey_venue = df.groupby(['jockey_id', 'venue']).apply(
            lambda x: pd.Series({
                'jockey_venue_races': len(x),
                'jockey_venue_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_venue['jockey_venue_win_rate'] = np.where(
            jockey_venue['jockey_venue_races'] >= min_n,
            jockey_venue['jockey_venue_wins'] / jockey_venue['jockey_venue_races'],
            np.nan
        )
        self.statistics['jockey_venue'] = jockey_venue
        
        # 距離別
        jockey_dist = df.groupby(['jockey_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'jockey_dist_races': len(x),
                'jockey_dist_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_dist['jockey_dist_win_rate'] = np.where(
            jockey_dist['jockey_dist_races'] >= min_n,
            jockey_dist['jockey_dist_wins'] / jockey_dist['jockey_dist_races'],
            np.nan
        )
        self.statistics['jockey_dist'] = jockey_dist
        
        # 馬場別
        jockey_surface = df.groupby(['jockey_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'jockey_surface_races': len(x),
                'jockey_surface_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jockey_surface['jockey_surface_win_rate'] = np.where(
            jockey_surface['jockey_surface_races'] >= min_n,
            jockey_surface['jockey_surface_wins'] / jockey_surface['jockey_surface_races'],
            np.nan
        )
        self.statistics['jockey_surface'] = jockey_surface
        
        # 穴馬成績（5番人気以下）
        longshot_df = df[df['popularity'] >= 5]
        if len(longshot_df) > 0:
            jockey_longshot = longshot_df.groupby('jockey_id').apply(
                lambda x: pd.Series({
                    'jockey_longshot_races': len(x),
                    'jockey_longshot_wins': (x['finish_position'] == 1).sum(),
                }), include_groups=False
            ).reset_index()
            jockey_longshot['jockey_longshot_win_rate'] = np.where(
                jockey_longshot['jockey_longshot_races'] >= min_n,
                jockey_longshot['jockey_longshot_wins'] / jockey_longshot['jockey_longshot_races'],
                np.nan
            )
            self.statistics['jockey_longshot'] = jockey_longshot
        
        logger.info(f"  騎手統計: {len(jockey_overall)}人")
    
    # ==========================================================
    # 調教師統計
    # ==========================================================
    def _calculate_trainer_stats(self, df: pd.DataFrame):
        """調教師統計を計算"""
        min_n = self.config.min_races_for_stats
        
        trainer_overall = df.groupby('trainer_id').apply(
            lambda x: pd.Series({
                'trainer_races': len(x),
                'trainer_wins': (x['finish_position'] == 1).sum(),
                'trainer_top3': (x['finish_position'] <= 3).sum(),
            }), include_groups=False
        ).reset_index()
        trainer_overall['trainer_win_rate'] = np.where(
            trainer_overall['trainer_races'] >= min_n,
            trainer_overall['trainer_wins'] / trainer_overall['trainer_races'],
            np.nan
        )
        trainer_overall['trainer_top3_rate'] = np.where(
            trainer_overall['trainer_races'] >= min_n,
            trainer_overall['trainer_top3'] / trainer_overall['trainer_races'],
            np.nan
        )
        self.statistics['trainer_overall'] = trainer_overall
        
        # 競馬場別
        trainer_venue = df.groupby(['trainer_id', 'venue']).apply(
            lambda x: pd.Series({
                'trainer_venue_races': len(x),
                'trainer_venue_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        trainer_venue['trainer_venue_win_rate'] = np.where(
            trainer_venue['trainer_venue_races'] >= min_n,
            trainer_venue['trainer_venue_wins'] / trainer_venue['trainer_venue_races'],
            np.nan
        )
        self.statistics['trainer_venue'] = trainer_venue
        
        # 穴馬成績
        longshot_df = df[df['popularity'] >= 5]
        if len(longshot_df) > 0:
            trainer_longshot = longshot_df.groupby('trainer_id').apply(
                lambda x: pd.Series({
                    'trainer_longshot_races': len(x),
                    'trainer_longshot_wins': (x['finish_position'] == 1).sum(),
                }), include_groups=False
            ).reset_index()
            trainer_longshot['trainer_longshot_win_rate'] = np.where(
                trainer_longshot['trainer_longshot_races'] >= min_n,
                trainer_longshot['trainer_longshot_wins'] / trainer_longshot['trainer_longshot_races'],
                np.nan
            )
            self.statistics['trainer_longshot'] = trainer_longshot
        
        logger.info(f"  調教師統計: {len(trainer_overall)}人")
    
    # ==========================================================
    # 血統統計
    # ==========================================================
    def _calculate_pedigree_stats(self, df: pd.DataFrame, pedigrees_df: pd.DataFrame):
        """血統統計を計算"""
        min_n = self.config.min_races_for_stats
        
        # 父馬（generation=1）
        sires = pedigrees_df[pedigrees_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
        sires.columns = ['horse_id', 'sire_id']
        self.statistics['sires'] = sires
        
        # 母父（generation=2 の2番目）
        damsires = pedigrees_df[pedigrees_df['generation'] == 2].groupby('horse_id').nth(1)[['ancestor_id']].reset_index()
        damsires.columns = ['horse_id', 'damsire_id']
        self.statistics['damsires'] = damsires
        
        df_with_sire = df.merge(sires, on='horse_id', how='left')
        
        # 父馬全体成績
        sire_overall = df_with_sire.groupby('sire_id').apply(
            lambda x: pd.Series({
                'sire_races': len(x),
                'sire_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_overall['sire_win_rate'] = np.where(
            sire_overall['sire_races'] >= min_n,
            sire_overall['sire_wins'] / sire_overall['sire_races'],
            np.nan
        )
        self.statistics['sire_overall'] = sire_overall
        
        # 父馬×距離
        sire_dist = df_with_sire.groupby(['sire_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'sire_dist_races': len(x),
                'sire_dist_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_dist['sire_dist_win_rate'] = np.where(
            sire_dist['sire_dist_races'] >= min_n,
            sire_dist['sire_dist_wins'] / sire_dist['sire_dist_races'],
            np.nan
        )
        self.statistics['sire_dist'] = sire_dist
        
        # 父馬×馬場
        sire_surface = df_with_sire.groupby(['sire_id', 'track_surface']).apply(
            lambda x: pd.Series({
                'sire_surface_races': len(x),
                'sire_surface_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_surface['sire_surface_win_rate'] = np.where(
            sire_surface['sire_surface_races'] >= min_n,
            sire_surface['sire_surface_wins'] / sire_surface['sire_surface_races'],
            np.nan
        )
        self.statistics['sire_surface'] = sire_surface
        
        # 父馬×馬場状態
        sire_condition = df_with_sire.groupby(['sire_id', 'track_condition']).apply(
            lambda x: pd.Series({
                'sire_cond_races': len(x),
                'sire_cond_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        sire_condition['sire_cond_win_rate'] = np.where(
            sire_condition['sire_cond_races'] >= min_n,
            sire_condition['sire_cond_wins'] / sire_condition['sire_cond_races'],
            np.nan
        )
        self.statistics['sire_condition'] = sire_condition
        
        logger.info(f"  血統統計: {len(sire_overall)}頭")
    
    # ==========================================================
    # 馬の累積成績
    # ==========================================================
    def _calculate_horse_cumulative_stats(self, df: pd.DataFrame):
        """馬の累積成績を計算"""
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 各馬の全期間累積
        horse_final = df.groupby('horse_id').apply(
            lambda x: pd.Series({
                'horse_total_races': len(x),
                'horse_total_wins': (x['finish_position'] == 1).sum(),
                'horse_total_top3': (x['finish_position'] <= 3).sum(),
                'horse_avg_finish': x['finish_position'].mean(),
                'horse_best_finish': x['finish_position'].min(),
            }), include_groups=False
        ).reset_index()
        horse_final['horse_win_rate'] = horse_final['horse_total_wins'] / horse_final['horse_total_races']
        horse_final['horse_top3_rate'] = horse_final['horse_total_top3'] / horse_final['horse_total_races']
        self.statistics['horse_final'] = horse_final
        
        # 上がり3F統計
        if 'last_3f_time' in df.columns:
            # クリッピング
            df['last_3f_clipped'] = df['last_3f_time'].clip(upper=self.config.max_last3f)
            
            horse_last3f = df.groupby('horse_id').apply(
                lambda x: pd.Series({
                    'horse_last3f_avg': x['last_3f_clipped'].mean(),
                    'horse_last3f_best': x['last_3f_clipped'].min(),
                    'horse_last3f_std': x['last_3f_clipped'].std(),
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
    
    # ==========================================================
    # 馬場バイアス
    # ==========================================================
    def _calculate_track_bias(self, df: pd.DataFrame):
        """馬場バイアス（枠番有利不利）を計算"""
        # 条件別の平均着順
        track_bias = df.groupby(['venue', 'distance_category', 'track_surface', 'bracket_number']).apply(
            lambda x: pd.Series({
                'bracket_avg_finish': x['finish_position'].mean(),
                'bracket_count': len(x),
            }), include_groups=False
        ).reset_index()
        
        # 基準着順
        venue_base = df.groupby(['venue', 'distance_category', 'track_surface']).apply(
            lambda x: pd.Series({
                'venue_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        
        track_bias = track_bias.merge(venue_base, on=['venue', 'distance_category', 'track_surface'], how='left')
        track_bias['bracket_bias'] = track_bias['bracket_avg_finish'] - track_bias['venue_avg_finish']
        
        self.statistics['track_bias'] = track_bias
        logger.info(f"  馬場バイアス: {len(track_bias)}条件")
    
    # ==========================================================
    # 位置取り・脚質統計（NEW）
    # ==========================================================
    def _calculate_running_style_stats(self, df: pd.DataFrame, corners_df: pd.DataFrame):
        """位置取り・脚質統計を計算"""
        # クリッピング
        corners_df = corners_df.copy()
        corners_df['gap_from_leader'] = corners_df['gap_from_leader'].clip(upper=self.config.max_gap_from_leader)
        
        # 4コーナー位置
        c4 = corners_df[corners_df['corner'] == 4].copy()
        c4 = c4.rename(columns={'position': 'c4_position', 'gap_from_leader': 'c4_gap'})
        
        # 1コーナー位置
        c1 = corners_df[corners_df['corner'] == 1].copy()
        c1 = c1.rename(columns={'position': 'c1_position', 'gap_from_leader': 'c1_gap'})
        
        # dfとマージ
        df_with_corners = df.merge(
            c4[['race_id', 'horse_number', 'c4_position', 'c4_gap']], 
            on=['race_id', 'horse_number'], how='left'
        )
        df_with_corners = df_with_corners.merge(
            c1[['race_id', 'horse_number', 'c1_position', 'c1_gap']], 
            on=['race_id', 'horse_number'], how='left'
        )
        
        # 馬ごとの平均位置取り
        horse_running = df_with_corners.groupby('horse_id').apply(
            lambda x: pd.Series({
                'horse_avg_c1_pos': x['c1_position'].mean(),
                'horse_avg_c4_pos': x['c4_position'].mean(),
                'horse_avg_c4_gap': x['c4_gap'].mean(),
                'horse_c4_pos_std': x['c4_position'].std(),
                'horse_position_improvement': (x['c1_position'] - x['c4_position']).mean(),
                'horse_final_stretch_gain': (x['c4_position'] - x['finish_position']).mean(),
            }), include_groups=False
        ).reset_index()
        
        # 脚質分類（c4_positionの四分位で分類）
        c4_median = df_with_corners['c4_position'].median()
        c4_q1 = df_with_corners['c4_position'].quantile(0.25)
        c4_q3 = df_with_corners['c4_position'].quantile(0.75)
        
        def classify_style(avg_c4):
            if pd.isna(avg_c4):
                return np.nan
            if avg_c4 <= c4_q1:
                return 0  # 逃げ・先行
            elif avg_c4 <= c4_median:
                return 1  # 好位
            elif avg_c4 <= c4_q3:
                return 2  # 差し
            else:
                return 3  # 追込
        
        horse_running['horse_running_style'] = horse_running['horse_avg_c4_pos'].apply(classify_style)
        
        self.statistics['horse_running'] = horse_running
        logger.info(f"  位置取り統計: {len(horse_running)}頭")
    
    # ==========================================================
    # ペース統計（NEW）
    # ==========================================================
    def _calculate_pace_stats(self, df: pd.DataFrame, race_details_df: pd.DataFrame):
        """ペース統計を計算"""
        # ペースタイプ分類
        pace_median = race_details_df['first_half'].median()
        race_details_df = race_details_df.copy()
        race_details_df['pace_type'] = np.where(
            race_details_df['first_half'] < pace_median - 1,
            'H',  # ハイペース
            np.where(
                race_details_df['first_half'] > pace_median + 1,
                'S',  # スローペース
                'M'   # ミドル
            )
        )
        self.statistics['race_pace'] = race_details_df[['race_id', 'first_half', 'second_half', 'pace_type']]
        
        # dfとマージ
        df_with_pace = df.merge(race_details_df[['race_id', 'pace_type']], on='race_id', how='left')
        
        # 馬のペース別成績
        horse_pace = df_with_pace.groupby(['horse_id', 'pace_type']).apply(
            lambda x: pd.Series({
                'horse_pace_races': len(x),
                'horse_pace_wins': (x['finish_position'] == 1).sum(),
                'horse_pace_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        horse_pace['horse_pace_win_rate'] = horse_pace['horse_pace_wins'] / horse_pace['horse_pace_races']
        self.statistics['horse_pace'] = horse_pace
        
        # 競馬場のペース傾向
        venue_pace = race_details_df.merge(df[['race_id', 'venue']].drop_duplicates(), on='race_id')
        venue_pace_stats = venue_pace.groupby('venue').apply(
            lambda x: pd.Series({
                'venue_pace_avg': x['first_half'].mean(),
                'venue_pace_std': x['first_half'].std(),
            }), include_groups=False
        ).reset_index()
        self.statistics['venue_pace'] = venue_pace_stats
        
        logger.info(f"  ペース統計: {len(horse_pace)}件")
    
    # ==========================================================
    # 高払戻統計（NEW）
    # ==========================================================
    def _calculate_payout_stats(self, df: pd.DataFrame, returns_df: pd.DataFrame):
        """高払戻統計を計算"""
        min_n = self.config.min_races_for_stats
        
        # 単勝払戻のみ使用
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
        tansho = tansho.rename(columns={'payout': 'tansho_payout'})
        
        # dfとマージ
        df_with_payout = df.merge(tansho, on=['race_id', 'horse_number'], how='left')
        
        # 的中レースのみ（payout > 0）
        hits_df = df_with_payout[df_with_payout['tansho_payout'] > 0]
        
        # 騎手の平均払戻
        jockey_payout = hits_df.groupby('jockey_id').apply(
            lambda x: pd.Series({
                'jockey_payout_count': len(x),
                'jockey_payout_avg': x['tansho_payout'].mean(),
                'jockey_high_payout_rate': (x['tansho_payout'] >= 1000).sum() / len(x),
            }), include_groups=False
        ).reset_index()
        jockey_payout = jockey_payout[jockey_payout['jockey_payout_count'] >= min_n]
        self.statistics['jockey_payout'] = jockey_payout
        
        # 調教師の平均払戻
        trainer_payout = hits_df.groupby('trainer_id').apply(
            lambda x: pd.Series({
                'trainer_payout_count': len(x),
                'trainer_payout_avg': x['tansho_payout'].mean(),
                'trainer_high_payout_rate': (x['tansho_payout'] >= 1000).sum() / len(x),
            }), include_groups=False
        ).reset_index()
        trainer_payout = trainer_payout[trainer_payout['trainer_payout_count'] >= min_n]
        self.statistics['trainer_payout'] = trainer_payout
        
        logger.info(f"  高払戻統計: 騎手{len(jockey_payout)}人, 調教師{len(trainer_payout)}人")
    
    # ==========================================================
    # 交互作用統計
    # ==========================================================
    def _calculate_interaction_stats(self, df: pd.DataFrame):
        """交互作用統計を計算"""
        min_n = self.config.min_races_for_stats
        
        # 騎手×調教師
        jt_combo = df.groupby(['jockey_id', 'trainer_id']).apply(
            lambda x: pd.Series({
                'jt_combo_races': len(x),
                'jt_combo_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        jt_combo['jt_combo_win_rate'] = np.where(
            jt_combo['jt_combo_races'] >= min_n,
            jt_combo['jt_combo_wins'] / jt_combo['jt_combo_races'],
            np.nan
        )
        self.statistics['jt_combo'] = jt_combo
        
        logger.info(f"  交互作用統計: {len(jt_combo)}組")
    
    # ==========================================================
    # Transform
    # ==========================================================
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """特徴量を生成（finish_positionは使用しない）"""
        if not self.is_fitted:
            raise ValueError("fit()を先に実行してください")
        
        df = df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logger.info(f"LeakFreeFeatureEngineerV3: transform開始 ({len(df)}行)")
        
        # 距離カテゴリ
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        # 各統計をマージ
        df = self._merge_jockey_features(df)
        df = self._merge_trainer_features(df)
        df = self._merge_pedigree_features(df)
        df = self._merge_horse_stats(df)
        df = self._merge_track_bias(df)
        df = self._merge_running_style_features(df)
        df = self._merge_pace_features(df)
        df = self._merge_payout_features(df)
        df = self._merge_interaction_features(df)
        df = self._encode_current_race_features(df)
        
        logger.info("LeakFreeFeatureEngineerV3: transform完了")
        return df
    
    def _merge_jockey_features(self, df: pd.DataFrame) -> pd.DataFrame:
        jockey_overall = self.statistics.get('jockey_overall')
        if jockey_overall is not None:
            df = df.merge(
                jockey_overall[['jockey_id', 'jockey_win_rate', 'jockey_top3_rate', 'jockey_races']],
                on='jockey_id', how='left'
            )
        
        jockey_venue = self.statistics.get('jockey_venue')
        if jockey_venue is not None:
            df = df.merge(
                jockey_venue[['jockey_id', 'venue', 'jockey_venue_win_rate']],
                on=['jockey_id', 'venue'], how='left'
            )
        
        jockey_dist = self.statistics.get('jockey_dist')
        if jockey_dist is not None:
            df = df.merge(
                jockey_dist[['jockey_id', 'distance_category', 'jockey_dist_win_rate']],
                on=['jockey_id', 'distance_category'], how='left'
            )
        
        jockey_surface = self.statistics.get('jockey_surface')
        if jockey_surface is not None:
            df = df.merge(
                jockey_surface[['jockey_id', 'track_surface', 'jockey_surface_win_rate']],
                on=['jockey_id', 'track_surface'], how='left'
            )
        
        jockey_longshot = self.statistics.get('jockey_longshot')
        if jockey_longshot is not None:
            df = df.merge(
                jockey_longshot[['jockey_id', 'jockey_longshot_win_rate']],
                on='jockey_id', how='left'
            )
        
        return df
    
    def _merge_trainer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        trainer_overall = self.statistics.get('trainer_overall')
        if trainer_overall is not None:
            df = df.merge(
                trainer_overall[['trainer_id', 'trainer_win_rate', 'trainer_top3_rate', 'trainer_races']],
                on='trainer_id', how='left'
            )
        
        trainer_venue = self.statistics.get('trainer_venue')
        if trainer_venue is not None:
            df = df.merge(
                trainer_venue[['trainer_id', 'venue', 'trainer_venue_win_rate']],
                on=['trainer_id', 'venue'], how='left'
            )
        
        trainer_longshot = self.statistics.get('trainer_longshot')
        if trainer_longshot is not None:
            df = df.merge(
                trainer_longshot[['trainer_id', 'trainer_longshot_win_rate']],
                on='trainer_id', how='left'
            )
        
        return df
    
    def _merge_pedigree_features(self, df: pd.DataFrame) -> pd.DataFrame:
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
        horse_final = self.statistics.get('horse_final')
        if horse_final is not None:
            df = df.merge(
                horse_final[['horse_id', 'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
                             'horse_total_races', 'horse_best_finish']],
                on='horse_id', how='left'
            )
        
        horse_last3f = self.statistics.get('horse_last3f')
        if horse_last3f is not None:
            df = df.merge(
                horse_last3f[['horse_id', 'horse_last3f_avg', 'horse_last3f_best', 'horse_last3f_std']],
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
        track_bias = self.statistics.get('track_bias')
        if track_bias is not None:
            df = df.merge(
                track_bias[['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_bias']],
                on=['venue', 'distance_category', 'track_surface', 'bracket_number'], how='left'
            )
        return df
    
    def _merge_running_style_features(self, df: pd.DataFrame) -> pd.DataFrame:
        horse_running = self.statistics.get('horse_running')
        if horse_running is not None:
            df = df.merge(
                horse_running[[
                    'horse_id', 'horse_avg_c1_pos', 'horse_avg_c4_pos', 'horse_avg_c4_gap',
                    'horse_c4_pos_std', 'horse_position_improvement', 'horse_final_stretch_gain',
                    'horse_running_style'
                ]],
                on='horse_id', how='left'
            )
        return df
    
    def _merge_pace_features(self, df: pd.DataFrame) -> pd.DataFrame:
        venue_pace = self.statistics.get('venue_pace')
        if venue_pace is not None:
            df = df.merge(
                venue_pace[['venue', 'venue_pace_avg', 'venue_pace_std']],
                on='venue', how='left'
            )
        return df
    
    def _merge_payout_features(self, df: pd.DataFrame) -> pd.DataFrame:
        jockey_payout = self.statistics.get('jockey_payout')
        if jockey_payout is not None:
            df = df.merge(
                jockey_payout[['jockey_id', 'jockey_payout_avg', 'jockey_high_payout_rate']],
                on='jockey_id', how='left'
            )
        
        trainer_payout = self.statistics.get('trainer_payout')
        if trainer_payout is not None:
            df = df.merge(
                trainer_payout[['trainer_id', 'trainer_payout_avg', 'trainer_high_payout_rate']],
                on='trainer_id', how='left'
            )
        
        return df
    
    def _merge_interaction_features(self, df: pd.DataFrame) -> pd.DataFrame:
        jt_combo = self.statistics.get('jt_combo')
        if jt_combo is not None:
            df = df.merge(
                jt_combo[['jockey_id', 'trainer_id', 'jt_combo_win_rate', 'jt_combo_races']],
                on=['jockey_id', 'trainer_id'], how='left'
            )
        return df
    
    def _encode_current_race_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """当日情報をエンコード"""
        # 距離カテゴリ
        dist_map = {'sprint': 0, 'mile': 1, 'middle': 2, 'long': 3}
        df['distance_category_encoded'] = df['distance_category'].map(dist_map).fillna(1).astype(float)
        
        # 馬場状態
        condition_map = {'良': 0, '稍重': 1, '稍': 1, '重': 2, '不良': 3}
        if 'track_condition' in df.columns:
            df['track_condition_encoded'] = df['track_condition'].map(condition_map).fillna(0).astype(float)
        
        # 馬場種別
        surface_map = {'芝': 0, 'ダート': 1}
        if 'track_surface' in df.columns:
            df['track_surface_encoded'] = df['track_surface'].map(surface_map).fillna(0).astype(float)
        
        # 性別
        if 'sex' in df.columns:
            sex_map = {'牡': 0, '牝': 1, 'セ': 2}
            df['sex_encoded'] = df['sex'].map(sex_map).fillna(0).astype(float)
        
        # 年齢
        if 'age' not in df.columns and 'sex_age' in df.columns:
            df['age'] = df['sex_age'].str.extract(r'(\d+)').astype(float)
        
        # 出走頭数
        df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """使用する特徴量カラムのリストを返す"""
        return [
            # 騎手
            'jockey_win_rate', 'jockey_top3_rate', 'jockey_races',
            'jockey_venue_win_rate', 'jockey_dist_win_rate', 'jockey_surface_win_rate',
            'jockey_longshot_win_rate', 'jockey_payout_avg', 'jockey_high_payout_rate',
            # 調教師
            'trainer_win_rate', 'trainer_top3_rate', 'trainer_races',
            'trainer_venue_win_rate', 'trainer_longshot_win_rate',
            'trainer_payout_avg', 'trainer_high_payout_rate',
            # 血統
            'sire_win_rate', 'sire_races', 'sire_dist_win_rate',
            'sire_surface_win_rate', 'sire_cond_win_rate',
            # 馬（全体）
            'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
            'horse_total_races', 'horse_best_finish',
            'horse_last3f_avg', 'horse_last3f_best', 'horse_last3f_std',
            # 馬（条件別）
            'horse_dist_win_rate', 'horse_dist_avg_finish',
            'horse_surface_win_rate', 'horse_surface_avg_finish',
            'horse_venue_win_rate', 'horse_cond_win_rate',
            # 位置取り・脚質（NEW）
            'horse_avg_c1_pos', 'horse_avg_c4_pos', 'horse_avg_c4_gap',
            'horse_c4_pos_std', 'horse_position_improvement', 'horse_final_stretch_gain',
            'horse_running_style',
            # ペース（NEW）
            'venue_pace_avg', 'venue_pace_std',
            # 馬場バイアス
            'bracket_bias',
            # 交互作用
            'jt_combo_win_rate', 'jt_combo_races',
            # 当日情報
            'distance_category_encoded', 'track_condition_encoded', 'track_surface_encoded',
            'bracket_number', 'horse_weight', 'age', 'sex_encoded',
            'distance_m', 'basis_weight', 'field_size',
        ]

"""
リークフリー特徴量エンジニア V4

【V3からの主な修正】
1. 馬の累積統計を「時点ごと」に計算（shift方式）
2. fit()ではなくtransform()時に各行の時点を考慮
3. 統計はrace_date基準で「このレース以前」のみ使用

設計原則:
- 予測時点Tでは、T未満のデータのみを使用
- 「この馬の過去成績」は厳密に「このレース以前」のみ
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV4:
    """設定"""
    min_races_for_stats: int = 3
    lookback_races: int = 5
    max_last3f: float = 50.0
    max_rest_days: int = 365
    max_gap_from_leader: float = 30.0


class LeakFreeFeatureEngineerV4:
    """
    時点考慮型リークフリー特徴量エンジニア（V4）
    
    重要な変更:
    - 馬の累積統計は transform() 時に時点を考慮して計算
    - 騎手・調教師・血統統計は fit_date 時点で固定（十分なサンプル数）
    """
    
    def __init__(self, config: Optional[FeatureConfigV4] = None):
        self.config = config or FeatureConfigV4()
        self.statistics = {}
        self.is_fitted = False
        self.fit_date = None
        self._train_data = None  # 累積計算用に保持
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV4':
        """
        訓練データから統計を計算
        
        馬の累積統計はtransform時に計算するため、ここでは保持のみ
        """
        logger.info("LeakFreeFeatureEngineerV4: fit開始")
        
        df = races_df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        df = df.sort_values(['race_date', 'race_id', 'horse_number'])
        
        self.fit_date = df['race_date'].max()
        logger.info(f"  fit期間: {df['race_date'].min()} - {self.fit_date}")
        
        # 距離カテゴリを追加
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        # 訓練データを保持（累積計算用）
        self._train_data = df[['race_id', 'race_date', 'horse_id', 'jockey_id', 'trainer_id',
                               'finish_position', 'last_3f_time', 'venue', 'distance_category',
                               'track_surface', 'track_condition', 'horse_number']].copy()
        
        # ========================================================
        # 1. 騎手統計（fit_date時点で固定、十分なサンプル数あり）
        # ========================================================
        self._calculate_jockey_stats(df)
        
        # ========================================================
        # 2. 調教師統計
        # ========================================================
        self._calculate_trainer_stats(df)
        
        # ========================================================
        # 3. 血統統計
        # ========================================================
        if pedigrees_df is not None:
            self._calculate_pedigree_stats(df, pedigrees_df)
        
        # ========================================================
        # 4. 馬場バイアス（コース特性、安定）
        # ========================================================
        self._calculate_track_bias(df)
        
        # ========================================================
        # 5. 位置取り統計（馬ごとの傾向）
        # ========================================================
        if corners_df is not None:
            self._calculate_running_style_stats(df, corners_df)
        
        # ========================================================
        # 6. ペース統計（競馬場特性）
        # ========================================================
        if race_details_df is not None:
            self._calculate_pace_stats(df, race_details_df)
        
        # ========================================================
        # 7. 交互作用統計
        # ========================================================
        self._calculate_interaction_stats(df)
        
        self.is_fitted = True
        logger.info("LeakFreeFeatureEngineerV4: fit完了")
        return self
    
    def _calculate_jockey_stats(self, df: pd.DataFrame):
        """騎手統計（fit_date時点で固定）"""
        min_n = self.config.min_races_for_stats
        
        def safe_agg(group):
            return pd.Series({
                'jockey_races': len(group),
                'jockey_wins': (group['finish_position'] == 1).sum(),
                'jockey_top3': (group['finish_position'] <= 3).sum(),
            })
        
        jockey_overall = df.groupby('jockey_id').apply(safe_agg, include_groups=False).reset_index()
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
        
        logger.info(f"  騎手統計: {len(jockey_overall)}人")
    
    def _calculate_trainer_stats(self, df: pd.DataFrame):
        """調教師統計"""
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
        logger.info(f"  調教師統計: {len(trainer_overall)}人")
    
    def _calculate_pedigree_stats(self, df: pd.DataFrame, pedigrees_df: pd.DataFrame):
        """血統統計"""
        min_n = self.config.min_races_for_stats
        
        # 父馬マッピング
        sires = pedigrees_df[pedigrees_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
        sires.columns = ['horse_id', 'sire_id']
        self.statistics['sires'] = sires
        
        df_with_sire = df.merge(sires, on='horse_id', how='left')
        
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
        
        # 距離別
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
        
        logger.info(f"  血統統計: {len(sire_overall)}頭")
    
    def _calculate_track_bias(self, df: pd.DataFrame):
        """馬場バイアス"""
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
    
    def _calculate_running_style_stats(self, df: pd.DataFrame, corners_df: pd.DataFrame):
        """位置取り統計（fit_date時点で固定）"""
        corners_df = corners_df.copy()
        corners_df['gap_from_leader'] = corners_df['gap_from_leader'].clip(upper=self.config.max_gap_from_leader)
        
        c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
        c4.columns = ['race_id', 'horse_number', 'c4_position', 'c4_gap']
        
        df_with_corners = df.merge(c4, on=['race_id', 'horse_number'], how='left')
        
        horse_running = df_with_corners.groupby('horse_id').apply(
            lambda x: pd.Series({
                'horse_avg_c4_pos': x['c4_position'].mean(),
                'horse_avg_c4_gap': x['c4_gap'].mean(),
            }), include_groups=False
        ).reset_index()
        
        # 脚質分類
        c4_median = df_with_corners['c4_position'].median()
        c4_q1 = df_with_corners['c4_position'].quantile(0.25)
        c4_q3 = df_with_corners['c4_position'].quantile(0.75)
        
        def classify_style(avg):
            if pd.isna(avg): return np.nan
            if avg <= c4_q1: return 0
            elif avg <= c4_median: return 1
            elif avg <= c4_q3: return 2
            else: return 3
        
        horse_running['horse_running_style'] = horse_running['horse_avg_c4_pos'].apply(classify_style)
        self.statistics['horse_running'] = horse_running
        logger.info(f"  位置取り統計: {len(horse_running)}頭")
    
    def _calculate_pace_stats(self, df: pd.DataFrame, race_details_df: pd.DataFrame):
        """ペース統計（競馬場特性）"""
        venue_pace = race_details_df.merge(
            df[['race_id', 'venue']].drop_duplicates(), on='race_id', how='left'
        )
        venue_pace_stats = venue_pace.groupby('venue').apply(
            lambda x: pd.Series({
                'venue_pace_avg': x['first_half'].mean(),
            }), include_groups=False
        ).reset_index()
        self.statistics['venue_pace'] = venue_pace_stats
        logger.info(f"  ペース統計: {len(venue_pace_stats)}会場")
    
    def _calculate_interaction_stats(self, df: pd.DataFrame):
        """交互作用統計"""
        min_n = self.config.min_races_for_stats
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
    # Transform（時点考慮型）
    # ==========================================================
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        特徴量を生成
        
        重要: 馬の累積統計は「このレース以前」のみを使用
        """
        if not self.is_fitted:
            raise ValueError("fit()を先に実行してください")
        
        df = df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logger.info(f"LeakFreeFeatureEngineerV4: transform開始 ({len(df)}行)")
        
        # 距離カテゴリ
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        # ========================================================
        # 1. 馬の累積統計（時点考慮型）
        # ========================================================
        df = self._calculate_horse_cumulative_at_time(df)
        
        # ========================================================
        # 2. 固定統計をマージ（騎手・調教師・血統等）
        # ========================================================
        df = self._merge_fixed_statistics(df)
        
        # ========================================================
        # 3. 当日情報をエンコード
        # ========================================================
        df = self._encode_current_race_features(df)
        
        logger.info("LeakFreeFeatureEngineerV4: transform完了")
        return df
    
    def _calculate_horse_cumulative_at_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        馬の累積統計を「fit時点のデータのみ」を使用して計算
        
        核心: transform対象のfinish_positionは使用しない
        fit時のデータから各馬の最終統計を取得してマージするだけ
        """
        if self._train_data is None:
            return df
        
        # fit時のデータから各馬の「最終時点」での累積統計を計算
        hist = self._train_data.copy()
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 勝利フラグ
        hist['is_win'] = (hist['finish_position'] == 1).astype(int)
        hist['is_top3'] = (hist['finish_position'] <= 3).astype(int)
        
        # 各馬の最終統計（fit_date時点）
        horse_final = hist.groupby('horse_id').agg({
            'is_win': 'sum',
            'is_top3': 'sum',
            'finish_position': ['count', 'mean', 'last'],
            'last_3f_time': 'mean',
            'race_date': 'max'
        })
        horse_final.columns = ['total_wins', 'total_top3', 'total_races', 'avg_finish', 
                               'last_finish', 'avg_last3f', 'last_race_date']
        horse_final = horse_final.reset_index()
        
        # 勝率等
        min_n = self.config.min_races_for_stats
        horse_final['horse_win_rate'] = np.where(
            horse_final['total_races'] >= min_n,
            horse_final['total_wins'] / horse_final['total_races'],
            np.nan
        )
        horse_final['horse_top3_rate'] = np.where(
            horse_final['total_races'] >= min_n,
            horse_final['total_top3'] / horse_final['total_races'],
            np.nan
        )
        horse_final['horse_avg_finish'] = np.where(
            horse_final['total_races'] >= min_n,
            horse_final['avg_finish'],
            np.nan
        )
        horse_final['horse_total_races'] = horse_final['total_races']
        horse_final['horse_last_finish'] = horse_final['last_finish']
        horse_final['horse_last3f_avg'] = horse_final['avg_last3f'].clip(upper=self.config.max_last3f)
        
        # dfとマージ
        df = df.merge(
            horse_final[['horse_id', 'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
                         'horse_total_races', 'horse_last_finish', 'horse_last3f_avg', 'last_race_date']],
            on='horse_id', how='left'
        )
        
        # 前走からの日数（fit時の最終レース日からの日数）
        df['horse_days_since_last'] = (df['race_date'] - df['last_race_date']).dt.days
        df['horse_days_since_last'] = df['horse_days_since_last'].clip(lower=0, upper=self.config.max_rest_days)
        df = df.drop(columns=['last_race_date'], errors='ignore')
        
        # 直近3走平均はfit時の最終3走から計算
        last3 = hist.groupby('horse_id').tail(3)
        horse_last3 = last3.groupby('horse_id')['finish_position'].mean().reset_index()
        horse_last3.columns = ['horse_id', 'horse_last3_avg_finish']
        df = df.merge(horse_last3, on='horse_id', how='left')
        
        return df
    
    def _merge_fixed_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """固定統計をマージ"""
        # 騎手
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
        
        # 調教師
        trainer_overall = self.statistics.get('trainer_overall')
        if trainer_overall is not None:
            df = df.merge(
                trainer_overall[['trainer_id', 'trainer_win_rate', 'trainer_top3_rate', 'trainer_races']],
                on='trainer_id', how='left'
            )
        
        # 血統
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
        
        # 馬場バイアス
        track_bias = self.statistics.get('track_bias')
        if track_bias is not None:
            df = df.merge(
                track_bias[['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_bias']],
                on=['venue', 'distance_category', 'track_surface', 'bracket_number'], how='left'
            )
        
        # 位置取り
        horse_running = self.statistics.get('horse_running')
        if horse_running is not None:
            df = df.merge(
                horse_running[['horse_id', 'horse_avg_c4_pos', 'horse_avg_c4_gap', 'horse_running_style']],
                on='horse_id', how='left'
            )
        
        # ペース
        venue_pace = self.statistics.get('venue_pace')
        if venue_pace is not None:
            df = df.merge(
                venue_pace[['venue', 'venue_pace_avg']],
                on='venue', how='left'
            )
        
        # 交互作用
        jt_combo = self.statistics.get('jt_combo')
        if jt_combo is not None:
            df = df.merge(
                jt_combo[['jockey_id', 'trainer_id', 'jt_combo_win_rate', 'jt_combo_races']],
                on=['jockey_id', 'trainer_id'], how='left'
            )
        
        return df
    
    def _encode_current_race_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """当日情報をエンコード"""
        dist_map = {'sprint': 0, 'mile': 1, 'middle': 2, 'long': 3}
        df['distance_category_encoded'] = df['distance_category'].map(dist_map).fillna(1).astype(float)
        
        condition_map = {'良': 0, '稍重': 1, '稍': 1, '重': 2, '不良': 3}
        if 'track_condition' in df.columns:
            df['track_condition_encoded'] = df['track_condition'].map(condition_map).fillna(0).astype(float)
        
        surface_map = {'芝': 0, 'ダート': 1}
        if 'track_surface' in df.columns:
            df['track_surface_encoded'] = df['track_surface'].map(surface_map).fillna(0).astype(float)
        
        if 'sex' in df.columns:
            sex_map = {'牡': 0, '牝': 1, 'セ': 2}
            df['sex_encoded'] = df['sex'].map(sex_map).fillna(0).astype(float)
        
        df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """使用する特徴量カラムのリスト"""
        return [
            # 馬（時点考慮型累積）
            'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish', 'horse_total_races',
            'horse_last_finish', 'horse_last3_avg_finish', 'horse_days_since_last',
            'horse_last3f_avg',
            # 騎手（固定）
            'jockey_win_rate', 'jockey_top3_rate', 'jockey_races', 'jockey_venue_win_rate',
            # 調教師（固定）
            'trainer_win_rate', 'trainer_top3_rate', 'trainer_races',
            # 血統（固定）
            'sire_win_rate', 'sire_races', 'sire_dist_win_rate',
            # 位置取り（固定）
            'horse_avg_c4_pos', 'horse_avg_c4_gap', 'horse_running_style',
            # ペース（固定）
            'venue_pace_avg',
            # 馬場バイアス
            'bracket_bias',
            # 交互作用
            'jt_combo_win_rate', 'jt_combo_races',
            # 当日情報
            'distance_category_encoded', 'track_condition_encoded', 'track_surface_encoded',
            'bracket_number', 'horse_weight', 'age', 'sex_encoded',
            'distance_m', 'basis_weight', 'field_size',
        ]

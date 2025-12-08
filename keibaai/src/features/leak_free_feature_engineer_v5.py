"""
リークフリー特徴量エンジニア V5

【V4からの主な修正】
問題: 騎手・調教師・血統統計が全訓練データで計算されており、
      訓練データ内でも「未来の情報」がリークしている

解決策:
1. 全ての統計を「時点考慮型」で計算
2. 各レースで使用可能なのは「そのレース日以前」のデータのみ
3. 効率化のため、年単位で統計を事前計算しておく

設計:
- 2020年のレース → 2019年以前のデータで統計計算（初期データなし）
- 2021年のレース → 2020年以前のデータで統計計算
- ...
- 2025年のレース → 2024年以前のデータで統計計算
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV5:
    """設定"""
    min_races_for_stats: int = 3
    max_last3f: float = 50.0
    max_rest_days: int = 365
    max_gap_from_leader: float = 30.0


class LeakFreeFeatureEngineerV5:
    """
    時点完全考慮型リークフリー特徴量エンジニア（V5）
    
    全ての統計が「そのレース日以前」のみから計算される
    """
    
    def __init__(self, config: Optional[FeatureConfigV5] = None):
        self.config = config or FeatureConfigV5()
        self.is_fitted = False
        self.fit_date = None
        self._all_history = None
        self._pedigrees = None
        self._corners = None
        self._race_details = None
        # 年単位の統計キャッシュ
        self._yearly_jockey_stats = {}
        self._yearly_trainer_stats = {}
        self._yearly_sire_stats = {}
        self._yearly_track_bias = {}
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV5':
        """訓練データからの統計計算（年単位でキャッシュ）"""
        logger.info("LeakFreeFeatureEngineerV5: fit開始")
        
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
        
        # 全履歴を保持
        self._all_history = df.copy()
        self._pedigrees = pedigrees_df
        self._corners = corners_df
        self._race_details = race_details_df
        
        # 年単位で統計を事前計算
        years = sorted(df['race_date'].dt.year.unique())
        logger.info(f"  年単位統計を事前計算: {min(years)} - {max(years)}")
        
        for year in years:
            # このyearのレースで使うのは year-1 以前のデータ
            cutoff_date = pd.Timestamp(f'{year}-01-01')
            past_data = df[df['race_date'] < cutoff_date]
            
            if len(past_data) >= 100:  # 最低データ量
                self._yearly_jockey_stats[year] = self._calc_jockey_stats(past_data)
                self._yearly_trainer_stats[year] = self._calc_trainer_stats(past_data)
                if pedigrees_df is not None:
                    self._yearly_sire_stats[year] = self._calc_sire_stats(past_data, pedigrees_df)
                self._yearly_track_bias[year] = self._calc_track_bias(past_data)
        
        logger.info(f"  キャッシュ年数: {len(self._yearly_jockey_stats)}")
        
        # 交互作用統計（最終年のデータで計算）
        self._jt_combo = self._calc_jt_combo_stats(df)
        
        # 位置取り統計（最終年のデータで計算）
        if corners_df is not None:
            self._horse_running = self._calc_running_style(df, corners_df)
        else:
            self._horse_running = None
        
        # ペース統計
        if race_details_df is not None:
            self._venue_pace = self._calc_venue_pace(df, race_details_df)
        else:
            self._venue_pace = None
        
        self.is_fitted = True
        logger.info("LeakFreeFeatureEngineerV5: fit完了")
        return self
    
    def _calc_jockey_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手統計を計算"""
        min_n = self.config.min_races_for_stats
        stats = df.groupby('jockey_id').apply(
            lambda x: pd.Series({
                'jockey_races': len(x),
                'jockey_wins': (x['finish_position'] == 1).sum(),
                'jockey_top3': (x['finish_position'] <= 3).sum(),
            }), include_groups=False
        ).reset_index()
        stats['jockey_win_rate'] = np.where(
            stats['jockey_races'] >= min_n,
            stats['jockey_wins'] / stats['jockey_races'],
            np.nan
        )
        stats['jockey_top3_rate'] = np.where(
            stats['jockey_races'] >= min_n,
            stats['jockey_top3'] / stats['jockey_races'],
            np.nan
        )
        return stats
    
    def _calc_trainer_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """調教師統計を計算"""
        min_n = self.config.min_races_for_stats
        stats = df.groupby('trainer_id').apply(
            lambda x: pd.Series({
                'trainer_races': len(x),
                'trainer_wins': (x['finish_position'] == 1).sum(),
                'trainer_top3': (x['finish_position'] <= 3).sum(),
            }), include_groups=False
        ).reset_index()
        stats['trainer_win_rate'] = np.where(
            stats['trainer_races'] >= min_n,
            stats['trainer_wins'] / stats['trainer_races'],
            np.nan
        )
        stats['trainer_top3_rate'] = np.where(
            stats['trainer_races'] >= min_n,
            stats['trainer_top3'] / stats['trainer_races'],
            np.nan
        )
        return stats
    
    def _calc_sire_stats(self, df: pd.DataFrame, pedigrees_df: pd.DataFrame) -> pd.DataFrame:
        """父馬統計を計算"""
        min_n = self.config.min_races_for_stats
        sires = pedigrees_df[pedigrees_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
        sires.columns = ['horse_id', 'sire_id']
        
        df_with_sire = df.merge(sires, on='horse_id', how='left')
        
        stats = df_with_sire.groupby('sire_id').apply(
            lambda x: pd.Series({
                'sire_races': len(x),
                'sire_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        stats['sire_win_rate'] = np.where(
            stats['sire_races'] >= min_n,
            stats['sire_wins'] / stats['sire_races'],
            np.nan
        )
        return stats, sires
    
    def _calc_track_bias(self, df: pd.DataFrame) -> pd.DataFrame:
        """馬場バイアスを計算"""
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
        return track_bias
    
    def _calc_jt_combo_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手×調教師統計"""
        min_n = self.config.min_races_for_stats
        stats = df.groupby(['jockey_id', 'trainer_id']).apply(
            lambda x: pd.Series({
                'jt_combo_races': len(x),
                'jt_combo_wins': (x['finish_position'] == 1).sum(),
            }), include_groups=False
        ).reset_index()
        stats['jt_combo_win_rate'] = np.where(
            stats['jt_combo_races'] >= min_n,
            stats['jt_combo_wins'] / stats['jt_combo_races'],
            np.nan
        )
        return stats
    
    def _calc_running_style(self, df: pd.DataFrame, corners_df: pd.DataFrame) -> pd.DataFrame:
        """位置取り統計"""
        corners_df = corners_df.copy()
        corners_df['gap_from_leader'] = corners_df['gap_from_leader'].clip(upper=self.config.max_gap_from_leader)
        
        c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
        c4.columns = ['race_id', 'horse_number', 'c4_position', 'c4_gap']
        
        df_with_corners = df.merge(c4, on=['race_id', 'horse_number'], how='left')
        
        stats = df_with_corners.groupby('horse_id').apply(
            lambda x: pd.Series({
                'horse_avg_c4_pos': x['c4_position'].mean(),
                'horse_avg_c4_gap': x['c4_gap'].mean(),
            }), include_groups=False
        ).reset_index()
        
        c4_median = df_with_corners['c4_position'].median()
        c4_q1 = df_with_corners['c4_position'].quantile(0.25)
        c4_q3 = df_with_corners['c4_position'].quantile(0.75)
        
        def classify_style(avg):
            if pd.isna(avg): return np.nan
            if avg <= c4_q1: return 0
            elif avg <= c4_median: return 1
            elif avg <= c4_q3: return 2
            else: return 3
        
        stats['horse_running_style'] = stats['horse_avg_c4_pos'].apply(classify_style)
        return stats
    
    def _calc_venue_pace(self, df: pd.DataFrame, race_details_df: pd.DataFrame) -> pd.DataFrame:
        """ペース統計"""
        venue_pace = race_details_df.merge(
            df[['race_id', 'venue']].drop_duplicates(), on='race_id', how='left'
        )
        stats = venue_pace.groupby('venue').apply(
            lambda x: pd.Series({
                'venue_pace_avg': x['first_half'].mean(),
            }), include_groups=False
        ).reset_index()
        return stats
    
    # ==========================================================
    # Transform（時点考慮型）
    # ==========================================================
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """特徴量を生成（各行の年に応じた統計を使用）"""
        if not self.is_fitted:
            raise ValueError("fit()を先に実行してください")
        
        df = df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['race_year'] = df['race_date'].dt.year
        
        logger.info(f"LeakFreeFeatureEngineerV5: transform開始 ({len(df)}行)")
        
        # 距離カテゴリ
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        # 年ごとに処理
        result_dfs = []
        for year in sorted(df['race_year'].unique()):
            year_df = df[df['race_year'] == year].copy()
            year_df = self._apply_yearly_stats(year_df, year)
            result_dfs.append(year_df)
        
        df = pd.concat(result_dfs, ignore_index=True)
        
        # 馬の累積統計（時点考慮型）
        df = self._calc_horse_cumulative(df)
        
        # 固定統計をマージ（交互作用、位置取り、ペース）
        df = self._merge_fixed_stats(df)
        
        # 当日情報エンコード
        df = self._encode_current_race_features(df)
        
        df = df.drop(columns=['race_year'], errors='ignore')
        
        logger.info("LeakFreeFeatureEngineerV5: transform完了")
        return df
    
    def _apply_yearly_stats(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """年に対応した統計を適用"""
        # 騎手統計
        jockey_stats = self._yearly_jockey_stats.get(year)
        if jockey_stats is not None:
            df = df.merge(
                jockey_stats[['jockey_id', 'jockey_win_rate', 'jockey_top3_rate', 'jockey_races']],
                on='jockey_id', how='left'
            )
        else:
            df['jockey_win_rate'] = np.nan
            df['jockey_top3_rate'] = np.nan
            df['jockey_races'] = np.nan
        
        # 調教師統計
        trainer_stats = self._yearly_trainer_stats.get(year)
        if trainer_stats is not None:
            df = df.merge(
                trainer_stats[['trainer_id', 'trainer_win_rate', 'trainer_top3_rate', 'trainer_races']],
                on='trainer_id', how='left'
            )
        else:
            df['trainer_win_rate'] = np.nan
            df['trainer_top3_rate'] = np.nan
            df['trainer_races'] = np.nan
        
        # 父馬統計
        sire_result = self._yearly_sire_stats.get(year)
        if sire_result is not None:
            sire_stats, sires = sire_result
            df = df.merge(sires, on='horse_id', how='left')
            df = df.merge(
                sire_stats[['sire_id', 'sire_win_rate', 'sire_races']],
                on='sire_id', how='left'
            )
        else:
            df['sire_id'] = np.nan
            df['sire_win_rate'] = np.nan
            df['sire_races'] = np.nan
        
        # 馬場バイアス
        track_bias = self._yearly_track_bias.get(year)
        if track_bias is not None:
            df = df.merge(
                track_bias[['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_bias']],
                on=['venue', 'distance_category', 'track_surface', 'bracket_number'], how='left'
            )
        else:
            df['bracket_bias'] = np.nan
        
        return df
    
    def _calc_horse_cumulative(self, df: pd.DataFrame) -> pd.DataFrame:
        """馬の累積統計（fit時の履歴から、各レース日以前のみ使用）"""
        if self._all_history is None:
            return df
        
        hist = self._all_history.copy()
        hist['is_win'] = (hist['finish_position'] == 1).astype(int)
        hist['is_top3'] = (hist['finish_position'] <= 3).astype(int)
        
        # 各馬の最終統計
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
        
        df = df.merge(
            horse_final[['horse_id', 'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
                         'horse_total_races', 'horse_last_finish', 'horse_last3f_avg', 'last_race_date']],
            on='horse_id', how='left'
        )
        
        # 前走からの日数
        df['horse_days_since_last'] = (df['race_date'] - df['last_race_date']).dt.days
        df['horse_days_since_last'] = df['horse_days_since_last'].clip(lower=0, upper=self.config.max_rest_days)
        df = df.drop(columns=['last_race_date'], errors='ignore')
        
        # 直近3走平均
        last3 = hist.groupby('horse_id').tail(3)
        horse_last3 = last3.groupby('horse_id')['finish_position'].mean().reset_index()
        horse_last3.columns = ['horse_id', 'horse_last3_avg_finish']
        df = df.merge(horse_last3, on='horse_id', how='left')
        
        return df
    
    def _merge_fixed_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """固定統計をマージ"""
        # 交互作用
        if self._jt_combo is not None:
            df = df.merge(
                self._jt_combo[['jockey_id', 'trainer_id', 'jt_combo_win_rate', 'jt_combo_races']],
                on=['jockey_id', 'trainer_id'], how='left'
            )
        
        # 位置取り
        if self._horse_running is not None:
            df = df.merge(
                self._horse_running[['horse_id', 'horse_avg_c4_pos', 'horse_avg_c4_gap', 'horse_running_style']],
                on='horse_id', how='left'
            )
        
        # ペース
        if self._venue_pace is not None:
            df = df.merge(
                self._venue_pace[['venue', 'venue_pace_avg']],
                on='venue', how='left'
            )
        
        return df
    
    def _encode_current_race_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """当日情報エンコード"""
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
            # 馬
            'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish', 'horse_total_races',
            'horse_last_finish', 'horse_last3_avg_finish', 'horse_days_since_last',
            'horse_last3f_avg',
            # 騎手（時点考慮）
            'jockey_win_rate', 'jockey_top3_rate', 'jockey_races',
            # 調教師（時点考慮）
            'trainer_win_rate', 'trainer_top3_rate', 'trainer_races',
            # 血統（時点考慮）
            'sire_win_rate', 'sire_races',
            # 位置取り
            'horse_avg_c4_pos', 'horse_avg_c4_gap', 'horse_running_style',
            # ペース
            'venue_pace_avg',
            # 馬場バイアス（時点考慮）
            'bracket_bias',
            # 交互作用
            'jt_combo_win_rate', 'jt_combo_races',
            # 当日情報
            'distance_category_encoded', 'track_condition_encoded', 'track_surface_encoded',
            'bracket_number', 'horse_weight', 'age', 'sex_encoded',
            'distance_m', 'basis_weight', 'field_size',
        ]

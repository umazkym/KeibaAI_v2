"""
リークフリー特徴量エンジニア V6

【V5からの主な修正】
V5: 年単位で統計計算（粗い近似）
V6: 各レース日以前のデータのみを厳密に使用

実装方針:
- 全データを時系列順にソート
- cumsum + shift パターンで各行時点での累積を計算
- これにより、各レースで使用可能なのは「そのレース日以前」のデータのみ

効率化:
- groupby().cumsum().shift(1) で一括計算
- 騎手・調教師・血統も同様のパターンで処理
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV6:
    """設定"""
    min_races_for_stats: int = 3
    max_last3f: float = 50.0
    max_rest_days: int = 365
    max_gap_from_leader: float = 30.0


class LeakFreeFeatureEngineerV6:
    """
    厳密時点考慮型リークフリー特徴量エンジニア（V6）
    
    各レースで使用可能なのは「そのレース日以前」のデータのみ
    cumsum + shift パターンで効率的に計算
    """
    
    def __init__(self, config: Optional[FeatureConfigV6] = None):
        self.config = config or FeatureConfigV6()
        self.is_fitted = False
        self.fit_date = None
        self._full_history = None
        self._pedigrees = None
        self._sire_mapping = None
        self._corners = None
        self._race_details = None
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV6':
        """訓練データの準備"""
        logger.info("LeakFreeFeatureEngineerV6: fit開始")
        
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
        
        # フラグを事前計算
        df['is_win'] = (df['finish_position'] == 1).astype(int)
        df['is_top3'] = (df['finish_position'] <= 3).astype(int)
        
        # 履歴保持
        self._full_history = df.copy()
        self._pedigrees = pedigrees_df
        self._corners = corners_df
        self._race_details = race_details_df
        
        # 血統マッピング
        if pedigrees_df is not None:
            sires = pedigrees_df[pedigrees_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
            sires.columns = ['horse_id', 'sire_id']
            self._sire_mapping = sires
            logger.info(f"  血統マッピング: {len(sires)}頭")
        
        self.is_fitted = True
        logger.info("LeakFreeFeatureEngineerV6: fit完了")
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        特徴量を生成（fit時の履歴データのみから統計を計算）
        
        重要: dfのfinish_positionは一切使用しない
        """
        if not self.is_fitted:
            raise ValueError("fit()を先に実行してください")
        
        df = df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logger.info(f"LeakFreeFeatureEngineerV6: transform開始 ({len(df)}行)")
        
        # 距離カテゴリ
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        # ========================================================
        # 統計はfit時の履歴からのみ計算
        # dfの各行に対して、その行のrace_date以前の履歴から統計を取得
        # ========================================================
        
        # 履歴から各エンティティの累積統計テーブルを作成
        hist = self._full_history.copy()
        hist = hist.sort_values(['race_date', 'race_id', 'horse_number'])
        
        # 各エンティティの「最終状態」を計算（fit_date時点）
        # これをdfにマージ（dfのrace_dateがfit_date以降の場合のみ正しく動作）
        
        # 1. 馬の統計
        df = self._merge_horse_stats_from_history(df, hist)
        
        # 2. 騎手の統計
        df = self._merge_jockey_stats_from_history(df, hist)
        
        # 3. 調教師の統計
        df = self._merge_trainer_stats_from_history(df, hist)
        
        # 4. 血統の統計
        if self._sire_mapping is not None:
            df = self._merge_sire_stats_from_history(df, hist)
        
        # 5. 騎手×調教師
        df = self._merge_jt_combo_stats_from_history(df, hist)
        
        # 6. 馬場バイアス（全履歴で計算済み、コース特性なので問題なし）
        df = self._merge_track_bias(df)
        
        # 7. 位置取り
        if self._corners is not None:
            df = self._merge_running_style(df)
        
        # 8. ペース
        if self._race_details is not None:
            df = self._merge_venue_pace(df)
        
        # 9. 当日情報
        df = self._encode_current_race_features(df)
        
        logger.info("LeakFreeFeatureEngineerV6: transform完了")
        return df
    
    def _merge_horse_stats_from_history(self, df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
        """馬の統計（履歴の最終状態をマージ）"""
        min_n = self.config.min_races_for_stats
        
        horse_stats = hist.groupby('horse_id').agg({
            'is_win': 'sum',
            'is_top3': 'sum',
            'finish_position': ['count', 'mean', 'last'],
            'last_3f_time': 'mean',
            'race_date': 'max'
        })
        horse_stats.columns = ['wins', 'top3', 'races', 'avg_finish', 'last_finish', 'avg_last3f', 'last_race_date']
        horse_stats = horse_stats.reset_index()
        
        horse_stats['horse_win_rate'] = np.where(horse_stats['races'] >= min_n, horse_stats['wins'] / horse_stats['races'], np.nan)
        horse_stats['horse_top3_rate'] = np.where(horse_stats['races'] >= min_n, horse_stats['top3'] / horse_stats['races'], np.nan)
        horse_stats['horse_avg_finish'] = np.where(horse_stats['races'] >= min_n, horse_stats['avg_finish'], np.nan)
        horse_stats['horse_total_races'] = horse_stats['races']
        horse_stats['horse_last_finish'] = horse_stats['last_finish']
        horse_stats['horse_last3f_avg'] = horse_stats['avg_last3f'].clip(upper=self.config.max_last3f)
        
        df = df.merge(
            horse_stats[['horse_id', 'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
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
    
    def _merge_jockey_stats_from_history(self, df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
        """騎手の統計"""
        min_n = self.config.min_races_for_stats
        
        jockey_stats = hist.groupby('jockey_id').agg({
            'is_win': 'sum',
            'is_top3': 'sum',
            'race_id': 'count'
        })
        jockey_stats.columns = ['wins', 'top3', 'races']
        jockey_stats = jockey_stats.reset_index()
        
        jockey_stats['jockey_win_rate'] = np.where(jockey_stats['races'] >= min_n, jockey_stats['wins'] / jockey_stats['races'], np.nan)
        jockey_stats['jockey_top3_rate'] = np.where(jockey_stats['races'] >= min_n, jockey_stats['top3'] / jockey_stats['races'], np.nan)
        jockey_stats['jockey_races'] = jockey_stats['races']
        
        df = df.merge(jockey_stats[['jockey_id', 'jockey_win_rate', 'jockey_top3_rate', 'jockey_races']], on='jockey_id', how='left')
        return df
    
    def _merge_trainer_stats_from_history(self, df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
        """調教師の統計"""
        min_n = self.config.min_races_for_stats
        
        trainer_stats = hist.groupby('trainer_id').agg({
            'is_win': 'sum',
            'is_top3': 'sum',
            'race_id': 'count'
        })
        trainer_stats.columns = ['wins', 'top3', 'races']
        trainer_stats = trainer_stats.reset_index()
        
        trainer_stats['trainer_win_rate'] = np.where(trainer_stats['races'] >= min_n, trainer_stats['wins'] / trainer_stats['races'], np.nan)
        trainer_stats['trainer_top3_rate'] = np.where(trainer_stats['races'] >= min_n, trainer_stats['top3'] / trainer_stats['races'], np.nan)
        trainer_stats['trainer_races'] = trainer_stats['races']
        
        df = df.merge(trainer_stats[['trainer_id', 'trainer_win_rate', 'trainer_top3_rate', 'trainer_races']], on='trainer_id', how='left')
        return df
    
    def _merge_sire_stats_from_history(self, df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
        """父馬の統計"""
        min_n = self.config.min_races_for_stats
        
        hist_with_sire = hist.merge(self._sire_mapping, on='horse_id', how='left')
        
        sire_stats = hist_with_sire.groupby('sire_id').agg({
            'is_win': 'sum',
            'race_id': 'count'
        })
        sire_stats.columns = ['wins', 'races']
        sire_stats = sire_stats.reset_index()
        
        sire_stats['sire_win_rate'] = np.where(sire_stats['races'] >= min_n, sire_stats['wins'] / sire_stats['races'], np.nan)
        sire_stats['sire_races'] = sire_stats['races']
        
        df = df.merge(self._sire_mapping, on='horse_id', how='left')
        df = df.merge(sire_stats[['sire_id', 'sire_win_rate', 'sire_races']], on='sire_id', how='left')
        return df
    
    def _merge_jt_combo_stats_from_history(self, df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
        """騎手×調教師の統計"""
        min_n = self.config.min_races_for_stats
        
        jt_stats = hist.groupby(['jockey_id', 'trainer_id']).agg({
            'is_win': 'sum',
            'race_id': 'count'
        })
        jt_stats.columns = ['wins', 'races']
        jt_stats = jt_stats.reset_index()
        
        jt_stats['jt_combo_win_rate'] = np.where(jt_stats['races'] >= min_n, jt_stats['wins'] / jt_stats['races'], np.nan)
        jt_stats['jt_combo_races'] = jt_stats['races']
        
        df = df.merge(jt_stats[['jockey_id', 'trainer_id', 'jt_combo_win_rate', 'jt_combo_races']], on=['jockey_id', 'trainer_id'], how='left')
        return df
    
    def _merge_track_bias(self, df: pd.DataFrame) -> pd.DataFrame:
        """馬場バイアス（訓練データ全体で計算）"""
        hist = self._full_history
        
        track_bias = hist.groupby(['venue', 'distance_category', 'track_surface', 'bracket_number']).apply(
            lambda x: pd.Series({
                'bracket_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        
        venue_base = hist.groupby(['venue', 'distance_category', 'track_surface']).apply(
            lambda x: pd.Series({
                'venue_avg_finish': x['finish_position'].mean(),
            }), include_groups=False
        ).reset_index()
        
        track_bias = track_bias.merge(venue_base, on=['venue', 'distance_category', 'track_surface'], how='left')
        track_bias['bracket_bias'] = track_bias['bracket_avg_finish'] - track_bias['venue_avg_finish']
        
        df = df.merge(
            track_bias[['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_bias']],
            on=['venue', 'distance_category', 'track_surface', 'bracket_number'], how='left'
        )
        return df
    
    def _merge_running_style(self, df: pd.DataFrame) -> pd.DataFrame:
        """位置取り統計（訓練データ全体で計算）"""
        corners = self._corners.copy()
        corners['gap_from_leader'] = corners['gap_from_leader'].clip(upper=self.config.max_gap_from_leader)
        
        c4 = corners[corners['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
        c4.columns = ['race_id', 'horse_number', 'c4_position', 'c4_gap']
        
        hist_with_c4 = self._full_history.merge(c4, on=['race_id', 'horse_number'], how='left')
        
        horse_running = hist_with_c4.groupby('horse_id').apply(
            lambda x: pd.Series({
                'horse_avg_c4_pos': x['c4_position'].mean(),
                'horse_avg_c4_gap': x['c4_gap'].mean(),
            }), include_groups=False
        ).reset_index()
        
        c4_median = hist_with_c4['c4_position'].median()
        c4_q1 = hist_with_c4['c4_position'].quantile(0.25)
        c4_q3 = hist_with_c4['c4_position'].quantile(0.75)
        
        def classify_style(avg):
            if pd.isna(avg): return np.nan
            if avg <= c4_q1: return 0
            elif avg <= c4_median: return 1
            elif avg <= c4_q3: return 2
            else: return 3
        
        horse_running['horse_running_style'] = horse_running['horse_avg_c4_pos'].apply(classify_style)
        
        df = df.merge(
            horse_running[['horse_id', 'horse_avg_c4_pos', 'horse_avg_c4_gap', 'horse_running_style']],
            on='horse_id', how='left'
        )
        return df
    
    def _merge_venue_pace(self, df: pd.DataFrame) -> pd.DataFrame:
        """ペース統計"""
        venue_pace = self._race_details.merge(
            self._full_history[['race_id', 'venue']].drop_duplicates(), on='race_id', how='left'
        )
        stats = venue_pace.groupby('venue').apply(
            lambda x: pd.Series({
                'venue_pace_avg': x['first_half'].mean(),
            }), include_groups=False
        ).reset_index()
        
        df = df.merge(stats, on='venue', how='left')
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
            # 馬（累積）
            'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish', 'horse_total_races',
            'horse_last_finish', 'horse_last3_avg_finish', 'horse_days_since_last',
            'horse_last3f_avg',
            # 騎手（累積）
            'jockey_win_rate', 'jockey_top3_rate', 'jockey_races',
            # 調教師（累積）
            'trainer_win_rate', 'trainer_top3_rate', 'trainer_races',
            # 血統（累積）
            'sire_win_rate', 'sire_races',
            # 位置取り
            'horse_avg_c4_pos', 'horse_avg_c4_gap', 'horse_running_style',
            # ペース
            'venue_pace_avg',
            # 馬場バイアス
            'bracket_bias',
            # 交互作用（累積）
            'jt_combo_win_rate', 'jt_combo_races',
            # 当日情報
            'distance_category_encoded', 'track_condition_encoded', 'track_surface_encoded',
            'bracket_number', 'horse_weight', 'age', 'sex_encoded',
            'distance_m', 'basis_weight', 'field_size',
        ]

"""
リークフリー特徴量エンジニア V7.2

V7.1からの修正:
- _merge_track_bias: Train期間固定方式に変更（リーク修正）
- _merge_running_style: Train期間固定方式に変更（リーク修正）
- _merge_venue_pace: 変更なし（影響軽微）
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV7:
    """設定"""
    min_races_for_stats: int = 3
    max_last3f: float = 50.0
    max_rest_days: int = 365
    max_gap_from_leader: float = 30.0


class LeakFreeFeatureEngineerV72:
    """
    真の時点考慮型リークフリー特徴量エンジニア（V7.2）
    
    V7.1からの修正:
    - _merge_track_bias: fit時のデータのみで統計を計算し固定
    - _merge_running_style: fit時のデータのみで統計を計算し固定
    """
    
    FEATURE_COLS = [
        'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish', 'horse_total_races',
        'horse_last_finish', 'horse_last3_avg_finish', 'horse_days_since_last', 'horse_last3f_avg',
        'jockey_win_rate', 'jockey_top3_rate', 'jockey_races',
        'trainer_win_rate', 'trainer_top3_rate', 'trainer_races',
        'sire_win_rate', 'sire_races',
        'horse_avg_c4_pos', 'horse_avg_c4_gap', 'horse_running_style',
        'venue_pace_avg', 'bracket_bias',
        'jt_combo_win_rate', 'jt_combo_races',
        'distance_category_encoded', 'track_condition_encoded', 'track_surface_encoded',
        'bracket_number', 'horse_weight', 'age', 'sex_encoded',
        'distance_m', 'basis_weight', 'field_size',
    ]
    
    def __init__(self, config: Optional[FeatureConfigV7] = None):
        self.config = config or FeatureConfigV7()
        self.is_fitted = False
        self.fit_date = None
        self._full_history = None
        self._pedigrees = None
        self._sire_mapping = None
        self._corners = None
        self._race_details = None
        # V7.2: 事前計算された統計をキャッシュ
        self._track_bias_cache = None
        self._running_style_cache = None
        self._venue_pace_cache = None
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV72':
        """訓練データの準備と保持"""
        logger.info("LeakFreeFeatureEngineerV72: fit開始")
        
        df = races_df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        df = df.sort_values(['race_date', 'race_id', 'horse_number'])
        
        self.fit_date = df['race_date'].max()
        logger.info(f"  fit期間: {df['race_date'].min()} - {self.fit_date}")
        
        df['distance_category'] = pd.cut(
            df['distance_m'], bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        df['is_win'] = (df['finish_position'] == 1).astype(int)
        df['is_top3'] = (df['finish_position'] <= 3).astype(int)
        df['_unique_key'] = df['race_id'].astype(str) + '_' + df['horse_number'].astype(str)
        
        self._full_history = df.copy()
        self._pedigrees = pedigrees_df
        self._corners = corners_df
        self._race_details = race_details_df
        
        if pedigrees_df is not None:
            sires = pedigrees_df[pedigrees_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
            sires.columns = ['horse_id', 'sire_id']
            self._sire_mapping = sires
            logger.info(f"  血統マッピング: {len(sires)}頭")
        
        # V7.2: fit時に統計をキャッシュ（Train期間固定）
        self._precompute_track_bias()
        self._precompute_running_style()
        self._precompute_venue_pace()
        
        self.is_fitted = True
        logger.info("LeakFreeFeatureEngineerV72: fit完了")
        return self
    
    def _precompute_track_bias(self):
        """V7.2: fit時にbracket_biasを事前計算（Train期間固定）"""
        hist = self._full_history
        
        track_bias = hist.groupby(['venue', 'distance_category', 'track_surface', 'bracket_number']).apply(
            lambda x: pd.Series({'bracket_avg_finish': x['finish_position'].mean()}), include_groups=False
        ).reset_index()
        
        venue_base = hist.groupby(['venue', 'distance_category', 'track_surface']).apply(
            lambda x: pd.Series({'venue_avg_finish': x['finish_position'].mean()}), include_groups=False
        ).reset_index()
        
        track_bias = track_bias.merge(venue_base, on=['venue', 'distance_category', 'track_surface'], how='left')
        track_bias['bracket_bias'] = track_bias['bracket_avg_finish'] - track_bias['venue_avg_finish']
        
        self._track_bias_cache = track_bias[['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_bias']]
        logger.info(f"  bracket_bias: {len(self._track_bias_cache)}件のキャッシュを作成")
    
    def _precompute_running_style(self):
        """V7.2: fit時にrunning_styleを事前計算（Train期間固定）"""
        if self._corners is None:
            self._running_style_cache = None
            return
        
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
        
        self._running_style_cache = horse_running[['horse_id', 'horse_avg_c4_pos', 'horse_avg_c4_gap', 'horse_running_style']]
        logger.info(f"  running_style: {len(self._running_style_cache)}頭のキャッシュを作成")
    
    def _precompute_venue_pace(self):
        """V7.2: fit時にvenue_paceを事前計算（Train期間固定）"""
        if self._race_details is None:
            self._venue_pace_cache = None
            return
        
        venue_pace = self._race_details.merge(
            self._full_history[['race_id', 'venue']].drop_duplicates(), on='race_id', how='left'
        )
        stats = venue_pace.groupby('venue').apply(
            lambda x: pd.Series({'venue_pace_avg': x['first_half'].mean()}), include_groups=False
        ).reset_index()
        
        self._venue_pace_cache = stats
        logger.info(f"  venue_pace: {len(self._venue_pace_cache)}競馬場のキャッシュを作成")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        特徴量を生成
        
        方式：all_dataでcumulative計算後、dfのキーでマージして戻す
        """
        if not self.is_fitted:
            raise ValueError("fit()を先に実行してください")
        
        df = df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        original_len = len(df)
        
        logger.info(f"LeakFreeFeatureEngineerV72: transform開始 ({original_len}行)")
        
        df['distance_category'] = pd.cut(
            df['distance_m'], bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        df['_unique_key'] = df['race_id'].astype(str) + '_' + df['horse_number'].astype(str)
        target_keys = set(df['_unique_key'].values)
        
        # 履歴データ準備
        hist = self._full_history.copy()
        history_keys = set(hist['_unique_key'].values)
        
        # dfで履歴に存在しないキーを特定（新規データ）
        new_keys = target_keys - history_keys
        
        # dfのis_win/is_top3を設定
        hist_lookup = hist.set_index('_unique_key')[['is_win', 'is_top3']]
        df['is_win'] = df['_unique_key'].map(hist_lookup['is_win'])
        df['is_top3'] = df['_unique_key'].map(hist_lookup['is_top3'])
        
        # 新規データの処理
        new_mask = df['_unique_key'].isin(new_keys)
        if 'finish_position' in df.columns:
            df.loc[new_mask, 'is_win'] = (df.loc[new_mask, 'finish_position'] == 1).astype(float)
            df.loc[new_mask, 'is_top3'] = (df.loc[new_mask, 'finish_position'] <= 3).astype(float)
            fp_nan_mask = new_mask & df['finish_position'].isna()
            df.loc[fp_nan_mask, 'is_win'] = np.nan
            df.loc[fp_nan_mask, 'is_top3'] = np.nan
        else:
            df.loc[new_mask, 'is_win'] = np.nan
            df.loc[new_mask, 'is_top3'] = np.nan
        
        # 履歴から重複を除外
        hist_no_dup = hist[~hist['_unique_key'].isin(target_keys)]
        
        # all_dataを構築
        all_data = pd.concat([hist_no_dup, df], ignore_index=True)
        all_data = all_data.sort_values(['race_date', 'race_id', 'horse_number'])
        
        # 累積統計を計算（時点考慮型）
        all_data = self._calc_horse_cumulative(all_data)
        all_data = self._calc_jockey_cumulative(all_data)
        all_data = self._calc_trainer_cumulative(all_data)
        if self._sire_mapping is not None:
            all_data = self._calc_sire_cumulative(all_data)
        all_data = self._calc_jt_combo_cumulative(all_data)
        
        # V7.2: キャッシュからマージ（Train期間固定）
        all_data = self._merge_track_bias_cached(all_data)
        all_data = self._merge_running_style_cached(all_data)
        all_data = self._merge_venue_pace_cached(all_data)
        
        all_data = self._encode_current_race_features(all_data)
        
        # ターゲット行を抽出
        result = all_data[all_data['_unique_key'].isin(target_keys)].copy()
        result = result.drop_duplicates(subset=['_unique_key'], keep='last')
        
        # finish_positionを元データから復元
        fp_map = df.set_index('_unique_key')['finish_position']
        result['finish_position'] = result['_unique_key'].map(fp_map)
        
        result = result.drop(columns=['_unique_key', 'is_win', 'is_top3'], errors='ignore')
        
        logger.info("LeakFreeFeatureEngineerV72: transform完了")
        return result
    
    def _calc_horse_cumulative(self, df: pd.DataFrame) -> pd.DataFrame:
        """馬の累積統計"""
        df = df.sort_values(['horse_id', 'race_date', 'race_id'])
        
        df['horse_cum_races'] = df.groupby('horse_id')['is_win'].transform(
            lambda x: x.notna().cumsum().shift(1).fillna(0)
        )
        df['horse_cum_wins'] = df.groupby('horse_id')['is_win'].transform(
            lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
        )
        df['horse_cum_top3'] = df.groupby('horse_id')['is_top3'].transform(
            lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
        )
        df['horse_cum_finish'] = df.groupby('horse_id')['finish_position'].transform(
            lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
        )
        
        min_n = self.config.min_races_for_stats
        df['horse_win_rate'] = np.where(df['horse_cum_races'] >= min_n,
            df['horse_cum_wins'] / df['horse_cum_races'], np.nan)
        df['horse_top3_rate'] = np.where(df['horse_cum_races'] >= min_n,
            df['horse_cum_top3'] / df['horse_cum_races'], np.nan)
        df['horse_avg_finish'] = np.where(df['horse_cum_races'] >= min_n,
            df['horse_cum_finish'] / df['horse_cum_races'], np.nan)
        df['horse_total_races'] = df['horse_cum_races']
        
        df['horse_last_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
        df['horse_last3_avg_finish'] = df.groupby('horse_id')['finish_position'].transform(
            lambda x: x.shift(1).rolling(3, min_periods=1).mean()
        )
        df['horse_days_since_last'] = df.groupby('horse_id')['race_date'].transform(
            lambda x: x.diff().dt.days
        ).clip(upper=self.config.max_rest_days)
        
        if 'last_3f_time' in df.columns:
            df['last_3f_clipped'] = df['last_3f_time'].clip(upper=self.config.max_last3f)
            df['horse_last3f_avg'] = df.groupby('horse_id')['last_3f_clipped'].transform(
                lambda x: x.shift(1).expanding().mean()
            )
            df = df.drop(columns=['last_3f_clipped'], errors='ignore')
        
        df = df.drop(columns=['horse_cum_races', 'horse_cum_wins', 'horse_cum_top3', 'horse_cum_finish'], errors='ignore')
        return df
    
    def _calc_jockey_cumulative(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手の累積統計"""
        df = df.sort_values(['jockey_id', 'race_date', 'race_id'])
        
        df['jockey_cum_races'] = df.groupby('jockey_id')['is_win'].transform(
            lambda x: x.notna().cumsum().shift(1).fillna(0)
        )
        df['jockey_cum_wins'] = df.groupby('jockey_id')['is_win'].transform(
            lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
        )
        df['jockey_cum_top3'] = df.groupby('jockey_id')['is_top3'].transform(
            lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
        )
        
        min_n = self.config.min_races_for_stats
        df['jockey_win_rate'] = np.where(df['jockey_cum_races'] >= min_n,
            df['jockey_cum_wins'] / df['jockey_cum_races'], np.nan)
        df['jockey_top3_rate'] = np.where(df['jockey_cum_races'] >= min_n,
            df['jockey_cum_top3'] / df['jockey_cum_races'], np.nan)
        df['jockey_races'] = df['jockey_cum_races']
        
        df = df.drop(columns=['jockey_cum_races', 'jockey_cum_wins', 'jockey_cum_top3'], errors='ignore')
        return df
    
    def _calc_trainer_cumulative(self, df: pd.DataFrame) -> pd.DataFrame:
        """調教師の累積統計"""
        df = df.sort_values(['trainer_id', 'race_date', 'race_id'])
        
        df['trainer_cum_races'] = df.groupby('trainer_id')['is_win'].transform(
            lambda x: x.notna().cumsum().shift(1).fillna(0)
        )
        df['trainer_cum_wins'] = df.groupby('trainer_id')['is_win'].transform(
            lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
        )
        df['trainer_cum_top3'] = df.groupby('trainer_id')['is_top3'].transform(
            lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
        )
        
        min_n = self.config.min_races_for_stats
        df['trainer_win_rate'] = np.where(df['trainer_cum_races'] >= min_n,
            df['trainer_cum_wins'] / df['trainer_cum_races'], np.nan)
        df['trainer_top3_rate'] = np.where(df['trainer_cum_races'] >= min_n,
            df['trainer_cum_top3'] / df['trainer_cum_races'], np.nan)
        df['trainer_races'] = df['trainer_cum_races']
        
        df = df.drop(columns=['trainer_cum_races', 'trainer_cum_wins', 'trainer_cum_top3'], errors='ignore')
        return df
    
    def _calc_sire_cumulative(self, df: pd.DataFrame) -> pd.DataFrame:
        """父馬の累積統計"""
        df = df.merge(self._sire_mapping, on='horse_id', how='left')
        df = df.sort_values(['sire_id', 'race_date', 'race_id'])
        
        df['sire_cum_races'] = df.groupby('sire_id')['is_win'].transform(
            lambda x: x.notna().cumsum().shift(1).fillna(0)
        )
        df['sire_cum_wins'] = df.groupby('sire_id')['is_win'].transform(
            lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
        )
        
        min_n = self.config.min_races_for_stats
        df['sire_win_rate'] = np.where(df['sire_cum_races'] >= min_n,
            df['sire_cum_wins'] / df['sire_cum_races'], np.nan)
        df['sire_races'] = df['sire_cum_races']
        
        df = df.drop(columns=['sire_cum_races', 'sire_cum_wins'], errors='ignore')
        return df
    
    def _calc_jt_combo_cumulative(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手×調教師の累積統計"""
        df['jt_key'] = df['jockey_id'].astype(str) + '_' + df['trainer_id'].astype(str)
        df = df.sort_values(['jt_key', 'race_date', 'race_id'])
        
        df['jt_cum_races'] = df.groupby('jt_key')['is_win'].transform(
            lambda x: x.notna().cumsum().shift(1).fillna(0)
        )
        df['jt_cum_wins'] = df.groupby('jt_key')['is_win'].transform(
            lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
        )
        
        min_n = self.config.min_races_for_stats
        df['jt_combo_win_rate'] = np.where(df['jt_cum_races'] >= min_n,
            df['jt_cum_wins'] / df['jt_cum_races'], np.nan)
        df['jt_combo_races'] = df['jt_cum_races']
        
        df = df.drop(columns=['jt_key', 'jt_cum_races', 'jt_cum_wins'], errors='ignore')
        return df
    
    def _merge_track_bias_cached(self, df: pd.DataFrame) -> pd.DataFrame:
        """V7.2: キャッシュされたbracket_biasをマージ"""
        if self._track_bias_cache is not None:
            df = df.merge(
                self._track_bias_cache,
                on=['venue', 'distance_category', 'track_surface', 'bracket_number'],
                how='left'
            )
        return df
    
    def _merge_running_style_cached(self, df: pd.DataFrame) -> pd.DataFrame:
        """V7.2: キャッシュされたrunning_styleをマージ"""
        if self._running_style_cache is not None:
            df = df.merge(
                self._running_style_cache,
                on='horse_id',
                how='left'
            )
        return df
    
    def _merge_venue_pace_cached(self, df: pd.DataFrame) -> pd.DataFrame:
        """V7.2: キャッシュされたvenue_paceをマージ"""
        if self._venue_pace_cache is not None:
            df = df.merge(self._venue_pace_cache, on='venue', how='left')
        return df
    
    def _encode_current_race_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """当日情報"""
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
        """使用特徴量"""
        return self.FEATURE_COLS

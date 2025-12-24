"""
リークフリー特徴量エンジニア V8

V7を継承し、以下の新特徴量を追加：
- 母父（damsire）統計
- 条件別統計（距離別・馬場別）
- クラス変動特徴量
- 体重変動トレンド
- 上がり3F順位
- 騎手×条件別統計
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7, FeatureConfigV7

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV8(FeatureConfigV7):
    """V8用設定"""
    min_races_for_conditional_stats: int = 2  # 条件別統計の最小出走数


class LeakFreeFeatureEngineerV8(LeakFreeFeatureEngineerV7):
    """
    リークフリー特徴量エンジニア V8
    
    V7の全特徴量に加えて、以下を追加：
    - damsire_win_rate, damsire_races (母父統計)
    - horse_win_rate_sprint/mile/middle/long (距離別勝率)
    - horse_win_rate_turf/dirt (馬場別勝率)
    - is_class_up, is_class_down, class_change (クラス変動)
    - weight_change_from_last (体重変動)
    - jockey_venue_win_rate, jockey_distance_win_rate (騎手条件別)
    """
    
    # V7の特徴量 + V8の新特徴量 (過学習対策済: V8.1)
    FEATURE_COLS = LeakFreeFeatureEngineerV7.FEATURE_COLS + [
        # クラス変動 (Test ROI 86.6%達成)
        'race_class', 'prev_race_class', 'class_change', 'is_class_up', 'is_class_down',
    ]
    
    # クラス階層（数値が大きいほど上位）
    CLASS_HIERARCHY = {
        '新馬': 1, '未勝利': 2, 
        '1勝': 3, '2勝': 4, '3勝': 5,
        'オープン': 6, 'OP': 6, 'L': 7, 'G3': 8, 'G2': 9, 'G1': 10,
        # 旧表記
        '500万': 3, '1000万': 4, '1600万': 5,
    }
    
    def __init__(self, config: Optional[FeatureConfigV8] = None):
        super().__init__(config or FeatureConfigV8())
        self._damsire_mapping = None
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV8':
        """V7のfitを拡張"""
        # V7のfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df)
        
        logger.info("LeakFreeFeatureEngineerV8: 追加fit処理開始")
        
        # 履歴データにクラス情報を追加
        if self._full_history is not None:
            self._full_history = self._add_race_class(self._full_history)
        
        logger.info("LeakFreeFeatureEngineerV8: fit完了")
        return self
    
    def _add_race_class(self, df: pd.DataFrame) -> pd.DataFrame:
        """レース名からクラス情報をパース"""
        if 'race_name' not in df.columns:
            df['race_class'] = np.nan
            return df
        
        def parse_class(name):
            if pd.isna(name):
                return np.nan
            name = str(name)
            
            # グレードレース
            if 'G1' in name or 'GⅠ' in name:
                return 10
            if 'G2' in name or 'GⅡ' in name:
                return 9
            if 'G3' in name or 'GⅢ' in name:
                return 8
            if '(L)' in name or 'リステッド' in name:
                return 7
            
            # 条件戦
            if 'オープン' in name or 'OP' in name:
                return 6
            if '3勝' in name or '1600万' in name:
                return 5
            if '2勝' in name or '1000万' in name:
                return 4
            if '1勝' in name or '500万' in name:
                return 3
            if '未勝利' in name:
                return 2
            if '新馬' in name:
                return 1
            
            return np.nan
        
        df['race_class'] = df['race_name'].apply(parse_class)
        return df
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V7のtransformを拡張"""
        # 入力データにクラス情報を追加
        df = df.copy()
        df = self._add_race_class(df)
        
        # V7のtransform
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV8: 追加transform開始")
        
        # V8追加特徴量を計算
        # 母父、条件別、体重、騎手条件別は過学習を招くため除外 (V8.1)
        result = self._calc_class_change(result)
        
        logger.info("LeakFreeFeatureEngineerV8: transform完了")
        return result
    
    def _calc_damsire_cumulative(self, df: pd.DataFrame) -> pd.DataFrame:
        """母父の累積統計（履歴データを使用）"""
        if self._damsire_mapping is None:
            df['damsire_win_rate'] = np.nan
            df['damsire_races'] = np.nan
            return df
        
        # 履歴データを使用して母父統計を計算
        hist = self._full_history.copy()
        hist = hist.merge(self._damsire_mapping, on='horse_id', how='left')
        
        # 母父ごとの勝率を計算
        damsire_stats = hist.groupby('damsire_id').apply(
            lambda x: pd.Series({
                'damsire_win_rate': x['is_win'].mean() if len(x) >= self.config.min_races_for_stats else np.nan,
                'damsire_races': len(x)
            }), include_groups=False
        ).reset_index()
        
        # 馬IDから母父を経由して統計をマージ
        horse_damsire = self._damsire_mapping.merge(damsire_stats, on='damsire_id', how='left')
        df = df.merge(horse_damsire[['horse_id', 'damsire_win_rate', 'damsire_races']], 
                     on='horse_id', how='left')
        
        return df
    
    def _calc_horse_conditional_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """馬の条件別統計（距離別・馬場別）"""
        # 履歴データを使用
        hist = self._full_history.copy()
        
        # 距離カテゴリ別勝率
        for dist_cat in ['sprint', 'mile', 'middle', 'long']:
            subset = hist[hist['distance_category'] == dist_cat]
            horse_dist_stats = subset.groupby('horse_id').apply(
                lambda x: pd.Series({
                    f'horse_win_rate_{dist_cat}': x['is_win'].mean() if len(x) >= self.config.min_races_for_conditional_stats else np.nan
                }), include_groups=False
            ).reset_index()
            df = df.merge(horse_dist_stats, on='horse_id', how='left')
        
        # 馬場別勝率
        if 'track_surface' in hist.columns:
            for surface, suffix in [('芝', 'turf'), ('ダート', 'dirt')]:
                subset = hist[hist['track_surface'] == surface]
                horse_surface_stats = subset.groupby('horse_id').apply(
                    lambda x: pd.Series({
                        f'horse_win_rate_{suffix}': x['is_win'].mean() if len(x) >= self.config.min_races_for_conditional_stats else np.nan
                    }), include_groups=False
                ).reset_index()
                df = df.merge(horse_surface_stats, on='horse_id', how='left')
        else:
            df['horse_win_rate_turf'] = np.nan
            df['horse_win_rate_dirt'] = np.nan
        
        return df
    
    def _calc_class_change(self, df: pd.DataFrame) -> pd.DataFrame:
        """クラス変動特徴量"""
        # 履歴から前走のクラスを取得
        hist = self._full_history.copy()
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        hist['prev_race_class'] = hist.groupby('horse_id')['race_class'].shift(1)
        
        # dfに前走クラスをマージ
        horse_prev_class = hist.groupby('horse_id').last()[['prev_race_class', 'race_class']].reset_index()
        horse_prev_class.columns = ['horse_id', 'last_race_class', 'last_race_class_current']
        
        df = df.merge(horse_prev_class[['horse_id', 'last_race_class']], on='horse_id', how='left')
        df['prev_race_class'] = df['last_race_class']
        
        # クラス変動を計算
        df['class_change'] = df['race_class'] - df['prev_race_class']
        df['is_class_up'] = (df['class_change'] > 0).astype(float)
        df['is_class_down'] = (df['class_change'] < 0).astype(float)
        
        # NAの場合は0
        df['class_change'] = df['class_change'].fillna(0)
        df['is_class_up'] = df['is_class_up'].fillna(0)
        df['is_class_down'] = df['is_class_down'].fillna(0)
        
        df = df.drop(columns=['last_race_class'], errors='ignore')
        return df
    
    def _calc_weight_change(self, df: pd.DataFrame) -> pd.DataFrame:
        """体重変動特徴量（Phase 2拡張: トレンド追加）"""
        hist = self._full_history.copy()
        if 'horse_weight' not in hist.columns:
            df['weight_change_from_last'] = np.nan
            df['weight_change_trend_3'] = np.nan
            return df
        
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 体重変化を計算（各レース間）
        hist['weight_diff'] = hist.groupby('horse_id')['horse_weight'].diff()
        
        # 直近3走の体重変動トレンド（正=増加傾向、負=減少傾向）
        hist['weight_trend_3'] = hist.groupby('horse_id')['weight_diff'].transform(
            lambda x: x.rolling(3, min_periods=1).mean()
        )
        
        # 最後のレコードの値を取得
        horse_weight_stats = hist.groupby('horse_id').last()[['horse_weight', 'weight_trend_3']].reset_index()
        horse_weight_stats.columns = ['horse_id', 'prev_weight', 'weight_change_trend_3']
        
        df = df.merge(horse_weight_stats, on='horse_id', how='left')
        df['weight_change_from_last'] = df['horse_weight'] - df['prev_weight']
        df = df.drop(columns=['prev_weight'], errors='ignore')
        
        return df
    
    def _calc_jockey_conditional_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手の条件別統計"""
        hist = self._full_history.copy()
        
        # 競馬場別騎手勝率
        jockey_venue = hist.groupby(['jockey_id', 'venue']).apply(
            lambda x: pd.Series({
                'jockey_venue_win_rate': x['is_win'].mean() if len(x) >= self.config.min_races_for_conditional_stats else np.nan
            }), include_groups=False
        ).reset_index()
        df = df.merge(jockey_venue, on=['jockey_id', 'venue'], how='left')
        
        # 距離別騎手勝率
        jockey_distance = hist.groupby(['jockey_id', 'distance_category']).apply(
            lambda x: pd.Series({
                'jockey_distance_win_rate': x['is_win'].mean() if len(x) >= self.config.min_races_for_conditional_stats else np.nan
            }), include_groups=False
        ).reset_index()
        df = df.merge(jockey_distance, on=['jockey_id', 'distance_category'], how='left')
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """使用特徴量"""
        return self.FEATURE_COLS
    
    def _calc_last3f_rank(self, df: pd.DataFrame) -> pd.DataFrame:
        """上がり3F順位の平均（Phase 2追加）"""
        hist = self._full_history.copy()
        
        if 'last_3f_time' not in hist.columns:
            df['horse_last3f_rank_avg'] = np.nan
            return df
        
        # レース内での上がり3F順位を計算
        hist['last3f_rank'] = hist.groupby('race_id')['last_3f_time'].rank(method='min')
        
        # 馬ごとの平均上がり順位を計算
        horse_last3f_stats = hist.groupby('horse_id').apply(
            lambda x: pd.Series({
                'horse_last3f_rank_avg': x['last3f_rank'].mean() if len(x) >= 2 else np.nan
            }), include_groups=False
        ).reset_index()
        
        df = df.merge(horse_last3f_stats, on='horse_id', how='left')
        return df

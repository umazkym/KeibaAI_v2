"""
リークフリー特徴量エンジニア V17 (Dynamic Version)

V15をベースに、距離適性と騎手変更特徴量を追加。
静的なfitによるリークを防ぐため、全てexpanding().shift(1)で動的に計算する。

【追加特徴量】
1. dist_cat_win_rate: 馬のその距離カテゴリでの過去勝率
   - 計算: dist_catごとに expanding().mean()
   - リーク対策: shift(1)

2. is_best_dist_cat: 今回の距離が、馬の過去最高勝率の距離カテゴリか
   - 計算: 過去の距離カテゴリ別勝率と比較
   - 注意: 実装が複雑になるため、今回はシンプルに「dist_cat_win_rate」そのものを特徴量とする

3. jockey_changed: 前走から騎手が変更されたか
   - 変更なし
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15, FeatureConfigV15

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV17(FeatureConfigV15):
    """V17用設定"""
    pass


class LeakFreeFeatureEngineerV17(LeakFreeFeatureEngineerV15):
    """
    リークフリー特徴量エンジニア V17 (Dynamic)
    
    追加特徴量：
    - dist_cat_win_rate: 今回の距離カテゴリでの過去勝率
    - dist_cat_run_count: 今回の距離カテゴリでの過去出走数
    - jockey_changed: 騎手変更フラグ
    """
    
    # V15の特徴量 + V17の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV15.FEATURE_COLS + [
        'dist_cat_win_rate',   # 距離カテゴリ別勝率
        'dist_cat_run_count',  # 距離カテゴリ別出走数
        'jockey_changed',      # 騎手変更フラグ
    ]
    
    def __init__(self, config: Optional[FeatureConfigV17] = None):
        super().__init__(config or FeatureConfigV17())
        # fitでの事前計算は不要になった
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        logger.info("LeakFreeFeatureEngineerV17: fit開始")
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        logger.info("LeakFreeFeatureEngineerV17: fit完了")
        return self
    
    def _get_distance_category(self, distance_m: int) -> str:
        """距離カテゴリを取得（8分割）"""
        if distance_m <= 1200: return 'sprint_short'
        elif distance_m <= 1400: return 'sprint_long'
        elif distance_m <= 1600: return 'mile_short'
        elif distance_m <= 1800: return 'mile_long'
        elif distance_m <= 2000: return 'middle_short'
        elif distance_m <= 2200: return 'middle_long'
        elif distance_m <= 2500: return 'long_short'
        else: return 'long_long'
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V17のtransform"""
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV17: 追加transform開始")
        result = self._add_distance_features_dynamic(result)
        result = self._add_jockey_change_features(result)
        logger.info("LeakFreeFeatureEngineerV17: transform完了")
        return result
    
    def _add_distance_features_dynamic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        距離適性特徴量を動的に付与 (expanding window)
        """
        logger.info("  距離適性特徴量を付与中(Dynamic)...")
        
        df = df.copy()
        
        # is_win作成
        if 'is_win' not in df.columns:
            df['is_win'] = (df['finish_position'] == 1).astype(int)
            
        # 距離カテゴリ
        df['dist_cat'] = df['distance_m'].apply(self._get_distance_category)
        
        # 過去データと結合して計算する必要がある
        # しかし、transformは全データを一度に受け取る前提なら、そのままgroupbyでOK
        # fit/transform分離の原則：transformには過去データが含まれている必要がある
        # LeakFreeFeatureEngineerは通常、全期間のデータ(または過去分を含むデータ)でtransformされることを想定
        
        # horse_id, dist_cat でグループ化し、expanding計算
        # sortは必須
        df = df.sort_values(['horse_id', 'race_date', 'race_id'])
        
        grouped = df.groupby(['horse_id', 'dist_cat'])
        
        # 過去の勝率 (shift(1)でリーク回避)
        df['dist_cat_win_rate'] = grouped['is_win'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 過去の出走数
        df['dist_cat_run_count'] = grouped['is_win'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        
        # NaN埋め
        df['dist_cat_win_rate'] = df['dist_cat_win_rate'].fillna(0)
        df['dist_cat_run_count'] = df['dist_cat_run_count'].fillna(0)
        
        return df
    
    def _add_jockey_change_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手変更特徴量を付与"""
        logger.info("  騎手変更特徴量を付与中...")
        df = df.copy()
        
        # ソート済み前提だが念のため
        df = df.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 前走騎手
        df['_prev_jockey'] = df.groupby('horse_id')['jockey_id'].shift(1)
        
        # 変更フラグ
        df['jockey_changed'] = (df['jockey_id'] != df['_prev_jockey']).astype(float)
        df.loc[df['_prev_jockey'].isna(), 'jockey_changed'] = np.nan
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

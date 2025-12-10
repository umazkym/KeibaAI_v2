"""
リークフリー特徴量エンジニア V19

V17をベースに、以下の新特徴量を追加:

【新特徴量】
1. jockey_horse_win_rate: 騎手×馬コンビの過去勝率
   - 計算: (horse_id, jockey_id)でグループ化し、expanding().mean().shift(1)
   - リーク対策: shift(1)
   - 最小レース数: 2回以上

2. jockey_horse_races: 騎手×馬コンビの過去出走数
   - 計算: (horse_id, jockey_id)でグループ化し、expanding().count().shift(1)

【分析結果に基づく設計判断】
- trainer_rest_pattern: 人気との相関0.73→市場織り込み済み→不採用
- distance_change_performance: ROIパターンなし→不採用
- jockey_horse_synergy: 人気相関-0.178、カバレッジ25.7%→採用
"""

import pandas as pd
import numpy as np
from typing import Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17, FeatureConfigV17

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV19(FeatureConfigV17):
    """V19用設定"""
    min_jh_races: int = 2  # 騎手×馬コンビの最小レース数


class LeakFreeFeatureEngineerV19(LeakFreeFeatureEngineerV17):
    """
    リークフリー特徴量エンジニア V19
    
    追加特徴量：
    - jockey_horse_win_rate: 騎手×馬コンビの過去勝率
    - jockey_horse_races: 騎手×馬コンビの過去出走数
    """
    
    # V17の特徴量 + V19の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV17.FEATURE_COLS + [
        'jockey_horse_win_rate',  # 騎手×馬コンビ勝率
        'jockey_horse_races',     # 騎手×馬コンビ出走数
    ]
    
    def __init__(self, config: Optional[FeatureConfigV19] = None):
        super().__init__(config or FeatureConfigV19())
        self.config = config or FeatureConfigV19()
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        logger.info("LeakFreeFeatureEngineerV19: fit開始")
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # fit時は特に事前計算は不要
        # 全てtransform時に動的計算（expanding + shift）
        
        logger.info("LeakFreeFeatureEngineerV19: fit完了")
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V19のtransform"""
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV19: 追加transform開始")
        result = self._add_jockey_horse_features(result)
        logger.info("LeakFreeFeatureEngineerV19: transform完了")
        return result
    
    def _add_jockey_horse_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        騎手×馬コンビ特徴量を動的に付与 (expanding window)
        
        【リーク防止】
        - expanding().xxx().shift(1) で当該レース以前のデータのみ使用
        - fit/transform分離: transform時に全レコードに対して動的計算
        
        【過学習防止】
        - 最小レース数要件（2回以上）
        - 要件を満たさない場合はNaN
        """
        logger.info("  騎手×馬コンビ特徴量を付与中...")
        df = df.copy()
        
        # is_win作成
        if 'is_win' not in df.columns:
            if 'finish_position' in df.columns:
                df['is_win'] = (df['finish_position'] == 1).astype(int)
            else:
                df['is_win'] = np.nan
        
        # ソート（時系列順）
        df = df.sort_values(['horse_id', 'jockey_id', 'race_date', 'race_id'])
        
        # 騎手×馬のキー
        df['_jh_key'] = df['jockey_id'].astype(str) + '_' + df['horse_id'].astype(str)
        
        # 累積勝利数（shift(1)でリーク回避）
        df['_jh_cum_wins'] = df.groupby('_jh_key')['is_win'].transform(
            lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
        )
        
        # 累積レース数
        df['jockey_horse_races'] = df.groupby('_jh_key')['is_win'].transform(
            lambda x: x.notna().cumsum().shift(1).fillna(0)
        )
        
        # 勝率計算（最小レース数要件）
        min_races = self.config.min_jh_races
        df['jockey_horse_win_rate'] = np.where(
            df['jockey_horse_races'] >= min_races,
            df['_jh_cum_wins'] / df['jockey_horse_races'],
            np.nan
        )
        
        # カバレッジ確認
        valid_count = df['jockey_horse_win_rate'].notna().sum()
        total_count = len(df)
        logger.info(f"    jockey_horse_win_rate非NaN: {valid_count:,}/{total_count:,} ({valid_count/total_count*100:.1f}%)")
        
        # 一時カラム削除
        df.drop(columns=['_jh_key', '_jh_cum_wins'], inplace=True, errors='ignore')
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

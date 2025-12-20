"""
リークフリー特徴量エンジニア V16 (Lightweight Optimized)

V15をベースに、効果の高かった「騎手×コース相性」のみを追加。
計算コストを抑えつつ、直近の傾向変化への適応力を高める。

【追加特徴量】
1. jockey_venue_win_rate: 騎手の競馬場別勝率
   - その騎手がその競馬場で過去どれだけ勝っているか
   
2. jockey_venue_races: 騎手の競馬場での過去レース数
   - 信頼度（サンプル数）として使用

【削除された特徴量】
初期V16で検討したが、効果が限定的だったため削除:
- race_interval_days
- relative_post
- relative_post_venue_advantage

【リーク対策】
- jockey_venue_*: 累積統計 + shift(1)
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15, FeatureConfigV15

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV16(FeatureConfigV15):
    """V16用設定"""
    # 騎手×コース統計の最小レース数
    min_jockey_venue_races: int = 10


class LeakFreeFeatureEngineerV16(LeakFreeFeatureEngineerV15):
    """
    リークフリー特徴量エンジニア V16 (軽量最適化版)
    
    V15に以下の特徴量を追加：
    - jockey_venue_win_rate: 騎手×コース勝率
    - jockey_venue_races: 騎手×コースレース数
    """
    
    FEATURE_COLS = LeakFreeFeatureEngineerV15.FEATURE_COLS + [
        'jockey_venue_win_rate',          # 騎手×コース勝率
        'jockey_venue_races',             # 騎手×コースレース数
    ]
    
    def __init__(self, config: Optional[FeatureConfigV16] = None):
        super().__init__(config or FeatureConfigV16())
        self._jockey_venue_stats: Dict = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        V16のfit拡張
        
        - 騎手×コースの累積統計
        """
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        logger.info("LeakFreeFeatureEngineerV16: fit開始")
        
        # 1. 騎手×コース統計を計算
        self._calc_jockey_venue_stats(races_df)
        
        logger.info("LeakFreeFeatureEngineerV16: fit完了")
        return self
    
    def _calc_jockey_venue_stats(self, races_df: pd.DataFrame):
        """
        騎手×コースの累積勝率を計算
        
        【リーク対策】
        - horse_id, race_dateでソート後に累積計算
        - shift(1)で当該レース直前までの統計を使用
        """
        logger.info("  騎手×コース統計を計算中...")
        
        df = races_df.copy()
        df = df.sort_values(['jockey_id', 'race_date', 'race_id'])
        
        # 勝利フラグ
        df['is_win'] = (df['finish_position'] == 1).astype(int)
        
        # 騎手×コースでグループ化して累積計算
        group_cols = ['jockey_id', 'venue']
        
        # グループ内でshiftしてリークを防ぐ
        # cumcount() は0, 1, 2... となるので、それがそのまま「過去のレース数」になる（現在を含まない）
        df['cum_races'] = df.groupby(group_cols).cumcount()
        
        # 勝利数も同様にshift
        # transformを使ってグループ内でシフトさせる
        df['cum_wins'] = df.groupby(group_cols)['is_win'].transform(lambda x: x.cumsum().shift(1)).fillna(0)
        
        # 勝率計算（レース数0の場合は0）
        df['jockey_venue_win_rate'] = np.where(
            df['cum_races'] > 0,
            df['cum_wins'] / df['cum_races'],
            0.0
        )
        
        # 辞書に格納（race_id, horse_idでユニークに）
        for _, row in df.iterrows():
            key = (row['race_id'], row.get('horse_id', row.get('horse_name', '')))
            self._jockey_venue_stats[key] = {
                'win_rate': row['jockey_venue_win_rate'],
                'races': row['cum_races']
            }
        
        # サマリーログ
        valid = df[df['cum_races'] >= self.config.min_jockey_venue_races]
        logger.info(f"    騎手×コース統計: {len(self._jockey_venue_stats):,}件 (有効データ: {len(valid):,}件)")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V16のtransform"""
        # V15のtransform
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV16: 追加transform開始")
        
        # V16の特徴量を追加
        result = self._add_jockey_venue_features(result)
        
        logger.info("LeakFreeFeatureEngineerV16: transform完了")
        
        return result
    
    def _add_jockey_venue_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """騎手×コース特徴量を付与"""
        logger.info("  騎手×コース特徴量を付与中...")
        
        win_rates = []
        races_counts = []
        
        for _, row in df.iterrows():
            key = (row['race_id'], row.get('horse_id', row.get('horse_name', '')))
            stats = self._jockey_venue_stats.get(key, {'win_rate': 0.0, 'races': 0})
            win_rates.append(stats['win_rate'])
            races_counts.append(stats['races'])
        
        df['jockey_venue_win_rate'] = win_rates
        df['jockey_venue_races'] = races_counts
        
        valid = df[df['jockey_venue_races'] >= self.config.min_jockey_venue_races]
        logger.info(f"    jockey_venue_win_rate有効: {len(valid):,}/{len(df):,} ({len(valid)/len(df)*100:.1f}%)")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

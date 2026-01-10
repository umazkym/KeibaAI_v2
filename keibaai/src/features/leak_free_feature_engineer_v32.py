# -*- coding: utf-8 -*-
"""
リークフリー特徴量エンジニア V32

V31をベースに、C4位置×上がり順位×ペースの複合特徴量を追加。

【分析結果に基づく設計】
- 前方×上がり速 = 勝率45.8%
- 追上げ型×上がり速 = ROI 230%
- 先頭馬&上がり1位 = 勝率97%
- ハイペース時は中団・後方×上がり速が有利

【追加特徴量 (8個)】

1. C4位置×上がり複合スコア (4個)
   - style_last3f_combo: 脚質×上がりの複合スコア
   - front_fast_last3f_ability: 前方馬の上がり能力
   - back_surge_last3f_ability: 後方馬の追い上げ+上がり能力
   - position_last3f_synergy: 位置取り×上がりのシナジースコア

2. レース内展開難易度 (2個)
   - race_front_last3f_quality: 先頭馬群の上がり水準（展開難易度）
   - race_surge_pressure: レース内の追い上げ圧力

3. ペース×脚質×上がり (2個)
   - pace_style_last3f_advantage: ペース×脚質×上がりの有利度
   - hi_pace_back_advantage: ハイペース時の後方馬有利度

【リーク対策】
- 全ての統計はshift(1)で過去データのみ使用
- 複合スコアは馬の過去成績ベース
- min_races=3
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v31 import LeakFreeFeatureEngineerV31, FeatureConfigV31

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV32(FeatureConfigV31):
    """V32用設定"""
    # 前方判定閾値
    front_threshold: float = 0.33
    # 上がり速判定閾値（上位33%）
    fast_last3f_threshold: float = 0.33


class LeakFreeFeatureEngineerV32(LeakFreeFeatureEngineerV31):
    """
    リークフリー特徴量エンジニア V32
    
    V31にC4位置×上がり×ペースの複合特徴量を追加
    """
    
    FEATURE_COLS = LeakFreeFeatureEngineerV31.FEATURE_COLS + [
        # C4位置×上がり複合スコア
        'style_last3f_combo',           # 脚質×上がり複合スコア
        'front_fast_last3f_ability',    # 前方馬の上がり能力
        'back_surge_last3f_ability',    # 後方追い上げ+上がり能力
        'position_last3f_synergy',      # 位置×上がりシナジー
        # レース内展開難易度
        'race_front_last3f_quality',    # 先頭馬群の上がり水準
        'race_surge_pressure',          # 追い上げ圧力
        # ペース×脚質×上がり
        'pace_style_last3f_advantage',  # ペース×脚質×上がり有利度
        'hi_pace_back_advantage',       # ハイペース後方有利度
    ]
    
    def __init__(self, config: Optional[FeatureConfigV32] = None):
        super().__init__(config or FeatureConfigV32())
        self._horse_combo_stats: Dict = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """fitメソッドの拡張"""
        logger.info("LeakFreeFeatureEngineerV32: fit開始")
        
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # 複合スコア統計を計算
        self._calc_combo_stats(races_df, corners_df)
        
        logger.info("LeakFreeFeatureEngineerV32: fit完了")
        return self
    
    def _calc_combo_stats(self, races_df: pd.DataFrame, corners_df: Optional[pd.DataFrame]):
        """C4位置×上がりの複合統計を計算"""
        logger.info("  C4×上がり複合統計を計算中...")
        
        if corners_df is None or self._full_history is None:
            logger.warning("  必要なデータがありません")
            return
        
        # 履歴データ
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date', 
                                   'last_3f_time', 'finish_position']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        # 出走頭数
        field_size = hist.groupby('race_id').size().reset_index(name='field_size')
        hist = hist.merge(field_size, on='race_id', how='left')
        
        # C4データを取得
        c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position']].copy()
        c4.columns = ['race_id', 'horse_number', 'c4_pos']
        hist = hist.merge(c4, on=['race_id', 'horse_number'], how='left')
        hist = hist.dropna(subset=['horse_id', 'c4_pos', 'last_3f_time'])
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 相対位置・順位
        hist['relative_c4'] = hist['c4_pos'] / hist['field_size']
        hist['last3f_rank'] = hist.groupby('race_id')['last_3f_time'].rank(method='first')
        hist['relative_last3f'] = hist['last3f_rank'] / hist['field_size']
        
        # カテゴリ分類
        front_thresh = self.config.front_threshold
        fast_thresh = self.config.fast_last3f_threshold
        
        hist['is_front'] = (hist['relative_c4'] <= front_thresh).astype(float)
        hist['is_back'] = (hist['relative_c4'] > 0.66).astype(float)
        hist['is_fast_last3f'] = (hist['relative_last3f'] <= fast_thresh).astype(float)
        
        # 複合パターン
        hist['front_fast'] = hist['is_front'] * hist['is_fast_last3f']
        hist['back_fast'] = hist['is_back'] * hist['is_fast_last3f']
        
        # 複合スコア（前方×上がり速は高評価、後方×上がり遅は低評価）
        hist['combo_score'] = (1 - hist['relative_c4']) * (1 - hist['relative_last3f'])
        
        # 累積統計（shift(1)でリーク防止）
        hist['cum_combo_score'] = hist.groupby('horse_id')['combo_score'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        hist['cum_front_fast_rate'] = hist.groupby('horse_id')['front_fast'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        hist['cum_back_fast_rate'] = hist.groupby('horse_id')['back_fast'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        hist['cum_relative_last3f'] = hist.groupby('horse_id')['relative_last3f'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        hist['cum_count'] = hist.groupby('horse_id')['combo_score'].transform(
            lambda x: x.shift(1).expanding().count()
        )
        
        # 最小レース数フィルタ
        min_races = self.config.min_style_races
        for col in ['cum_combo_score', 'cum_front_fast_rate', 'cum_back_fast_rate', 'cum_relative_last3f']:
            hist.loc[hist['cum_count'] < min_races, col] = np.nan
        
        # 最新統計を保存
        latest = hist.groupby('horse_id').last()[
            ['cum_combo_score', 'cum_front_fast_rate', 'cum_back_fast_rate', 'cum_relative_last3f']
        ].reset_index()
        
        self._horse_combo_stats = {}
        for _, row in latest.iterrows():
            self._horse_combo_stats[row['horse_id']] = {
                'combo_score': row['cum_combo_score'],
                'front_fast_rate': row['cum_front_fast_rate'],
                'back_fast_rate': row['cum_back_fast_rate'],
                'relative_last3f_avg': row['cum_relative_last3f'],
            }
        
        logger.info(f"    複合統計: {len(self._horse_combo_stats):,}頭")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V32のtransform"""
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV32: 追加transform開始")
        
        result = self._apply_combo_features(result)
        result = self._calc_race_combo_features(result)
        
        logger.info("LeakFreeFeatureEngineerV32: transform完了")
        return result
    
    def _apply_combo_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """複合特徴量を付与"""
        logger.info("  複合特徴量を付与中...")
        
        df = df.copy()
        
        # 馬ごとの複合スコア
        df['style_last3f_combo'] = df['horse_id'].map(
            lambda x: self._horse_combo_stats.get(x, {}).get('combo_score', np.nan)
        )
        
        # 前方×上がり速パターン能力
        front_fast_rate = df['horse_id'].map(
            lambda x: self._horse_combo_stats.get(x, {}).get('front_fast_rate', np.nan)
        )
        df['front_fast_last3f_ability'] = front_fast_rate
        
        # 後方×上がり速パターン能力
        back_fast_rate = df['horse_id'].map(
            lambda x: self._horse_combo_stats.get(x, {}).get('back_fast_rate', np.nan)
        )
        df['back_surge_last3f_ability'] = back_fast_rate
        
        # 位置×上がりのシナジー（前方脚質かつ上がり能力が高い）
        style_score = df['horse_style_score_avg'].fillna(0.5)
        last3f_avg = df['horse_id'].map(
            lambda x: self._horse_combo_stats.get(x, {}).get('relative_last3f_avg', np.nan)
        )
        df['position_last3f_synergy'] = (1 - style_score) * (1 - last3f_avg.fillna(0.5))
        
        non_null = df['style_last3f_combo'].notna().sum()
        logger.info(f"    style_last3f_combo非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_race_combo_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """レース内の複合特徴量を計算"""
        logger.info("  レース内複合特徴量を計算中...")
        
        df = df.copy()
        
        style_score = df['horse_style_score_avg'].fillna(0.5)
        front_fast = df['front_fast_last3f_ability'].fillna(0)
        
        # 先頭馬群の上がり水準（展開難易度）
        # 前方脚質馬の平均上がり能力 → 高いと先頭争いが激しい
        is_front = (style_score < self.config.front_threshold).astype(float)
        df['race_front_last3f_quality'] = df.groupby('race_id')[front_fast.name].transform('mean')
        
        # 追い上げ圧力（後方×上がり速馬が多いほど高い）
        back_surge = df['back_surge_last3f_ability'].fillna(0)
        df['race_surge_pressure'] = df.groupby('race_id')[back_surge.name].transform('sum')
        
        # ペース×脚質×上がり有利度
        # 前方馬かつ上がり能力が高く、追い上げ圧力が低ければ有利
        surge_max = df['race_surge_pressure'].max()
        if surge_max > 0:
            df['pace_style_last3f_advantage'] = (
                is_front * df['front_fast_last3f_ability'].fillna(0) * 
                (1 - df['race_surge_pressure'] / surge_max)
            )
        else:
            df['pace_style_last3f_advantage'] = is_front * df['front_fast_last3f_ability'].fillna(0)
        
        # ハイペース時後方馬有利度（後方馬×上がり能力が高ければ有利）
        df['hi_pace_back_advantage'] = (
            df['back_surge_last3f_ability'].fillna(0) * 
            (1 - df['race_front_last3f_quality'].fillna(0))
        )
        
        logger.info(f"    race_front_last3f_quality平均: {df['race_front_last3f_quality'].mean():.3f}")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

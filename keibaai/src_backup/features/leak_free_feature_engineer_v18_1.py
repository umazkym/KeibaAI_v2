"""
リークフリー特徴量エンジニア V18.1

V17をベースに、コーナー馬身差・脚質特徴量のみを追加。
elo_ratingとrecency_bias_indicatorは除外。

【除外理由】
- elo_rating: 的中重視でROI向上に貢献しにくい（人気馬優先の傾向）
- recency_bias_indicator: カバレッジが2.7%と極めて低い

【追加特徴量 (7個)】
1. gap_c1_avg: 1コーナー平均馬身差
2. gap_c2_avg: 2コーナー平均馬身差
3. gap_c3_avg: 3コーナー平均馬身差
4. gap_reduction_c1_c4: C1→C4の馬身差縮小量（全体追い込み力）
5. gap_reduction_c3_c4: C3→C4の馬身差縮小量（直線瞬発力）
6. senkou_rate: 先行率（C1で2-4番手の割合）
7. sashi_rate: 差し率（C1で5-8番手＋途中上昇の割合）

【ROI向上の理論】
- 「どう走るか」の脚質情報は市場で織り込まれにくい
- 特に追い込み力（gap_reduction）は着順だけでは分からない
- コーナーごとの位置取りは展開予測に貢献

【リーク対策】
- 全ての累積統計にexpanding().shift(1)を適用
- fit時に計算し、transform時はマップのみ
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17, FeatureConfigV17

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV18_1(FeatureConfigV17):
    """V18.1用設定"""
    # Corner Gap設定
    min_corner_races_for_gap: int = 5


class LeakFreeFeatureEngineerV18_1(LeakFreeFeatureEngineerV17):
    """
    リークフリー特徴量エンジニア V18.1
    
    V17にコーナー馬身差・脚質特徴量のみを追加。
    elo_rating, recency_bias_indicatorは除外。
    
    追加特徴量：
    - gap_c1_avg, gap_c2_avg, gap_c3_avg: 全コーナー馬身差
    - gap_reduction_c1_c4, gap_reduction_c3_c4: 追い込み力
    - senkou_rate, sashi_rate: 詳細脚質
    """
    
    # V17の特徴量 + V18.1の新特徴量（elo, recency除外）
    FEATURE_COLS = LeakFreeFeatureEngineerV17.FEATURE_COLS + [
        'gap_c1_avg',                # 1コーナー平均馬身差
        'gap_c2_avg',                # 2コーナー平均馬身差
        'gap_c3_avg',                # 3コーナー平均馬身差
        'gap_reduction_c1_c4',       # C1→C4追い込み力
        'gap_reduction_c3_c4',       # C3→C4直線瞬発力
        'senkou_rate',               # 先行率
        'sashi_rate',                # 差し率
    ]
    
    def __init__(self, config: Optional[FeatureConfigV18_1] = None):
        super().__init__(config or FeatureConfigV18_1())
        self._corner_gap_stats: Dict[str, Dict[str, float]] = {}
        self._running_style_stats: Dict[str, Dict[str, float]] = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """fitメソッドの拡張"""
        logger.info("LeakFreeFeatureEngineerV18_1: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # 全コーナー馬身差統計を計算
        self._calc_all_corner_gap_stats(races_df, corners_df)
        
        # 詳細脚質統計を計算
        self._calc_running_style_stats(races_df, corners_df)
        
        logger.info("LeakFreeFeatureEngineerV18_1: fit完了")
        return self
    
    def _calc_all_corner_gap_stats(self, races_df: pd.DataFrame, corners_df: Optional[pd.DataFrame]):
        """全コーナー（C1, C2, C3, C4）の馬身差統計を計算"""
        logger.info("  全コーナー馬身差統計を計算中...")
        
        if corners_df is None or len(corners_df) == 0:
            logger.warning("  corners_dfがないため、コーナー統計はスキップ")
            return
        
        config = self.config
        min_races = config.min_corner_races_for_gap
        
        if self._full_history is None:
            return
        
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        result_stats: Dict[str, Dict[str, float]] = {}
        
        for corner_num in [1, 2, 3, 4]:
            corner_data = corners_df[corners_df['corner'] == corner_num][
                ['race_id', 'horse_number', 'position', 'gap_from_leader']
            ].copy()
            corner_data.columns = ['race_id', 'horse_number', f'c{corner_num}_pos', f'c{corner_num}_gap']
            
            corner_with_id = corner_data.merge(hist, on=['race_id', 'horse_number'], how='left')
            corner_with_id = corner_with_id.dropna(subset=['horse_id'])
            corner_with_id = corner_with_id.sort_values(['horse_id', 'race_date', 'race_id'])
            
            gap_col = f'c{corner_num}_gap'
            corner_with_id[f'cum_gap_c{corner_num}_avg'] = corner_with_id.groupby('horse_id')[gap_col].transform(
                lambda x: x.expanding().mean().shift(1)
            )
            corner_with_id[f'cum_count_c{corner_num}'] = corner_with_id.groupby('horse_id')[gap_col].transform(
                lambda x: x.expanding().count().shift(1)
            )
            
            corner_with_id.loc[corner_with_id[f'cum_count_c{corner_num}'] < min_races, f'cum_gap_c{corner_num}_avg'] = np.nan
            
            latest = corner_with_id.groupby('horse_id').last()[[f'cum_gap_c{corner_num}_avg']].reset_index()
            
            for _, row in latest.iterrows():
                horse_id = row['horse_id']
                if horse_id not in result_stats:
                    result_stats[horse_id] = {}
                result_stats[horse_id][f'gap_c{corner_num}_avg'] = row[f'cum_gap_c{corner_num}_avg']
        
        self._corner_gap_stats = result_stats
        logger.info(f"    コーナー馬身差統計: {len(result_stats):,}頭")
    
    def _calc_running_style_stats(self, races_df: pd.DataFrame, corners_df: Optional[pd.DataFrame]):
        """詳細脚質統計（先行率・差し率）を計算"""
        logger.info("  詳細脚質統計を計算中...")
        
        if corners_df is None or len(corners_df) == 0:
            logger.warning("  corners_dfがないため、脚質統計はスキップ")
            return
        
        if self._full_history is None:
            return
        
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date', 'finish_position']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        c1_data = corners_df[corners_df['corner'] == 1][['race_id', 'horse_number', 'position']].copy()
        c1_data.columns = ['race_id', 'horse_number', 'c1_pos']
        
        c4_data = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position']].copy()
        c4_data.columns = ['race_id', 'horse_number', 'c4_pos']
        
        merged = hist.merge(c1_data, on=['race_id', 'horse_number'], how='left')
        merged = merged.merge(c4_data, on=['race_id', 'horse_number'], how='left')
        merged = merged.dropna(subset=['horse_id', 'c1_pos', 'c4_pos'])
        merged = merged.sort_values(['horse_id', 'race_date', 'race_id'])
        
        def classify_style(row):
            c1 = row['c1_pos']
            c4 = row['c4_pos']
            if c1 == 1:
                return 'nige'
            elif c1 <= 4:
                return 'senkou'
            elif c1 <= 8 and c4 < c1:
                return 'sashi'
            else:
                return 'oikomi'
        
        merged['running_style'] = merged.apply(classify_style, axis=1)
        
        merged['is_nige'] = (merged['running_style'] == 'nige').astype(int)
        merged['is_senkou'] = (merged['running_style'] == 'senkou').astype(int)
        merged['is_sashi'] = (merged['running_style'] == 'sashi').astype(int)
        
        for style in ['senkou', 'sashi']:
            merged[f'cum_{style}_rate'] = merged.groupby('horse_id')[f'is_{style}'].transform(
                lambda x: x.expanding().mean().shift(1)
            )
        
        merged['cum_race_count'] = merged.groupby('horse_id')['is_nige'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        min_races = self.config.min_corner_races_for_gap
        
        latest = merged.groupby('horse_id').last()[['cum_senkou_rate', 'cum_sashi_rate', 'cum_race_count']].reset_index()
        
        result_stats: Dict[str, Dict[str, float]] = {}
        for _, row in latest.iterrows():
            horse_id = row['horse_id']
            race_count = row['cum_race_count']
            
            if race_count < min_races:
                senkou = np.nan
                sashi = np.nan
            else:
                senkou = row['cum_senkou_rate']
                sashi = row['cum_sashi_rate']
            
            result_stats[horse_id] = {
                'senkou_rate': senkou,
                'sashi_rate': sashi,
            }
        
        self._running_style_stats = result_stats
        logger.info(f"    脚質統計: {len(result_stats):,}頭")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V18.1のtransform"""
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV18_1: 追加transform開始")
        
        result = self._add_corner_gap_features(result)
        result = self._add_running_style_features(result)
        
        logger.info("LeakFreeFeatureEngineerV18_1: transform完了")
        return result
    
    def _add_corner_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """全コーナー馬身差と追い込み力特徴量を付与"""
        logger.info("  コーナー馬身差を付与中...")
        
        df = df.copy()
        
        df['gap_c1_avg'] = df['horse_id'].apply(
            lambda x: self._corner_gap_stats.get(x, {}).get('gap_c1_avg', np.nan)
        )
        df['gap_c2_avg'] = df['horse_id'].apply(
            lambda x: self._corner_gap_stats.get(x, {}).get('gap_c2_avg', np.nan)
        )
        df['gap_c3_avg'] = df['horse_id'].apply(
            lambda x: self._corner_gap_stats.get(x, {}).get('gap_c3_avg', np.nan)
        )
        
        if 'horse_c4_gap_avg' in df.columns:
            c4_gap = df['horse_c4_gap_avg']
        else:
            c4_gap = df['horse_id'].apply(
                lambda x: self._corner_gap_stats.get(x, {}).get('gap_c4_avg', np.nan)
            )
        
        df['gap_reduction_c1_c4'] = df['gap_c1_avg'] - c4_gap
        df['gap_reduction_c3_c4'] = df['gap_c3_avg'] - c4_gap
        
        for col in ['gap_c1_avg', 'gap_c2_avg', 'gap_c3_avg', 'gap_reduction_c1_c4', 'gap_reduction_c3_c4']:
            non_null = df[col].notna().sum()
            logger.info(f"    {col}非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _add_running_style_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """詳細脚質特徴量を付与"""
        logger.info("  詳細脚質を付与中...")
        
        df = df.copy()
        
        df['senkou_rate'] = df['horse_id'].apply(
            lambda x: self._running_style_stats.get(x, {}).get('senkou_rate', np.nan)
        )
        df['sashi_rate'] = df['horse_id'].apply(
            lambda x: self._running_style_stats.get(x, {}).get('sashi_rate', np.nan)
        )
        
        for col in ['senkou_rate', 'sashi_rate']:
            non_null = df[col].notna().sum()
            logger.info(f"    {col}非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

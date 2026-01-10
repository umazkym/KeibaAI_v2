# -*- coding: utf-8 -*-
"""
リークフリー特徴量エンジニア V31

V30をベースに、データ駆動型の脚質・展開特徴量を追加。

【設計思想（ユーザー指摘反映）】
- 「逃げ/先行/差し/追込」という人間定義ではなく、データから定量的に脚質を導出
- C4だけでなくC1-C4すべてのコーナーデータを活用
- 連続値スケールで脚質を表現

【追加特徴量 (14個)】

1. データ駆動型脚質スコア（4個）
   - horse_style_score_avg: 過去の平均相対コーナー位置（C1-C4平均）
   - horse_position_change_avg: 過去の平均位置変動（C1→C4）
   - horse_style_consistency: 脚質の安定性（位置取りのσ）
   - horse_gap_avg: 過去の平均馬身差

2. 位置変動パターン（3個）
   - horse_surge_rate: 追い上げ成功率（大きく追い上げた割合）
   - horse_fade_rate: 後退率（位置を落とした割合）
   - horse_steady_rate: 位置維持率

3. 各コーナー別指標（3個）
   - horse_c1_avg: 過去C1平均相対位置
   - horse_c2_avg: 過去C2平均相対位置
   - horse_c3_avg: 過去C3平均相対位置

4. レース内展開予測（4個）
   - race_front_density: レース内の前方密集度（予測）
   - race_pace_pressure: ペース圧力
   - style_advantage_score: 脚質有利度スコア
   - position_change_advantage: 追い上げ型有利度

【根拠（分析結果）】
- 大追い上げ: 勝率12.7%, ROI 122.6%
- 大後退: 勝率0.6%, ROI 6.4%
- C4相対位置 vs 着順の相関: 0.378
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v30 import LeakFreeFeatureEngineerV30, FeatureConfigV30

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV31(FeatureConfigV30):
    """V31用設定"""
    min_style_races: int = 3
    surge_threshold: float = -0.15
    fade_threshold: float = 0.10
    front_threshold: float = 0.35


class LeakFreeFeatureEngineerV31(LeakFreeFeatureEngineerV30):
    """
    リークフリー特徴量エンジニア V31
    
    V30に以下のデータ駆動型脚質特徴量を追加：
    - C1-C4全てを活用した脚質スコア
    - 位置変動パターン（追い上げ/後退）
    - レース内展開予測
    """
    
    FEATURE_COLS = LeakFreeFeatureEngineerV30.FEATURE_COLS + [
        # データ駆動型脚質スコア
        'horse_style_score_avg',
        'horse_position_change_avg',
        'horse_style_consistency',
        'horse_gap_avg',
        # 位置変動パターン
        'horse_surge_rate',
        'horse_fade_rate',
        'horse_steady_rate',
        # 各コーナー別指標
        'horse_c1_avg',
        'horse_c2_avg',
        'horse_c3_avg',
        # レース内展開予測
        'race_front_density',
        'race_pace_pressure',
        'style_advantage_score',
        'position_change_advantage',
    ]
    
    def __init__(self, config: Optional[FeatureConfigV31] = None):
        super().__init__(config or FeatureConfigV31())
        self._horse_corner_stats: Dict = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """fitメソッドの拡張"""
        logger.info("LeakFreeFeatureEngineerV31: fit開始")
        
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # C1-C4全てを活用した脚質統計を計算
        self._calc_corner_stats_all(races_df, corners_df)
        
        logger.info("LeakFreeFeatureEngineerV31: fit完了")
        return self
    
    def _calc_corner_stats_all(self, races_df: pd.DataFrame, corners_df: Optional[pd.DataFrame]):
        """C1-C4全てのコーナーデータから脚質統計を計算"""
        logger.info("  C1-C4脚質統計を計算中...")
        
        if corners_df is None or self._full_history is None:
            logger.warning("  必要なデータがありません")
            return
        
        # 履歴データ
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        # 出走頭数
        field_size = hist.groupby('race_id').size().reset_index(name='field_size')
        hist = hist.merge(field_size, on='race_id', how='left')
        
        # 各コーナーデータをピボット
        corner_pivot = corners_df.pivot_table(
            index=['race_id', 'horse_number'],
            columns='corner',
            values=['position', 'gap_from_leader']
        ).reset_index()
        
        # カラム名をフラット化
        corner_pivot.columns = [f'{col[0]}_c{col[1]}' if isinstance(col, tuple) and col[1] else col[0] for col in corner_pivot.columns]
        
        # 履歴とマージ
        hist_corners = hist.merge(corner_pivot, on=['race_id', 'horse_number'], how='left')
        hist_corners = hist_corners.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 相対位置を計算（各コーナー）
        for c in [1, 2, 3, 4]:
            pos_col = f'position_c{c}'
            if pos_col in hist_corners.columns:
                hist_corners[f'relative_c{c}'] = hist_corners[pos_col] / hist_corners['field_size']
        
        # C1-C4の平均相対位置（style_score_avg）
        rel_cols = [f'relative_c{c}' for c in [1, 2, 3, 4] if f'relative_c{c}' in hist_corners.columns]
        hist_corners['style_score'] = hist_corners[rel_cols].mean(axis=1)
        
        # C1→C4の位置変動（position_change）
        if 'relative_c1' in hist_corners.columns and 'relative_c4' in hist_corners.columns:
            hist_corners['position_change'] = hist_corners['relative_c4'] - hist_corners['relative_c1']
        else:
            hist_corners['position_change'] = np.nan
        
        # 馬身差平均
        gap_cols = [f'gap_from_leader_c{c}' for c in [1, 2, 3, 4] if f'gap_from_leader_c{c}' in hist_corners.columns]
        if gap_cols:
            hist_corners['gap_avg'] = hist_corners[gap_cols].mean(axis=1)
        else:
            hist_corners['gap_avg'] = np.nan
        
        # 追い上げ/後退フラグ
        surge_thresh = self.config.surge_threshold
        fade_thresh = self.config.fade_threshold
        hist_corners['is_surge'] = (hist_corners['position_change'] < surge_thresh).astype(float)
        hist_corners['is_fade'] = (hist_corners['position_change'] > fade_thresh).astype(float)
        hist_corners['is_steady'] = (
            (hist_corners['position_change'] >= surge_thresh) & 
            (hist_corners['position_change'] <= fade_thresh)
        ).astype(float)
        
        # 累積統計（shift(1)でリーク防止）
        hist_corners['cum_style_score'] = hist_corners.groupby('horse_id')['style_score'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        hist_corners['cum_position_change'] = hist_corners.groupby('horse_id')['position_change'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        hist_corners['cum_style_std'] = hist_corners.groupby('horse_id')['style_score'].transform(
            lambda x: x.shift(1).expanding().std()
        )
        hist_corners['cum_gap_avg'] = hist_corners.groupby('horse_id')['gap_avg'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        
        # 位置変動パターン
        hist_corners['cum_surge_rate'] = hist_corners.groupby('horse_id')['is_surge'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        hist_corners['cum_fade_rate'] = hist_corners.groupby('horse_id')['is_fade'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        hist_corners['cum_steady_rate'] = hist_corners.groupby('horse_id')['is_steady'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        
        # 各コーナー平均
        for c in [1, 2, 3]:
            rel_col = f'relative_c{c}'
            if rel_col in hist_corners.columns:
                hist_corners[f'cum_c{c}_avg'] = hist_corners.groupby('horse_id')[rel_col].transform(
                    lambda x: x.shift(1).expanding().mean()
                )
        
        # カウント
        hist_corners['cum_count'] = hist_corners.groupby('horse_id')['style_score'].transform(
            lambda x: x.shift(1).expanding().count()
        )
        
        # 最小レース数フィルタ
        min_races = self.config.min_style_races
        for col in ['cum_style_score', 'cum_position_change', 'cum_style_std', 'cum_gap_avg',
                    'cum_surge_rate', 'cum_fade_rate', 'cum_steady_rate',
                    'cum_c1_avg', 'cum_c2_avg', 'cum_c3_avg']:
            if col in hist_corners.columns:
                hist_corners.loc[hist_corners['cum_count'] < min_races, col] = np.nan
        
        # 最新統計を保存
        stat_cols = ['cum_style_score', 'cum_position_change', 'cum_style_std', 'cum_gap_avg',
                     'cum_surge_rate', 'cum_fade_rate', 'cum_steady_rate']
        for c in [1, 2, 3]:
            col = f'cum_c{c}_avg'
            if col in hist_corners.columns:
                stat_cols.append(col)
        
        available_cols = [c for c in stat_cols if c in hist_corners.columns]
        latest = hist_corners.groupby('horse_id').last()[available_cols].reset_index()
        
        self._horse_corner_stats = {}
        for _, row in latest.iterrows():
            self._horse_corner_stats[row['horse_id']] = {
                'style_score_avg': row.get('cum_style_score', np.nan),
                'position_change_avg': row.get('cum_position_change', np.nan),
                'style_consistency': row.get('cum_style_std', np.nan),
                'gap_avg': row.get('cum_gap_avg', np.nan),
                'surge_rate': row.get('cum_surge_rate', np.nan),
                'fade_rate': row.get('cum_fade_rate', np.nan),
                'steady_rate': row.get('cum_steady_rate', np.nan),
                'c1_avg': row.get('cum_c1_avg', np.nan),
                'c2_avg': row.get('cum_c2_avg', np.nan),
                'c3_avg': row.get('cum_c3_avg', np.nan),
            }
        
        logger.info(f"    C1-C4統計: {len(self._horse_corner_stats):,}頭")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V31のtransform"""
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV31: 追加transform開始")
        
        result = self._apply_corner_features(result)
        result = self._calc_race_composition(result)
        
        logger.info("LeakFreeFeatureEngineerV31: transform完了")
        return result
    
    def _apply_corner_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """C1-C4ベースの脚質特徴量を付与"""
        logger.info("  C1-C4脚質特徴量を付与中...")
        
        df = df.copy()
        
        # 各特徴量をマッピング
        features = [
            ('horse_style_score_avg', 'style_score_avg'),
            ('horse_position_change_avg', 'position_change_avg'),
            ('horse_style_consistency', 'style_consistency'),
            ('horse_gap_avg', 'gap_avg'),
            ('horse_surge_rate', 'surge_rate'),
            ('horse_fade_rate', 'fade_rate'),
            ('horse_steady_rate', 'steady_rate'),
            ('horse_c1_avg', 'c1_avg'),
            ('horse_c2_avg', 'c2_avg'),
            ('horse_c3_avg', 'c3_avg'),
        ]
        
        for col_name, stat_key in features:
            df[col_name] = df['horse_id'].map(
                lambda x: self._horse_corner_stats.get(x, {}).get(stat_key, np.nan)
            )
        
        non_null = df['horse_style_score_avg'].notna().sum()
        logger.info(f"    horse_style_score_avg非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_race_composition(self, df: pd.DataFrame) -> pd.DataFrame:
        """レース内展開予測を計算"""
        logger.info("  レース内展開予測を計算中...")
        
        df = df.copy()
        
        style_score = df['horse_style_score_avg'].fillna(0.5)
        front_thresh = self.config.front_threshold
        
        # 前方密集度（レース内で前方脚質の馬の割合）
        is_front = (style_score < front_thresh).astype(float)
        df['race_front_density'] = df.groupby('race_id')[style_score.name].transform(
            lambda x: (x < front_thresh).mean()
        )
        
        # ペース圧力（前方脚質の馬数）
        df['race_pace_pressure'] = df.groupby('race_id')[style_score.name].transform(
            lambda x: (x < front_thresh).sum()
        )
        
        # 脚質有利度（前方脚質かつペース圧力が低い）
        df['style_advantage_score'] = is_front * (1 - df['race_front_density'])
        
        # 追い上げ型有利度
        surge_rate = df['horse_surge_rate'].fillna(0)
        df['position_change_advantage'] = surge_rate * (1 - style_score)
        
        logger.info(f"    race_front_density平均: {df['race_front_density'].mean():.2f}")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

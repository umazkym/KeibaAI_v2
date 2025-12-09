"""
リークフリー特徴量エンジニア V15

V14をベースに、データ分析で発見された高ROIパターンを特徴量化。

【追加特徴量 (4個)】
1. race_front_runner_count: レース内逃げ予備軍数
   - 各馬のhorse_front_runner_rate > 0.3をカウント
   - ペース予測向上、逃げ競合リスク検出

2. horse_c4_gap_avg: 過去C4馬身差平均
   - 「前で競馬できる力」を馬身差で数値化
   - 順位だけでなく、どれだけ先頭に近いかを評価

3. post_style_conflict: 馬番×脚質の「不適合」スコア
   - |relative_post - avg_relative_c4|
   - 注意: 分析で「適合度高→ROI低」が判明
   - → 市場が既に織り込んでいるため、「不適合」を検出

4. front_runner_competition: 自分以外の逃げ予備軍数
   - 逃げ馬にとっての競合リスク

【リーク対策】
- 全ての累積統計はcumsum() + shift(1)で計算
- race_front_runner_countは他馬の「過去統計」のみ使用
- min_races=5に強化（過学習対策）

【過学習対策】
- 高次元組み合わせを避ける（馬×距離×馬場×ペース等は実装しない）
- min_races要件を強化（3→5）
- 特徴量数を抑制（4個のみ追加）
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v14 import LeakFreeFeatureEngineerV14, FeatureConfigV14

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV15(FeatureConfigV14):
    """V15用設定"""
    # 逃げ予備軍判定閾値
    front_runner_threshold_rate: float = 0.3
    # 最小レース数要件（過学習対策で強化）
    min_corner_races: int = 5


class LeakFreeFeatureEngineerV15(LeakFreeFeatureEngineerV14):
    """
    リークフリー特徴量エンジニア V15
    
    V14に以下の特徴量を追加：
    - race_front_runner_count: レース内逃げ予備軍数
    - horse_c4_gap_avg: 過去C4馬身差平均
    - post_style_conflict: 馬番×脚質不適合スコア
    - front_runner_competition: 逃げ競合スコア
    
    【重要】過学習対策
    - min_races=5に強化
    - 高次元組み合わせは追加しない
    """
    
    # V14の特徴量 + V15の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV14.FEATURE_COLS + [
        'race_front_runner_count',      # レース内逃げ予備軍数
        'horse_c4_gap_avg',             # 過去C4馬身差平均
        'post_style_conflict',          # 馬番×脚質不適合スコア
        'front_runner_competition',     # 逃げ競合スコア
    ]
    
    def __init__(self, config: Optional[FeatureConfigV15] = None):
        super().__init__(config or FeatureConfigV15())
        self._horse_c4_gap_stats: Dict = {}
        self._horse_relative_c4_stats: Dict = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        fitメソッドの拡張
        
        - C4馬身差の累積統計を事前計算
        - 相対C4位置の累積統計を事前計算
        """
        logger.info("LeakFreeFeatureEngineerV15: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # C4馬身差・相対位置の統計を事前計算
        self._calc_c4_gap_stats(races_df, corners_df)
        
        logger.info("LeakFreeFeatureEngineerV15: fit完了")
        return self
    
    def _calc_c4_gap_stats(self, races_df: pd.DataFrame, corners_df: Optional[pd.DataFrame]):
        """
        C4馬身差と相対C4位置の累積統計を計算（Train期間のみ）
        
        【リーク対策】
        - fit時にTrain期間のデータのみで計算
        - shift(1)適用済み
        """
        logger.info("  C4馬身差・相対位置統計を計算中...")
        
        if corners_df is None or len(corners_df) == 0:
            logger.warning("  corners_dfがないため、C4統計はスキップ")
            return
        
        # C4データを取得
        c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
        c4.columns = ['race_id', 'horse_number', 'c4_pos', 'c4_gap']
        
        # 履歴データとマージ
        if self._full_history is None:
            return
        
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        # 出走頭数を追加
        field_size = hist.groupby('race_id').size().reset_index(name='field_size')
        hist = hist.merge(field_size, on='race_id', how='left')
        
        c4_with_id = c4.merge(hist, on=['race_id', 'horse_number'], how='left')
        c4_with_id = c4_with_id.dropna(subset=['horse_id'])
        c4_with_id = c4_with_id.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 相対C4位置を計算
        c4_with_id['relative_c4'] = c4_with_id['c4_pos'] / c4_with_id['field_size']
        
        # 累積統計を計算（shift(1)でリーク防止）
        # C4馬身差平均
        c4_with_id['cum_gap_avg'] = c4_with_id.groupby('horse_id')['c4_gap'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        c4_with_id['cum_count_gap'] = c4_with_id.groupby('horse_id')['c4_gap'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        
        # 相対C4位置平均
        c4_with_id['cum_relative_c4'] = c4_with_id.groupby('horse_id')['relative_c4'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 最小レース数フィルタ
        min_races = self.config.min_corner_races
        c4_with_id.loc[c4_with_id['cum_count_gap'] < min_races, 'cum_gap_avg'] = np.nan
        c4_with_id.loc[c4_with_id['cum_count_gap'] < min_races, 'cum_relative_c4'] = np.nan
        
        # 最新統計を保存
        latest = c4_with_id.groupby('horse_id').last()[['cum_gap_avg', 'cum_relative_c4']].reset_index()
        self._horse_c4_gap_stats = latest.set_index('horse_id')['cum_gap_avg'].to_dict()
        self._horse_relative_c4_stats = latest.set_index('horse_id')['cum_relative_c4'].to_dict()
        
        logger.info(f"  C4馬身差統計: {len(self._horse_c4_gap_stats):,}頭")
        logger.info(f"  相対C4位置統計: {len(self._horse_relative_c4_stats):,}頭")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V15のtransform"""
        
        # 1. 親クラスのtransform（V14の特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV15: 追加transform開始")
        
        # 2. V15特徴量の計算
        result = self._calc_c4_gap_features(result)
        result = self._calc_post_style_conflict(result)
        result = self._calc_race_front_runner_features(result)
        
        logger.info("LeakFreeFeatureEngineerV15: transform完了")
        return result
    
    def _calc_c4_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        C4馬身差平均を付与
        
        【リーク対策】
        - fit時に計算した統計値をマップするのみ
        """
        logger.info("  C4馬身差を付与中...")
        
        df['horse_c4_gap_avg'] = df['horse_id'].map(self._horse_c4_gap_stats)
        
        non_null = df['horse_c4_gap_avg'].notna().sum()
        logger.info(f"    horse_c4_gap_avg非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_post_style_conflict(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        馬番×脚質不適合スコアを計算
        
        【計算方法】
        - relative_post = horse_number / field_size
        - avg_relative_c4 = 過去のC4相対位置平均（shift済み）
        - conflict = |relative_post - avg_relative_c4|
        
        【リーク対策】
        - avg_relative_c4はfit時に計算済み（shift適用済み）
        - 当日馬番と過去統計の組み合わせのみ
        """
        logger.info("  馬番×脚質不適合スコアを計算中...")
        
        df = df.copy()
        
        # 出走頭数を計算
        if 'field_size' not in df.columns:
            df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
        # 相対馬番
        df['relative_post'] = df['horse_number'] / df['field_size']
        
        # 過去の相対C4位置
        df['avg_relative_c4'] = df['horse_id'].map(self._horse_relative_c4_stats)
        
        # 不適合スコア（差の絶対値）
        # 大きいほど「枠順と過去の脚質が合っていない」
        df['post_style_conflict'] = (df['relative_post'] - df['avg_relative_c4']).abs()
        
        # 一時カラムを削除
        df = df.drop(columns=['relative_post', 'avg_relative_c4'], errors='ignore')
        
        non_null = df['post_style_conflict'].notna().sum()
        logger.info(f"    post_style_conflict非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_race_front_runner_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        レース内逃げ予備軍数と競合スコアを計算
        
        【計算方法】
        - race_front_runner_count: horse_front_runner_rate > threshold の馬をレース内でカウント
        - front_runner_competition: 自分以外の逃げ予備軍数
        
        【リーク対策】
        - horse_front_runner_rateは過去累積統計（shift済み）
        - 他馬の「過去統計」のみを使用
        """
        logger.info("  レース内逃げ予備軍特徴量を計算中...")
        
        df = df.copy()
        threshold = self.config.front_runner_threshold_rate
        
        # 逃げ予備軍フラグ
        df['_is_front_candidate'] = (df['horse_front_runner_rate'] > threshold).astype(float)
        df['_is_front_candidate'] = df['_is_front_candidate'].fillna(0)
        
        # レース内逃げ予備軍数
        df['race_front_runner_count'] = df.groupby('race_id')['_is_front_candidate'].transform('sum')
        
        # 逃げ競合スコア（自分以外）
        df['front_runner_competition'] = df['race_front_runner_count'] - df['_is_front_candidate']
        
        # 一時カラムを削除
        df = df.drop(columns=['_is_front_candidate'], errors='ignore')
        
        logger.info(f"    race_front_runner_count平均: {df['race_front_runner_count'].mean():.2f}")
        logger.info(f"    front_runner_competition平均: {df['front_runner_competition'].mean():.2f}")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

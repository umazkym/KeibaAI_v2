"""
リークフリー特徴量エンジニア V15.1

V15から冗長な特徴量を削減した最適化版。

【変更点】
- race_front_runner_count を削除（t検定で有意差なし、front_runner_competitionと冗長）
- front_runner_competition を削除（t検定で有意差なし）

【残す特徴量】
- horse_c4_gap_avg (t=-8.32***、強い予測力)
- post_style_conflict (t=-4.42***、予測力あり)

【期待効果】
- モデルの簡素化（55個 → 53個）
-特徴量間の冗長性を解消
- 過学習リスクのさらなる低減
"""

import logging
import pandas as pd
import numpy as np

from keibaai.src.features.leak_free_feature_engineer_v14 import LeakFreeFeatureEngineerV14

logger = logging.getLogger(__name__)


class LeakFreeFeatureEngineerV15_1(LeakFreeFeatureEngineerV14):
    """リークフリー特徴量エンジニア V15.1
    
    V15から冗長な特徴量を削減した最適化版。
    
    【追加特徴量】
    - horse_c4_gap_avg: 馬ごとの過去C4馬身差平均（前で競馬する力）
    - post_style_conflict: 馬番の相対位置と馬の過去相対C4位置の差（不適合度）
    
    【削減した特徴量】
    - race_front_runner_count: 削除（t検定で有意差なし）
    - front_runner_competition: 削除（t検定で有意差なし、冗長）
    """
    
    MIN_RACES_FOR_STATS = 5  # 過学習対策のためV14の3から強化
    
    def __init__(self):
        super().__init__()
        self._c4_gap_stats = None
        self._relative_c4_stats = None
    
    def fit(self, train_df: pd.DataFrame, pedigrees_df: pd.DataFrame, 
            corners_df: pd.DataFrame = None, race_details_df: pd.DataFrame = None,
            horses_df: pd.DataFrame = None) -> 'LeakFreeFeatureEngineerV15_1':
        """学習データでフィット"""
        super().fit(train_df, pedigrees_df, corners_df, race_details_df, horses_df)
        
        logger.info("LeakFreeFeatureEngineerV15_1: fit開始")
        
        # C4馬身差と相対C4位置の統計を計算
        self._calc_c4_gap_stats(train_df, corners_df)
        
        logger.info("LeakFreeFeatureEngineerV15_1: fit完了")
        return self
    
    def _calc_c4_gap_stats(self, train_df: pd.DataFrame, corners_df: pd.DataFrame):
        """C4馬身差・相対C4位置の累積統計を計算（V15と同じ実装）"""
        logger.info("  C4馬身差・相対位置統計を計算中...")
        
        if corners_df is None or len(corners_df) == 0:
            self._c4_gap_stats = {}
            self._relative_c4_stats = {}
            return
        
        # C4データを取得
        c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
        c4.columns = ['race_id', 'horse_number', 'c4_pos', 'c4_gap']
        
        # 履歴データとマージ
        if self._full_history is None:
            self._c4_gap_stats = {}
            self._relative_c4_stats = {}
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
        min_races = self.MIN_RACES_FOR_STATS
        c4_with_id.loc[c4_with_id['cum_count_gap'] < min_races, 'cum_gap_avg'] = np.nan
        c4_with_id.loc[c4_with_id['cum_count_gap'] < min_races, 'cum_relative_c4'] = np.nan
        
        # 最新統計を保存
        latest = c4_with_id.groupby('horse_id').last()[['cum_gap_avg', 'cum_relative_c4']].reset_index()
        self._c4_gap_stats = latest.set_index('horse_id')['cum_gap_avg'].to_dict()
        self._relative_c4_stats = latest.set_index('horse_id')['cum_relative_c4'].to_dict()
        
        logger.info(f"  C4馬身差統計: {len(self._c4_gap_stats)}頭")
        logger.info(f"  相対C4位置統計: {len(self._relative_c4_stats)}頭")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """特徴量変換"""
        df = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV15_1: 追加transform開始")
        
        # C4馬身差特徴量
        self._calc_c4_gap_features(df)
        
        # 馬番×脚質不適合スコア
        self._calc_post_style_conflict(df)
        
        logger.info("LeakFreeFeatureEngineerV15_1: transform完了")
        return df
    
    def _calc_c4_gap_features(self, df: pd.DataFrame):
        """C4馬身差特徴量を付与"""
        logger.info("  C4馬身差を付与中...")
        
        df['horse_c4_gap_avg'] = df['horse_id'].map(self._c4_gap_stats)
        
        non_nan = df['horse_c4_gap_avg'].notna().sum()
        logger.info(f"    horse_c4_gap_avg非NaN: {non_nan:,}/{len(df):,} ({non_nan/len(df)*100:.1f}%)")
    
    def _calc_post_style_conflict(self, df: pd.DataFrame):
        """馬番×脚質不適合スコアを計算（V15と同じ実装）"""
        logger.info("  馬番×脚質不適合スコアを計算中...")
        
        # 出走頭数を計算
        if 'field_size' not in df.columns:
            df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
        # 相対馬番
        df['relative_post'] = df['horse_number'] / df['field_size']
        
        # 過去の相対C4位置
        df['avg_relative_c4'] = df['horse_id'].map(self._relative_c4_stats)
        
        # 不適合スコア（差の絶対値）
        df['post_style_conflict'] = (df['relative_post'] - df['avg_relative_c4']).abs()
        
        # 一時カラムを削除
        df.drop(columns=['relative_post', 'avg_relative_c4'], inplace=True, errors='ignore')
        
        non_nan = df['post_style_conflict'].notna().sum()
        logger.info(f"    post_style_conflict非NaN: {non_nan:,}/{len(df):,} ({non_nan/len(df)*100:.1f}%)")
    
    def get_feature_columns(self):
        """使用特徴量リスト"""
        base_cols = super().get_feature_columns()
        new_cols = [
            'horse_c4_gap_avg',
            'post_style_conflict',
        ]
        return base_cols + new_cols

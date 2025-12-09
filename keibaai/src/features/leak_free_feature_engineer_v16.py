"""
リークフリー特徴量エンジニア V16

V15をベースに、時期×競馬場×条件別の「標準タイム」を活用した特徴量を追加。

【追加特徴量 (3個)】
1. venue_month_condition_standard_time: 場×距離×月×馬場状態の標準タイム（Train固定）
   - 目的: 時期・条件による馬場速度差のベースライン
   - リーク対策: fit時にTrain期間のみで計算

2. horse_time_deviation_avg: 馬の過去タイム偏差の累積平均
   - 計算: (実タイム - 標準タイム) の expanding().mean().shift(1)
   - 目的: 馬の「相対的な速さ」を条件差を補正して評価
   
3. horse_best_time_deviation: 馬の過去最小タイム偏差
   - 計算: (実タイム - 標準タイム) の expanding().min().shift(1)
   - 目的: 馬のベストパフォーマンスを評価

【リーク対策】
- 標準タイムはfit時（Train期間）のみで計算
- 全ての累積統計はshift(1)を適用
- 当日のレースタイムは使用しない（過去レースのみ）

【過学習対策】
- 標準タイムは最低10サンプル以上のグループのみ
- 累積統計は最低3レース以上
- 高次元組み合わせは追加しない
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
    # 標準タイム計算の最小サンプル数
    min_standard_time_samples: int = 10
    # タイム偏差計算の最小レース数
    min_time_deviation_races: int = 3
    # 最大タイム偏差（外れ値クリッピング）
    max_time_deviation: float = 10.0


class LeakFreeFeatureEngineerV16(LeakFreeFeatureEngineerV15):
    """
    リークフリー特徴量エンジニア V16
    
    V15に以下の特徴量を追加：
    - venue_month_condition_standard_time: 場×距離×月×馬場の標準タイム
    - horse_time_deviation_avg: 馬の過去タイム偏差平均
    - horse_best_time_deviation: 馬の過去ベストタイム偏差
    
    【重要】リーク対策
    - 標準タイムはfit時の1着馬タイムのみで計算
    - 全ての累積統計はshift(1)を適用
    """
    
    # V15の特徴量 + V16の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV15.FEATURE_COLS + [
        'venue_month_condition_standard_time',  # 標準タイム
        'horse_time_deviation_avg',             # タイム偏差平均
        'horse_best_time_deviation',            # ベストタイム偏差
    ]
    
    def __init__(self, config: Optional[FeatureConfigV16] = None):
        super().__init__(config or FeatureConfigV16())
        self._standard_times: Dict = {}
        self._horse_time_deviation_stats: Dict = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        fitメソッドの拡張
        
        - 標準タイムを計算（場×距離×月×馬場状態、1着馬のみ）
        - 馬の過去タイム偏差統計を事前計算
        """
        logger.info("LeakFreeFeatureEngineerV16: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # 標準タイムを計算（Train期間、1着馬のみ）
        self._calc_standard_times(races_df)
        
        # 馬の過去タイム偏差統計を計算
        self._calc_horse_time_deviation_stats(races_df)
        
        logger.info("LeakFreeFeatureEngineerV16: fit完了")
        return self
    
    def _calc_standard_times(self, races_df: pd.DataFrame):
        """
        標準タイムを計算（Train期間、1着馬のみ）
        
        【リーク対策】
        - fit時にTrain期間の1着馬タイムのみで計算
        - transform時はこの値をマップするのみ
        """
        logger.info("  標準タイムを計算中...")
        
        df = races_df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # 必要なカラムの確認
        required_cols = ['finish_position', 'finish_time_seconds', 'venue', 
                         'distance_m', 'track_surface', 'track_condition']
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            logger.warning(f"  標準タイム計算に必要なカラムがありません: {missing}")
            return
        
        # 1着馬のみ、タイム情報があるもの
        df_1st = df[
            (df['finish_position'] == 1) & 
            df['finish_time_seconds'].notna()
        ].copy()
        
        # 月を追加
        df_1st['month'] = df_1st['race_date'].dt.month
        
        # 場×距離×月×馬場状態でグループ化
        grouped = df_1st.groupby(
            ['venue', 'distance_m', 'track_surface', 'month', 'track_condition']
        ).agg({
            'finish_time_seconds': ['mean', 'std', 'count']
        })
        grouped.columns = ['std_time', 'std_time_std', 'count']
        grouped = grouped.reset_index()
        
        # 最小サンプル数を満たすものだけ採用
        min_samples = self.config.min_standard_time_samples
        valid_groups = grouped[grouped['count'] >= min_samples]
        
        # 辞書に変換（高速なルックアップのため）
        for _, row in valid_groups.iterrows():
            key = (row['venue'], row['distance_m'], row['track_surface'], 
                   row['month'], row['track_condition'])
            self._standard_times[key] = row['std_time']
        
        logger.info(f"  標準タイムグループ数: {len(self._standard_times)}")
    
    def _calc_horse_time_deviation_stats(self, races_df: pd.DataFrame):
        """
        馬の過去タイム偏差統計を計算（累積、shift済み）
        
        【リーク対策】
        - shift(1)で当該レースの結果を含めない
        - fit時に計算し、transform時はマップするのみ
        """
        logger.info("  馬のタイム偏差統計を計算中...")
        
        df = races_df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # 必要なカラムの確認
        if 'finish_time_seconds' not in df.columns:
            logger.warning("  finish_time_secondsがありません")
            return
        
        # 月を追加
        df['month'] = df['race_date'].dt.month
        
        # 標準タイムをマップ
        def get_standard_time(row):
            key = (row['venue'], row['distance_m'], row['track_surface'],
                   row['month'], row['track_condition'])
            return self._standard_times.get(key, np.nan)
        
        df['standard_time'] = df.apply(get_standard_time, axis=1)
        
        # タイム偏差を計算
        df['time_deviation'] = df['finish_time_seconds'] - df['standard_time']
        
        # 外れ値をクリッピング
        max_dev = self.config.max_time_deviation
        df['time_deviation'] = df['time_deviation'].clip(-max_dev, max_dev)
        
        # NaNを除外してソート
        df_valid = df[df['time_deviation'].notna()].copy()
        df_valid = df_valid.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 累積統計を計算（shift済み）
        df_valid['cum_time_dev_sum'] = df_valid.groupby('horse_id')['time_deviation'].transform(
            lambda x: x.cumsum().shift(1)
        )
        df_valid['cum_time_dev_count'] = df_valid.groupby('horse_id')['time_deviation'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        df_valid['cum_time_dev_min'] = df_valid.groupby('horse_id')['time_deviation'].transform(
            lambda x: x.expanding().min().shift(1)
        )
        
        # 平均を計算
        df_valid['time_dev_avg'] = np.where(
            df_valid['cum_time_dev_count'] >= self.config.min_time_deviation_races,
            df_valid['cum_time_dev_sum'] / df_valid['cum_time_dev_count'],
            np.nan
        )
        df_valid['time_dev_min'] = np.where(
            df_valid['cum_time_dev_count'] >= self.config.min_time_deviation_races,
            df_valid['cum_time_dev_min'],
            np.nan
        )
        
        # 馬ごとの最新統計を保存
        latest = df_valid.groupby('horse_id').last()[['time_dev_avg', 'time_dev_min']].reset_index()
        
        self._horse_time_deviation_stats = {
            'time_dev_avg': latest.set_index('horse_id')['time_dev_avg'].to_dict(),
            'time_dev_min': latest.set_index('horse_id')['time_dev_min'].to_dict()
        }
        
        non_nan_avg = sum(1 for v in self._horse_time_deviation_stats['time_dev_avg'].values() if pd.notna(v))
        logger.info(f"  タイム偏差統計: {len(self._horse_time_deviation_stats['time_dev_avg']):,}頭 (非NaN: {non_nan_avg:,})")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V16のtransform"""
        
        # 1. 親クラスのtransform（V15の特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV16: 追加transform開始")
        
        # 2. V16特徴量の計算
        result = self._add_standard_time(result)
        result = self._add_time_deviation_features(result)
        
        logger.info("LeakFreeFeatureEngineerV16: transform完了")
        return result
    
    def _add_standard_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        標準タイムを付与
        
        【リーク対策】
        - fit時に計算した統計値をマップするのみ
        """
        logger.info("  標準タイムを付与中...")
        
        df = df.copy()
        
        # 月を追加（一時的）
        if 'month' not in df.columns:
            df['_temp_month'] = pd.to_datetime(df['race_date']).dt.month
        else:
            df['_temp_month'] = df['month']
        
        # 標準タイムをマップ
        def get_standard_time(row):
            key = (row['venue'], row['distance_m'], row['track_surface'],
                   row['_temp_month'], row['track_condition'])
            return self._standard_times.get(key, np.nan)
        
        df['venue_month_condition_standard_time'] = df.apply(get_standard_time, axis=1)
        
        # 一時カラムを削除
        df = df.drop(columns=['_temp_month'], errors='ignore')
        
        non_null = df['venue_month_condition_standard_time'].notna().sum()
        logger.info(f"    standard_time非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _add_time_deviation_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        馬のタイム偏差特徴量を付与
        
        【リーク対策】
        - fit時に計算した統計値をマップするのみ
        - 当日のレースタイムは使用しない
        """
        logger.info("  タイム偏差特徴量を付与中...")
        
        df = df.copy()
        
        # タイム偏差平均をマップ
        df['horse_time_deviation_avg'] = df['horse_id'].map(
            self._horse_time_deviation_stats.get('time_dev_avg', {})
        )
        
        # ベストタイム偏差をマップ
        df['horse_best_time_deviation'] = df['horse_id'].map(
            self._horse_time_deviation_stats.get('time_dev_min', {})
        )
        
        non_null_avg = df['horse_time_deviation_avg'].notna().sum()
        non_null_best = df['horse_best_time_deviation'].notna().sum()
        
        logger.info(f"    time_deviation_avg非NaN: {non_null_avg:,}/{len(df):,} ({non_null_avg/len(df)*100:.1f}%)")
        logger.info(f"    best_time_deviation非NaN: {non_null_best:,}/{len(df):,} ({non_null_best/len(df)*100:.1f}%)")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

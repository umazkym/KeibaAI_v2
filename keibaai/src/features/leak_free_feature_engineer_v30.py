# -*- coding: utf-8 -*-
"""
リークフリー特徴量エンジニア V30

V15をベースに、タイムベース特徴量を追加。
「順位」だけでなく「レース内でのパフォーマンス（タイム）」を特徴量化。

【追加特徴量 (9個)】

1. 着差系（3個）
   - prev_margin_seconds: 前走の着差（秒）
   - prev3_avg_margin: 過去3走の平均着差
   - close_loss_flag: 前走が僅差負け（0.2秒以内）フラグ

2. 上がり系（3個）
   - prev_last3f_rank: 前走の上がり順位
   - prev3_avg_last3f_rank: 過去3走の平均上がり順位
   - prev_fastest_last3f_flag: 前走で上がり最速だったフラグ

3. レース内相対強さ系（3個）
   - prev_time_zscore: 前走のレース内z-score
   - prev3_avg_zscore: 過去3走の平均z-score
   - time_finish_combo: タイムパフォーマンス×着順の複合スコア

【人気通りにならない工夫】
- 着差やタイムは人気順（オッズ）に直接反映されにくい情報
- 「順位は悪いが僅差だった馬」を発見可能
- 上がり3ハロンは展開次第で人気低でも好タイムを出せる

【リーク対策】
- 全ての累積統計はshift(1)で計算
- レース内z-scoreは当該レースの結果を使用しない（前走のみ）
- min_races=5に維持

【過学習対策】
- 強い正則化パラメータを推奨
- 特徴量数を抑制（V15の66個 + 9個 = 75個）
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15, FeatureConfigV15

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV30(FeatureConfigV15):
    """V30用設定"""
    # 僅差判定閾値（秒）
    close_loss_threshold: float = 0.2
    # 上がり順位計算の最小レース数
    min_last3f_races: int = 5


class LeakFreeFeatureEngineerV30(LeakFreeFeatureEngineerV15):
    """
    リークフリー特徴量エンジニア V30
    
    V15に以下のタイムベース特徴量を追加：
    - 着差系: prev_margin_seconds, prev3_avg_margin, close_loss_flag
    - 上がり系: prev_last3f_rank, prev3_avg_last3f_rank, prev_fastest_last3f_flag
    - 相対強さ系: prev_time_zscore, prev3_avg_zscore, time_finish_combo
    
    【重要】
    - 人気通りにならないよう、タイム情報を活用
    - 過学習対策として強い正則化を推奨
    """
    
    # V15の特徴量 + V30の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV15.FEATURE_COLS + [
        # 着差系
        'prev_margin_seconds',          # 前走の着差（秒）
        'prev3_avg_margin',             # 過去3走の平均着差
        'close_loss_flag',              # 前走が僅差負け
        # 上がり系
        'prev_last3f_rank',             # 前走の上がり順位
        'prev3_avg_last3f_rank',        # 過去3走の平均上がり順位
        'prev_fastest_last3f_flag',     # 前走で上がり最速
        # 相対強さ系
        'prev_time_zscore',             # 前走のレース内z-score
        'prev3_avg_zscore',             # 過去3走の平均z-score
        'time_finish_combo',            # タイム×着順複合スコア
    ]
    
    def __init__(self, config: Optional[FeatureConfigV30] = None):
        super().__init__(config or FeatureConfigV30())
        # タイムベース統計の格納
        self._horse_margin_stats: Dict = {}
        self._horse_last3f_rank_stats: Dict = {}
        self._horse_zscore_stats: Dict = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        fitメソッドの拡張
        
        - 着差の累積統計を事前計算
        - 上がり順位の累積統計を事前計算
        - レース内z-scoreの累積統計を事前計算
        """
        logger.info("LeakFreeFeatureEngineerV30: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # タイムベース統計を事前計算
        self._calc_time_based_stats(races_df)
        
        logger.info("LeakFreeFeatureEngineerV30: fit完了")
        return self
    
    def _calc_time_based_stats(self, races_df: pd.DataFrame):
        """
        タイムベースの累積統計を計算（Train期間のみ）
        
        【リーク対策】
        - fit時にTrain期間のデータのみで計算
        - shift(1)適用済み
        """
        logger.info("  タイムベース統計を計算中...")
        
        if self._full_history is None:
            logger.warning("  履歴データがないため、スキップ")
            return
        
        # 履歴データをコピー
        hist = self._full_history.copy()
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # ===== 1. 着差統計 =====
        self._calc_margin_stats(hist)
        
        # ===== 2. 上がり順位統計 =====
        self._calc_last3f_stats(hist)
        
        # ===== 3. レース内z-score統計 =====
        self._calc_zscore_stats(hist)
    
    def _calc_margin_stats(self, hist: pd.DataFrame):
        """着差（margin_seconds）の累積統計"""
        logger.info("    着差統計を計算中...")
        
        if 'margin_seconds' not in hist.columns:
            logger.warning("    margin_secondsカラムがありません")
            return
        
        # 累積平均（直近3走）
        hist['_prev_margin'] = hist.groupby('horse_id')['margin_seconds'].shift(1)
        hist['_prev3_margin_avg'] = hist.groupby('horse_id')['margin_seconds'].transform(
            lambda x: x.shift(1).rolling(3, min_periods=1).mean()
        )
        
        # 僅差負けフラグ（前走）
        close_threshold = self.config.close_loss_threshold
        hist['_close_loss'] = (
            (hist['_prev_margin'].notna()) & 
            (hist['_prev_margin'] > 0) & 
            (hist['_prev_margin'] <= close_threshold)
        ).astype(float)
        
        # 最新値を記録
        latest = hist.groupby('horse_id').last()[
            ['_prev_margin', '_prev3_margin_avg', '_close_loss']
        ].reset_index()
        
        self._horse_margin_stats = {
            row['horse_id']: {
                'prev_margin': row['_prev_margin'],
                'prev3_avg_margin': row['_prev3_margin_avg'],
                'close_loss_flag': row['_close_loss']
            }
            for _, row in latest.iterrows()
        }
        
        logger.info(f"    着差統計: {len(self._horse_margin_stats):,}頭")
    
    def _calc_last3f_stats(self, hist: pd.DataFrame):
        """上がり3ハロン順位の累積統計"""
        logger.info("    上がり統計を計算中...")
        
        if 'last_3f_time' not in hist.columns:
            logger.warning("    last_3f_timeカラムがありません")
            return
        
        # レース内での上がり順位を計算
        hist['_last3f_rank'] = hist.groupby('race_id')['last_3f_time'].rank(method='first')
        
        # 上がり最速フラグ
        hist['_fastest_last3f'] = (hist['_last3f_rank'] == 1).astype(float)
        
        # 累積統計（shift(1)）
        hist['_prev_last3f_rank'] = hist.groupby('horse_id')['_last3f_rank'].shift(1)
        hist['_prev3_last3f_rank_avg'] = hist.groupby('horse_id')['_last3f_rank'].transform(
            lambda x: x.shift(1).rolling(3, min_periods=1).mean()
        )
        hist['_prev_fastest'] = hist.groupby('horse_id')['_fastest_last3f'].shift(1)
        
        # 最新値を記録
        latest = hist.groupby('horse_id').last()[
            ['_prev_last3f_rank', '_prev3_last3f_rank_avg', '_prev_fastest']
        ].reset_index()
        
        self._horse_last3f_rank_stats = {
            row['horse_id']: {
                'prev_last3f_rank': row['_prev_last3f_rank'],
                'prev3_avg_last3f_rank': row['_prev3_last3f_rank_avg'],
                'prev_fastest_last3f_flag': row['_prev_fastest']
            }
            for _, row in latest.iterrows()
        }
        
        logger.info(f"    上がり統計: {len(self._horse_last3f_rank_stats):,}頭")
    
    def _calc_zscore_stats(self, hist: pd.DataFrame):
        """レース内z-scoreの累積統計"""
        logger.info("    z-score統計を計算中...")
        
        if 'finish_time_seconds' not in hist.columns:
            logger.warning("    finish_time_secondsカラムがありません")
            return
        
        # レース内でのz-score（タイム）を計算
        race_mean = hist.groupby('race_id')['finish_time_seconds'].transform('mean')
        race_std = hist.groupby('race_id')['finish_time_seconds'].transform('std')
        hist['_time_zscore'] = (hist['finish_time_seconds'] - race_mean) / (race_std + 0.001)
        
        # 累積統計（shift(1)）
        hist['_prev_zscore'] = hist.groupby('horse_id')['_time_zscore'].shift(1)
        hist['_prev3_zscore_avg'] = hist.groupby('horse_id')['_time_zscore'].transform(
            lambda x: x.shift(1).rolling(3, min_periods=1).mean()
        )
        
        # 最新値を記録
        latest = hist.groupby('horse_id').last()[
            ['_prev_zscore', '_prev3_zscore_avg']
        ].reset_index()
        
        self._horse_zscore_stats = {
            row['horse_id']: {
                'prev_time_zscore': row['_prev_zscore'],
                'prev3_avg_zscore': row['_prev3_zscore_avg']
            }
            for _, row in latest.iterrows()
        }
        
        logger.info(f"    z-score統計: {len(self._horse_zscore_stats):,}頭")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V30のtransform"""
        
        # 1. 親クラスのtransform（V15の特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV30: 追加transform開始")
        
        # 2. タイムベース特徴量の付与
        result = self._apply_time_based_features(result)
        
        logger.info("LeakFreeFeatureEngineerV30: transform完了")
        return result
    
    def _apply_time_based_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        タイムベース特徴量を付与
        
        【リーク対策】
        - fit時に計算した統計値をマップするのみ
        """
        logger.info("  タイムベース特徴量を付与中...")
        
        df = df.copy()
        
        # ===== 1. 着差系 =====
        df['prev_margin_seconds'] = df['horse_id'].map(
            lambda x: self._horse_margin_stats.get(x, {}).get('prev_margin', np.nan)
        )
        df['prev3_avg_margin'] = df['horse_id'].map(
            lambda x: self._horse_margin_stats.get(x, {}).get('prev3_avg_margin', np.nan)
        )
        df['close_loss_flag'] = df['horse_id'].map(
            lambda x: self._horse_margin_stats.get(x, {}).get('close_loss_flag', np.nan)
        )
        
        # ===== 2. 上がり系 =====
        df['prev_last3f_rank'] = df['horse_id'].map(
            lambda x: self._horse_last3f_rank_stats.get(x, {}).get('prev_last3f_rank', np.nan)
        )
        df['prev3_avg_last3f_rank'] = df['horse_id'].map(
            lambda x: self._horse_last3f_rank_stats.get(x, {}).get('prev3_avg_last3f_rank', np.nan)
        )
        df['prev_fastest_last3f_flag'] = df['horse_id'].map(
            lambda x: self._horse_last3f_rank_stats.get(x, {}).get('prev_fastest_last3f_flag', np.nan)
        )
        
        # ===== 3. レース内相対強さ系 =====
        df['prev_time_zscore'] = df['horse_id'].map(
            lambda x: self._horse_zscore_stats.get(x, {}).get('prev_time_zscore', np.nan)
        )
        df['prev3_avg_zscore'] = df['horse_id'].map(
            lambda x: self._horse_zscore_stats.get(x, {}).get('prev3_avg_zscore', np.nan)
        )
        
        # ===== 4. 複合特徴量 =====
        # time_finish_combo: 前走の着順と前走のz-scoreを組み合わせ
        # → 「順位は悪いが実は惜しかった」馬を発見
        prev_finish = df.get('prev_finish_category', df.get('horse_last_finish', np.nan))
        if prev_finish is not None and 'prev_time_zscore' in df.columns:
            # z-scoreが低い（速い）ほど良い、着順が良いほど良い
            # → (-z-score) + (1/着順) を組み合わせ
            df['time_finish_combo'] = (
                -df['prev_time_zscore'].fillna(0) * 0.5 + 
                (1 / (df['horse_last_finish'].fillna(10) + 1)) * 0.5
            )
        else:
            df['time_finish_combo'] = np.nan
        
        # ログ出力
        for col in ['prev_margin_seconds', 'prev_last3f_rank', 'prev_time_zscore']:
            non_null = df[col].notna().sum()
            logger.info(f"    {col}非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

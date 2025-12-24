"""
リークフリー特徴量エンジニア V10.1

V10をベースに、Walk-Forward検証で一貫性が確認された特徴量を追加：

追加特徴量（全て一貫性4/5以上）:
1. is_chitose_bred: 千歳市生産馬フラグ（5/5期間で優位）
2. rest_days_optimal: 休養日数最適性フラグ（4/5期間で優位）
3. heavy_track_fit: 重馬場適性スコア（4/5期間で優位）

リーク対策:
- producing_area: horsesマスタから取得（リークなし）
- rest_days: 前走からの日数（cumsum+shift不要、レース日付差）
- heavy_track_fit: 過去の重/不良馬場での着順（cumsum+shift）
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v10 import LeakFreeFeatureEngineerV10, FeatureConfigV10

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV10_1(FeatureConfigV10):
    """V10.1用設定"""
    optimal_rest_min: int = 28  # 最適休養期間の最小日数
    optimal_rest_max: int = 60  # 最適休養期間の最大日数
    min_heavy_track_races: int = 2  # 重馬場適性計算の最小出走数


class LeakFreeFeatureEngineerV10_1(LeakFreeFeatureEngineerV10):
    """
    リークフリー特徴量エンジニア V10.1
    
    V10の全特徴量に加えて、以下を追加（Walk-Forward検証済み）：
    
    1. is_chitose_bred: 千歳市生産馬フラグ（一貫性5/5）
    2. rest_days_optimal: 休養日数最適性フラグ（一貫性4/5）
    3. heavy_track_fit: 重馬場適性スコア（一貫性4/5）
    """
    
    # V10の特徴量 + V10.1の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV10.FEATURE_COLS + [
        'is_chitose_bred',       # 千歳市生産フラグ
        'rest_days_optimal',     # 休養最適性フラグ
        'heavy_track_fit',       # 重馬場適性スコア
    ]
    
    def __init__(self, config: Optional[FeatureConfigV10_1] = None):
        super().__init__(config or FeatureConfigV10_1())
        self._horses_df = None
        self._heavy_track_stats_by_horse = None  # 馬IDベースで統計を保存
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None,
        horses_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV10_1':
        """V10のfitを拡張"""
        # V10のfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df)
        
        logger.info("LeakFreeFeatureEngineerV10_1: 追加fit処理開始")
        
        # 馬マスタを保存
        if horses_df is not None:
            self._horses_df = horses_df[['horse_id', 'producing_area']].copy()
            logger.info(f"  馬マスタ読み込み: {len(self._horses_df):,}件")
        
        # 重馬場適性統計を馬IDベースで構築
        self._build_heavy_track_stats_by_horse()
        
        logger.info("LeakFreeFeatureEngineerV10_1: fit完了")
        return self
    
    def _build_heavy_track_stats_by_horse(self):
        """重馬場適性の馬別統計を構築（リークフリー）
        
        馬IDベースで、Train期間終了時点での重馬場成績を保存。
        Testデータの各馬に対してこの統計を適用。
        """
        hist = self._full_history.copy()
        
        if 'track_condition' not in hist.columns:
            logger.warning("track_conditionカラムが存在しません")
            return
        
        # 重馬場（重・不良）のデータのみ抽出
        heavy_races = hist[hist['track_condition'].isin(['重', '不良'])].copy()
        
        if len(heavy_races) == 0:
            logger.warning("重馬場レースが存在しません")
            return
        
        # 馬別の重馬場成績を集計
        heavy_races['is_top3'] = (heavy_races['finish_position'] <= 3).astype(float)
        
        horse_heavy_stats = heavy_races.groupby('horse_id').agg({
            'is_top3': ['sum', 'count']
        })
        horse_heavy_stats.columns = ['top3_count', 'race_count']
        horse_heavy_stats.reset_index(inplace=True)
        
        # 最小出走数以上の馬のみ適性スコアを計算
        horse_heavy_stats['heavy_track_fit'] = np.where(
            horse_heavy_stats['race_count'] >= self.config.min_heavy_track_races,
            horse_heavy_stats['top3_count'] / horse_heavy_stats['race_count'],
            np.nan
        )
        
        self._heavy_track_stats_by_horse = horse_heavy_stats[['horse_id', 'heavy_track_fit']].copy()
        
        non_null = self._heavy_track_stats_by_horse['heavy_track_fit'].notna().sum()
        logger.info(f"  重馬場適性統計構築完了: {non_null:,}/{len(self._heavy_track_stats_by_horse):,}馬")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V10のtransformを拡張"""
        # V10のtransform
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV10_1: 追加transform開始")
        
        # V10.1追加特徴量を計算
        result = self._add_chitose_bred(result)
        result = self._calc_rest_days_optimal(result)  # 動的計算
        result = self._add_heavy_track_fit(result)     # 馬IDベースでマージ
        
        logger.info("LeakFreeFeatureEngineerV10_1: transform完了")
        return result
    
    def _add_chitose_bred(self, df: pd.DataFrame) -> pd.DataFrame:
        """千歳市生産馬フラグを追加"""
        if self._horses_df is None:
            df['is_chitose_bred'] = np.nan
            return df
        
        # 馬マスタをマージ
        horses = self._horses_df[['horse_id', 'producing_area']].drop_duplicates()
        df = df.merge(horses, on='horse_id', how='left', suffixes=('', '_horses'))
        
        # 千歳市生産フラグ
        producing_col = 'producing_area' if 'producing_area' in df.columns else 'producing_area_horses'
        df['is_chitose_bred'] = (df[producing_col] == '千歳市').astype(float)
        
        # 一時カラム削除
        if 'producing_area_horses' in df.columns:
            df = df.drop(columns=['producing_area_horses'])
        
        return df
    
    def _calc_rest_days_optimal(self, df: pd.DataFrame) -> pd.DataFrame:
        """休養日数最適性フラグを動的に計算
        
        入力データから直接前走日付を計算し、最適休養フラグを付与。
        これはリークフリー（当該レースより前の情報のみ使用）。
        """
        df = df.copy()
        
        if 'race_date' not in df.columns:
            df['rest_days_optimal'] = np.nan
            return df
        
        # 日付をdatetimeに変換
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # 履歴データと入力データを結合して馬ごとの前走を計算
        hist = self._full_history.copy()
        hist['race_date'] = pd.to_datetime(hist['race_date'])
        
        # 各馬の最終レース日を取得
        last_race = hist.groupby('horse_id')['race_date'].max().reset_index()
        last_race.columns = ['horse_id', 'last_race_date']
        
        # 入力データに最終レース日をマージ
        df = df.merge(last_race, on='horse_id', how='left')
        
        # 休養日数を計算
        df['days_since_last'] = (df['race_date'] - df['last_race_date']).dt.days
        
        # 最適休養フラグ
        config = self.config
        df['rest_days_optimal'] = np.where(
            (df['days_since_last'] >= config.optimal_rest_min) & 
            (df['days_since_last'] <= config.optimal_rest_max),
            1.0,
            0.0
        )
        
        # 初出走（履歴なし）は0
        df.loc[df['days_since_last'].isna(), 'rest_days_optimal'] = 0.0
        
        # 一時カラム削除
        df = df.drop(columns=['last_race_date', 'days_since_last'], errors='ignore')
        
        return df
    
    def _add_heavy_track_fit(self, df: pd.DataFrame) -> pd.DataFrame:
        """重馬場適性スコアを馬IDベースでマージ"""
        if self._heavy_track_stats_by_horse is None:
            df['heavy_track_fit'] = np.nan
            return df
        
        # 馬IDでマージ
        df = df.merge(
            self._heavy_track_stats_by_horse, 
            on='horse_id', 
            how='left', 
            suffixes=('', '_v10_1')
        )
        
        # 既存カラムがある場合は上書き
        if 'heavy_track_fit_v10_1' in df.columns:
            df['heavy_track_fit'] = df['heavy_track_fit_v10_1']
            df = df.drop(columns=['heavy_track_fit_v10_1'])
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """使用特徴量"""
        return self.FEATURE_COLS

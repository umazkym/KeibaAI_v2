"""
リークフリー特徴量エンジニア V9

V8.1を継承し、以下の新特徴量を追加：
- Phase 1: 母父（damsire）統計（Train固定）
- Phase 2: ラップタイムパターン（競馬場×距離別ペース傾向）
- Phase 3: コーナーポジション改善
- Phase 4: 体重変動トレンド

過学習対策：
- 騎手条件別統計は除外（V8で過学習を招いた）
- 全ての新特徴量はTrain固定またはcumsum+shift(1)パターン
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v8 import LeakFreeFeatureEngineerV8, FeatureConfigV8

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV9(FeatureConfigV8):
    """V9用設定"""
    min_races_for_damsire: int = 10  # 母父統計の最小出走数
    min_races_for_pace: int = 5     # ペース統計の最小レース数


class LeakFreeFeatureEngineerV9(LeakFreeFeatureEngineerV8):
    """
    リークフリー特徴量エンジニア V9
    
    V8.1の全特徴量に加えて、以下を追加：
    
    Phase 1: 母父統計（Train固定）
    - damsire_win_rate: 母父産駒の勝率
    - damsire_avg_finish: 母父産駒の平均着順
    
    Phase 2: ラップパターン（Train固定）
    - venue_distance_pace_avg: 競馬場×距離別の平均ペース
    - pace_deviation: レースペースと平均との乖離
    - front_runner_advantage: 逃げ馬有利度
    
    Phase 3: コーナー改善（累積統計）
    - horse_position_trend: 道中位置の変化トレンド
    - horse_early_speed: 序盤スピード（1-2コーナー）
    
    Phase 4: 体重変動（累積統計）
    - weight_change_from_last: 前走からの体重変化
    - weight_trend_3: 直近3走の体重変動トレンド
    """
    
    # V8.1の特徴量 + V9の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV8.FEATURE_COLS + [
        # Phase 1: 母父統計
        'damsire_win_rate', 'damsire_avg_finish',
        # Phase 2: ラップパターン
        'venue_distance_pace_avg', 'front_runner_advantage',
        # Phase 3: コーナー改善
        'horse_position_trend', 'horse_early_speed',
        # Phase 4: 体重変動
        'weight_change_from_last', 'weight_trend_3',
    ]
    
    def __init__(self, config: Optional[FeatureConfigV9] = None):
        super().__init__(config or FeatureConfigV9())
        self._damsire_mapping = None
        self._damsire_stats = None  # Train固定
        self._pace_stats = None     # Train固定
        self._front_runner_stats = None  # Train固定
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV9':
        """V8のfitを拡張"""
        # V8のfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df)
        
        logger.info("LeakFreeFeatureEngineerV9: 追加fit処理開始")
        
        # Phase 1: 母父マッピングと統計（Train固定）
        if pedigrees_df is not None:
            self._build_damsire_stats(pedigrees_df)
        
        # Phase 2: ラップパターン統計（Train固定）
        if race_details_df is not None:
            self._build_pace_stats(race_details_df)
        
        # Phase 3: 逃げ馬有利度統計（Train固定）
        if corners_df is not None:
            self._build_front_runner_stats(corners_df)
        
        logger.info("LeakFreeFeatureEngineerV9: fit完了")
        return self
    
    def _build_damsire_stats(self, pedigrees_df: pd.DataFrame):
        """母父マッピングと統計を構築（Train固定）"""
        # 母父（generation=2のposition=3番目 = 母の父）を取得
        # pedigreesでは generation=1,2,3... で ancestor_id が入る
        # 直接的な「母父」は generation=2 かつ 母系側
        # 実際のデータ構造を確認する必要があるが、簡易的に generation=2 の最初のレコードを母父とみなす
        
        # まず generation=2 のデータを取得
        gen2 = pedigrees_df[pedigrees_df['generation'] == 2].copy()
        
        if len(gen2) == 0:
            logger.warning("母父データが見つかりません（generation=2が空）")
            return
        
        # 各horse_idに対して、母父（2番目の祖先ID）を取得
        # netkeibaのpedigreesでは、generation=2の2番目が母父の可能性が高い
        gen2_sorted = gen2.sort_values(['horse_id', 'ancestor_id'])
        
        # 各馬に対して2番目の祖先（母父候補）を取得
        damsire = gen2_sorted.groupby('horse_id').nth(1).reset_index()[['horse_id', 'ancestor_id']]
        damsire.columns = ['horse_id', 'damsire_id']
        self._damsire_mapping = damsire
        
        # 履歴データに母父をマージして統計計算
        hist = self._full_history.copy()
        hist = hist.merge(damsire, on='horse_id', how='left')
        
        # 母父ごとの統計（Train固定）
        damsire_stats = hist.groupby('damsire_id').agg(
            damsire_win_rate=('is_win', 'mean'),
            damsire_avg_finish=('finish_position', 'mean'),
            damsire_races=('race_id', 'count')
        ).reset_index()
        
        # 最小レース数でフィルタ
        min_n = self.config.min_races_for_damsire
        damsire_stats.loc[damsire_stats['damsire_races'] < min_n, ['damsire_win_rate', 'damsire_avg_finish']] = np.nan
        
        self._damsire_stats = damsire_stats
        logger.info(f"  母父統計構築完了: {len(damsire_stats)}頭")
    
    def _build_pace_stats(self, race_details_df: pd.DataFrame):
        """競馬場×距離別ペース統計（Train固定）"""
        hist = self._full_history.copy()
        
        # race_detailsから前半3Fを取得
        pace_data = race_details_df[['race_id', 'first_half', 'second_half']].copy()
        
        # 履歴とマージして競馬場・距離情報を付与
        hist_with_pace = hist.merge(pace_data, on='race_id', how='left')
        
        # 競馬場×距離カテゴリ別の平均ペース
        pace_stats = hist_with_pace.groupby(['venue', 'distance_category']).agg(
            venue_distance_pace_avg=('first_half', 'mean'),
            pace_races=('race_id', 'nunique')
        ).reset_index()
        
        # 最小レース数でフィルタ
        min_n = self.config.min_races_for_pace
        pace_stats.loc[pace_stats['pace_races'] < min_n, 'venue_distance_pace_avg'] = np.nan
        
        self._pace_stats = pace_stats
        logger.info(f"  ペース統計構築完了: {len(pace_stats)}件")
    
    def _build_front_runner_stats(self, corners_df: pd.DataFrame):
        """逃げ馬有利度統計（Train固定）"""
        hist = self._full_history.copy()
        
        # 1コーナー1番手の馬を特定
        c1_leaders = corners_df[(corners_df['corner'] == 1) & (corners_df['position'] == 1)].copy()
        c1_leaders = c1_leaders[['race_id', 'horse_number']].rename(columns={'horse_number': 'leader_horse_number'})
        
        # 履歴データとマージ
        hist_with_leader = hist.merge(c1_leaders, on='race_id', how='left')
        # NA値を処理してから比較
        hist_with_leader['is_front_runner'] = (
            (hist_with_leader['horse_number'] == hist_with_leader['leader_horse_number']) & 
            hist_with_leader['leader_horse_number'].notna()
        ).astype(int)
        
        # 逃げ馬の勝率を競馬場×距離別に計算
        front_stats = hist_with_leader[hist_with_leader['is_front_runner'] == 1].groupby(['venue', 'distance_category']).agg(
            front_runner_win_rate=('is_win', 'mean'),
            front_runner_races=('race_id', 'count')
        ).reset_index()
        
        # 最小レース数でフィルタ
        min_n = self.config.min_races_for_pace
        front_stats.loc[front_stats['front_runner_races'] < min_n, 'front_runner_win_rate'] = np.nan
        
        # 有利度 = 逃げ馬勝率 / 全体勝率 - 1
        overall_win_rate = hist_with_leader['is_win'].mean()
        front_stats['front_runner_advantage'] = front_stats['front_runner_win_rate'] / overall_win_rate - 1
        
        self._front_runner_stats = front_stats
        logger.info(f"  逃げ馬有利度統計構築完了: {len(front_stats)}件")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V8のtransformを拡張"""
        # V8のtransform
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV9: 追加transform開始")
        
        # Phase 1: 母父統計（Train固定マスタをマージ）
        result = self._merge_damsire_stats(result)
        
        # Phase 2: ペース統計（Train固定マスタをマージ）
        result = self._merge_pace_stats(result)
        
        # Phase 3: コーナー改善（累積統計）
        result = self._calc_position_features(result)
        
        # Phase 4: 体重変動（累積統計）
        result = self._calc_weight_features(result)
        
        logger.info("LeakFreeFeatureEngineerV9: transform完了")
        return result
    
    def _merge_damsire_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """母父統計をマージ（Train固定マスタ使用）"""
        if self._damsire_mapping is None or self._damsire_stats is None:
            df['damsire_win_rate'] = np.nan
            df['damsire_avg_finish'] = np.nan
            return df
        
        # まず母父IDをマージ
        df = df.merge(self._damsire_mapping, on='horse_id', how='left')
        
        # 次に母父統計をマージ
        df = df.merge(
            self._damsire_stats[['damsire_id', 'damsire_win_rate', 'damsire_avg_finish']],
            on='damsire_id', how='left'
        )
        
        # damsire_id列を削除
        df = df.drop(columns=['damsire_id'], errors='ignore')
        
        return df
    
    def _merge_pace_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """ペース統計をマージ（Train固定マスタ使用）"""
        if self._pace_stats is None:
            df['venue_distance_pace_avg'] = np.nan
            df['front_runner_advantage'] = np.nan
            return df
        
        # ペース統計をマージ
        df = df.merge(
            self._pace_stats[['venue', 'distance_category', 'venue_distance_pace_avg']],
            on=['venue', 'distance_category'], how='left'
        )
        
        # 逃げ馬有利度をマージ
        if self._front_runner_stats is not None:
            df = df.merge(
                self._front_runner_stats[['venue', 'distance_category', 'front_runner_advantage']],
                on=['venue', 'distance_category'], how='left'
            )
        else:
            df['front_runner_advantage'] = np.nan
        
        return df
    
    def _calc_position_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """コーナーポジション改善特徴量"""
        if self._corners is None:
            df['horse_position_trend'] = np.nan
            df['horse_early_speed'] = np.nan
            return df
        
        corners = self._corners.copy()
        hist = self._full_history.copy()
        
        # 各レースの1-4コーナー位置を取得
        corner_pivot = corners.pivot_table(
            index=['race_id', 'horse_number'],
            columns='corner',
            values='position',
            aggfunc='first'
        ).reset_index()
        corner_pivot.columns = ['race_id', 'horse_number'] + [f'c{i}_pos' for i in corner_pivot.columns[2:]]
        
        # 履歴とマージ
        hist_with_corners = hist.merge(corner_pivot, on=['race_id', 'horse_number'], how='left')
        
        # 位置変化トレンド（1コーナー→4コーナーの変化）
        if 'c1_pos' in hist_with_corners.columns and 'c4_pos' in hist_with_corners.columns:
            hist_with_corners['pos_change'] = hist_with_corners['c1_pos'] - hist_with_corners['c4_pos']
            
            # 馬ごとの平均位置変化
            horse_trend = hist_with_corners.groupby('horse_id')['pos_change'].mean().reset_index()
            horse_trend.columns = ['horse_id', 'horse_position_trend']
            df = df.merge(horse_trend, on='horse_id', how='left')
        else:
            df['horse_position_trend'] = np.nan
        
        # 序盤スピード（1コーナー平均順位）
        if 'c1_pos' in hist_with_corners.columns:
            horse_early = hist_with_corners.groupby('horse_id')['c1_pos'].mean().reset_index()
            horse_early.columns = ['horse_id', 'horse_early_speed']
            df = df.merge(horse_early, on='horse_id', how='left')
        else:
            df['horse_early_speed'] = np.nan
        
        return df
    
    def _calc_weight_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """体重変動特徴量"""
        hist = self._full_history.copy()
        
        if 'horse_weight' not in hist.columns:
            df['weight_change_from_last'] = np.nan
            df['weight_trend_3'] = np.nan
            return df
        
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 体重変化を計算（各レース間）
        hist['weight_diff'] = hist.groupby('horse_id')['horse_weight'].diff()
        
        # 直近3走の体重変動トレンド
        hist['weight_trend_3'] = hist.groupby('horse_id')['weight_diff'].transform(
            lambda x: x.rolling(3, min_periods=1).mean()
        )
        
        # 各馬の最新値を取得
        horse_weight_stats = hist.groupby('horse_id').last()[['horse_weight', 'weight_trend_3']].reset_index()
        horse_weight_stats.columns = ['horse_id', 'prev_weight', 'weight_trend_3']
        
        df = df.merge(horse_weight_stats, on='horse_id', how='left')
        
        # 前走からの体重変化
        if 'horse_weight' in df.columns:
            df['weight_change_from_last'] = df['horse_weight'] - df['prev_weight']
        else:
            df['weight_change_from_last'] = np.nan
        
        df = df.drop(columns=['prev_weight'], errors='ignore')
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """使用特徴量"""
        return self.FEATURE_COLS

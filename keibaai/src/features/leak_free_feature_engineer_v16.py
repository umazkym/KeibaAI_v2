"""
リークフリー特徴量エンジニア V16

V15に、race_details.parquetを活用した個馬のペース適性特徴量を統合。

【追加特徴量 (3個)】
1. horse_pace_preference: 馬の得意ペース（好走時のpace_diff累積平均、shift済み）
2. horse_avg_pace_lf: 馬の全レース平均ペース（累積平均、shift済み）
3. pace_fit: 馬のペースとコースのペースの適合度（差の絶対値×-1）

【根拠】
- race_details.parquet（39,811件）のfirst_half/second_halfを活用
- pace_features.pyで実装済みのロジックを統合
- 穴馬発掘に寄与する「ペースミスマッチ」を検出

【リーク対策】
- horse_pace_preference: expanding().mean().shift(1)で当該レース前までの情報のみ使用
- venue_surface_pace_tendency: Train期間（train_cutoff以前）のデータのみで計算
- レース結果（通過順、上がり3F等）は使用しない

【過学習対策】
- min_races=5に強化
- 複雑な組み合わせ特徴量は追加しない
- V17-V20の教訓を活かし、効果がなければロールバック可能
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
    # ペース特徴量の最小レース数
    min_pace_races: int = 5
    # 会場×馬場のペース統計サンプル数閾値
    min_pace_samples: int = 30
    # Train期間のペース統計cutoff
    pace_train_cutoff: str = '2023-01-01'


class LeakFreeFeatureEngineerV16(LeakFreeFeatureEngineerV15):
    """
    リークフリー特徴量エンジニア V16
    
    V15にペース適性特徴量を追加：
    - horse_pace_preference: 馬の得意ペース（好走時）
    - horse_avg_pace_lf: 馬の全レース平均ペース
    - pace_fit: ペース適合度
    - venue_surface_pace_tendency: 会場×馬場のペース傾向（V14と別実装）
    
    【重要】過学習対策
    - min_pace_races=5に強化
    - Train期間のみでコース統計を固定
    """
    
    # V15の特徴量 + V16の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV15.FEATURE_COLS + [
        'horse_pace_preference',      # 馬の得意ペース（好走時）
        'horse_avg_pace_lf',          # 馬の全レース平均ペース
        'pace_fit_score',             # ペース適合度スコア
        'venue_surface_pace_trend',   # 会場×馬場のペース傾向（別名でV14と区別）
    ]
    
    def __init__(self, config: Optional[FeatureConfigV16] = None):
        super().__init__(config or FeatureConfigV16())
        self._venue_surface_pace_stats: Dict = {}
        self._race_details_merged: Optional[pd.DataFrame] = None
        self._horse_pace_stats: Dict[str, Dict] = {}  # horse_id -> {preference, avg_pace}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        fitメソッドの拡張
        
        - race_detailsからペース統計を事前計算
        - 会場×馬場のペース傾向をTrain期間で固定
        - 馬ごとのペース適性を累積計算
        """
        logger.info("LeakFreeFeatureEngineerV16: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # ペース関連の事前計算
        if race_details_df is not None and len(race_details_df) > 0:
            self._prepare_pace_features(races_df, race_details_df)
        else:
            logger.warning("  race_detailsがないため、ペース特徴量はスキップ")
        
        logger.info("LeakFreeFeatureEngineerV16: fit完了")
        return self
    
    def _prepare_pace_features(self, races_df: pd.DataFrame, race_details_df: pd.DataFrame):
        """
        ペース特徴量の事前計算（リークフリー）
        
        1. 会場×馬場のペース傾向（Train期間で固定）
        2. 馬ごとのペース適性（累積・shift済み）
        """
        logger.info("  ペース特徴量の事前計算中...")
        
        # 型変換
        rd = race_details_df.copy()
        rd['race_id'] = rd['race_id'].astype(str)
        
        # races_dfからレース情報を取得
        races = races_df.copy()
        races['race_id'] = races['race_id'].astype(str)
        races['race_date'] = pd.to_datetime(races['race_date'])
        
        # race_detailsにレース情報を結合
        race_info = races.drop_duplicates('race_id')[['race_id', 'race_date', 'venue', 'track_surface', 'distance_m']]
        rd_merged = rd.merge(race_info, on='race_id', how='left')
        
        # ペース差を計算 (first_half - second_half)
        # 正: 前傾（ハイペース）、負: 後傾（スローペース）
        rd_merged['first_half'] = pd.to_numeric(rd_merged['first_half'], errors='coerce')
        rd_merged['second_half'] = pd.to_numeric(rd_merged['second_half'], errors='coerce')
        rd_merged['pace_diff'] = rd_merged['first_half'] - rd_merged['second_half']
        
        self._race_details_merged = rd_merged
        
        # 1. 会場×馬場のペース傾向（Train期間で固定）
        self._calc_venue_surface_pace_tendency(rd_merged)
        
        # 2. 馬ごとのペース適性（累積・shift済み）
        self._calc_horse_pace_stats(races, rd_merged)
    
    def _calc_venue_surface_pace_tendency(self, rd_merged: pd.DataFrame):
        """
        【特徴量1】会場×馬場のペース傾向（Train期間固定）
        
        リーク防止: Train期間（pace_train_cutoff以前）のデータのみで計算
        """
        logger.info("    会場×馬場ペース傾向を計算中...（Train期間固定）")
        
        train_cutoff = pd.Timestamp(self.config.pace_train_cutoff)
        
        # Train期間のデータのみで統計計算
        train_rd = rd_merged[rd_merged['race_date'] < train_cutoff].copy()
        
        if len(train_rd) == 0:
            logger.warning("    Train期間のrace_detailsがありません")
            return
        
        # 会場×馬場別の平均ペース差
        pace_stats = train_rd.groupby(['venue', 'track_surface'])['pace_diff'].agg(['mean', 'std', 'count']).reset_index()
        
        # サンプル数が少ない場合はNaN
        min_samples = self.config.min_pace_samples
        pace_stats.loc[pace_stats['count'] < min_samples, 'mean'] = np.nan
        
        # Dictに保存
        self._venue_surface_pace_stats = {}
        for _, row in pace_stats.iterrows():
            if pd.notna(row['mean']):
                key = (row['venue'], row['track_surface'])
                self._venue_surface_pace_stats[key] = row['mean']
        
        logger.info(f"    ペース統計パターン数: {len(self._venue_surface_pace_stats)}")
    
    def _calc_horse_pace_stats(self, races_df: pd.DataFrame, rd_merged: pd.DataFrame):
        """
        【特徴量2,3】馬ごとのペース適性（累積・shift済み）
        
        - horse_pace_preference: 好走時（3着以内）のペース傾向
        - horse_avg_pace_lf: 全レースのペース傾向
        
        リーク対策: expanding().mean().shift(1)で当該レース前までの情報のみ使用
        """
        logger.info("    馬ごとのペース適性を計算中...（累積・shift済み）")
        
        # races_dfからhorse_id, race_date, finish_positionを取得
        perf = races_df[['race_id', 'horse_id', 'race_date', 'finish_position']].copy()
        perf['horse_id'] = perf['horse_id'].astype(str)
        perf['race_id'] = perf['race_id'].astype(str)
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce')
        
        # race_detailsのペース情報を結合
        perf = perf.merge(
            rd_merged[['race_id', 'pace_diff']],
            on='race_id',
            how='left'
        )
        
        # pace_diffがないレースを除外
        perf = perf.dropna(subset=['pace_diff'])
        
        if len(perf) == 0:
            logger.warning("    pace_diffのあるレースがありません")
            return
        
        # 3着以内のレースのみ考慮（好走時のペース傾向を学習）
        perf['is_good_run'] = (perf['finish_position'] <= 3).fillna(False)
        perf['pace_diff_good'] = np.where(perf['is_good_run'], perf['pace_diff'], np.nan)
        
        # 時系列でソート
        perf = perf.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 累積平均（shift(1)で当該レース前まで）
        perf['horse_pace_preference'] = perf.groupby('horse_id')['pace_diff_good'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 全レースの平均ペースも計算（好走時に限らない）
        perf['horse_avg_pace_lf'] = perf.groupby('horse_id')['pace_diff'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # レース数カウント
        perf['pace_race_count'] = perf.groupby('horse_id')['pace_diff'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        
        # 最小レース数フィルタ
        min_races = self.config.min_pace_races
        perf.loc[perf['pace_race_count'] < min_races, 'horse_pace_preference'] = np.nan
        perf.loc[perf['pace_race_count'] < min_races, 'horse_avg_pace_lf'] = np.nan
        
        # horse_idごとの最新統計を取得
        latest = perf.groupby('horse_id').last()[['horse_pace_preference', 'horse_avg_pace_lf']].reset_index()
        
        # Dictに保存
        self._horse_pace_stats = {}
        for _, row in latest.iterrows():
            self._horse_pace_stats[row['horse_id']] = {
                'preference': row['horse_pace_preference'],
                'avg_pace': row['horse_avg_pace_lf']
            }
        
        valid_count = sum(1 for v in self._horse_pace_stats.values() if pd.notna(v['preference']))
        logger.info(f"    馬ペース統計: {len(self._horse_pace_stats):,}頭, 有効: {valid_count:,}頭")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V16のtransform"""
        
        # 1. 親クラスのtransform（V15の特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV16: 追加transform開始")
        
        # 2. V16ペース特徴量の計算
        result = self._add_pace_features(result)
        
        logger.info("LeakFreeFeatureEngineerV16: transform完了")
        return result
    
    def _add_pace_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ペース特徴量を付与
        
        1. venue_surface_pace_trend: 会場×馬場のペース傾向
        2. horse_pace_preference: 馬の得意ペース
        3. horse_avg_pace_lf: 馬の全レース平均ペース
        4. pace_fit_score: ペース適合度スコア
        """
        logger.info("  ペース特徴量を付与中...")
        
        df = df.copy()
        
        # 1. 会場×馬場のペース傾向
        def get_venue_pace(row):
            key = (row['venue'], row['track_surface'])
            return self._venue_surface_pace_stats.get(key, 0.0)
        
        if self._venue_surface_pace_stats:
            df['venue_surface_pace_trend'] = df.apply(get_venue_pace, axis=1)
        else:
            df['venue_surface_pace_trend'] = 0.0
        
        # 2. 馬のペース適性
        def get_horse_pace_preference(horse_id):
            stats = self._horse_pace_stats.get(str(horse_id), {})
            return stats.get('preference', np.nan)
        
        def get_horse_avg_pace(horse_id):
            stats = self._horse_pace_stats.get(str(horse_id), {})
            return stats.get('avg_pace', np.nan)
        
        if self._horse_pace_stats:
            df['horse_pace_preference'] = df['horse_id'].apply(get_horse_pace_preference)
            df['horse_avg_pace_lf'] = df['horse_id'].apply(get_horse_avg_pace)
        else:
            df['horse_pace_preference'] = np.nan
            df['horse_avg_pace_lf'] = np.nan
        
        # 3. ペース適合度スコア
        # 馬の得意ペースとコースのペースの差（小さいほど良い）の符号反転
        horse_pref = df['horse_pace_preference'].fillna(0)
        venue_pace = df['venue_surface_pace_trend'].fillna(0)
        df['pace_fit_score'] = -np.abs(horse_pref - venue_pace)
        
        # 欠損値は0（影響なし）
        df['pace_fit_score'] = df['pace_fit_score'].fillna(0)
        
        # ログ出力
        non_null_pref = df['horse_pace_preference'].notna().sum()
        non_null_avg = df['horse_avg_pace_lf'].notna().sum()
        logger.info(f"    horse_pace_preference非NaN: {non_null_pref:,}/{len(df):,} ({non_null_pref/len(df)*100:.1f}%)")
        logger.info(f"    horse_avg_pace_lf非NaN: {non_null_avg:,}/{len(df):,} ({non_null_avg/len(df)*100:.1f}%)")
        logger.info(f"    pace_fit_score平均: {df['pace_fit_score'].mean():.3f}")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

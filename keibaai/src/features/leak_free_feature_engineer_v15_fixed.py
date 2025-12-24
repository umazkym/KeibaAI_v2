"""
リークフリー特徴量エンジニア V15 Fixed

V15のコーナー位置特徴量のカバレッジ問題を修正。

【問題】
V15ではfit時に「訓練期間の最新統計のみ」を保存していたため、
テスト期間の馬（fit期間に出走履歴がない馬）で特徴量が欠損していた。
→ カバレッジ14.5%

【解決策】
transform時に、各レースより前のデータのみを使用して累積統計を計算。
→ カバレッジ50%以上を目標

【リーク対策（厳格）】
1. 各レースでの計算時、そのレース以前のデータのみを使用
2. shift(1)を厳守し、当該レースのデータは絶対に使わない
3. race_dateでソートしてから処理
4. 未来のレース結果は一切参照しない

【過学習対策】
- min_races=3に緩和（92%カバー、統計的に十分）
- 特徴量数は変更なし（V15と同じ）
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v14 import LeakFreeFeatureEngineerV14, FeatureConfigV14
from ..utils.course_feature_provider import CourseFeatureProvider

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV15Fixed(FeatureConfigV14):
    """V15 Fixed用設定"""
    # 逃げ予備軍判定閾値
    front_runner_threshold_rate: float = 0.3
    # 最小レース数要件（3に緩和してカバレッジ向上）
    min_corner_races: int = 3


class LeakFreeFeatureEngineerV15Fixed(LeakFreeFeatureEngineerV14):
    """
    リークフリー特徴量エンジニア V15 Fixed
    
    V15と同じ特徴量だが、カバレッジ向上のため
    transform時に累積計算を行う方式に変更。
    
    【追加特徴量】（V15と同じ）
    - race_front_runner_count: レース内逃げ予備軍数
    - horse_c4_gap_avg: 過去C4馬身差平均
    - post_style_conflict: 馬番×脚質不適合スコア
    - front_runner_competition: 逃げ競合スコア
    - コース特徴量各種
    """
    
    # V14の特徴量 + V15の新特徴量（V15と同じリスト）
    FEATURE_COLS = LeakFreeFeatureEngineerV14.FEATURE_COLS + [
        'race_front_runner_count',
        'horse_c4_gap_avg',
        'post_style_conflict',
        'front_runner_competition',
        'course_corner_count',
        'course_start_to_corner_m',
        'course_final_straight_m',
        'course_slope_percent',
        'course_is_outer',
        'course_turn_direction',
        'straight_ratio',
        'is_long_straight',
        'style_straight_match',
        'front_slope_disadvantage',
        'closer_long_straight_advantage',
    ]
    
    def __init__(self, config: Optional[FeatureConfigV15Fixed] = None):
        super().__init__(config or FeatureConfigV15Fixed())
        self._course_provider = CourseFeatureProvider()
        # コーナーデータをfit時に保存
        self._corners_df: Optional[pd.DataFrame] = None
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """fitメソッドの拡張"""
        logger.info("LeakFreeFeatureEngineerV15Fixed: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # コーナーデータを保存（transform時に使用）
        if corners_df is not None:
            self._corners_df = corners_df.copy()
            self._corners_df['race_id'] = self._corners_df['race_id'].astype(str)
            logger.info(f"  コーナーデータ保存: {len(self._corners_df):,}件")
        
        logger.info("LeakFreeFeatureEngineerV15Fixed: fit完了")
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V15 Fixedのtransform"""
        
        # 1. 親クラスのtransform（V14の特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV15Fixed: 追加transform開始")
        
        # 2. コーナー位置特徴量（改良版：transform時に計算）
        result = self._calc_corner_features_improved(result)
        
        # 3. コース特徴量（V15と同じ）
        result = self._calc_course_features(result)
        
        logger.info("LeakFreeFeatureEngineerV15Fixed: transform完了")
        return result
    
    def _calc_corner_features_improved(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        コーナー位置特徴量を計算（改良版）
        
        【改善点】
        - transform時に各レースより前のデータから累積統計を計算
        - これにより、fit期間に出走履歴がない馬でも特徴量を計算可能
        
        【リーク対策】
        - 各レースのrace_date以前のコーナーデータのみを使用
        - shift(1)相当の処理を内部で実施
        """
        logger.info("  コーナー特徴量（改良版）を計算中...")
        
        df = df.copy()
        df['race_id'] = df['race_id'].astype(str)
        df['horse_id'] = df['horse_id'].astype(str)
        
        # デフォルト値を設定
        df['horse_c4_gap_avg'] = np.nan
        df['horse_front_runner_rate'] = np.nan
        df['post_style_conflict'] = np.nan
        df['race_front_runner_count'] = 0.0
        df['front_runner_competition'] = 0.0
        
        if self._corners_df is None or len(self._corners_df) == 0:
            logger.warning("  コーナーデータがありません")
            return df
        
        # ===== Step 1: C4データの準備 =====
        c4 = self._corners_df[self._corners_df['corner'] == 4].copy()
        c4 = c4[['race_id', 'horse_number', 'position', 'gap_from_leader']]
        c4.columns = ['race_id', 'horse_number', 'c4_pos', 'c4_gap']
        
        # ===== 履歴データを構築 =====
        # _full_historyとdfの両方を使用して履歴データを作成
        # これにより、fit期間+transform期間の両方のデータを使える
        
        hist_parts = []
        
        # 1. fit時のデータ（_full_history）
        if self._full_history is not None:
            hist_fit = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date']].copy()
            hist_fit['race_id'] = hist_fit['race_id'].astype(str)
            hist_fit['horse_id'] = hist_fit['horse_id'].astype(str)
            hist_parts.append(hist_fit)
        
        # 2. transform時のデータ（df）
        if 'race_date' in df.columns:
            hist_df = df[['race_id', 'horse_number', 'horse_id', 'race_date']].copy()
            hist_parts.append(hist_df)
        
        if not hist_parts:
            return df
        
        hist = pd.concat(hist_parts, ignore_index=True)
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        # 出走頭数を計算
        field_size = hist.groupby('race_id').size().reset_index(name='field_size')
        hist = hist.merge(field_size, on='race_id', how='left')
        
        # C4データにhorse_id, race_date, field_sizeを付与
        c4_full = c4.merge(hist, on=['race_id', 'horse_number'], how='inner')
        c4_full = c4_full.dropna(subset=['horse_id', 'race_date'])
        c4_full['race_date'] = pd.to_datetime(c4_full['race_date'])
        
        # 相対C4位置を計算
        c4_full['relative_c4'] = c4_full['c4_pos'] / c4_full['field_size']
        
        # 逃げ馬フラグ（C4位置が2以下）
        threshold = self.config.front_runner_threshold
        c4_full['is_front_runner'] = (c4_full['c4_pos'] <= threshold).astype(float)
        
        # 時系列ソート
        c4_full = c4_full.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # ===== Step 2: 馬ごとの累積統計を計算 =====
        # 【リーク対策】shift(1)で当該レースを除外
        min_races = self.config.min_corner_races
        
        # C4馬身差の累積平均
        c4_full['cum_c4_gap'] = c4_full.groupby('horse_id')['c4_gap'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        c4_full['cum_c4_count'] = c4_full.groupby('horse_id')['c4_gap'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        
        # 相対C4位置の累積平均
        c4_full['cum_relative_c4'] = c4_full.groupby('horse_id')['relative_c4'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 逃げ馬率の累積平均
        c4_full['cum_front_runner_rate'] = c4_full.groupby('horse_id')['is_front_runner'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 最小レース数フィルタ
        c4_full.loc[c4_full['cum_c4_count'] < min_races, 'cum_c4_gap'] = np.nan
        c4_full.loc[c4_full['cum_c4_count'] < min_races, 'cum_relative_c4'] = np.nan
        c4_full.loc[c4_full['cum_c4_count'] < min_races, 'cum_front_runner_rate'] = np.nan
        
        # ===== Step 3: dfにマージ =====
        # 各レース×馬の累積統計を取得
        merge_cols = ['race_id', 'horse_id', 'cum_c4_gap', 'cum_relative_c4', 'cum_front_runner_rate']
        c4_stats = c4_full[merge_cols].drop_duplicates(['race_id', 'horse_id'])
        
        df = df.merge(c4_stats, on=['race_id', 'horse_id'], how='left', suffixes=('', '_new'))
        
        # 新しい値で上書き
        if 'cum_c4_gap' in df.columns:
            df['horse_c4_gap_avg'] = df['cum_c4_gap']
        if 'cum_front_runner_rate' in df.columns:
            # 既存のhorse_front_runner_rateを上書き
            df['horse_front_runner_rate'] = df['cum_front_runner_rate'].combine_first(
                df.get('horse_front_runner_rate', pd.Series([np.nan] * len(df)))
            )
        
        # 一時カラムを削除
        for col in ['cum_c4_gap', 'cum_relative_c4', 'cum_front_runner_rate']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        # ===== Step 4: 派生特徴量を計算 =====
        # 馬番×脚質不適合スコア
        if 'field_size' not in df.columns:
            df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
        df['relative_post'] = df['horse_number'] / df['field_size']
        
        # cum_relative_c4を再取得
        if 'cum_relative_c4_new' in df.columns:
            avg_relative_c4 = df['cum_relative_c4_new']
        else:
            df = df.merge(
                c4_stats[['race_id', 'horse_id', 'cum_relative_c4']].rename(
                    columns={'cum_relative_c4': 'avg_relative_c4'}),
                on=['race_id', 'horse_id'], how='left'
            )
            avg_relative_c4 = df['avg_relative_c4']
        
        df['post_style_conflict'] = (df['relative_post'] - avg_relative_c4.fillna(0.5)).abs()
        
        # 一時カラムを削除
        for col in ['relative_post', 'avg_relative_c4', 'cum_relative_c4_new']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        # ===== Step 5: レース内逃げ予備軍特徴量 =====
        threshold_rate = self.config.front_runner_threshold_rate
        df['_is_front_candidate'] = (df['horse_front_runner_rate'] > threshold_rate).astype(float)
        df['_is_front_candidate'] = df['_is_front_candidate'].fillna(0)
        
        df['race_front_runner_count'] = df.groupby('race_id')['_is_front_candidate'].transform('sum')
        df['front_runner_competition'] = df['race_front_runner_count'] - df['_is_front_candidate']
        
        df = df.drop(columns=['_is_front_candidate'], errors='ignore')
        
        # ログ出力
        non_null_gap = df['horse_c4_gap_avg'].notna().sum()
        non_null_fr = df['horse_front_runner_rate'].notna().sum()
        logger.info(f"    horse_c4_gap_avg非NaN: {non_null_gap:,}/{len(df):,} ({non_null_gap/len(df)*100:.1f}%)")
        logger.info(f"    horse_front_runner_rate非NaN: {non_null_fr:,}/{len(df):,} ({non_null_fr/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_course_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """コース特徴量を付与（V15と同じ）"""
        logger.info("  コース特徴量を付与中...")
        
        df = df.copy()
        
        features_list = []
        
        for idx, row in df.iterrows():
            venue = row.get('venue', '')
            surface = row.get('track_surface', '')
            distance = row.get('distance_m', 0)
            is_outer = row.get('is_outer_course')
            
            if venue and surface and distance:
                features = self._course_provider.get_course_features(
                    venue=venue,
                    surface=surface,
                    distance=int(distance) if pd.notna(distance) else 0,
                    is_outer=is_outer
                )
            else:
                features = {
                    'corner_count': None,
                    'start_to_corner_m': None,
                    'final_straight_m': None,
                    'slope_percent': None,
                    'course_type': None,
                    'turn_direction': None,
                }
            features_list.append(features)
        
        features_df = pd.DataFrame(features_list, index=df.index)
        
        df['course_corner_count'] = features_df['corner_count']
        df['course_start_to_corner_m'] = features_df['start_to_corner_m']
        df['course_final_straight_m'] = features_df['final_straight_m']
        df['course_slope_percent'] = features_df['slope_percent']
        df['course_is_outer'] = (features_df['course_type'] == 'outer').astype(float)
        df['course_turn_direction'] = (features_df['turn_direction'] == 'right').astype(float)
        
        df['straight_ratio'] = df['course_final_straight_m'] / df['distance_m']
        df['straight_ratio'] = df['straight_ratio'].fillna(0)
        
        df['is_long_straight'] = (df['course_final_straight_m'] > 450).astype(float)
        
        fr_rate = df['horse_front_runner_rate'].fillna(0.5)
        df['style_straight_match'] = (1 - fr_rate) * df['straight_ratio']
        
        slope = df['course_slope_percent'].fillna(0)
        df['front_slope_disadvantage'] = fr_rate * slope
        
        is_closer = (fr_rate < 0.3).astype(float)
        df['closer_long_straight_advantage'] = is_closer * df['is_long_straight']
        
        non_null = df['course_final_straight_m'].notna().sum()
        logger.info(f"    course_final_straight_m非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS

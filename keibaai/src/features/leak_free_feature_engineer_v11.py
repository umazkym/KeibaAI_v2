"""
リークフリー特徴量エンジニア V11

V10.2（Stableモデル）をベースに、Deep Data Exploration（2025-12-09）で
発見された高ROIパターンを活用する特徴量を追加。

追加特徴量（全てリークフリー実装）:
1. prev_finish_category: 前走着順カテゴリ（巻き返し検出）
2. distance_change: 距離変化（m）
3. distance_extend_flag: 距離延長フラグ（+200m以上）
4. surface_switch_flag: 芝ダート転向フラグ
5. basis_weight_light_flag: 軽斤量フラグ（<52kg）

リーク対策:
- 全ての特徴量は「前走」または「当日情報」のみ使用
- 累積統計は使用しない（V10.2で既に十分）
- jt_combo系は引き続き除外（過学習対策）

ROI根拠（Deep Data Exploration結果）:
- prev_finish >= 10 → 334% ROI（巻き返し効果）
- distance_change +200-400m → 531% ROI
- basis_weight < 52kg → 528% ROI
"""

import pandas as pd
import numpy as np
from typing import Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v10_2 import LeakFreeFeatureEngineerV10_2, FeatureConfigV10_2

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV11(FeatureConfigV10_2):
    """V11用設定"""
    distance_extend_threshold: int = 200  # 距離延長とみなす閾値（m）
    basis_weight_light_threshold: float = 52.0  # 軽斤量閾値（kg）
    prev_finish_poor_threshold: int = 10  # 巻き返し対象の前走順位閾値


class LeakFreeFeatureEngineerV11(LeakFreeFeatureEngineerV10_2):
    """
    リークフリー特徴量エンジニア V11
    
    V10.2の安定した特徴量セットに、以下を追加：
    
    1. prev_finish_category: 前走着順カテゴリ（0-3）
       - 0: 1st, 1: 2-3rd, 2: 4-9th, 3: 10+th（巻き返し候補）
    
    2. distance_change: 前走からの距離変化（m）
       - 正: 延長、負: 短縮
    
    3. distance_extend_flag: 距離延長フラグ
       - 1 if distance_change >= 200m else 0
       - ROI根拠: +200-400m延長で531% ROI
    
    4. surface_switch_flag: 芝ダート転向フラグ
       - 1 if prev_surface != current_surface else 0
       - 注: ダート→芝は365% ROIで低め（ペナルティ）
    
    5. basis_weight_light_flag: 軽斤量フラグ
       - 1 if basis_weight < 52kg else 0
       - ROI根拠: <52kgで528% ROI
    
    【リークフリー保証】
    - prev_finish: 前走の着順（shift済み）
    - prev_distance: 前走の距離（shift済み）
    - prev_surface: 前走の馬場（shift済み）
    - basis_weight: 当日の斤量（レース開始前に確定）
    """
    
    # V10.2の特徴量 + V11の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV10_2.FEATURE_COLS + [
        'prev_finish_category',      # 前走着順カテゴリ（巻き返し検出）
        'distance_change',           # 距離変化（m）
        'distance_extend_flag',      # 距離延長フラグ
        'surface_switch_flag',       # 芝ダート転向フラグ
        'basis_weight_light_flag',   # 軽斤量フラグ
    ]
    
    def __init__(self, config: Optional[FeatureConfigV11] = None):
        super().__init__(config or FeatureConfigV11())
        self._prev_race_info = None  # 前走情報を保存
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None,
        horses_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV11':
        """V10.2のfitを拡張"""
        # V10.2のfit（horses_dfを渡す）
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        logger.info("LeakFreeFeatureEngineerV11: 追加fit処理開始")
        
        # 前走情報を構築
        self._build_prev_race_info()
        
        logger.info("LeakFreeFeatureEngineerV11: fit完了")
        return self
    
    def _build_prev_race_info(self):
        """前走情報のためのインデックスを構築（リークフリー）
        
        fitで構築されるのは馬別の最終レース情報のみ。
        transform時に動的に前走情報を計算する。
        """
        # V11はtransform時に動的計算するため、ここでは何もしない
        logger.info("  前走情報: transform時に動的計算")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V10.2のtransformを拡張"""
        # V10.2のtransform
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV11: 追加transform開始")
        
        # 前走情報を動的に計算（リークフリー）
        result = self._calc_prev_race_info(result)
        
        # V11追加特徴量を計算
        result = self._add_prev_finish_category(result)
        result = self._add_distance_features(result)
        result = self._add_surface_switch_flag(result)
        result = self._add_basis_weight_light_flag(result)
        
        logger.info("LeakFreeFeatureEngineerV11: transform完了")
        return result
    
    def _calc_prev_race_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """前走情報を動的に計算（リークフリー）
        
        入力データの各馬に対して、_full_history（Train期間）から
        最終レースの情報を取得する。
        
        【リークフリー保証】
        - Train期間のデータのみを使用（fit時に確定済み）
        - 入力データのレース結果は参照しない
        """
        if not hasattr(self, '_full_history') or self._full_history is None:
            df['prev_finish'] = np.nan
            df['prev_distance'] = np.nan
            df['prev_surface'] = np.nan
            return df
        
        # 履歴から各馬の最終レース情報を取得
        hist = self._full_history.copy()
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 各馬の最終レースの情報（=入力データの「前走」相当）
        last_race = hist.groupby('horse_id').agg({
            'finish_position': 'last',
            'distance_m': 'last',
            'track_surface': 'last'
        }).reset_index()
        last_race.columns = ['horse_id', 'prev_finish', 'prev_distance', 'prev_surface']
        
        # horse_idでマージ
        df = df.merge(last_race, on='horse_id', how='left', suffixes=('', '_prev'))
        
        # 既存カラムとの重複対応
        for col in ['prev_finish', 'prev_distance', 'prev_surface']:
            if f'{col}_prev' in df.columns:
                df[col] = df[f'{col}_prev']
                df = df.drop(columns=[f'{col}_prev'])
        
        non_null = df['prev_finish'].notna().sum()
        logger.info(f"  前走情報計算完了: {non_null:,}/{len(df):,}件（非Null）")
        
        return df
    
    def _add_prev_finish_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """前走着順カテゴリを追加
        
        カテゴリ:
        - 0: 1st（連勝狙い）
        - 1: 2-3rd（好調維持）
        - 2: 4-9th（安定圏）
        - 3: 10+th（巻き返し候補）→ ROI 334%
        """
        if 'prev_finish' not in df.columns:
            df['prev_finish_category'] = np.nan
            return df
        
        # 数値型に変換（nullable対応）
        prev_finish = pd.to_numeric(df['prev_finish'], errors='coerce')
        
        # pd.cutを使用してカテゴリ分け（NAを自動的にNaNとして扱う）
        # bins: [0, 1, 3, 9, inf] → labels: [0, 1, 2, 3]
        df['prev_finish_category'] = pd.cut(
            prev_finish,
            bins=[0, 1, 3, 9, float('inf')],
            labels=[0, 1, 2, 3],
            include_lowest=True
        ).astype(float)  # float に変換してNaNを保持
        
        return df
    
    def _add_distance_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """距離変化関連特徴量を追加"""
        # distance_change: 現在の距離 - 前走の距離
        if 'prev_distance' in df.columns and 'distance_m' in df.columns:
            # 数値型に変換（NaN対応）
            dist_m = pd.to_numeric(df['distance_m'], errors='coerce')
            prev_dist = pd.to_numeric(df['prev_distance'], errors='coerce')
            df['distance_change'] = dist_m - prev_dist
        else:
            df['distance_change'] = np.nan
        
        # distance_extend_flag: 距離延長フラグ
        threshold = self.config.distance_extend_threshold
        dist_change = pd.to_numeric(df['distance_change'], errors='coerce')
        is_na = dist_change.isna()
        df['distance_extend_flag'] = (dist_change.fillna(0) >= threshold).astype(float)
        df.loc[is_na, 'distance_extend_flag'] = np.nan
        
        return df
    
    def _add_surface_switch_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """芝ダート転向フラグを追加"""
        if 'prev_surface' not in df.columns or 'track_surface' not in df.columns:
            df['surface_switch_flag'] = np.nan
            return df
        
        # 文字列比較（NaN対応）
        prev_surf = df['prev_surface'].astype(str)
        curr_surf = df['track_surface'].astype(str)
        is_na = df['prev_surface'].isna()
        
        df['surface_switch_flag'] = (prev_surf != curr_surf).astype(float)
        df.loc[is_na, 'surface_switch_flag'] = np.nan
        
        return df
    
    def _add_basis_weight_light_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """軽斤量フラグを追加"""
        if 'basis_weight' not in df.columns:
            df['basis_weight_light_flag'] = np.nan
            return df
        
        threshold = self.config.basis_weight_light_threshold
        basis_wt = pd.to_numeric(df['basis_weight'], errors='coerce')
        is_na = basis_wt.isna()
        
        df['basis_weight_light_flag'] = (basis_wt.fillna(999) < threshold).astype(float)
        df.loc[is_na, 'basis_weight_light_flag'] = np.nan
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """使用特徴量"""
        return self.FEATURE_COLS

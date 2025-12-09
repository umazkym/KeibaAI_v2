"""
リークフリー特徴量エンジニア V11.1

V10.2をベースに、過学習リスクの低い特徴量のみを追加する保守的バージョン。

【V11からの変更点】
- prev_finish_category: 削除（44.6% NaN、過学習の主因）
- distance_change: 削除（42.3% NaN）
- distance_extend_flag: 削除（distance_changeに依存）
- surface_switch_flag: 削除（44.6% NaN）

【保持する特徴量】
- basis_weight_light_flag: 100% Non-Null、当日情報のみ使用

【設計原則】
1. NaN率が高い特徴量は過学習を引き起こしやすい
2. 当日確定情報のみを使用し、履歴依存を最小化
3. Train-Test Gap < 25% を目標

ROI根拠:
- basis_weight < 52kg → 528% ROI（Deep Data Exploration結果）
"""

import pandas as pd
import numpy as np
from typing import Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v10_2 import LeakFreeFeatureEngineerV10_2, FeatureConfigV10_2

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV11_1(FeatureConfigV10_2):
    """V11.1用設定"""
    basis_weight_light_threshold: float = 52.0  # 軽斤量閾値（kg）


class LeakFreeFeatureEngineerV11_1(LeakFreeFeatureEngineerV10_2):
    """
    リークフリー特徴量エンジニア V11.1 (Conservative)
    
    V10.2の安定した特徴量セットに、以下の1特徴量のみを追加：
    
    1. basis_weight_light_flag: 軽斤量フラグ
       - 1 if basis_weight < 52kg else 0
       - ROI根拠: <52kgで528% ROI
       - 100% Non-Null（当日確定情報）
    
    【過学習対策】
    - 高NaN率の特徴量は全て除外
    - 履歴依存の計算を最小化
    - 当日確定情報のみ使用
    """
    
    # V10.2の特徴量 + 1個の安全な新特徴量のみ
    FEATURE_COLS = LeakFreeFeatureEngineerV10_2.FEATURE_COLS + [
        'basis_weight_light_flag',   # 軽斤量フラグ（100% Non-Null）
    ]
    
    def __init__(self, config: Optional[FeatureConfigV11_1] = None):
        super().__init__(config or FeatureConfigV11_1())
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None,
        horses_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV11_1':
        """V10.2のfitをそのまま使用"""
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        logger.info("LeakFreeFeatureEngineerV11_1: fit完了（V10.2と同一）")
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V10.2のtransformを拡張（1特徴量のみ追加）"""
        # V10.2のtransform
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV11_1: 追加transform開始")
        
        # 軽斤量フラグのみ追加
        result = self._add_basis_weight_light_flag(result)
        
        logger.info("LeakFreeFeatureEngineerV11_1: transform完了")
        return result
    
    def _add_basis_weight_light_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """軽斤量フラグを追加（当日確定情報のみ使用）"""
        if 'basis_weight' not in df.columns:
            df['basis_weight_light_flag'] = np.nan
            return df
        
        threshold = self.config.basis_weight_light_threshold
        basis_wt = pd.to_numeric(df['basis_weight'], errors='coerce')
        is_na = basis_wt.isna()
        
        df['basis_weight_light_flag'] = (basis_wt.fillna(999) < threshold).astype(float)
        df.loc[is_na, 'basis_weight_light_flag'] = np.nan
        
        non_null = df['basis_weight_light_flag'].notna().sum()
        logger.info(f"  basis_weight_light_flag: {non_null:,}/{len(df):,}件（{non_null/len(df)*100:.1f}%）Non-Null")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """使用特徴量"""
        return self.FEATURE_COLS

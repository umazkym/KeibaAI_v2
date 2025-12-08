"""
リークフリー特徴量エンジニア V9.1

V8.1をベースに、深層分析で発見された有望な特徴量のみを追加：

発見された有効な条件:
1. 前走4-5着 × 50-100倍オッズ → ROI 140%超
2. 前走2着 × 20-50倍オッズ → ROI 134%

追加する特徴量:
- prev_finish: 前走着順
- prev_finish_top3: 前走上位フラグ
- prev_finish_bins: 前走着順カテゴリ

削除する特徴量（効果なし）:
- 母父統計（市場織り込み済み）
- ペース統計
- 体重変動
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v8 import LeakFreeFeatureEngineerV8, FeatureConfigV8

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV91(FeatureConfigV8):
    """V9.1用設定"""
    pass


class LeakFreeFeatureEngineerV91(LeakFreeFeatureEngineerV8):
    """
    リークフリー特徴量エンジニア V9.1
    
    V8.1（クラス変動）に加えて、前走着順の詳細情報を追加。
    
    追加特徴量：
    - prev_finish: 前走着順（数値）
    - prev_finish_top3: 前走1-3着フラグ
    - prev_finish_4_5: 前走4-5着フラグ（ROI高い条件）
    """
    
    # V8.1の特徴量 + V9.1の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV8.FEATURE_COLS + [
        'prev_finish',
        'prev_finish_top3',
        'prev_finish_4_5',
    ]
    
    def __init__(self, config: Optional[FeatureConfigV91] = None):
        super().__init__(config or FeatureConfigV91())
        self._prev_finish_stats = None
    
    def fit(
        self, 
        races_df: pd.DataFrame,
        pedigrees_df: Optional[pd.DataFrame] = None,
        corners_df: Optional[pd.DataFrame] = None,
        race_details_df: Optional[pd.DataFrame] = None,
        returns_df: Optional[pd.DataFrame] = None
    ) -> 'LeakFreeFeatureEngineerV91':
        """V8のfitを拡張"""
        # V8のfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df)
        
        logger.info("LeakFreeFeatureEngineerV91: 追加fit処理開始")
        
        # 履歴データの前走着順を計算・保存
        self._build_prev_finish_mapping()
        
        logger.info("LeakFreeFeatureEngineerV91: fit完了")
        return self
    
    def _build_prev_finish_mapping(self):
        """前走着順マッピングを構築"""
        hist = self._full_history.copy()
        hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 前走着順を計算
        hist['prev_finish'] = hist.groupby('horse_id')['finish_position'].shift(1)
        
        # 各馬×レースの前走着順を保存
        self._prev_finish_stats = hist[['race_id', 'horse_number', 'horse_id', 'prev_finish']].copy()
        self._prev_finish_stats['_unique_key'] = (
            self._prev_finish_stats['race_id'].astype(str) + '_' + 
            self._prev_finish_stats['horse_number'].astype(str)
        )
        
        logger.info(f"  前走着順マッピング構築完了: {len(self._prev_finish_stats):,}件")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V8のtransformを拡張"""
        # V8のtransform
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV91: 追加transform開始")
        
        # 前走着順特徴量を追加
        result = self._add_prev_finish_features(result, df)
        
        logger.info("LeakFreeFeatureEngineerV91: transform完了")
        return result
    
    def _add_prev_finish_features(self, result: pd.DataFrame, original_df: pd.DataFrame) -> pd.DataFrame:
        """前走着順特徴量を追加"""
        if self._prev_finish_stats is None:
            result['prev_finish'] = np.nan
            result['prev_finish_top3'] = np.nan
            result['prev_finish_4_5'] = np.nan
            return result
        
        # 元データにunique_keyを追加
        original_with_key = original_df.copy()
        original_with_key['_unique_key'] = (
            original_with_key['race_id'].astype(str) + '_' + 
            original_with_key['horse_number'].astype(str)
        )
        
        # 履歴から前走着順を取得
        prev_finish_lookup = self._prev_finish_stats.set_index('_unique_key')['prev_finish']
        
        # resultにマージ（race_id + horse_numberで）
        result_keys = result['race_id'].astype(str) + '_' + result['horse_number'].astype(str)
        result['prev_finish'] = result_keys.map(prev_finish_lookup)
        
        # 新規データ（履歴にない）の場合は、元データから計算
        # ただし、transformで渡されたデータ内での前走は計算しない（リーク防止）
        # 履歴にないものはNaNのまま
        
        # 派生特徴量
        result['prev_finish_top3'] = (result['prev_finish'] <= 3).astype(float)
        result['prev_finish_top3'] = result['prev_finish_top3'].where(result['prev_finish'].notna(), np.nan)
        
        result['prev_finish_4_5'] = ((result['prev_finish'] >= 4) & (result['prev_finish'] <= 5)).astype(float)
        result['prev_finish_4_5'] = result['prev_finish_4_5'].where(result['prev_finish'].notna(), np.nan)
        
        return result
    
    def get_feature_columns(self) -> List[str]:
        """使用特徴量"""
        return self.FEATURE_COLS

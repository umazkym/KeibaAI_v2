"""
リークフリー特徴量エンジニア V10.2

V10.1をベースに、過学習の主因となっている「騎手×調教師コンビ（jt_combo）」特徴量を削除。

削除特徴量:
- jt_combo_win_rate
- jt_combo_races

目的:
- Train-Test Gap（現在36%）の削減
- 新特徴量（千歳市、休養、重馬場）の有効性再評価
"""

from typing import Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v10_1 import LeakFreeFeatureEngineerV10_1, FeatureConfigV10_1

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV10_2(FeatureConfigV10_1):
    """V10.2用設定"""
    pass


class LeakFreeFeatureEngineerV10_2(LeakFreeFeatureEngineerV10_1):
    """
    リークフリー特徴量エンジニア V10.2
    
    変更点:
    - jt_combo_win_rate, jt_combo_races を削除（過学習対策）
    """
    
    # V10.1の特徴量からjt_combo系を除外
    FEATURE_COLS = [
        col for col in LeakFreeFeatureEngineerV10_1.FEATURE_COLS 
        if col not in ['jt_combo_win_rate', 'jt_combo_races']
    ]
    
    def __init__(self, config: Optional[FeatureConfigV10_2] = None):
        super().__init__(config or FeatureConfigV10_2())
    
    # fit/transformはV10.1と同じ
    
    def get_feature_columns(self) -> List[str]:
        """使用特徴量"""
        return self.FEATURE_COLS

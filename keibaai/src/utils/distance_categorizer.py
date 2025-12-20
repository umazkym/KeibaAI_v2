"""
距離カテゴリユーティリティ

距離(distance_m)をカテゴリ化し、短距離レースの特性を明示的に表現する。

使用例:
    from keibaai.src.utils.distance_categorizer import DistanceCategorizer
    
    categorizer = DistanceCategorizer()
    
    # 単一値のカテゴリ化
    category = categorizer.categorize(1200)
    # -> 'sprint'
    
    # DataFrameへの一括適用
    df = categorizer.add_features(df)
    # -> distance_category, is_sprint, is_mile, is_middle, is_long, has_4_corners が追加
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class DistanceConfig:
    """距離カテゴリの設定"""
    # カテゴリ境界（m）
    sprint_max: int = 1400       # 〜1400m: スプリント
    mile_max: int = 1600         # 1401-1600m: マイル
    middle_max: int = 2200       # 1601-2200m: 中距離
    long_max: int = 2800         # 2201-2800m: 長距離
    # 2800m超: 超長距離（ステイヤーズS等）
    
    # コーナー数の境界
    two_corners_max: int = 1600  # 1600m以下は2コーナー（一部例外あり）
    

class DistanceCategorizer:
    """距離カテゴリ化ユーティリティ"""
    
    # カテゴリ名
    SPRINT = 'sprint'           # スプリント（〜1400m）
    MILE = 'mile'               # マイル（1401-1600m）
    MIDDLE = 'middle'           # 中距離（1601-2200m）
    LONG = 'long'               # 長距離（2201-2800m）
    EXTREME_LONG = 'extreme'    # 超長距離（2800m超）
    
    # カテゴリ→序数マッピング
    CATEGORY_ORDINAL = {
        SPRINT: 1,
        MILE: 2,
        MIDDLE: 3,
        LONG: 4,
        EXTREME_LONG: 5
    }
    
    # 4コーナーがあるコースのマッピング（競馬場→距離リスト）
    # 実データのPO4存在率から判定
    FOUR_CORNERS_MAPPING = {
        '中京': [1800, 1900, 2000, 2200, 3000, 3300, 3330, 3900],
        '中山': [1800, 2000, 2200, 2400, 2500, 3210, 3350, 3570, 4250, 4260],
        '京都': [1800, 1900, 2000, 2200, 2400, 3000, 3170, 3200, 3930],
        '函館': [1700, 1800, 2000, 2400, 2600],
        '小倉': [1700, 1800, 2000, 2400, 2600, 3390],
        '新潟': [1800, 2200, 2400, 2500, 3250],
        '札幌': [1700, 1800, 2000, 2400, 2600],
        '東京': [2100, 2300, 2400, 2500, 3110, 3400],  # 東京1800/2000は3コーナー
        '福島': [1700, 1800, 2000, 2400, 2600],
        '阪神': [1800, 2000, 2200, 2400, 2600, 3000, 3140, 3900],
    }
    
    def __init__(self, config: Optional[DistanceConfig] = None):
        """初期化"""
        self.config = config or DistanceConfig()
        logger.info("DistanceCategorizer initialized")
    
    def categorize(self, distance_m: Optional[float]) -> Optional[str]:
        """
        距離をカテゴリに変換
        
        Args:
            distance_m: 距離（メートル）
        
        Returns:
            カテゴリ名（sprint/mile/middle/long/extreme）
        """
        if distance_m is None or pd.isna(distance_m):
            return None
        
        dist = int(distance_m)
        
        if dist <= self.config.sprint_max:
            return self.SPRINT
        elif dist <= self.config.mile_max:
            return self.MILE
        elif dist <= self.config.middle_max:
            return self.MIDDLE
        elif dist <= self.config.long_max:
            return self.LONG
        else:
            return self.EXTREME_LONG
    
    def get_ordinal(self, category: Optional[str]) -> Optional[int]:
        """カテゴリ→序数"""
        if category is None:
            return None
        return self.CATEGORY_ORDINAL.get(category)
    
    def has_4_corners(self, venue: Optional[str], distance_m: Optional[float]) -> Optional[bool]:
        """
        4コーナーがあるレースかどうかを判定（競馬場×距離で判定）
        
        Args:
            venue: 競馬場名
            distance_m: 距離（メートル）
        
        Returns:
            True: 4コーナーあり（PO4が存在するはず）
            False: 2-3コーナー以下（PO4はNULLが正常）
            None: 判定不能
        """
        if venue is None or distance_m is None or pd.isna(venue) or pd.isna(distance_m):
            return None
        
        dist = int(distance_m)
        venue_str = str(venue)
        
        # マッピングから判定
        if venue_str in self.FOUR_CORNERS_MAPPING:
            return dist in self.FOUR_CORNERS_MAPPING[venue_str]
        
        # 未知の競馬場は距離で推定（1800m以上なら4コーナー）
        return dist >= 1800
    
    def add_features(self, df: pd.DataFrame, 
                     distance_col: str = 'distance_m',
                     venue_col: str = 'venue') -> pd.DataFrame:
        """
        DataFrameに距離カテゴリ特徴量を追加
        
        Args:
            df: 入力DataFrame
            distance_col: 距離カラム名
            venue_col: 競馬場カラム名
        
        Returns:
            以下のカラムが追加されたDataFrame:
            - distance_category: カテゴリ名
            - distance_category_ordinal: カテゴリ序数（1-5）
            - is_sprint: スプリントフラグ
            - is_mile: マイルフラグ
            - is_middle: 中距離フラグ
            - is_long: 長距離フラグ
            - is_extreme_long: 超長距離フラグ
            - has_4_corners: 4コーナーありフラグ（競馬場×距離で判定）
        """
        df = df.copy()
        
        # カテゴリ化
        df['distance_category'] = df[distance_col].apply(self.categorize)
        df['distance_category_ordinal'] = df['distance_category'].map(self.get_ordinal)
        
        # One-hotフラグ
        df['is_sprint'] = (df['distance_category'] == self.SPRINT).astype('Int64')
        df['is_mile'] = (df['distance_category'] == self.MILE).astype('Int64')
        df['is_middle'] = (df['distance_category'] == self.MIDDLE).astype('Int64')
        df['is_long'] = (df['distance_category'] == self.LONG).astype('Int64')
        df['is_extreme_long'] = (df['distance_category'] == self.EXTREME_LONG).astype('Int64')
        
        # コーナー数フラグ（競馬場×距離で判定）
        df['has_4_corners'] = df.apply(
            lambda row: self.has_4_corners(row.get(venue_col), row.get(distance_col)),
            axis=1
        )
        
        # 統計ログ
        if 'distance_category' in df.columns:
            cat_counts = df['distance_category'].value_counts()
            logger.info(f"距離カテゴリ分布:")
            for cat, cnt in cat_counts.items():
                logger.info(f"  {cat}: {cnt:,} ({cnt/len(df)*100:.1f}%)")
        
        return df


# 便利関数
def categorize_distance(distance_m: Optional[float]) -> Optional[str]:
    """距離をカテゴリに変換（便利関数）"""
    global _categorizer_instance
    if '_categorizer_instance' not in globals() or _categorizer_instance is None:
        _categorizer_instance = DistanceCategorizer()
    return _categorizer_instance.categorize(distance_m)


def has_4_corners(distance_m: Optional[float]) -> Optional[bool]:
    """4コーナーがあるかどうかを判定（便利関数）"""
    global _categorizer_instance
    if '_categorizer_instance' not in globals() or _categorizer_instance is None:
        _categorizer_instance = DistanceCategorizer()
    return _categorizer_instance.has_4_corners(distance_m)


# シングルトンインスタンス
_categorizer_instance: Optional[DistanceCategorizer] = None

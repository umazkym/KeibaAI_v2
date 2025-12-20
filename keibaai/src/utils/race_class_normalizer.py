"""
JRA公式クラス制度への正規化ユーティリティ

race_classを統一された形式に正規化するためのユーティリティクラス。
設定ファイル(race_class_mapping.yaml)でマッピングルールを管理。

使用例:
    from keibaai.src.utils.race_class_normalizer import RaceClassNormalizer
    
    normalizer = RaceClassNormalizer()
    
    # 単一値の正規化
    normalized = normalizer.normalize(raw_class='500', race_name='3歳以上500万下')
    # -> '1勝クラス'
    
    # DataFrameの一括正規化
    df = normalizer.normalize_dataframe(df)
    # -> df['race_class_normalized'], df['race_class_ordinal'] が追加
"""

import re
import logging
from pathlib import Path
from typing import Optional, Dict, List, Any

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


class RaceClassNormalizer:
    """JRA公式クラス制度への正規化ユーティリティ"""
    
    # デフォルトの設定ファイルパス
    DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent / 'configs' / 'race_class_mapping.yaml'
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        初期化
        
        Args:
            config_path: 設定ファイルのパス。Noneの場合はデフォルトパスを使用。
        """
        self.config_path = config_path or self.DEFAULT_CONFIG_PATH
        self.config = self._load_config()
        
        # 設定から各種マッピングを構築
        self._normalized_classes = self.config.get('normalized_classes', {})
        self._legacy_mapping = self.config.get('legacy_mapping', {})
        self._extraction_patterns = self.config.get('extraction_patterns', [])
        
        logger.info(f"RaceClassNormalizer initialized with {len(self._normalized_classes)} classes")
    
    def _load_config(self) -> Dict[str, Any]:
        """設定ファイルを読み込む"""
        if not self.config_path.exists():
            logger.warning(f"Config file not found: {self.config_path}. Using default configuration.")
            return self._get_default_config()
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}. Using default configuration.")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """デフォルト設定を返す（フォールバック用）"""
        return {
            'normalized_classes': {
                'G1': 10, 'G2': 9, 'G3': 8, 'リステッド': 7, 'オープン': 6,
                '3勝クラス': 5, '2勝クラス': 4, '1勝クラス': 3, '未勝利': 2, '新馬': 1
            },
            'legacy_mapping': {
                '500': '1勝クラス', '1000': '2勝クラス', '1600': '3勝クラス', 'OP': 'オープン'
            },
            'extraction_patterns': []
        }
    
    def normalize(self, raw_class: Optional[str], race_name: Optional[str] = None) -> Optional[str]:
        """
        race_classを正規化する
        
        Args:
            raw_class: 元のrace_class値（パーサー出力）
            race_name: レース名（race_classがNoneの場合に使用）
        
        Returns:
            正規化されたクラス名。判定不能の場合はNone。
        """
        # 特殊ケース: パーサーが'G1'と判定している場合
        # パーサーのバグで 'GI' in 'GIII' がTrueになるため、race_nameから再判定
        if raw_class == 'G1' and race_name:
            extracted = self._extract_from_name(race_name)
            if extracted in ('G1', 'G2', 'G3'):
                return extracted
            # 抽出できなければG1として扱う
            return 'G1'
        
        # 1. raw_classが正規化済みクラスならそのまま返す（G1は上で処理済み）
        if raw_class in self._normalized_classes:
            return raw_class
        
        # 2. legacy_mappingでマッピング
        if raw_class and raw_class in self._legacy_mapping:
            return self._legacy_mapping[raw_class]
        
        # 3. race_nameからパターン抽出
        if race_name:
            extracted = self._extract_from_name(race_name)
            if extracted:
                return extracted
        
        # 4. 判定不能
        return None
    
    def _extract_from_name(self, race_name: str) -> Optional[str]:
        """race_nameからクラスを抽出"""
        if not race_name or pd.isna(race_name):
            return None
        
        name = str(race_name)
        
        for pattern_group in self._extraction_patterns:
            patterns = pattern_group.get('patterns', [])
            target_class = pattern_group.get('class')
            
            for pattern in patterns:
                if pattern in name:
                    return target_class
        
        return None
    
    def get_ordinal(self, normalized_class: Optional[str]) -> Optional[int]:
        """
        クラスの強さスコア（序数）を返す
        
        Args:
            normalized_class: 正規化されたクラス名
        
        Returns:
            強さスコア（1-10）。新馬=1、G1=10。
            不明な場合はNone。
        """
        if normalized_class is None:
            return None
        return self._normalized_classes.get(normalized_class)
    
    def normalize_dataframe(self, df: pd.DataFrame, 
                            raw_class_col: str = 'race_class',
                            race_name_col: str = 'race_name',
                            output_class_col: str = 'race_class_normalized',
                            output_ordinal_col: str = 'race_class_ordinal') -> pd.DataFrame:
        """
        DataFrameのrace_classを一括正規化
        
        Args:
            df: 入力DataFrame
            raw_class_col: 元のrace_classカラム名
            race_name_col: レース名カラム名
            output_class_col: 出力する正規化クラスカラム名
            output_ordinal_col: 出力する序数カラム名
        
        Returns:
            正規化カラムが追加されたDataFrame
        """
        df = df.copy()
        
        # 正規化
        df[output_class_col] = df.apply(
            lambda row: self.normalize(
                row.get(raw_class_col), 
                row.get(race_name_col)
            ),
            axis=1
        )
        
        # 序数
        df[output_ordinal_col] = df[output_class_col].map(self.get_ordinal)
        
        # 統計をログ出力
        null_count = df[output_class_col].isna().sum()
        if null_count > 0:
            logger.warning(f"{null_count} records could not be normalized ({null_count/len(df)*100:.1f}%)")
        
        return df
    
    def get_class_summary(self, df: pd.DataFrame, class_col: str = 'race_class_normalized') -> pd.DataFrame:
        """
        クラス別の統計サマリーを取得
        
        Args:
            df: 正規化済みDataFrame
            class_col: クラスカラム名
        
        Returns:
            クラス別の統計DataFrame
        """
        if class_col not in df.columns:
            raise ValueError(f"Column '{class_col}' not found. Run normalize_dataframe() first.")
        
        summary = df.groupby(class_col).agg({
            'race_id': 'nunique',
            'horse_id': 'nunique'
        }).rename(columns={'race_id': 'races', 'horse_id': 'horses'})
        
        summary['records'] = df.groupby(class_col).size()
        summary['pct'] = (summary['records'] / len(df) * 100).round(1)
        summary['ordinal'] = summary.index.map(self.get_ordinal)
        
        return summary.sort_values('ordinal', ascending=False)


def normalize_race_class(raw_class: Optional[str], race_name: Optional[str] = None) -> Optional[str]:
    """
    便利関数: シングルトンインスタンスを使用して正規化
    
    Args:
        raw_class: 元のrace_class値
        race_name: レース名（オプション）
    
    Returns:
        正規化されたクラス名
    """
    global _normalizer_instance
    if '_normalizer_instance' not in globals() or _normalizer_instance is None:
        _normalizer_instance = RaceClassNormalizer()
    return _normalizer_instance.normalize(raw_class, race_name)


def get_race_class_ordinal(normalized_class: Optional[str]) -> Optional[int]:
    """
    便利関数: クラスの序数を取得
    
    Args:
        normalized_class: 正規化されたクラス名
    
    Returns:
        強さスコア（1-10）
    """
    global _normalizer_instance
    if '_normalizer_instance' not in globals() or _normalizer_instance is None:
        _normalizer_instance = RaceClassNormalizer()
    return _normalizer_instance.get_ordinal(normalized_class)


# シングルトンインスタンス（遅延初期化）
_normalizer_instance: Optional[RaceClassNormalizer] = None

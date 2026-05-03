"""
レース競争力分析モジュール

【目的】
レースの「接戦度合い」と「荒れやすさ」を多角的に分析し、
戦略の自動切り替えを支援する。

【分析内容】
1. 予測ベース分析（当日使用可能）
   - place_prob分散 → 予測上の実力差
   - win_probエントロピー → 混戦度

2. 履歴ベース分析（過去データから算出）
   - クラス別平均着差 → このクラスは接戦になりやすい？
   - 馬別僅差負け率 → 惜しい馬が多い？
   - 競馬場別荒れ率 → この会場は荒れやすい？

3. 統合荒れ度スコア
   - 複数シグナルを組み合わせた総合判定

【リーク対策】★重要★
- 当該レースの実際の結果（着差、タイム）は使用不可
- 過去レースの統計のみ使用（shift適用済み）
- 予測確率は「予測時点で利用可能」なのでOK

作成日: 2026-01-11
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass
import logging
import warnings

warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class RaceCompetitivenessConfig:
    """レース競争力分析の設定"""
    
    # 僅差の閾値（秒）
    close_margin_threshold: float = 0.3
    
    # 荒れレース判定閾値
    volatile_place_prob_std: float = 0.10
    volatile_close_loss_avg: float = 0.30
    volatile_class_close_rate: float = 0.40
    
    # クラス別統計の最小サンプル数
    min_class_samples: int = 100
    
    # 外れ値除外
    max_valid_margin: float = 10.0


class RaceCompetitivenessAnalyzer:
    """
    レースの競争力を多角的に分析
    
    【設計思想】
    「荒れやすさ」を3つの視点から分析：
    1. 予測視点: モデルが「差がない」と判断しているか
    2. 馬視点: 「惜しい馬」が多いか
    3. レース条件視点: このクラス・距離・会場は接戦になりやすいか
    """
    
    def __init__(self, config: Optional[RaceCompetitivenessConfig] = None):
        self.config = config or RaceCompetitivenessConfig()
        
        # fit時に計算する統計
        self._class_margin_stats: Dict[str, Dict] = {}
        self._venue_volatility_stats: Dict[str, float] = {}
        self._distance_volatility_stats: Dict[int, float] = {}
        self._fitted = False
    
    def fit(self, races_df: pd.DataFrame) -> 'RaceCompetitivenessAnalyzer':
        """
        過去レースデータからクラス・会場・距離別の接戦傾向を計算
        
        【リーク対策】
        - fit時にTrain期間の統計を計算
        - 個別レースごとのshift不要（集計統計なので）
        - ただし、Test期間のレースはfit対象に含めない
        
        Args:
            races_df: 過去レース結果データ
        
        Returns:
            self
        """
        logger.info("RaceCompetitivenessAnalyzer: fit開始")
        
        df = races_df.copy()
        
        # データクリーニング
        df = df[df['margin_seconds'].notna()].copy()
        df = df[df['margin_seconds'] <= self.config.max_valid_margin]
        df = df[df['finish_position'].notna()]
        df = df[df['finish_position'] <= 18]
        
        # 接戦フラグ（3着以内が0.3秒以内）
        df['is_close_race'] = (
            (df['finish_position'] <= 3) & 
            (df['margin_seconds'] <= self.config.close_margin_threshold)
        ).astype(int)
        
        # クラス別統計
        self._calc_class_stats(df)
        
        # 会場別統計
        self._calc_venue_stats(df)
        
        # 距離別統計
        self._calc_distance_stats(df)
        
        self._fitted = True
        logger.info("RaceCompetitivenessAnalyzer: fit完了")
        
        return self
    
    def _calc_class_stats(self, df: pd.DataFrame):
        """クラス別の接戦傾向を計算"""
        logger.info("  クラス別統計を計算中...")
        
        if 'race_class' not in df.columns:
            logger.warning("  race_classカラムがないためスキップ")
            return
        
        # レース単位で接戦判定
        race_level = df.groupby(['race_id', 'race_class']).agg({
            'is_close_race': 'max',  # レース内で1件でも僅差なら接戦
            'margin_seconds': ['mean', 'std'],  # 平均と標準偏差
        }).reset_index()
        race_level.columns = ['race_id', 'race_class', 'is_close', 'avg_margin', 'std_margin']
        
        # クラス別集計
        class_stats = race_level.groupby('race_class').agg({
            'is_close': 'mean',      # 接戦率
            'avg_margin': 'mean',    # 平均着差
            'std_margin': 'mean',    # 着差標準偏差
            'race_id': 'count',      # サンプル数
        }).reset_index()
        class_stats.columns = ['race_class', 'close_rate', 'avg_margin', 'margin_std', 'sample_count']
        
        # 最小サンプル数フィルタ
        class_stats = class_stats[class_stats['sample_count'] >= self.config.min_class_samples]
        
        self._class_margin_stats = class_stats.set_index('race_class').to_dict('index')
        
        logger.info(f"    クラス別統計: {len(self._class_margin_stats)}クラス")
        for rc, stats in self._class_margin_stats.items():
            logger.info(f"      {rc}: 接戦率={stats['close_rate']*100:.1f}%, 平均着差={stats['avg_margin']:.2f}秒")
    
    def _calc_venue_stats(self, df: pd.DataFrame):
        """会場別の荒れ傾向を計算"""
        logger.info("  会場別統計を計算中...")
        
        if 'venue' not in df.columns:
            logger.warning("  venueカラムがないためスキップ")
            return
        
        # レース単位で「荒れ」判定（1番人気が3着以下）
        race_level = df.groupby(['race_id', 'venue']).apply(
            lambda x: (x[x['popularity'] == 1]['finish_position'] > 3).any()
        ).reset_index(name='is_upset')
        
        # 会場別集計
        venue_stats = race_level.groupby('venue')['is_upset'].mean()
        
        self._venue_volatility_stats = venue_stats.to_dict()
        
        logger.info(f"    会場別荒れ率: {len(self._venue_volatility_stats)}会場")
        for venue, rate in sorted(self._venue_volatility_stats.items(), key=lambda x: -x[1])[:5]:
            logger.info(f"      {venue}: {rate*100:.1f}%")
    
    def _calc_distance_stats(self, df: pd.DataFrame):
        """距離別の接戦傾向を計算"""
        logger.info("  距離別統計を計算中...")
        
        if 'distance_m' not in df.columns:
            logger.warning("  distance_mカラムがないためスキップ")
            return
        
        # 距離を100m単位に丸める
        df['distance_bin'] = (df['distance_m'] / 100).round() * 100
        
        # 距離別接戦率
        race_level = df.groupby(['race_id', 'distance_bin'])['is_close_race'].max().reset_index()
        distance_stats = race_level.groupby('distance_bin')['is_close_race'].mean()
        
        self._distance_volatility_stats = distance_stats.to_dict()
        
        logger.info(f"    距離別接戦率: {len(self._distance_volatility_stats)}距離帯")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        レース条件に基づく接戦傾向を特徴量として付与
        
        Args:
            df: レースデータ
        
        Returns:
            特徴量が追加されたデータフレーム
        """
        if not self._fitted:
            raise RuntimeError("fit()を先に実行してください")
        
        logger.info("RaceCompetitivenessAnalyzer: transform開始")
        
        result = df.copy()
        
        # クラス別接戦率
        if 'race_class' in result.columns:
            result['class_close_rate'] = result['race_class'].map(
                lambda x: self._class_margin_stats.get(x, {}).get('close_rate', np.nan)
            )
            result['class_avg_margin'] = result['race_class'].map(
                lambda x: self._class_margin_stats.get(x, {}).get('avg_margin', np.nan)
            )
        
        # 会場別荒れ率
        if 'venue' in result.columns:
            result['venue_upset_rate'] = result['venue'].map(
                lambda x: self._venue_volatility_stats.get(x, np.nan)
            )
        
        # 距離別接戦率
        if 'distance_m' in result.columns:
            result['distance_bin'] = (result['distance_m'] / 100).round() * 100
            result['distance_close_rate'] = result['distance_bin'].map(
                lambda x: self._distance_volatility_stats.get(x, np.nan)
            )
            result = result.drop(columns=['distance_bin'])
        
        logger.info("RaceCompetitivenessAnalyzer: transform完了")
        
        return result
    
    def calc_integrated_volatility(self, 
                                    predictions_df: pd.DataFrame,
                                    horse_margin_features: pd.DataFrame) -> pd.DataFrame:
        """
        複数シグナルを統合した荒れ度スコアを計算
        
        【統合シグナル】
        1. place_prob標準偏差 < 0.10 → 予測上の実力差が小さい
        2. 競馬場荒れ率 > 平均 → 荒れやすい会場
        3. クラス接戦率 > 0.40 → 接戦になりやすいクラス
        4. 出走馬の平均僅差負け率 > 0.30 → 惜しい馬が多い
        
        Args:
            predictions_df: MultiTargetPredictor予測結果（place_prob含む）
            horse_margin_features: TimeMarginFeatureEngineer出力
        
        Returns:
            統合荒れ度スコアが追加されたデータフレーム
        """
        logger.info("RaceCompetitivenessAnalyzer: 統合荒れ度計算開始")
        
        # データをマージ
        result = predictions_df.merge(
            horse_margin_features[['race_id', 'horse_number', 'horse_close_loss_rate',
                                   'class_close_rate', 'venue_upset_rate']],
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        # シグナル1: place_prob標準偏差
        result['race_place_prob_std'] = result.groupby('race_id')['place_prob'].transform('std')
        result['signal_low_variance'] = (
            result['race_place_prob_std'] < self.config.volatile_place_prob_std
        ).astype(int)
        
        # シグナル2: レース内平均僅差負け率
        result['race_avg_close_loss_rate'] = result.groupby('race_id')['horse_close_loss_rate'].transform('mean')
        result['signal_many_close_losers'] = (
            result['race_avg_close_loss_rate'] > self.config.volatile_close_loss_avg
        ).astype(int)
        
        # シグナル3: クラス接戦率
        result['signal_class_close'] = (
            result['class_close_rate'] > self.config.volatile_class_close_rate
        ).astype(int)
        
        # シグナル4: 会場荒れ率（平均以上）
        avg_venue_upset = np.nanmean(list(self._venue_volatility_stats.values()))
        result['signal_venue_upset'] = (
            result['venue_upset_rate'] > avg_venue_upset
        ).astype(int)
        
        # 統合スコア（0-1）
        signal_cols = ['signal_low_variance', 'signal_many_close_losers', 
                       'signal_class_close', 'signal_venue_upset']
        result['volatility_score'] = result[signal_cols].fillna(0).mean(axis=1)
        
        # 分類
        result['volatility_type'] = result['volatility_score'].apply(
            lambda x: 'volatile' if x >= 0.5 else ('moderate' if x >= 0.25 else 'stable')
        )
        
        # 統計ログ
        type_dist = result.groupby('race_id')['volatility_type'].first().value_counts()
        logger.info(f"  レース分類: {type_dist.to_dict()}")
        logger.info(f"  平均volatility_score: {result['volatility_score'].mean():.3f}")
        
        # シグナル別発火率
        for sig in signal_cols:
            rate = result.groupby('race_id')[sig].first().mean()
            logger.info(f"  {sig}発火率: {rate*100:.1f}%")
        
        logger.info("RaceCompetitivenessAnalyzer: 統合荒れ度計算完了")
        
        return result
    
    def get_feature_columns(self) -> List[str]:
        """生成する特徴量カラム名のリスト"""
        return [
            'class_close_rate',
            'class_avg_margin',
            'venue_upset_rate',
            'distance_close_rate',
            'race_place_prob_std',
            'race_avg_close_loss_rate',
            'volatility_score',
            'volatility_type',
        ]

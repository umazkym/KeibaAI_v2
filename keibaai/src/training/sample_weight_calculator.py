#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
サンプル重み計算モジュール

競馬予測に特化したサンプル重み付けを行う。

【設計原則】
1. 人気馬(1-5番人気)が好走(1-5着) → 重み増（見落とし防止）
2. 穴馬(6-9番人気)が好走 → 重み増大（発見価値）
3. 大穴(10番人気以下/80倍+)の好走 → 運要素として軽減
4. 1番人気10着以下 → 外れ値として除外
5. 拮抗オッズレースでの好走 → 重み増大（精度価値）

【リーク対策】
- finish_position, actual_odds など結果に依存 → 学習時のみ使用可能
- 予測時はデフォルト重み1.0を使用

[5-3改修] enable_popularity_weighting フラグを追加。
popularityは確定オッズ由来であり、重み付けに使用すると
微妙なデータリークが発生し得る。このフラグで無効化可能。
"""

import numpy as np
import pandas as pd
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class SampleWeightCalculator:
    """サンプル重み計算クラス"""
    
    # 重み設定（調整可能）
    DEFAULT_WEIGHT = 1.0
    
    # 除外・軽減
    OUTLIER_WEIGHT = 0.0      # 外れ値（1番人気大敗）
    LUCK_WEIGHT = 0.3         # 運要素（大穴好走）
    
    # 重視
    POPULAR_GOOD_WEIGHT = 1.5       # 人気馬好走
    MID_LONGSHOT_WEIGHT = 2.0       # 中穴好走
    COMPETITIVE_RACE_WEIGHT = 2.5   # 拮抗レース好走
    
    # 閾値
    POPULAR_THRESHOLD = 5      # 人気馬の定義
    MID_LONGSHOT_MIN = 6       # 中穴の最小人気
    MID_LONGSHOT_MAX = 9       # 中穴の最大人気
    EXTREME_LONGSHOT = 10      # 大穴の定義
    EXTREME_ODDS = 80.0        # 大穴オッズの定義
    GOOD_FINISH = 5            # 好走の定義
    BAD_FINISH = 10            # 大敗の定義
    COMPETITIVE_QUANTILE = 0.3 # 拮抗レースの定義（オッズ標準偏差下位30%）
    
    def __init__(
        self,
        popular_threshold: int = 5,
        extreme_longshot: int = 10,
        extreme_odds: float = 80.0,
        good_finish: int = 5,
        bad_finish: int = 10,
        enable_competitive_bonus: bool = True,
        enable_popularity_weighting: bool = True  # [5-3改修] 人気依存重みの無効化オプション
    ):
        """
        Args:
            popular_threshold: 人気馬とみなす最大人気順位
            extreme_longshot: 大穴とみなす最小人気順位
            extreme_odds: 大穴とみなす最小オッズ
            good_finish: 好走とみなす最大着順
            bad_finish: 大敗とみなす最小着順
            enable_competitive_bonus: 拮抗レースボーナスを有効化
            enable_popularity_weighting: [5-3改修] 人気依存の重み付けを有効化。
                Falseにするとpopularityに依存しない重み付け（着順のみ）になり、
                確定オッズ由来の微妙なリークを防止できる。
        """
        self.popular_threshold = popular_threshold
        self.extreme_longshot = extreme_longshot
        self.extreme_odds = extreme_odds
        self.good_finish = good_finish
        self.bad_finish = bad_finish
        self.enable_competitive_bonus = enable_competitive_bonus
        self.enable_popularity_weighting = enable_popularity_weighting
        
        self._race_odds_std_threshold = None
    
    def fit(self, df: pd.DataFrame) -> 'SampleWeightCalculator':
        """
        拮抗レースの閾値を学習データから計算
        
        Args:
            df: 学習データ（race_id, win_odds必須）
        """
        if self.enable_competitive_bonus and 'win_odds' in df.columns:
            # レースごとのオッズ標準偏差を計算
            race_odds_std = df.groupby('race_id')['win_odds'].transform('std')
            self._race_odds_std_threshold = race_odds_std.quantile(self.COMPETITIVE_QUANTILE)
            logger.info(f"拮抗レース閾値（オッズstd）: {self._race_odds_std_threshold:.2f}")
        
        return self
    
    def calc_weights(
        self, 
        df: pd.DataFrame,
        for_training: bool = True
    ) -> np.ndarray:
        """
        サンプル重みを計算
        
        Args:
            df: データフレーム（popularity, finish_position, win_odds, race_id必須）
            for_training: 学習用かどうか（Falseなら全て1.0を返す）
        
        Returns:
            weights: 各サンプルの重み配列
        """
        n = len(df)
        weights = np.ones(n) * self.DEFAULT_WEIGHT
        
        if not for_training:
            return weights
        
        # 必須カラムチェック
        required = ['popularity', 'finish_position']
        for col in required:
            if col not in df.columns:
                logger.warning(f"'{col}'カラムがありません。デフォルト重みを返します。")
                return weights
        
        pop = df['popularity'].values if 'popularity' in df.columns else np.ones(n) * 5
        finish = df['finish_position'].values
        odds = df['win_odds'].values if 'win_odds' in df.columns else np.ones(n) * 5.0
        
        # [5-3改修] popularity依存の重み付けを無効化可能に
        if not self.enable_popularity_weighting:
            # 人気に依存しない重み付け: 着順のみで重みを計算
            # 1-3着: 1.5, 4-5着: 1.2, 6着以降: 1.0
            good_mask = finish <= 3
            weights[good_mask] = 1.5
            ok_mask = (finish >= 4) & (finish <= self.good_finish)
            weights[ok_mask] = 1.2
            logger.info(f"[5-3] Popularity-free重み付け: 上位{good_mask.sum()}件=1.5, 中位{ok_mask.sum()}件=1.2")
            return weights
        
        # === 1. 外れ値除外: 1番人気で大敗 ===
        outlier_mask = (pop == 1) & (finish >= self.bad_finish)
        weights[outlier_mask] = self.OUTLIER_WEIGHT
        logger.debug(f"外れ値除外: {outlier_mask.sum()}件")
        
        # === 2. 運要素軽減: 大穴好走 ===
        luck_mask = ((pop >= self.extreme_longshot) | (odds >= self.extreme_odds)) & (finish <= 3)
        weights[luck_mask] = self.LUCK_WEIGHT
        logger.debug(f"運要素軽減: {luck_mask.sum()}件")
        
        # === 3. 人気馬好走: 重み増加 ===
        popular_good = (pop <= self.popular_threshold) & (finish <= self.good_finish)
        # 外れ値・運要素に該当しないもののみ
        popular_good = popular_good & (weights == self.DEFAULT_WEIGHT)
        weights[popular_good] = self.POPULAR_GOOD_WEIGHT
        logger.debug(f"人気馬好走: {popular_good.sum()}件")
        
        # === 4. 中穴好走: 重み大増加 ===
        mid_longshot_good = (
            (pop >= self.MID_LONGSHOT_MIN) & 
            (pop <= self.MID_LONGSHOT_MAX) & 
            (finish <= self.good_finish)
        )
        # 外れ値・運要素に該当しないもののみ
        mid_longshot_good = mid_longshot_good & (weights == self.DEFAULT_WEIGHT)
        weights[mid_longshot_good] = self.MID_LONGSHOT_WEIGHT
        logger.debug(f"中穴好走: {mid_longshot_good.sum()}件")
        
        # === 5. 拮抗オッズレース好走: 重み最大 ===
        if self.enable_competitive_bonus and self._race_odds_std_threshold is not None:
            if 'race_id' in df.columns:
                race_odds_std = df.groupby('race_id')['win_odds'].transform('std')
                competitive_mask = (
                    (race_odds_std < self._race_odds_std_threshold) &
                    (finish <= self.good_finish)
                )
                # 既に高い重みが付いていないもののみ
                competitive_mask = competitive_mask & (weights < self.COMPETITIVE_RACE_WEIGHT)
                weights[competitive_mask] = self.COMPETITIVE_RACE_WEIGHT
                logger.debug(f"拮抗レース好走: {competitive_mask.sum()}件")
        
        # 統計ログ
        logger.info(f"サンプル重み統計: mean={weights.mean():.2f}, "
                   f"min={weights.min():.2f}, max={weights.max():.2f}, "
                   f"non-default={(weights != self.DEFAULT_WEIGHT).sum()}/{n}")
        
        return weights
    
    def get_weight_stats(self, df: pd.DataFrame) -> dict:
        """重み付けの統計情報を返す"""
        weights = self.calc_weights(df, for_training=True)
        
        stats = {
            'total_samples': len(weights),
            'mean_weight': weights.mean(),
            'outlier_excluded': (weights == self.OUTLIER_WEIGHT).sum(),
            'luck_reduced': (weights == self.LUCK_WEIGHT).sum(),
            'popular_good_count': (weights == self.POPULAR_GOOD_WEIGHT).sum(),
            'mid_longshot_count': (weights == self.MID_LONGSHOT_WEIGHT).sum(),
            'competitive_count': (weights == self.COMPETITIVE_RACE_WEIGHT).sum(),
        }
        
        return stats


def calc_sample_weight(df: pd.DataFrame) -> np.ndarray:
    """
    簡易インターフェース: サンプル重みを計算して返す
    
    Args:
        df: データフレーム（popularity, finish_position, win_odds必須）
    
    Returns:
        weights: サンプル重み配列
    """
    calc = SampleWeightCalculator()
    calc.fit(df)
    return calc.calc_weights(df, for_training=True)

"""
Edge Calculator Module (v5.1)

複数の専門モデルの予測を統合し、エッジスコアを計算する核心モジュール。

【Edge Scoreの定義】
Edge Score = モデル統合確率 / オッズ暗示確率

Edge Score > 1.0: モデルが市場より高評価（ベット候補）
Edge Score < 1.0: モデルが市場より低評価（避けるべき）
Edge Score >= 1.2: 強いベット候補

【統合方式】
統合確率 = main_prob × (1 + pace_adj) × (1 + pedigree_adj) × (1 + condition_adj)
- 各調整値は ±0.2 程度にクリップして過剰調整を防止
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class EdgeCalculator:
    """
    エッジスコア計算（核心モジュール）
    
    複数の専門モデルの出力を統合し、市場オッズに対するエッジを計算する。
    """
    
    def __init__(
        self,
        takeout_rate: float = 0.20,  # JRA控除率
        pace_weight: float = 1.0,
        pedigree_weight: float = 1.0,
        condition_weight: float = 1.0,
        max_adjustment: float = 0.3,  # 最大調整幅
        min_edge_threshold: float = 1.0
    ):
        """
        Parameters:
        -----------
        takeout_rate : float
            控除率（JRAは約20%）
        pace_weight : float
            展開予測の重み
        pedigree_weight : float
            血統適性の重み
        condition_weight : float
            状態変化の重み
        max_adjustment : float
            各専門モデルの最大調整幅
        min_edge_threshold : float
            最小エッジ閾値
        """
        self.takeout_rate = takeout_rate
        self.pace_weight = pace_weight
        self.pedigree_weight = pedigree_weight
        self.condition_weight = condition_weight
        self.max_adjustment = max_adjustment
        self.min_edge_threshold = min_edge_threshold
    
    def calculate_odds_implied_prob(self, odds: np.ndarray) -> np.ndarray:
        """
        オッズから暗示確率を計算
        
        オッズ暗示確率 = (1/odds) / Σ(1/odds) × (1 - 控除率)
        """
        odds = np.clip(odds, 1.0, 1000.0)  # 極端なオッズをクリップ
        raw_prob = 1.0 / odds
        total_prob = raw_prob.sum()
        implied_prob = (raw_prob / total_prob) * (1 - self.takeout_rate)
        return implied_prob
    
    def calculate_model_prob(self, scores: np.ndarray) -> np.ndarray:
        """
        モデルスコアから確率を計算（softmax）
        
        LightGBM Rankerのスコアをsoftmax変換して確率に
        """
        # 温度パラメータで調整（1.0がデフォルト）
        temperature = 1.0
        exp_scores = np.exp(scores / temperature)
        probs = exp_scores / exp_scores.sum()
        return probs
    
    def integrate_models(
        self,
        main_prob: np.ndarray,
        pace_adjustment: Optional[np.ndarray] = None,
        pedigree_adjustment: Optional[np.ndarray] = None,
        condition_adjustment: Optional[np.ndarray] = None
    ) -> np.ndarray:
        """
        複数モデルの予測を統合
        
        統合確率 = main_prob × (1 + pace_adj) × (1 + pedigree_adj) × (1 + condition_adj)
        
        Parameters:
        -----------
        main_prob : np.ndarray
            メインモデルの確率予測
        pace_adjustment : np.ndarray, optional
            展開予測からの調整値（-0.2 ~ +0.2）
        pedigree_adjustment : np.ndarray, optional
            血統適性からの調整値（-0.15 ~ +0.15）
        condition_adjustment : np.ndarray, optional
            状態変化からの調整値（-0.1 ~ +0.1）
            
        Returns:
        --------
        np.ndarray : 統合確率
        """
        adjusted_prob = main_prob.copy()
        
        # 展開予測からの調整
        if pace_adjustment is not None:
            pace_adj = np.clip(pace_adjustment * self.pace_weight, 
                              -self.max_adjustment, self.max_adjustment)
            adjusted_prob = adjusted_prob * (1 + pace_adj)
        
        # 血統適性からの調整
        if pedigree_adjustment is not None:
            pedigree_adj = np.clip(pedigree_adjustment * self.pedigree_weight,
                                  -self.max_adjustment, self.max_adjustment)
            adjusted_prob = adjusted_prob * (1 + pedigree_adj)
        
        # 状態変化からの調整
        if condition_adjustment is not None:
            condition_adj = np.clip(condition_adjustment * self.condition_weight,
                                   -self.max_adjustment, self.max_adjustment)
            adjusted_prob = adjusted_prob * (1 + condition_adj)
        
        # 全体の調整幅を制限（0.5〜2.0倍）
        adjustment_factor = adjusted_prob / main_prob
        adjustment_factor = np.clip(adjustment_factor, 0.5, 2.0)
        adjusted_prob = main_prob * adjustment_factor
        
        # 正規化
        adjusted_prob = adjusted_prob / adjusted_prob.sum()
        
        return adjusted_prob
    
    def calculate_edge_scores(
        self,
        odds: np.ndarray,
        main_scores: np.ndarray,
        pace_adjustment: Optional[np.ndarray] = None,
        pedigree_adjustment: Optional[np.ndarray] = None,
        condition_adjustment: Optional[np.ndarray] = None
    ) -> Tuple[np.ndarray, np.ndarray, float]:
        """
        エッジスコアを計算
        
        Returns:
        --------
        edge_scores : np.ndarray
            各馬のエッジスコア
        adjusted_probs : np.ndarray
            統合確率
        confidence : float
            予測の信頼度（0-1）
        """
        # オッズ暗示確率
        odds_probs = self.calculate_odds_implied_prob(odds)
        
        # メインモデル確率
        main_prob = self.calculate_model_prob(main_scores)
        
        # 統合確率
        adjusted_probs = self.integrate_models(
            main_prob, pace_adjustment, pedigree_adjustment, condition_adjustment
        )
        
        # Edge Score計算
        edge_scores = adjusted_probs / odds_probs
        
        # 信頼度計算（モデル間の一致度）
        adjustment_magnitude = np.abs(adjusted_probs - main_prob).mean()
        confidence = 1.0 - adjustment_magnitude * 5
        confidence = np.clip(confidence, 0, 1)
        
        return edge_scores, adjusted_probs, confidence
    
    def select_bets(
        self,
        edge_scores: np.ndarray,
        adjusted_probs: np.ndarray,
        odds: np.ndarray,
        confidence: float,
        edge_threshold: float = 1.2,
        min_confidence: float = 0.6,
        ev_threshold: float = 1.0
    ) -> List[Dict]:
        """
        ベット対象を選択
        
        Parameters:
        -----------
        edge_threshold : float
            エッジスコアの閾値
        min_confidence : float
            最低信頼度
        ev_threshold : float
            最低期待値
            
        Returns:
        --------
        List[Dict] : ベット対象のリスト
        """
        bets = []
        
        for i, (edge, prob, odd) in enumerate(zip(edge_scores, adjusted_probs, odds)):
            ev = prob * odd  # 期待値
            
            if edge >= edge_threshold and confidence >= min_confidence and ev >= ev_threshold:
                bets.append({
                    'horse_index': i,
                    'edge_score': float(edge),
                    'model_prob': float(prob),
                    'odds': float(odd),
                    'expected_value': float(ev),
                    'confidence': float(confidence)
                })
        
        # エッジスコア順にソート
        bets = sorted(bets, key=lambda x: x['edge_score'], reverse=True)
        
        return bets


class EdgeOptimizer:
    """
    エッジ閾値の最適化
    
    過去データを使用して最適なエッジ閾値を探索する。
    """
    
    def __init__(self, min_bet_count: int = 100):
        self.min_bet_count = min_bet_count
        self.optimal_threshold = None
        self.threshold_results = []
    
    def optimize(
        self,
        edge_scores: np.ndarray,
        is_win: np.ndarray,
        odds: np.ndarray,
        thresholds: List[float] = None
    ) -> Dict:
        """
        最適閾値を探索
        
        Returns:
        --------
        Dict : 最適閾値と評価結果
        """
        if thresholds is None:
            thresholds = [0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.8, 2.0, 2.5]
        
        self.threshold_results = []
        best_roi = 0
        best_threshold = None
        
        for threshold in thresholds:
            mask = edge_scores >= threshold
            n_bets = mask.sum()
            
            if n_bets < self.min_bet_count:
                continue
            
            wins = is_win[mask].sum()
            total_payout = (is_win[mask] * odds[mask]).sum()
            roi = total_payout / n_bets
            hit_rate = wins / n_bets
            
            result = {
                'threshold': threshold,
                'bet_count': int(n_bets),
                'win_count': int(wins),
                'hit_rate': float(hit_rate),
                'roi': float(roi),
                'avg_odds': float(odds[mask].mean())
            }
            self.threshold_results.append(result)
            
            if roi > best_roi:
                best_roi = roi
                best_threshold = threshold
        
        self.optimal_threshold = best_threshold
        
        return {
            'optimal_threshold': best_threshold,
            'optimal_roi': best_roi,
            'all_results': self.threshold_results
        }


if __name__ == "__main__":
    # テスト
    np.random.seed(42)
    
    # サンプルデータ（16頭立て）
    n_horses = 16
    odds = np.array([2.5, 4.0, 6.0, 8.0, 12.0, 15.0, 20.0, 25.0,
                     30.0, 40.0, 50.0, 80.0, 100.0, 120.0, 150.0, 200.0])
    main_scores = np.array([3.0, 2.5, 2.2, 2.0, 1.8, 1.6, 1.4, 1.2,
                           1.0, 0.8, 0.6, 0.4, 0.2, 0.0, -0.2, -0.4])
    
    # 専門モデルからの調整
    pace_adjustment = np.random.uniform(-0.15, 0.15, n_horses)  # 展開予測
    pedigree_adjustment = np.random.uniform(-0.10, 0.10, n_horses)  # 血統適性
    
    # EdgeCalculatorのテスト
    calculator = EdgeCalculator()
    edge_scores, adjusted_probs, confidence = calculator.calculate_edge_scores(
        odds, main_scores, pace_adjustment, pedigree_adjustment
    )
    
    print("=" * 60)
    print("EdgeCalculator テスト結果")
    print("=" * 60)
    print(f"信頼度: {confidence:.2f}")
    print("\n馬番 | オッズ | メイン確率 | 統合確率 | Edge Score")
    print("-" * 60)
    
    main_probs = calculator.calculate_model_prob(main_scores)
    for i in range(n_horses):
        print(f" {i+1:2d}  | {odds[i]:6.1f} | {main_probs[i]:9.1%} | {adjusted_probs[i]:8.1%} | {edge_scores[i]:6.2f}")
    
    # ベット選択
    bets = calculator.select_bets(edge_scores, adjusted_probs, odds, confidence, 
                                  edge_threshold=1.3)
    
    print("\n" + "=" * 60)
    print("ベット対象")
    print("=" * 60)
    for bet in bets:
        print(f"馬番{bet['horse_index']+1}: Edge={bet['edge_score']:.2f}, EV={bet['expected_value']:.2f}")

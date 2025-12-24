"""
Layer 3: Race Simulator (シミュレーション層)

全馬券種の出現確率を算出する。

Methods:
- Harville Formula: 解析的に計算（高速だが独立性仮定）
- Monte Carlo: シミュレーションで計算（柔軟だが計算コスト高）

Supported Bet Types:
- 単勝 (win)
- 複勝 (place)
- 馬連 (quinella)
- 馬単 (exacta)
- 三連複 (trio)
- 三連単 (trifecta)
- ワイド (wide)
"""

import numpy as np
from itertools import permutations, combinations
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

# Numbaがある場合は高速化に使用
try:
    import numba
    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False


class Layer3_RaceSimulator:
    """
    Layer 3: 着順シミュレーション層
    
    勝率から各馬券種の確率を算出
    """
    
    def __init__(
        self, 
        method: str = 'harville', 
        n_simulations: int = 10000
    ):
        """
        Args:
            method: 'harville' or 'monte_carlo'
            n_simulations: モンテカルロ法のシミュレーション回数
        """
        self.method = method
        self.n_simulations = n_simulations
        
    def simulate_race(
        self, 
        win_probs: np.ndarray, 
        ability_scores: Optional[np.ndarray] = None
    ) -> Dict[str, Dict]:
        """
        レースをシミュレートし、全馬券種の確率を返す
        
        Args:
            win_probs: 各馬の勝率（合計1.0）
            ability_scores: 能力スコア（モンテカルロ法で使用）
        
        Returns:
            {
                'win': {馬番: 確率},
                'exacta': {(1着, 2着): 確率},
                'quinella': {(馬1, 馬2): 確率},  # 順不同
                'trifecta': {(1着, 2着, 3着): 確率},
                'trio': {(馬1, 馬2, 馬3): 確率},  # 順不同
            }
        """
        if self.method == 'harville':
            return self._harville_simulation(win_probs)
        elif self.method == 'monte_carlo':
            if ability_scores is None:
                # 確率からスコアを逆算（logit変換）
                ability_scores = np.log(win_probs + 1e-10) - np.log(1 - win_probs + 1e-10)
            return self._monte_carlo_simulation(win_probs, ability_scores)
        else:
            raise ValueError(f"Unknown method: {self.method}")
    
    def _harville_simulation(self, win_probs: np.ndarray) -> Dict[str, Dict]:
        """
        ハービル法による全馬券種確率計算
        """
        return {
            'win': self._calculate_win_prob(win_probs),
            'exacta': self._calculate_exacta_prob(win_probs),
            'quinella': self._calculate_quinella_prob(win_probs),
            'trifecta': self._calculate_trifecta_prob(win_probs),
            'trio': self._calculate_trio_prob(win_probs),
            'wide': self._calculate_wide_prob(win_probs),
        }
    
    def _calculate_win_prob(self, win_probs: np.ndarray) -> Dict[int, float]:
        """単勝確率"""
        return {i: p for i, p in enumerate(win_probs)}
    
    def _calculate_exacta_prob(self, win_probs: np.ndarray) -> Dict[Tuple[int, int], float]:
        """
        馬単（1着-2着）の全組み合わせ確率を計算
        
        ハービル法:
        P(i→j) = P(i wins) × P(j wins | i won)
               = P(i) × P(j) / (1 - P(i))
        """
        n = len(win_probs)
        exacta_probs = {}
        
        for i in range(n):
            for j in range(n):
                if i != j:
                    p_i = win_probs[i]
                    p_j = win_probs[j]
                    prob = p_i * (p_j / (1 - p_i + 1e-10))
                    exacta_probs[(i, j)] = prob
        
        return exacta_probs
    
    def _calculate_quinella_prob(self, win_probs: np.ndarray) -> Dict[Tuple[int, int], float]:
        """
        馬連（1-2着の組み合わせ、順不同）の確率を計算
        
        P(i-j) = P(i→j) + P(j→i)
        """
        exacta = self._calculate_exacta_prob(win_probs)
        quinella_probs = {}
        
        n = len(win_probs)
        for i in range(n):
            for j in range(i + 1, n):
                prob = exacta.get((i, j), 0) + exacta.get((j, i), 0)
                quinella_probs[(i, j)] = prob
        
        return quinella_probs
    
    def _calculate_trifecta_prob(self, win_probs: np.ndarray) -> Dict[Tuple[int, int, int], float]:
        """
        三連単（1着-2着-3着）の全順列確率を計算
        
        ハービル法:
        P(i→j→k) = P(i) × P(j)/(1-P(i)) × P(k)/(1-P(i)-P(j))
        """
        n = len(win_probs)
        trifecta_probs = {}
        
        for i in range(n):
            p_i = win_probs[i]
            remaining_after_i = 1 - p_i
            
            for j in range(n):
                if j == i:
                    continue
                    
                p_j = win_probs[j]
                p_j_given_i = p_j / (remaining_after_i + 1e-10)
                remaining_after_ij = remaining_after_i - p_j
                
                for k in range(n):
                    if k == i or k == j:
                        continue
                    
                    p_k = win_probs[k]
                    p_k_given_ij = p_k / (remaining_after_ij + 1e-10)
                    
                    prob = p_i * p_j_given_i * p_k_given_ij
                    trifecta_probs[(i, j, k)] = prob
        
        return trifecta_probs
    
    def _calculate_trio_prob(self, win_probs: np.ndarray) -> Dict[Tuple[int, int, int], float]:
        """
        三連複（1-2-3着の組み合わせ、順不同）の確率を計算
        """
        trifecta = self._calculate_trifecta_prob(win_probs)
        trio_probs = {}
        
        n = len(win_probs)
        for combo in combinations(range(n), 3):
            total_prob = 0.0
            for perm in permutations(combo):
                total_prob += trifecta.get(perm, 0)
            trio_probs[combo] = total_prob
        
        return trio_probs
    
    def _calculate_wide_prob(self, win_probs: np.ndarray) -> Dict[Tuple[int, int], float]:
        """
        ワイド（1-2着 or 1-3着 or 2-3着）の確率を計算
        
        P(wide i-j) = P(i,jともに3着以内)
        """
        n = len(win_probs)
        wide_probs = {}
        
        # 三連複を使って計算（i,jが含まれる三連複の合計）
        trio = self._calculate_trio_prob(win_probs)
        
        for i in range(n):
            for j in range(i + 1, n):
                prob = 0.0
                for combo, p in trio.items():
                    if i in combo and j in combo:
                        prob += p
                wide_probs[(i, j)] = prob
        
        return wide_probs
    
    def _monte_carlo_simulation(
        self, 
        win_probs: np.ndarray, 
        ability_scores: np.ndarray
    ) -> Dict[str, Dict]:
        """
        モンテカルロ・シミュレーションによる確率計算
        
        スコアにガンベル分布のノイズを加えてレースをシミュレート
        """
        n = len(win_probs)
        
        # 結果カウンター
        win_counts = np.zeros(n)
        exacta_counts = {}
        trifecta_counts = {}
        
        for _ in range(self.n_simulations):
            # ガンベル分布のノイズを加えてパフォーマンスを生成
            noise = np.random.gumbel(0, 1, n)
            performance = ability_scores + noise
            
            # 着順決定（高いほど良い）
            finish_order = np.argsort(-performance)
            
            # カウント
            win_counts[finish_order[0]] += 1
            
            exacta_key = (finish_order[0], finish_order[1])
            exacta_counts[exacta_key] = exacta_counts.get(exacta_key, 0) + 1
            
            trifecta_key = (finish_order[0], finish_order[1], finish_order[2])
            trifecta_counts[trifecta_key] = trifecta_counts.get(trifecta_key, 0) + 1
        
        # 確率に変換
        win_probs_mc = {i: c / self.n_simulations for i, c in enumerate(win_counts)}
        exacta_probs_mc = {k: v / self.n_simulations for k, v in exacta_counts.items()}
        trifecta_probs_mc = {k: v / self.n_simulations for k, v in trifecta_counts.items()}
        
        # 馬連・三連複は馬単・三連単から計算
        quinella_probs_mc = {}
        for (i, j), p in exacta_probs_mc.items():
            key = (min(i, j), max(i, j))
            quinella_probs_mc[key] = quinella_probs_mc.get(key, 0) + p
        
        trio_probs_mc = {}
        for (i, j, k), p in trifecta_probs_mc.items():
            key = tuple(sorted([i, j, k]))
            trio_probs_mc[key] = trio_probs_mc.get(key, 0) + p
        
        return {
            'win': win_probs_mc,
            'exacta': exacta_probs_mc,
            'quinella': quinella_probs_mc,
            'trifecta': trifecta_probs_mc,
            'trio': trio_probs_mc,
        }
    
    def get_top_tickets(
        self, 
        race_probs: Dict[str, Dict], 
        bet_type: str, 
        top_n: int = 10
    ) -> List[Tuple]:
        """
        指定した馬券種の確率上位を取得
        
        Returns:
            [(組み合わせ, 確率), ...]
        """
        probs = race_probs.get(bet_type, {})
        sorted_probs = sorted(probs.items(), key=lambda x: x[1], reverse=True)
        return sorted_probs[:top_n]


# ============================================================
# 高速化バージョン（Numba使用時）
# ============================================================
if NUMBA_AVAILABLE:
    @numba.jit(nopython=True, cache=True)
    def _harville_trifecta_fast(win_probs: np.ndarray) -> np.ndarray:
        """
        Numbaによる三連単確率の高速計算
        
        Returns:
            3D array of shape (n, n, n) with trifecta probabilities
        """
        n = len(win_probs)
        results = np.zeros((n, n, n))
        
        for i in range(n):
            p_i = win_probs[i]
            remaining_i = 1 - p_i
            
            for j in range(n):
                if j == i:
                    continue
                p_j = win_probs[j]
                p_j_given_i = p_j / (remaining_i + 1e-10)
                remaining_ij = remaining_i - p_j
                
                for k in range(n):
                    if k == i or k == j:
                        continue
                    p_k = win_probs[k]
                    p_k_given_ij = p_k / (remaining_ij + 1e-10)
                    
                    results[i, j, k] = p_i * p_j_given_i * p_k_given_ij
        
        return results
    
    @numba.jit(nopython=True, cache=True)
    def _monte_carlo_fast(
        ability_scores: np.ndarray, 
        n_simulations: int
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Numbaによるモンテカルロシミュレーションの高速化
        """
        n = len(ability_scores)
        win_counts = np.zeros(n)
        
        for _ in range(n_simulations):
            noise = np.random.standard_normal(n)  # ガンベルの代わりに正規分布
            performance = ability_scores + noise
            
            # 着順決定
            order = np.argsort(-performance)
            win_counts[order[0]] += 1
        
        return win_counts / n_simulations


class Layer3_RaceSimulatorFast(Layer3_RaceSimulator):
    """
    高速版レースシミュレーター（Numba使用）
    """
    
    def _calculate_trifecta_prob(self, win_probs: np.ndarray) -> Dict[Tuple[int, int, int], float]:
        """Numbaで高速化された三連単計算"""
        if NUMBA_AVAILABLE:
            result_array = _harville_trifecta_fast(win_probs)
            
            # Dictに変換
            n = len(win_probs)
            trifecta_probs = {}
            for i in range(n):
                for j in range(n):
                    for k in range(n):
                        if result_array[i, j, k] > 0:
                            trifecta_probs[(i, j, k)] = result_array[i, j, k]
            return trifecta_probs
        else:
            return super()._calculate_trifecta_prob(win_probs)

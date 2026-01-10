# -*- coding: utf-8 -*-
"""
EV (Expected Value) Calculator
期待値計算モジュール

全券種の期待値を計算し、EV >= threshold の組み合わせを抽出する。
オッズデータと確率予測を組み合わせてEVを算出。

対応券種:
- 単勝（Win）
- 複勝（Place）
- 馬連（Quinella）
- ワイド（Quinella Place）
- 馬単（Exacta）
- 三連複（Trio）
- 三連単（Trifecta）
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from itertools import combinations, permutations
import logging

from .probability_calibrator import Layer2_ProbabilityCalibrator

logger = logging.getLogger(__name__)


def harville_probability(win_probs: np.ndarray, finish_order: List[int]) -> float:
    """
    ハービル法で指定着順の確率を計算
    
    Args:
        win_probs: 各馬の勝率（レース内で合計=1.0に正規化済み）
        finish_order: 着順リスト [i, j, k] = i番→j番→k番の順で入着
    
    Returns:
        その着順になる確率
    
    Example:
        >>> probs = np.array([0.3, 0.25, 0.2, 0.15, 0.1])
        >>> harville_probability(probs, [0, 1])  # 馬番0が1着、馬番1が2着
    """
    prob = 1.0
    remaining = 1.0
    
    for horse_idx in finish_order:
        if remaining <= 0:
            return 0.0
        prob *= win_probs[horse_idx] / remaining
        remaining -= win_probs[horse_idx]
    
    return prob


def calculate_quinella_probability(win_probs: np.ndarray, horse1: int, horse2: int) -> float:
    """
    馬連確率を計算（順不同で2頭が1-2着）
    
    P({i,j} ∈ Top2) = P(i→j) + P(j→i)
    """
    p_ij = harville_probability(win_probs, [horse1, horse2])
    p_ji = harville_probability(win_probs, [horse2, horse1])
    return p_ij + p_ji


def calculate_exacta_probability(win_probs: np.ndarray, horse1: int, horse2: int) -> float:
    """
    馬単確率を計算（horse1が1着、horse2が2着）
    """
    return harville_probability(win_probs, [horse1, horse2])


def calculate_trio_probability(win_probs: np.ndarray, horse1: int, horse2: int, horse3: int) -> float:
    """
    三連複確率を計算（順不同で3頭が1-2-3着）
    
    6通りの順列の確率を合計
    """
    horses = [horse1, horse2, horse3]
    total_prob = 0.0
    
    for perm in permutations(horses):
        total_prob += harville_probability(win_probs, list(perm))
    
    return total_prob


def calculate_trifecta_probability(win_probs: np.ndarray, horse1: int, horse2: int, horse3: int) -> float:
    """
    三連単確率を計算（horse1→horse2→horse3の順で入着）
    """
    return harville_probability(win_probs, [horse1, horse2, horse3])


class EVCalculator:
    """
    期待値計算クラス
    
    モデルの予測確率とオッズデータを組み合わせて、
    各組み合わせの期待値（EV）を計算する。
    """
    
    def __init__(self, calibrator: Optional[Layer2_ProbabilityCalibrator] = None):
        """
        Args:
            calibrator: 確率キャリブレーター（Noneの場合は直接確率を使用）
        """
        self.calibrator = calibrator
    
    def calculate_win_ev(
        self,
        win_probs: np.ndarray,
        odds_data: List[Dict],
        horse_numbers: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        単勝EVを計算
        
        Args:
            win_probs: 各馬の勝率（レース内で正規化済み）
            odds_data: オッズデータ [{'horse_number': int, 'win_odds': float}, ...]
            horse_numbers: 馬番リスト（Noneの場合は1からの連番）
        
        Returns:
            DataFrame: horse_number, win_prob, odds, ev
        """
        if horse_numbers is None:
            horse_numbers = list(range(1, len(win_probs) + 1))
        
        # オッズをマッピング
        odds_map = {d['horse_number']: d['win_odds'] for d in odds_data if d.get('win_odds')}
        
        results = []
        for i, (horse_num, prob) in enumerate(zip(horse_numbers, win_probs)):
            odds = odds_map.get(horse_num)
            if odds is None or odds <= 0:
                continue
            
            ev = prob * odds
            results.append({
                'horse_number': horse_num,
                'win_prob': prob,
                'odds': odds,
                'ev': ev
            })
        
        return pd.DataFrame(results)
    
    def calculate_place_ev(
        self,
        win_probs: np.ndarray,
        odds_data: List[Dict],
        horse_numbers: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        複勝EVを計算
        
        Args:
            win_probs: 各馬の勝率（レース内で正規化済み）
            odds_data: オッズデータ [{'horse_number': int, 'place_odds_min': float, 'place_odds_max': float}, ...]
            horse_numbers: 馬番リスト
        
        Returns:
            DataFrame: horse_number, top3_prob, odds_min, odds_max, ev_min, ev_max, ev_avg
        """
        if horse_numbers is None:
            horse_numbers = list(range(1, len(win_probs) + 1))
        
        n = len(win_probs)
        
        # 3着以内確率を計算
        top3_probs = self._calculate_top3_probs(win_probs)
        
        # オッズをマッピング
        odds_map = {
            d['horse_number']: (d.get('place_odds_min'), d.get('place_odds_max'))
            for d in odds_data
        }
        
        results = []
        for i, (horse_num, top3_prob) in enumerate(zip(horse_numbers, top3_probs)):
            odds_min, odds_max = odds_map.get(horse_num, (None, None))
            if odds_min is None or odds_max is None:
                continue
            
            # EV計算（オッズの中央値を使用）
            odds_avg = (odds_min + odds_max) / 2
            ev_min = top3_prob * odds_min
            ev_max = top3_prob * odds_max
            ev_avg = top3_prob * odds_avg
            
            results.append({
                'horse_number': horse_num,
                'top3_prob': top3_prob,
                'odds_min': odds_min,
                'odds_max': odds_max,
                'ev_min': ev_min,
                'ev_max': ev_max,
                'ev_avg': ev_avg
            })
        
        return pd.DataFrame(results)
    
    def _calculate_top3_probs(self, win_probs: np.ndarray) -> np.ndarray:
        """
        3着以内確率を計算（ハービル法）
        
        P(i ∈ Top3) = P(i 1着) + P(i 2着) + P(i 3着)
        """
        n = len(win_probs)
        top3_probs = np.zeros(n)
        
        for i in range(n):
            # P(i 1着)
            p_1st = win_probs[i]
            
            # P(i 2着) = Σ_j P(j 1着) * P(i 2着 | j 1着)
            p_2nd = 0.0
            for j in range(n):
                if j != i:
                    p_2nd += harville_probability(win_probs, [j, i])
            
            # P(i 3着) = Σ_j Σ_k P(j→k→i)
            p_3rd = 0.0
            for j in range(n):
                if j != i:
                    for k in range(n):
                        if k != i and k != j:
                            p_3rd += harville_probability(win_probs, [j, k, i])
            
            top3_probs[i] = p_1st + p_2nd + p_3rd
        
        return top3_probs
    
    def calculate_quinella_ev(
        self,
        win_probs: np.ndarray,
        odds_data: List[Dict],
        horse_numbers: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        馬連EVを全組み合わせで計算
        
        Args:
            win_probs: 各馬の勝率
            odds_data: オッズデータ [{'horse1': int, 'horse2': int, 'odds': float}, ...]
            horse_numbers: 馬番リスト
        
        Returns:
            DataFrame: horse1, horse2, prob, odds, ev
        """
        if horse_numbers is None:
            horse_numbers = list(range(1, len(win_probs) + 1))
        
        # オッズをマッピング（小さい番号, 大きい番号）
        odds_map = {}
        for d in odds_data:
            h1, h2 = d['horse1'], d['horse2']
            key = (min(h1, h2), max(h1, h2))
            odds_map[key] = d['odds']
        
        # 馬番→インデックスのマッピング
        num_to_idx = {num: idx for idx, num in enumerate(horse_numbers)}
        
        results = []
        for h1, h2 in combinations(horse_numbers, 2):
            key = (h1, h2)
            odds = odds_map.get(key)
            if odds is None or odds <= 0:
                continue
            
            idx1, idx2 = num_to_idx[h1], num_to_idx[h2]
            prob = calculate_quinella_probability(win_probs, idx1, idx2)
            ev = prob * odds
            
            results.append({
                'horse1': h1,
                'horse2': h2,
                'prob': prob,
                'odds': odds,
                'ev': ev
            })
        
        return pd.DataFrame(results)
    
    def calculate_wide_ev(
        self,
        win_probs: np.ndarray,
        odds_data: List[Dict],
        horse_numbers: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        ワイドEVを全組み合わせで計算
        
        P(i,j ∈ Top3) = Σ P(i,j,k が Top3) for all k ≠ i,j
        """
        if horse_numbers is None:
            horse_numbers = list(range(1, len(win_probs) + 1))
        
        # オッズをマッピング
        odds_map = {}
        for d in odds_data:
            h1, h2 = d['horse1'], d['horse2']
            key = (min(h1, h2), max(h1, h2))
            odds_map[key] = d['odds']
        
        num_to_idx = {num: idx for idx, num in enumerate(horse_numbers)}
        n = len(horse_numbers)
        
        results = []
        for h1, h2 in combinations(horse_numbers, 2):
            key = (h1, h2)
            odds = odds_map.get(key)
            if odds is None or odds <= 0:
                continue
            
            idx1, idx2 = num_to_idx[h1], num_to_idx[h2]
            
            # P(i,j ∈ Top3) = Σ P({i,j,k} ∈ Top3) for k ≠ i,j
            prob = 0.0
            for k in range(n):
                if k != idx1 and k != idx2:
                    prob += calculate_trio_probability(win_probs, idx1, idx2, k)
            
            ev = prob * odds
            
            results.append({
                'horse1': h1,
                'horse2': h2,
                'prob': prob,
                'odds': odds,
                'ev': ev
            })
        
        return pd.DataFrame(results)
    
    def calculate_exacta_ev(
        self,
        win_probs: np.ndarray,
        odds_data: List[Dict],
        horse_numbers: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        馬単EVを全組み合わせで計算
        """
        if horse_numbers is None:
            horse_numbers = list(range(1, len(win_probs) + 1))
        
        # オッズをマッピング（順序あり）
        odds_map = {}
        for d in odds_data:
            key = (d['horse1'], d['horse2'])
            odds_map[key] = d['odds']
        
        num_to_idx = {num: idx for idx, num in enumerate(horse_numbers)}
        
        results = []
        for h1, h2 in permutations(horse_numbers, 2):
            key = (h1, h2)
            odds = odds_map.get(key)
            if odds is None or odds <= 0:
                continue
            
            idx1, idx2 = num_to_idx[h1], num_to_idx[h2]
            prob = calculate_exacta_probability(win_probs, idx1, idx2)
            ev = prob * odds
            
            results.append({
                'horse1': h1,
                'horse2': h2,
                'prob': prob,
                'odds': odds,
                'ev': ev
            })
        
        return pd.DataFrame(results)
    
    def calculate_trio_ev(
        self,
        win_probs: np.ndarray,
        odds_data: List[Dict],
        horse_numbers: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        三連複EVを全組み合わせで計算
        """
        if horse_numbers is None:
            horse_numbers = list(range(1, len(win_probs) + 1))
        
        # オッズをマッピング（ソート済みキー）
        odds_map = {}
        for d in odds_data:
            horses = tuple(sorted([d['horse1'], d['horse2'], d['horse3']]))
            odds_map[horses] = d['odds']
        
        num_to_idx = {num: idx for idx, num in enumerate(horse_numbers)}
        
        results = []
        for h1, h2, h3 in combinations(horse_numbers, 3):
            key = (h1, h2, h3)
            odds = odds_map.get(key)
            if odds is None or odds <= 0:
                continue
            
            idx1, idx2, idx3 = num_to_idx[h1], num_to_idx[h2], num_to_idx[h3]
            prob = calculate_trio_probability(win_probs, idx1, idx2, idx3)
            ev = prob * odds
            
            results.append({
                'horse1': h1,
                'horse2': h2,
                'horse3': h3,
                'prob': prob,
                'odds': odds,
                'ev': ev,
                'rank': d.get('rank')
            })
        
        return pd.DataFrame(results)
    
    def calculate_trifecta_ev(
        self,
        win_probs: np.ndarray,
        odds_data: List[Dict],
        horse_numbers: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        三連単EVを全組み合わせで計算
        """
        if horse_numbers is None:
            horse_numbers = list(range(1, len(win_probs) + 1))
        
        # オッズをマッピング（順序あり）
        odds_map = {}
        for d in odds_data:
            key = (d['horse1'], d['horse2'], d['horse3'])
            odds_map[key] = d['odds']
        
        num_to_idx = {num: idx for idx, num in enumerate(horse_numbers)}
        
        results = []
        for h1, h2, h3 in permutations(horse_numbers, 3):
            key = (h1, h2, h3)
            odds = odds_map.get(key)
            if odds is None or odds <= 0:
                continue
            
            idx1, idx2, idx3 = num_to_idx[h1], num_to_idx[h2], num_to_idx[h3]
            prob = calculate_trifecta_probability(win_probs, idx1, idx2, idx3)
            ev = prob * odds
            
            results.append({
                'horse1': h1,
                'horse2': h2,
                'horse3': h3,
                'prob': prob,
                'odds': odds,
                'ev': ev,
                'rank': odds_data[0].get('rank') if odds_data else None
            })
        
        return pd.DataFrame(results)
    
    def calculate_all_ev(
        self,
        win_probs: np.ndarray,
        odds_json: Dict[str, Any],
        horse_numbers: Optional[List[int]] = None,
        ev_threshold: float = 1.0
    ) -> Dict[str, pd.DataFrame]:
        """
        全券種のEVを計算し、閾値以上の組み合わせを抽出
        
        Args:
            win_probs: 各馬の勝率（レース内で正規化済み）
            odds_json: スクレイピングで取得したオッズJSON
            horse_numbers: 馬番リスト
            ev_threshold: EV閾値（これ以上のみ抽出）
        
        Returns:
            {
                'win': DataFrame,
                'place': DataFrame,
                'quinella': DataFrame,
                'quinella_place': DataFrame,
                'exacta': DataFrame,
                'trio': DataFrame,
                'trifecta': DataFrame,
                'summary': Dict
            }
        """
        results = {}
        
        # 単勝
        if 'win_place' in odds_json:
            win_df = self.calculate_win_ev(win_probs, odds_json['win_place']['data'], horse_numbers)
            results['win'] = win_df[win_df['ev'] >= ev_threshold] if len(win_df) > 0 else win_df
            
            # 複勝
            place_df = self.calculate_place_ev(win_probs, odds_json['win_place']['data'], horse_numbers)
            results['place'] = place_df[place_df['ev_avg'] >= ev_threshold] if len(place_df) > 0 else place_df
        
        # 馬連
        if 'quinella' in odds_json:
            quinella_df = self.calculate_quinella_ev(win_probs, odds_json['quinella']['data'], horse_numbers)
            results['quinella'] = quinella_df[quinella_df['ev'] >= ev_threshold] if len(quinella_df) > 0 else quinella_df
        
        # ワイド
        if 'quinella_place' in odds_json:
            wide_df = self.calculate_wide_ev(win_probs, odds_json['quinella_place']['data'], horse_numbers)
            results['quinella_place'] = wide_df[wide_df['ev'] >= ev_threshold] if len(wide_df) > 0 else wide_df
        
        # 馬単
        if 'exacta' in odds_json:
            exacta_df = self.calculate_exacta_ev(win_probs, odds_json['exacta']['data'], horse_numbers)
            results['exacta'] = exacta_df[exacta_df['ev'] >= ev_threshold] if len(exacta_df) > 0 else exacta_df
        
        # 三連複
        if 'trio' in odds_json:
            trio_df = self.calculate_trio_ev(win_probs, odds_json['trio']['data'], horse_numbers)
            results['trio'] = trio_df[trio_df['ev'] >= ev_threshold] if len(trio_df) > 0 else trio_df
        
        # 三連単
        if 'trifecta' in odds_json:
            trifecta_df = self.calculate_trifecta_ev(win_probs, odds_json['trifecta']['data'], horse_numbers)
            results['trifecta'] = trifecta_df[trifecta_df['ev'] >= ev_threshold] if len(trifecta_df) > 0 else trifecta_df
        
        # サマリー
        results['summary'] = {
            'threshold': ev_threshold,
            'candidates': {
                bet_type: len(df) for bet_type, df in results.items() 
                if isinstance(df, pd.DataFrame) and len(df) > 0
            }
        }
        
        return results


def filter_by_ev(ev_results: Dict[str, pd.DataFrame], ev_threshold: float = 1.0) -> Dict[str, pd.DataFrame]:
    """
    EV閾値でフィルタリング
    """
    filtered = {}
    for bet_type, df in ev_results.items():
        if bet_type == 'summary':
            continue
        if not isinstance(df, pd.DataFrame) or len(df) == 0:
            continue
        
        if 'ev' in df.columns:
            filtered[bet_type] = df[df['ev'] >= ev_threshold].copy()
        elif 'ev_avg' in df.columns:
            filtered[bet_type] = df[df['ev_avg'] >= ev_threshold].copy()
    
    return filtered

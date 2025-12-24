"""
Layer 4: EV Optimizer (期待値最適化層)

期待値ベースで馬券を選別し、資金配分を最適化する。

Features:
- JRA控除率を考慮したEV計算
- 高オッズ補正（50倍以上で減衰）
- EVフィルタリング（閾値以上のみ購入）
- 不確実性考慮のKelly基準
- ポートフォリオ最適化
"""

import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class BetTicket:
    """馬券チケット"""
    bet_type: str          # 'win', 'exacta', 'quinella', 'trifecta', 'trio'
    combination: Tuple     # (馬番, ...) のタプル
    model_prob: float      # モデル予測確率
    odds: float            # オッズ
    ev: float              # 期待値
    kelly_fraction: float  # ケリー基準によるベット割合
    amount: int            # 実際のベット額（円）


class Layer4_EVOptimizer:
    """
    Layer 4: 期待値最適化層
    
    EV > 閾値 の馬券を抽出し、Kelly基準で資金配分
    """
    
    # JRA控除率（1 - 控除率 = 払戻率）
    PAYOUT_RATES = {
        'win': 0.80,       # 単勝: 控除20%
        'place': 0.80,     # 複勝: 控除20%
        'quinella': 0.775, # 馬連: 控除22.5%
        'exacta': 0.775,   # 馬単: 控除22.5%
        'wide': 0.775,     # ワイド: 控除22.5%
        'trio': 0.725,     # 三連複: 控除27.5%
        'trifecta': 0.725, # 三連単: 控除27.5%
    }
    
    # 高オッズ補正の閾値
    HIGH_ODDS_THRESHOLD = 50.0
    
    def __init__(
        self, 
        ev_threshold: float = 1.10,
        prob_threshold: float = 0.01,
        kelly_fraction: float = 0.25,
        max_kelly: float = 0.05,
        uncertainty: float = 0.1,
        bankroll: int = 100000
    ):
        """
        Args:
            ev_threshold: EV閾値（これ以上のみ購入）
            prob_threshold: 確率閾値（これ以下は無視）
            kelly_fraction: ケリー基準の縮小率
            max_kelly: 最大ベット割合
            uncertainty: 確率推定の不確実性
            bankroll: 運用資金（円）
        """
        self.ev_threshold = ev_threshold
        self.prob_threshold = prob_threshold
        self.kelly_fraction = kelly_fraction
        self.max_kelly = max_kelly
        self.uncertainty = uncertainty
        self.bankroll = bankroll
    
    def calculate_ev(
        self, 
        prob: float, 
        odds: float, 
        bet_type: str
    ) -> float:
        """
        期待値を計算
        
        EV = P(model) × Odds × PayoutRate
        
        Args:
            prob: モデル予測確率
            odds: オッズ
            bet_type: 馬券種別
        
        Returns:
            期待値（1.0 = ブレークイーブン）
        """
        payout_rate = self.PAYOUT_RATES.get(bet_type, 0.80)
        
        # 高オッズ補正
        adjusted_odds = self._adjust_high_odds(odds)
        
        ev = prob * adjusted_odds * payout_rate
        
        return ev
    
    def _adjust_high_odds(self, odds: float) -> float:
        """
        高オッズを補正（過大評価を防ぐ）
        
        50倍以上のオッズは減衰させる
        adjusted = base + (odds - threshold) / (1 + decay * (odds - threshold))
        """
        if odds <= self.HIGH_ODDS_THRESHOLD:
            return odds
        
        base = self.HIGH_ODDS_THRESHOLD
        excess = odds - self.HIGH_ODDS_THRESHOLD
        decay = 0.02  # 減衰係数
        
        adjusted = base + excess / (1 + decay * excess)
        
        return adjusted
    
    def calculate_kelly(
        self, 
        prob: float, 
        odds: float,
        use_safe_kelly: bool = True
    ) -> float:
        """
        ケリー基準によるベット割合を計算
        
        f* = (bp - q) / b
        where:
            b = odds - 1 (net profit per unit bet)
            p = win probability
            q = 1 - p
        
        Args:
            prob: 予測確率
            odds: オッズ
            use_safe_kelly: 不確実性を考慮した安全版を使用
        
        Returns:
            ベット割合（0〜1）
        """
        if use_safe_kelly:
            return self._safe_kelly(prob, odds)
        
        b = odds - 1
        p = prob
        q = 1 - p
        
        kelly = (b * p - q) / (b + 1e-10)
        kelly = max(0, min(kelly, self.max_kelly))
        kelly *= self.kelly_fraction
        
        return kelly
    
    def _safe_kelly(self, prob: float, odds: float) -> float:
        """
        不確実性を考慮した安全なケリー基準
        
        悲観的シナリオ（確率 - uncertainty）で計算
        """
        # 悲観的確率
        pessimistic_prob = max(0.01, prob - self.uncertainty)
        
        b = odds - 1
        p = pessimistic_prob
        q = 1 - p
        
        kelly = (b * p - q) / (b + 1e-10)
        kelly = max(0, min(kelly, self.max_kelly))
        kelly *= self.kelly_fraction
        
        return kelly
    
    def select_tickets(
        self, 
        race_probs: Dict[str, Dict], 
        race_odds: Dict[str, Dict]
    ) -> List[BetTicket]:
        """
        EV閾値を超える馬券を選択
        
        Args:
            race_probs: {bet_type: {combination: prob}}
            race_odds: {bet_type: {combination: odds}}
        
        Returns:
            BetTicketのリスト
        """
        selected = []
        
        for bet_type in race_probs:
            probs = race_probs[bet_type]
            odds_dict = race_odds.get(bet_type, {})
            
            for combo, prob in probs.items():
                # 確率閾値チェック
                if prob < self.prob_threshold:
                    continue
                
                # オッズ取得
                odds = odds_dict.get(combo)
                if odds is None or odds <= 1.0:
                    continue
                
                # EV計算
                ev = self.calculate_ev(prob, odds, bet_type)
                
                # EV閾値チェック
                if ev < self.ev_threshold:
                    continue
                
                # ケリー基準
                kelly = self.calculate_kelly(prob, odds)
                
                # ベット額
                amount = int(self.bankroll * kelly / 100) * 100  # 100円単位
                amount = max(amount, 100)  # 最低100円
                
                ticket = BetTicket(
                    bet_type=bet_type,
                    combination=combo,
                    model_prob=prob,
                    odds=odds,
                    ev=ev,
                    kelly_fraction=kelly,
                    amount=amount
                )
                
                selected.append(ticket)
        
        # EVの高い順にソート
        selected.sort(key=lambda x: x.ev, reverse=True)
        
        return selected
    
    def optimize_portfolio(
        self, 
        tickets: List[BetTicket],
        max_total_bet: Optional[int] = None
    ) -> List[BetTicket]:
        """
        ポートフォリオ最適化
        
        総ベット額が予算を超えないように調整
        
        Args:
            tickets: 選択された馬券リスト
            max_total_bet: 最大総ベット額（None = bankrollの10%）
        
        Returns:
            調整後の馬券リスト
        """
        if not tickets:
            return []
        
        if max_total_bet is None:
            max_total_bet = int(self.bankroll * 0.10)
        
        # 現在の合計
        total = sum(t.amount for t in tickets)
        
        if total <= max_total_bet:
            return tickets
        
        # 比例縮小
        scale = max_total_bet / total
        
        for ticket in tickets:
            new_amount = int(ticket.amount * scale / 100) * 100
            new_amount = max(new_amount, 100) if new_amount >= 50 else 0
            ticket.amount = new_amount
        
        # 0円のチケットを除外
        tickets = [t for t in tickets if t.amount > 0]
        
        return tickets
    
    def calculate_expected_returns(
        self, 
        tickets: List[BetTicket]
    ) -> Dict[str, float]:
        """
        期待リターンの計算
        """
        if not tickets:
            return {
                'total_bet': 0,
                'expected_return': 0,
                'expected_profit': 0,
                'expected_roi': 0.0,
                'n_tickets': 0
            }
        
        total_bet = sum(t.amount for t in tickets)
        expected_return = sum(t.amount * t.ev for t in tickets)
        expected_profit = expected_return - total_bet
        expected_roi = expected_return / total_bet if total_bet > 0 else 0
        
        return {
            'total_bet': total_bet,
            'expected_return': expected_return,
            'expected_profit': expected_profit,
            'expected_roi': expected_roi,
            'n_tickets': len(tickets)
        }


def check_hit(
    tickets: List[BetTicket], 
    actual_result: Tuple[int, int, int]
) -> List[BetTicket]:
    """
    的中判定
    
    Args:
        tickets: 購入した馬券リスト
        actual_result: 実際の着順 (1着, 2着, 3着) の馬番
    
    Returns:
        的中した馬券リスト
    """
    first, second, third = actual_result
    hits = []
    
    for ticket in tickets:
        combo = ticket.combination
        bet_type = ticket.bet_type
        
        if bet_type == 'win':
            if isinstance(combo, int):
                is_hit = combo == first
            else:
                is_hit = combo[0] == first
        
        elif bet_type == 'exacta':
            is_hit = combo == (first, second)
        
        elif bet_type == 'quinella':
            is_hit = set(combo) == {first, second}
        
        elif bet_type == 'trifecta':
            is_hit = combo == (first, second, third)
        
        elif bet_type == 'trio':
            is_hit = set(combo) == {first, second, third}
        
        elif bet_type == 'wide':
            top3 = {first, second, third}
            is_hit = set(combo).issubset(top3) and len(set(combo)) == 2
        
        else:
            is_hit = False
        
        if is_hit:
            hits.append(ticket)
    
    return hits


def calculate_payout(
    hits: List[BetTicket], 
    odds_dict: Dict[str, Dict]
) -> int:
    """
    払戻額の計算
    
    Args:
        hits: 的中した馬券リスト
        odds_dict: 実際のオッズ
    
    Returns:
        総払戻額（円）
    """
    total_payout = 0
    
    for ticket in hits:
        actual_odds = odds_dict.get(ticket.bet_type, {}).get(ticket.combination, ticket.odds)
        payout = int(ticket.amount * actual_odds)
        total_payout += payout
    
    return total_payout

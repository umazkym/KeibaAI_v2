#!/usr/bin/env python3
# src/optimizer/daily_allocator.py
"""
日次予算配分モジュール
1日の総予算を、各レースの「魅力度（期待リターン）」に基づいて配分する。
"""

import logging
import numpy as np
from typing import Dict, List, Optional

class DailyAllocator:
    """
    日次予算配分クラス
    """
    
    def __init__(self, config: Dict):
        """
        Args:
            config: 最適化設定辞書 (configs/optimization.yaml)
        """
        self.config = config.get('optimizer', {})
        self.logger = logging.getLogger(__name__)

    def allocate_budget(
        self,
        daily_simulations: List[Dict],
        daily_odds: List[Dict],
        total_daily_budget: float
    ) -> Dict[str, float]:
        """
        1日の総予算を各レースに配分する

        Args:
            daily_simulations: 1日分のシミュレーション結果リスト
            daily_odds: 1日分のオッズデータリスト (daily_simulationsとインデックス対応)
            total_daily_budget: 1日の総予算 (円)

        Returns:
            Dict[race_id, allocated_budget]: レースごとの配分額
        """
        self.logger.info(f"日次予算配分開始: 予算 ¥{total_daily_budget:,.0f}, レース数 {len(daily_simulations)}")

        race_scores = []
        race_ids = []

        # 1. 各レースの「スコア」を計算
        # スコア = そのレースで期待できる「最大対数リターン」や「期待値の総和」など
        # ここでは簡易的に「Kelly基準で投資した場合の期待対数リターン」をスコアとする
        
        for sim_result, odds_data in zip(daily_simulations, daily_odds):
            race_id = sim_result['race_id']
            race_ids.append(race_id)
            
            score = self._calculate_race_score(sim_result, odds_data)
            race_scores.append(score)
            self.logger.debug(f"レース {race_id} スコア: {score:.4f}")

        race_scores = np.array(race_scores)
        
        # 2. スコアに基づいて予算を配分
        # スコアが正のレースのみに配分
        positive_mask = race_scores > 0
        if not np.any(positive_mask):
            self.logger.warning("投資価値のあるレースがありません。全レース予算0円とします。")
            return {rid: 0.0 for rid in race_ids}

        total_score = np.sum(race_scores[positive_mask])
        
        allocations = {}
        for i, race_id in enumerate(race_ids):
            if race_scores[i] > 0:
                # 比例配分
                ratio = race_scores[i] / total_score
                budget = total_daily_budget * ratio
                
                # 最小単位(100円)で丸める
                budget = np.floor(budget / 100) * 100
                allocations[race_id] = budget
            else:
                allocations[race_id] = 0.0

        # 3. 結果ログ
        cnt_active = np.sum(race_scores > 0)
        self.logger.info(f"配分完了: {cnt_active}/{len(race_ids)} レースに配分")
        
        return allocations

    def _calculate_race_score(self, sim_result: Dict, odds_data: Dict) -> float:
        """
        レースの魅力度（スコア）を計算
        簡易的なKelly最適化を行い、その時の目的関数値（期待対数リターン）をスコアとする
        """
        # 簡易的に単勝(win)のみで評価
        win_probs = sim_result.get('win_probs', {})
        win_odds = odds_data.get('win', {})
        
        if not win_probs or not win_odds:
            return 0.0

        # 候補作成
        candidates = []
        ev_threshold = self.config.get('min_expected_value', 1.1)
        
        for horse_num, prob in win_probs.items():
            horse_num = str(horse_num)
            odds = win_odds.get(horse_num)
            if odds is None: continue
            
            ev = prob * odds
            if ev > ev_threshold:
                candidates.append({'prob': prob, 'odds': odds})

        if not candidates:
            return 0.0

        # 簡易Kelly計算 (独立事象と仮定して総和をとる近似)
        # 本来は相関を考慮すべきだが、予算配分用なので軽量な近似で良い
        # Score = sum( f_i * E[logR] ) ≒ sum( (p*o - 1)/(o-1) * ... ) 
        # ここではもっと単純に「期待値の超過分の総和」を使う手もあるが、
        # Kelly的な「確実性」も重視したいので、p * log(1 + f(o-1)) + (1-p) * log(1 - f) の最大値をスコアにする
        
        # 簡易実装: 各馬独立にKelly最適解を求め、その期待成長率を足し合わせる
        total_growth_rate = 0.0
        
        for cand in candidates:
            p = cand['prob']
            b = cand['odds'] - 1 # net odds
            
            # Kelly fraction f* = p - q/b = p - (1-p)/b = (pb - 1 + p) / b = (p(b+1) - 1) / b
            # f* = (ExpectedValue - 1) / (Odds - 1)
            
            if b <= 0: continue
            
            f_star = (p * (b + 1) - 1) / b
            
            # 制約: 0 <= f <= 1
            f_star = max(0.0, min(1.0, f_star))
            
            if f_star > 0:
                # 期待対数成長率 g(f) = p * log(1 + f*b) + (1-p) * log(1 - f)
                growth = p * np.log(1 + f_star * b) + (1 - p) * np.log(1 - f_star)
                total_growth_rate += growth

        return total_growth_rate

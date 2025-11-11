#!/usr/bin/env python3
# src/optimizer/optimizer.py
"""
ポートフォリオ最適化モジュール
仕様書 10.2章 に基づく実装
scipy.optimizeベース
"""

import logging
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
from scipy.optimize import minimize, LinearConstraint, Bounds
import json
from pathlib import Path
from datetime import datetime, timezone

class PortfolioOptimizer:
    """
    ポートフォリオ最適化クラス
    仕様書 10.2
    """
    
    def __init__(self, config: Dict):
        """
        Args:
            config: 最適化設定辞書 (configs/optimization.yaml)
        """
        # configs/optimization.yaml の 'optimizer' セクションを想定
        self.config = config.get('optimizer', {}) 
        logging.info("PortfolioOptimizer が初期化されました")

    def _create_candidates(
        self,
        simulation_results: Dict,
        odds_data: Dict
    ) -> List[Dict]:
        """
        投資候補（期待値が閾値を超える馬券）を作成
        仕様書 10.2
        
        Args:
            simulation_results: シミュレーション結果 (win_probs, exacta_probsなど)
            odds_data: オッズデータ (JRAオッズ)
        
        Returns:
            候補リスト
        """
        candidates = []
        
        ev_threshold = self.config.get('min_expected_value', 1.05) # 期待値の閾値
        prob_threshold = self.config.get('prob_threshold', 0.01) # 最小確率
        
        # --- 1. 単勝(win)候補 ---
        win_probs = simulation_results.get('win_probs', {})
        win_odds = odds_data.get('win', {})
        
        for horse_num_str, prob in win_probs.items():
            horse_num = str(horse_num_str) # キーを文字列に統一
            odds = win_odds.get(horse_num)
            
            if odds is None or odds < 1.0:
                continue
            
            ev = prob * odds
            
            if ev >= ev_threshold and prob >= prob_threshold:
                candidates.append({
                    'type': 'win',
                    'selection': (horse_num,),
                    'prob': prob,
                    'odds': odds,
                    'ev': ev,
                    'liquidity': self.config.get('constraints', {}).get('max_investment_per_bet', 1000)
                })
        
        # --- 2. 複勝(place)候補 ---
        place_probs = simulation_results.get('place_probs', {})
        place_odds = odds_data.get('place', {})
        
        for horse_num_str, prob in place_probs.items():
            horse_num = str(horse_num_str)
            odds = place_odds.get(horse_num) # 複勝は通常下限オッズを使用

            if odds is None or odds < 1.0:
                continue
            
            ev = prob * odds
            
            if ev >= ev_threshold and prob >= prob_threshold:
                candidates.append({
                    'type': 'place',
                    'selection': (horse_num,),
                    'prob': prob,
                    'odds': odds,
                    'ev': ev,
                    'liquidity': self.config.get('constraints', {}).get('max_investment_per_bet', 1000)
                })
                
        # --- 3. 馬連(exacta/quinella)候補 ---
        # (注: simulator.pyの実装はソート済みキー '1-2' を返すため馬連)
        exacta_probs = simulation_results.get('exacta_probs', {})
        exacta_odds = odds_data.get('exacta', {}) # オッズ側も '1-2' のキーを想定

        for selection_str, prob in exacta_probs.items():
            odds = exacta_odds.get(selection_str)
            
            if odds is None or odds < 1.0:
                continue

            ev = prob * odds
            
            if ev >= ev_threshold and prob >= prob_threshold:
                candidates.append({
                    'type': 'exacta', # 馬連
                    'selection': tuple(selection_str.split('-')),
                    'prob': prob,
                    'odds': odds,
                    'ev': ev,
                    'liquidity': self.config.get('constraints', {}).get('max_investment_per_bet', 1000)
                })

        logging.info(f"{len(candidates)}個の投資候補を作成 (EV >= {ev_threshold})")
        
        return candidates

    def _create_simulation_payoffs(
        self,
        candidates: List[Dict],
        simulation_results: Dict
    ) -> np.ndarray:
        """
        シミュレーションペイオフ行列を作成 (K x M)
        仕様書 10.2 (ただし simulator.py の出力形式に合わせる)
        
        各行(k): シミュレーション試行
        各列(m): 投資候補
        要素: 的中時の払い戻し倍率（外れ時は0）
        
        Args:
            candidates: 投資候補リスト (M)
            simulation_results: シミュレーション結果 (Kを含む)
        
        Returns:
            ペイオフ行列 (K, M)
        """
        # ★注意★
        # 本来は simulator.py から生のランキング行列 (K, n_horses) を受け取り、
        # それに基づいてペイオフを計算する (仕様書 10.2) のが最も正確（相関を考慮できる）。
        #
        # しかし、simulator.py の現実装 ではメモリ節約のため生行列を返さず、
        # 集計済みの確率 (win_probs, exacta_probs) のみを返す。
        #
        # そのため、ここでは「各馬券は独立」と仮定し、
        # 集計済み確率に基づいてランダムにペイオフ行列を「再生成」する
        # （これは仕様書 10.2 とは異なる簡易実装）。
        
        K = simulation_results['K']
        M = len(candidates)
        
        payoffs = np.zeros((K, M))
        
        for j, cand in enumerate(candidates):
            prob = cand['prob']
            odds = cand['odds']
            
            # 確率 prob で的中 (odds)、 確率 (1-prob) でハズレ (0)
            hits = np.random.binomial(n=1, p=prob, size=K)
            payoffs[:, j] = hits * odds
            
        return payoffs
        
    def _solve_optimization(
        self,
        sim_payoffs: np.ndarray,
        W_0: float
    ) -> np.ndarray:
        """
        最適化問題を解く
        仕様書 10.2 (Fractional Kelly)
        
        Args:
            sim_payoffs: ペイオフ行列 (K, M)
            W_0: 初期資金
        
        Returns:
            最適投資額配分 (M,)
        """
        K, M = sim_payoffs.shape
        
        # Fractional Kelly の係数 (仕様書 10.4)
        fraction = self.config.get('fractional_kelly', {}).get('fraction', 0.1) # デフォルト 0.1 (10%)
        
        def objective(x):
            """
            目的関数: 負の期待対数リターン
            maximize E[log(1 + R)]
            """
            
            # x は「投資比率 (x_j / W_0)」ではなく「投資額 (x_j)」とする
            
            total_investment = np.sum(x)
            
            # 各試行(k)の総リターン (円)
            # (payoffs はオッズ (倍率) なので、投資額 x_j を掛ける)
            k_returns_yen = np.dot(sim_payoffs, x) 
            
            # 各試行(k)の純利益 (円)
            k_profit_yen = k_returns_yen - total_investment
            
            # 各試行(k)の資金変動後の総資産
            k_capital_after = W_0 + k_profit_yen
            
            # 対数リターン log(W_k / W_0)
            log_returns = np.log(k_capital_after / W_0)
            
            # 期待対数リターン
            expected_log_return = np.mean(log_returns)
            
            # 最小化問題なので負号をつけ、fractionを乗じる
            return -fraction * expected_log_return

        # --- 制約条件 (仕様書 10.4) ---
        
        # 1. 予算制約: Σx_j <= W_0 * c_max
        c_max = self.config.get('constraints', {}).get('max_investment_per_race', 10000)
        budget_constraint = LinearConstraint(
            A=np.ones(M),
            lb=0,
            ub=c_max
        )
        
        # 2. 個別投資上限: x_j <= L_j
        max_per_bet = self.config.get('constraints', {}).get('max_investment_per_bet', 1000)
        
        # 3. 非負制約: x_j >= min_bet_unit
        min_bet_unit = self.config.get('constraints', {}).get('min_bet_unit', 100)
        
        bounds = Bounds(lb=0.0, ub=max_per_bet) # 一旦0以上で計算
        
        # 初期値 (均等配分)
        x0 = np.ones(M) * (min_bet_unit)
        x0 = np.minimum(x0, max_per_bet)
        
        # 最適化実行
        solver_config = self.config.get('solver', {})
        result = minimize(
            objective,
            x0=x0,
            method=solver_config.get('method', 'SLSQP'),
            bounds=bounds,
            constraints=[budget_constraint],
            options={
                'maxiter': solver_config.get('maxiter', 1000),
                'ftol': solver_config.get('ftol', 1e-6)
            }
        )
        
        if not result.success:
            logging.warning(f"最適化が収束しませんでした: {result.message}")
            return np.zeros(M) # 収束失敗時は投資しない

        allocation = result.x
        
        # 最小投資単位で丸め、それ未満は切り捨て
        allocation = np.floor(allocation / min_bet_unit) * min_bet_unit
        
        return allocation
    
    def optimize(
        self,
        simulation_results: Dict,
        odds_data: Dict,
        W_0: float
    ) -> Dict:
        """
        ポートフォリオ最適化を実行 (メイン関数)
        仕様書 10.2
        
        Args:
            simulation_results: シミュレーション結果
            odds_data: オッズデータ
            W_0: 初期資金
        
        Returns:
            最適化結果辞書
        """
        logging.info("ポートフォリオ最適化開始")
        
        # 1. 投資候補の作成
        candidates = self._create_candidates(simulation_results, odds_data)
        
        if len(candidates) == 0:
            logging.warning("投資候補が見つかりませんでした")
            return self._format_result(np.array([]), [], np.array([[]]), W_0)
        
        # 2. シミュレーションペイオフ行列の作成
        sim_payoffs = self._create_simulation_payoffs(
            candidates, simulation_results
        )
        
        # 3. 最適化実行
        allocation = self._solve_optimization(
            sim_payoffs=sim_payoffs,
            W_0=W_0
        )
        
        # 4. 結果を整形
        result = self._format_result(allocation, candidates, sim_payoffs, W_0)
        
        logging.info(f"ポートフォリオ最適化完了: {len(result['bets'])}件の投資案")
        
        return result
    
    def _format_result(
        self,
        allocation: np.ndarray,
        candidates: List[Dict],
        sim_payoffs: np.ndarray,
        W_0: float
    ) -> Dict:
        """
        最適化結果を整形
        仕様書 10.2
        """
        bets = []
        
        for j, amount in enumerate(allocation):
            if amount < self.config.get('constraints', {}).get('min_bet_unit', 100):
                continue
            
            candidate = candidates[j]
            
            bets.append({
                'type': candidate['type'],
                'selection': candidate['selection'],
                'amount': float(amount),
                'odds': candidate['odds'],
                'prob': candidate['prob'],
                'ev': candidate['ev']
            })
        
        # 期待リターン計算
        total_investment = np.sum(allocation)
        
        if total_investment > 0:
            # (ペイオフ行列を使ったシミュレーションベースの期待リターン)
            k_returns_yen = np.dot(sim_payoffs, allocation) 
            k_profit_yen = k_returns_yen - total_investment
            k_returns_pct = k_profit_yen / W_0
            
            expected_return = np.mean(k_returns_pct)
            std_return = np.std(k_returns_pct)
            sharpe_ratio = expected_return / (std_return + 1e-6)
        else:
            expected_return = 0.0
            std_return = 0.0
            sharpe_ratio = 0.0
        
        return {
            'bets': bets,
            'total_investment': float(total_investment),
            'expected_return': float(expected_return),
            'std_return': float(std_return),
            'sharpe_ratio': float(sharpe_ratio),
            'W_0': W_0
        }
    
    def save_allocation(
        self,
        race_id: str,
        allocation_result: Dict,
        output_dir: str = 'data/orders'
    ):
        """
        配分結果をJSONとして保存
        仕様書 10.2
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # order_id生成
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        order_id = f"{timestamp}_{race_id}"
        
        # 保存データ
        order_data = {
            'order_id': order_id,
            'race_id': race_id,
            'created_ts': datetime.now(timezone.utc).isoformat(),
            'status': 'pending_manual', # 自動発注は無効
            'bets': allocation_result['bets'],
            'total_investment': allocation_result['total_investment'],
            'expected_return': allocation_result['expected_return'],
            'std_return': allocation_result['std_return'],
            'sharpe_ratio': allocation_result['sharpe_ratio'],
            'W_0': allocation_result['W_0']
        }
        
        # JSON保存
        output_file = output_path / f"{order_id}.json"
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                # (Numpyの型をPythonネイティブ型に変換)
                json.dump(order_data, f, ensure_ascii=False, indent=2, default=float)
            logging.info(f"配分結果保存: {output_file}")
        except Exception as e:
            logging.error(f"配分結果のJSON保存に失敗: {e}")
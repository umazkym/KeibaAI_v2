#!/usr/bin/env python3
# keibaai/src/optimizer/optimizer.py
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
        仕様書 10.2 準拠
        
        各行(k): シミュレーション試行
        各列(m): 投資候補
        要素: 的中時の払い戻し倍率（外れ時は0）
        
        Args:
            candidates: 投資候補リスト (M)
            simulation_results: シミュレーション結果 (Kを含む)
                - 'rankings' キーがある場合: 相関を考慮した正確なペイオフ計算
                - ない場合: 独立ベルヌーイによる近似（後方互換）
        
        Returns:
            ペイオフ行列 (K, M)
        
        Note (v5.2改修):
            [1-1] ランキング行列がある場合は相関を考慮した正確なペイオフを計算。
            同一レースの馬券は「馬1が1着なら馬2は非1着」という制約が自然に満たされる。
            旧実装の独立ベルヌーイでは「馬1も馬2も同時に1着」が発生し得た。
        """
        K = simulation_results['K']
        M = len(candidates)
        
        payoffs = np.zeros((K, M))
        
        # [1-1修正] ランキング行列がある場合は相関を考慮
        rankings = simulation_results.get('rankings', None)
        
        if rankings is not None:
            # ランキング行列 (K, n_horses) からペイオフを直接計算
            # rankings[k, pos] = pos位の馬のインデックス
            for j, cand in enumerate(candidates):
                odds = cand['odds']
                bet_type = cand.get('type', 'win')
                selection = cand.get('selection', [])
                
                if bet_type == 'win':
                    # 単勝: selection[0] が1着
                    horse_idx = int(selection[0])
                    for k_idx in range(K):
                        if rankings[k_idx, 0] == horse_idx:
                            payoffs[k_idx, j] = odds
                            
                elif bet_type == 'place':
                    # 複勝: selection[0] が3着以内
                    horse_idx = int(selection[0])
                    for k_idx in range(K):
                        if (rankings[k_idx, 0] == horse_idx or 
                            rankings[k_idx, 1] == horse_idx or
                            rankings[k_idx, 2] == horse_idx):
                            payoffs[k_idx, j] = odds
                            
                elif bet_type == 'quinella':
                    # 馬連: selection[0],selection[1] が1-2着（順不同）
                    h1, h2 = int(selection[0]), int(selection[1])
                    for k_idx in range(K):
                        top2 = {rankings[k_idx, 0], rankings[k_idx, 1]}
                        if h1 in top2 and h2 in top2:
                            payoffs[k_idx, j] = odds
                            
                elif bet_type == 'exacta':
                    # 馬単: selection[0]が1着, selection[1]が2着
                    h1, h2 = int(selection[0]), int(selection[1])
                    for k_idx in range(K):
                        if rankings[k_idx, 0] == h1 and rankings[k_idx, 1] == h2:
                            payoffs[k_idx, j] = odds
                            
                elif bet_type == 'wide':
                    # ワイド: selection[0],selection[1] がともに3着以内
                    h1, h2 = int(selection[0]), int(selection[1])
                    for k_idx in range(K):
                        top3 = {rankings[k_idx, 0], rankings[k_idx, 1], rankings[k_idx, 2]}
                        if h1 in top3 and h2 in top3:
                            payoffs[k_idx, j] = odds
                
                elif bet_type == 'trio':
                    # 三連複: selection[0,1,2] が1-3着（順不同）
                    h1, h2, h3 = int(selection[0]), int(selection[1]), int(selection[2])
                    for k_idx in range(K):
                        top3 = {rankings[k_idx, 0], rankings[k_idx, 1], rankings[k_idx, 2]}
                        if h1 in top3 and h2 in top3 and h3 in top3:
                            payoffs[k_idx, j] = odds
                
                elif bet_type == 'trifecta':
                    # 三連単: selection[0]が1着, selection[1]が2着, selection[2]が3着
                    h1, h2, h3 = int(selection[0]), int(selection[1]), int(selection[2])
                    for k_idx in range(K):
                        if (rankings[k_idx, 0] == h1 and 
                            rankings[k_idx, 1] == h2 and 
                            rankings[k_idx, 2] == h3):
                            payoffs[k_idx, j] = odds
                
                else:
                    # 未知の券種: 確率ベースの近似（後方互換）
                    prob = cand['prob']
                    hits = np.random.binomial(n=1, p=prob, size=K)
                    payoffs[:, j] = hits * odds
        else:
            # [後方互換] ランキング行列がない場合は独立ベルヌーイで近似
            # Note: この方式では馬券間の相関が無視される
            logging.warning(
                "ランキング行列がありません。独立ベルヌーイで近似します。"
                "正確なペイオフ計算にはsimulation_results['rankings']を渡してください。"
            )
            for j, cand in enumerate(candidates):
                prob = cand['prob']
                odds = cand['odds']
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
    
    @staticmethod
    def optimize_kelly_fraction(
        backtest_results: List[Dict],
        fraction_range: Tuple[float, float] = (0.01, 0.5),
        max_drawdown_limit: float = 0.3,
        n_steps: int = 50
    ) -> Dict:
        """
        [5-1改修] バックテスト結果からKelly Fractionを最適化
        
        固定のfraction (0.1) ではなく、過去の的中/外れデータに基づいて
        幾何平均リターンを最大化しつつ最大ドローダウンを制限する
        最適なfractionを探索する。
        
        Args:
            backtest_results: バックテスト結果のリスト
                各要素: {
                    'odds': float,       # 的中時のオッズ 
                    'hit': bool,         # 的中/不的中
                    'prob': float,       # モデル予測確率
                }
            fraction_range: 探索範囲 (min, max)
            max_drawdown_limit: 最大ドローダウン制限
            n_steps: 探索のグリッド数
        
        Returns:
            Dict: {
                'optimal_fraction': float,
                'geometric_growth_rate': float,
                'max_drawdown': float,
                'n_bets': int,
                'win_rate': float,
                'fraction_curve': List[Dict]  # fraction vs growth rate
            }
        """
        if not backtest_results:
            logging.warning("バックテスト結果が空です。デフォルトfraction=0.1を返します。")
            return {'optimal_fraction': 0.1}
        
        fractions = np.linspace(fraction_range[0], fraction_range[1], n_steps)
        results_curve = []
        
        best_fraction = 0.1
        best_growth = -np.inf
        
        for fraction in fractions:
            # シミュレーション: 各ベットでのリターンを計算
            bankroll = 1.0
            peak = 1.0
            max_dd = 0.0
            
            for bet in backtest_results:
                odds = bet['odds']
                hit = bet['hit']
                prob = bet['prob']
                
                # Kelly bet size
                kelly_f = (prob * (odds - 1) - (1 - prob)) / (odds - 1)
                kelly_f = max(0, kelly_f) * fraction
                kelly_f = min(kelly_f, 0.25)  # 単一ベットの上限
                
                if hit:
                    bankroll *= (1 + kelly_f * (odds - 1))
                else:
                    bankroll *= (1 - kelly_f)
                
                peak = max(peak, bankroll)
                dd = (peak - bankroll) / peak
                max_dd = max(max_dd, dd)
            
            # 幾何平均リターン
            n_bets = len(backtest_results)
            geometric_growth = bankroll ** (1.0 / n_bets) - 1 if bankroll > 0 else -1
            
            results_curve.append({
                'fraction': float(fraction),
                'geometric_growth': float(geometric_growth),
                'max_drawdown': float(max_dd),
                'final_bankroll': float(bankroll),
            })
            
            # ドローダウン制限を満たす最良のfractionを選択
            if max_dd <= max_drawdown_limit and geometric_growth > best_growth:
                best_growth = geometric_growth
                best_fraction = float(fraction)
        
        win_rate = sum(1 for b in backtest_results if b['hit']) / len(backtest_results)
        
        logging.info(f"[5-1] Kelly Fraction最適化完了:")
        logging.info(f"  最適fraction: {best_fraction:.4f}")
        logging.info(f"  幾何成長率: {best_growth:.6f}")
        logging.info(f"  ベット数: {len(backtest_results)}, 勝率: {win_rate:.1%}")
        
        return {
            'optimal_fraction': best_fraction,
            'geometric_growth_rate': float(best_growth),
            'n_bets': len(backtest_results),
            'win_rate': float(win_rate),
            'fraction_curve': results_curve
        }
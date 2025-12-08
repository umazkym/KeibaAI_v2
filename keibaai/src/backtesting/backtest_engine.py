"""
Backtest Engine (v5.1)

ROIシミュレーションとモンテカルロ分析を行うバックテストエンジン。

【機能】
- Edge Score閾値別のROIシミュレーション
- 年間ベット数と回収率の分析
- 最大ドローダウンの計算
- モンテカルロシミュレーション
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class BacktestResult:
    """バックテスト結果"""
    period: str
    total_bets: int
    total_wins: int
    hit_rate: float
    total_investment: float
    total_return: float
    roi: float
    avg_odds: float
    max_drawdown: float
    profit_factor: float
    
    def to_dict(self) -> Dict:
        return {
            'period': self.period,
            'total_bets': self.total_bets,
            'total_wins': self.total_wins,
            'hit_rate': self.hit_rate,
            'total_investment': self.total_investment,
            'total_return': self.total_return,
            'roi': self.roi,
            'avg_odds': self.avg_odds,
            'max_drawdown': self.max_drawdown,
            'profit_factor': self.profit_factor
        }


class BacktestEngine:
    """
    バックテストエンジン
    
    ROIシミュレーションと統計分析を行う。
    """
    
    def __init__(self, bet_amount: float = 100.0):
        """
        Parameters:
        -----------
        bet_amount : float
            1ベットあたりの金額（円）
        """
        self.bet_amount = bet_amount
        self.results_history = []
    
    def run_backtest(
        self,
        predictions_df: pd.DataFrame,
        edge_threshold: float = 1.0,
        min_ev: float = 1.0,
        date_col: str = 'race_date',
        race_id_col: str = 'race_id'
    ) -> BacktestResult:
        """
        バックテストを実行
        
        Parameters:
        -----------
        predictions_df : pd.DataFrame
            予測結果（edge_score, win_odds, is_win, race_date必須）
        edge_threshold : float
            エッジスコア閾値
        min_ev : float
            最低期待値
        """
        df = predictions_df.copy()
        
        # 必須カラムチェック
        required_cols = ['edge_score', 'win_odds', 'is_win']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"必須カラムがありません: {col}")
        
        # フィルタリング
        if 'model_prob' in df.columns:
            df['ev'] = df['model_prob'] * df['win_odds']
        else:
            df['ev'] = df['edge_score']  # EVがなければedge_scoreを使用
        
        bet_df = df[(df['edge_score'] >= edge_threshold) & (df['ev'] >= min_ev)].copy()
        
        if len(bet_df) == 0:
            return BacktestResult(
                period='N/A', total_bets=0, total_wins=0, hit_rate=0,
                total_investment=0, total_return=0, roi=0, avg_odds=0,
                max_drawdown=0, profit_factor=0
            )
        
        # 基本統計
        total_bets = len(bet_df)
        total_wins = int(bet_df['is_win'].sum())
        hit_rate = total_wins / total_bets
        total_investment = total_bets * self.bet_amount
        total_return = (bet_df['is_win'] * bet_df['win_odds'] * self.bet_amount).sum()
        roi = total_return / total_investment
        avg_odds = bet_df['win_odds'].mean()
        
        # 期間
        if date_col in bet_df.columns:
            min_date = bet_df[date_col].min()
            max_date = bet_df[date_col].max()
            period = f"{min_date} ~ {max_date}"
        else:
            period = 'N/A'
        
        # ドローダウン計算
        max_drawdown = self._calculate_max_drawdown(bet_df, date_col)
        
        # プロフィットファクター
        gross_profit = (bet_df[bet_df['is_win'] == 1]['win_odds'] * self.bet_amount).sum()
        gross_loss = (bet_df[bet_df['is_win'] == 0].shape[0]) * self.bet_amount
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        result = BacktestResult(
            period=str(period),
            total_bets=total_bets,
            total_wins=total_wins,
            hit_rate=hit_rate,
            total_investment=total_investment,
            total_return=total_return,
            roi=roi,
            avg_odds=avg_odds,
            max_drawdown=max_drawdown,
            profit_factor=profit_factor
        )
        
        self.results_history.append(result)
        return result
    
    def _calculate_max_drawdown(self, df: pd.DataFrame, date_col: str) -> float:
        """最大ドローダウンを計算"""
        if date_col not in df.columns:
            return 0.0
        
        df = df.sort_values(date_col).copy()
        
        # 各ベットの損益
        df['pnl'] = df.apply(
            lambda r: (r['win_odds'] - 1) * self.bet_amount if r['is_win'] else -self.bet_amount,
            axis=1
        )
        
        # 累積損益
        df['cumulative_pnl'] = df['pnl'].cumsum()
        
        # ドローダウン
        df['running_max'] = df['cumulative_pnl'].cummax()
        df['drawdown'] = df['running_max'] - df['cumulative_pnl']
        
        max_drawdown = df['drawdown'].max()
        
        return max_drawdown
    
    def optimize_threshold(
        self,
        predictions_df: pd.DataFrame,
        thresholds: List[float] = None,
        min_bets: int = 100
    ) -> Dict:
        """
        最適なエッジ閾値を探索
        
        Returns:
        --------
        Dict : 最適閾値と各閾値の結果
        """
        if thresholds is None:
            thresholds = [0.9, 1.0, 1.1, 1.2, 1.3, 1.5, 1.8, 2.0, 2.5, 3.0]
        
        results = []
        best_roi = 0
        best_threshold = None
        
        for threshold in thresholds:
            result = self.run_backtest(predictions_df, edge_threshold=threshold)
            
            if result.total_bets >= min_bets:
                results.append({
                    'threshold': threshold,
                    **result.to_dict()
                })
                
                if result.roi > best_roi:
                    best_roi = result.roi
                    best_threshold = threshold
        
        return {
            'optimal_threshold': best_threshold,
            'optimal_roi': best_roi,
            'all_results': results
        }
    
    def monte_carlo_simulation(
        self,
        predictions_df: pd.DataFrame,
        edge_threshold: float = 1.0,
        n_simulations: int = 1000,
        n_bets: int = None
    ) -> Dict:
        """
        モンテカルロシミュレーション
        
        ランダムにベットをサンプリングして結果の分布を計算
        
        Returns:
        --------
        Dict : シミュレーション結果（平均ROI、5/95パーセンタイル等）
        """
        df = predictions_df[predictions_df['edge_score'] >= edge_threshold].copy()
        
        if len(df) == 0:
            return {'error': 'No bets match the criteria'}
        
        if n_bets is None:
            n_bets = min(100, len(df))
        
        roi_results = []
        
        for _ in range(n_simulations):
            sample = df.sample(n=min(n_bets, len(df)), replace=True)
            
            investment = len(sample) * self.bet_amount
            returns = (sample['is_win'] * sample['win_odds'] * self.bet_amount).sum()
            roi = returns / investment if investment > 0 else 0
            
            roi_results.append(roi)
        
        roi_results = np.array(roi_results)
        
        return {
            'mean_roi': float(roi_results.mean()),
            'std_roi': float(roi_results.std()),
            'percentile_5': float(np.percentile(roi_results, 5)),
            'percentile_25': float(np.percentile(roi_results, 25)),
            'median_roi': float(np.percentile(roi_results, 50)),
            'percentile_75': float(np.percentile(roi_results, 75)),
            'percentile_95': float(np.percentile(roi_results, 95)),
            'prob_profit': float((roi_results > 1.0).mean()),
            'n_simulations': n_simulations,
            'n_bets_per_sim': n_bets
        }


if __name__ == "__main__":
    print("=" * 60)
    print("BacktestEngine テスト")
    print("=" * 60)
    
    # テストデータ生成
    np.random.seed(42)
    n_samples = 1000
    
    # 仮想的な予測データ
    test_df = pd.DataFrame({
        'race_id': [f'R{i:04d}' for i in range(n_samples)],
        'race_date': pd.date_range('2023-01-01', periods=n_samples, freq='D'),
        'edge_score': np.random.uniform(0.8, 2.5, n_samples),
        'win_odds': np.random.exponential(10, n_samples) + 2,
        'model_prob': np.random.uniform(0.05, 0.35, n_samples)
    })
    
    # 勝敗を確率に基づいて生成
    test_df['is_win'] = (np.random.random(n_samples) < test_df['model_prob']).astype(int)
    
    # バックテスト実行
    engine = BacktestEngine(bet_amount=100)
    
    print("\n【閾値別バックテスト】")
    print("-" * 60)
    print(f"{'閾値':>6} | {'ベット数':>8} | {'的中率':>8} | {'ROI':>8} | {'平均配当':>8}")
    print("-" * 60)
    
    for threshold in [1.0, 1.2, 1.5, 2.0]:
        result = engine.run_backtest(test_df, edge_threshold=threshold)
        print(f"{threshold:>6.1f} | {result.total_bets:>8} | {result.hit_rate:>7.1%} | {result.roi:>7.1%} | {result.avg_odds:>8.1f}")
    
    print("\n【モンテカルロシミュレーション】")
    mc_result = engine.monte_carlo_simulation(test_df, edge_threshold=1.2, n_simulations=500)
    print(f"平均ROI: {mc_result['mean_roi']:.1%}")
    print(f"5-95パーセンタイル: {mc_result['percentile_5']:.1%} ~ {mc_result['percentile_95']:.1%}")
    print(f"利益確率: {mc_result['prob_profit']:.1%}")

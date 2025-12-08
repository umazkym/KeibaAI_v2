#!/usr/bin/env python3
"""
Phase 5: 確率キャリブレーション + SafeKelly

【目的】
モデルスコアを「真の勝率」に校正し、
より正確なEdge Score計算と資金配分を実現する。

【実装】
1. IsotonicRegression で確率校正
2. 校正済み確率でEdge Score再計算
3. SafeKelly基準で資金配分シミュレーション

【期待効果】
- 確率校正によりEdge Score精度向上
- Kelly基準で「自信度に応じた」資金配分
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import pickle
import json
import logging
from sklearn.isotonic import IsotonicRegression
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ProbabilityCalibrator:
    """
    確率キャリブレーター
    
    IsotonicRegressionでモデル確率を校正し、
    「真の勝率」に近づける。
    """
    
    def __init__(self):
        self.calibrator = None
    
    def fit(self, probs: np.ndarray, labels: np.ndarray):
        """
        キャリブレーターを学習
        
        Args:
            probs: モデル予測確率
            labels: 実際の勝敗（0/1）
        """
        self.calibrator = IsotonicRegression(out_of_bounds='clip')
        self.calibrator.fit(probs, labels)
    
    def transform(self, probs: np.ndarray) -> np.ndarray:
        """確率を校正"""
        if self.calibrator is None:
            return probs
        return self.calibrator.transform(probs)
    
    def fit_transform(self, probs: np.ndarray, labels: np.ndarray) -> np.ndarray:
        self.fit(probs, labels)
        return self.transform(probs)


class SafeKellyCriterion:
    """
    安全なケリー基準
    """
    
    def __init__(self, fraction: float = 0.25, max_fraction: float = 0.05,
                 min_bet: int = 100, max_bet: int = 10000):
        self.fraction = fraction
        self.max_fraction = max_fraction
        self.min_bet = min_bet
        self.max_bet = max_bet
    
    def calculate(self, prob: float, odds: float, bankroll: float) -> int:
        """
        ベット金額を計算
        """
        if prob <= 0 or odds <= 1:
            return 0
        
        b = odds - 1
        q = 1 - prob
        kelly = (prob * b - q) / b
        
        if kelly <= 0:
            return 0
        
        bet = kelly * self.fraction * bankroll
        bet = min(bet, bankroll * self.max_fraction)
        bet = max(bet, self.min_bet)
        bet = min(bet, self.max_bet)
        
        return int(round(bet / 100) * 100)


class CalibratedBettingSimulator:
    """
    校正済み確率を使用したベットシミュレーター
    """
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/calibrated_betting')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.feature_names = None
        self.calibrator = ProbabilityCalibrator()
        self.kelly = SafeKellyCriterion(fraction=0.25, max_fraction=0.03)
    
    def load_model_and_data(self):
        """モデルとデータ読み込み"""
        logging.info("=" * 60)
        logging.info("モデル・データ読み込み")
        logging.info("=" * 60)
        
        with open(self.model_dir / 'mu_v11_0_ranker.pkl', 'rb') as f:
            self.model = pickle.load(f)
        with open(self.model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            self.feature_names = json.load(f)
        
        base_data = pd.read_parquet('keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet')
        base_data['race_date'] = pd.to_datetime(base_data['race_date'])
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        
        logging.info(f"  データ: {len(base_data):,}件")
        
        return base_data, races_df
    
    def generate_predictions(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """予測生成"""
        df = df.copy()
        
        for f in self.feature_names:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        preds = self.model.predict(df[self.feature_names])
        df['model_score'] = preds
        
        # レース内ランク
        df['rank_in_race'] = df.groupby('race_id')['model_score'].rank(ascending=False, method='first')
        
        # softmax確率
        def softmax_prob(x):
            exp_x = np.exp(x - x.max())
            return exp_x / exp_x.sum()
        
        df['raw_prob'] = df.groupby('race_id')['model_score'].transform(softmax_prob)
        
        # 結果マージ
        df['race_id'] = df['race_id'].astype(str)
        df['horse_id'] = df['horse_id'].astype(str)
        
        for col in ['finish_position', 'win_odds']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'finish_position', 'win_odds']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        df['is_win'] = (df['finish_position'] == 1).fillna(False).astype(int)
        
        return df
    
    def calibrate_probabilities(self, train_df: pd.DataFrame, valid_df: pd.DataFrame):
        """確率を校正"""
        logging.info("\n確率キャリブレーション中...")
        
        # Train期間でcalibratorを学習
        self.calibrator.fit(
            train_df['raw_prob'].values,
            train_df['is_win'].values
        )
        
        # Valid期間に適用
        valid_df = valid_df.copy()
        valid_df['calibrated_prob'] = self.calibrator.transform(valid_df['raw_prob'].values)
        
        # Brier Score計算
        raw_brier = brier_score_loss(valid_df['is_win'], valid_df['raw_prob'])
        cal_brier = brier_score_loss(valid_df['is_win'], valid_df['calibrated_prob'])
        
        logging.info(f"  Raw Brier Score: {raw_brier:.4f}")
        logging.info(f"  Calibrated Brier Score: {cal_brier:.4f}")
        logging.info(f"  改善率: {(raw_brier - cal_brier) / raw_brier * 100:.1f}%")
        
        return valid_df
    
    def calculate_calibrated_edge(self, df: pd.DataFrame) -> pd.DataFrame:
        """校正済み確率でEdge Score計算"""
        df = df.copy()
        
        df['odds_implied_prob'] = 1 / df['win_odds'].clip(lower=1.1)
        
        # 校正済みEdge Score
        df['calibrated_edge'] = df['calibrated_prob'] / df['odds_implied_prob'].clip(lower=0.01)
        
        # EV (Expected Value)
        df['ev'] = df['calibrated_prob'] * df['win_odds']
        
        return df
    
    def simulate_kelly_betting(self, df: pd.DataFrame, initial_bankroll: float = 100000):
        """Kelly基準でのベットシミュレーション"""
        logging.info("\n" + "=" * 60)
        logging.info("Kelly基準ベットシミュレーション")
        logging.info("=" * 60)
        
        # 予測1位のみ
        df = df[df['rank_in_race'] == 1].copy()
        df = df.sort_values('race_date')
        
        results = []
        
        # 閾値別にシミュレーション
        for edge_thresh in [1.0, 1.2, 1.5, 2.0]:
            for ev_thresh in [1.0, 1.05, 1.1]:
                bankroll = initial_bankroll
                bets = []
                
                for _, row in df.iterrows():
                    if row['calibrated_edge'] >= edge_thresh and row['ev'] >= ev_thresh:
                        bet_amount = self.kelly.calculate(
                            row['calibrated_prob'], 
                            row['win_odds'], 
                            bankroll
                        )
                        
                        if bet_amount > 0:
                            pnl = (row['win_odds'] - 1) * bet_amount if row['is_win'] else -bet_amount
                            bankroll += pnl
                            bets.append({
                                'race_date': row['race_date'],
                                'bet': bet_amount,
                                'pnl': pnl,
                                'bankroll': bankroll,
                                'is_win': row['is_win']
                            })
                
                if len(bets) >= 20:
                    bets_df = pd.DataFrame(bets)
                    total_bet = bets_df['bet'].sum()
                    total_pnl = bets_df['pnl'].sum()
                    roi = (initial_bankroll + total_pnl) / initial_bankroll
                    max_dd = (bets_df['bankroll'].cummax() - bets_df['bankroll']).max() / initial_bankroll
                    
                    results.append({
                        'edge_thresh': edge_thresh,
                        'ev_thresh': ev_thresh,
                        'bet_count': len(bets),
                        'total_bet': total_bet,
                        'total_pnl': total_pnl,
                        'final_bankroll': bankroll,
                        'roi': roi,
                        'max_drawdown': max_dd,
                        'win_rate': bets_df['is_win'].mean()
                    })
        
        results_df = pd.DataFrame(results)
        
        if len(results_df) > 0:
            logging.info(f"{'Edge閾値':>8} | {'EV閾値':>6} | {'ベット':>6} | {'ROI':>8} | {'最大DD':>8}")
            logging.info("-" * 55)
            for _, r in results_df.sort_values('roi', ascending=False).iterrows():
                logging.info(f"{r['edge_thresh']:>8.1f} | {r['ev_thresh']:>6.2f} | {r['bet_count']:>6} | {r['roi']:>7.1%} | {r['max_drawdown']:>7.1%}")
            
            best = results_df.loc[results_df['roi'].idxmax()]
            logging.info(f"\n【最高ROI】: {best['roi']:.1%} (Edge≥{best['edge_thresh']}, EV≥{best['ev_thresh']})")
            
            if best['roi'] >= 1.03:
                logging.info("✅ 目標達成: ROI >= 103%")
            else:
                logging.info(f"⚠️ 目標未達: あと{1.03 - best['roi']:.1%}")
        
        return results_df
    
    def run(self):
        """メイン実行"""
        base_data, races_df = self.load_model_and_data()
        
        # 予測生成
        logging.info("\n予測生成中...")
        base_data = self.generate_predictions(base_data, races_df)
        
        # Train/Valid分割
        train_df = base_data[base_data['race_date'] < '2023-01-01']
        valid_df = base_data[(base_data['race_date'] >= '2023-01-01') & 
                             (base_data['race_date'] < '2024-01-01')]
        test_df = base_data[base_data['race_date'] >= '2024-01-01']
        
        logging.info(f"  Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
        
        # Valid期間でキャリブレーション
        valid_df = self.calibrate_probabilities(train_df, valid_df)
        test_df = test_df.copy()
        test_df['calibrated_prob'] = self.calibrator.transform(test_df['raw_prob'].values)
        
        # Edge Score計算
        valid_df = self.calculate_calibrated_edge(valid_df)
        test_df = self.calculate_calibrated_edge(test_df)
        
        # Kellyシミュレーション
        logging.info("\n【Valid期間】")
        valid_results = self.simulate_kelly_betting(valid_df)
        
        logging.info("\n【Test期間】")
        test_results = self.simulate_kelly_betting(test_df)
        
        # 保存
        valid_results.to_csv(self.output_dir / 'valid_kelly_results.csv', index=False)
        test_results.to_csv(self.output_dir / 'test_kelly_results.csv', index=False)
        
        return valid_results, test_results


if __name__ == "__main__":
    simulator = CalibratedBettingSimulator()
    valid_results, test_results = simulator.run()

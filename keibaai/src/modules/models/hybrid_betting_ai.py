"""
Hybrid Horse Racing AI - 4層ハイブリッドアンサンブルシステム

4つのレイヤーを統合したメインクラス:
- Layer 1: 能力推定（AbilityEstimator）
- Layer 2: 確率変換（ProbabilityCalibrator）
- Layer 3: シミュレーション（RaceSimulator）
- Layer 4: 期待値最適化（EVOptimizer）

使用方法:
    ai = HybridHorseRacingAI()
    ai.train(X_train, y_train, groups_train, X_val, y_val, groups_val)
    tickets = ai.predict_race(race_features, race_odds)
    results = ai.backtest(test_data)
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import logging
from pathlib import Path

from keibaai.src.modules.models.ability_estimator import Layer1_AbilityEstimator
from keibaai.src.modules.models.probability_calibrator import (
    Layer2_ProbabilityCalibrator, 
    evaluate_calibration
)
from keibaai.src.modules.sim.race_simulator_v2 import Layer3_RaceSimulator
from keibaai.src.modules.optimizer.ev_optimizer import (
    Layer4_EVOptimizer, 
    BetTicket, 
    check_hit, 
    calculate_payout
)

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """バックテスト結果"""
    n_races: int
    n_bets: int
    total_bet: int
    total_payout: int
    roi: float
    hit_rate: float
    favorite_selection_rate: float
    monthly_rois: Dict[str, float]


class HybridHorseRacingAI:
    """
    4層ハイブリッドアンサンブル競馬AI
    
    目標: ROI 85-90%（Phase 1）
    """
    
    def __init__(
        self, 
        config: Optional[Dict] = None,
        calibration_method: str = 'isotonic_softmax',
        simulation_method: str = 'harville',
        ev_threshold: float = 1.10,
        kelly_fraction: float = 0.25,
        bankroll: int = 100000
    ):
        """
        Args:
            config: モデルハイパーパラメータ
            calibration_method: キャリブレーション方式
            simulation_method: シミュレーション方式
            ev_threshold: EV閾値
            kelly_fraction: ケリー基準縮小率
            bankroll: 運用資金
        """
        self.config = config or {}
        
        # 4層を初期化
        self.layer1 = Layer1_AbilityEstimator(config=config)
        self.layer2 = Layer2_ProbabilityCalibrator(method=calibration_method)
        self.layer3 = Layer3_RaceSimulator(method=simulation_method)
        self.layer4 = Layer4_EVOptimizer(
            ev_threshold=ev_threshold,
            kelly_fraction=kelly_fraction,
            bankroll=bankroll
        )
        
        self.is_trained = False
        self.calibration_metrics = None
    
    def train(
        self, 
        X_train: pd.DataFrame, 
        y_train: pd.Series,
        groups_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
        groups_val: Optional[pd.Series] = None
    ) -> 'HybridHorseRacingAI':
        """
        全レイヤーを学習
        
        Args:
            X_train: 訓練特徴量
            y_train: 訓練着順
            groups_train: 訓練レースID
            X_val: 検証特徴量
            y_val: 検証着順
            groups_val: 検証レースID
        """
        logger.info("=" * 70)
        logger.info("HybridHorseRacingAI 学習開始")
        logger.info("=" * 70)
        
        # ========================================
        # Layer 1: 能力推定モデル学習
        # ========================================
        logger.info("\n[Step 1/3] Layer 1: 能力推定モデル学習")
        self.layer1.train(
            X_train, y_train, groups_train,
            X_val, y_val, groups_val
        )
        
        # ========================================
        # Layer 1 予測 → Layer 2 学習用データ
        # ========================================
        logger.info("\n[Step 2/3] Layer 1 予測 + Layer 2 キャリブレーション学習")
        
        # 訓練データで予測
        train_pred = self.layer1.predict(X_train, groups_train)
        
        # Layer 2 学習
        self.layer2.fit(
            scores=train_pred['ability_score'].values,
            labels=y_train.values,
            groups=groups_train.values
        )
        
        # ========================================
        # 検証データでキャリブレーション品質評価
        # ========================================
        if X_val is not None:
            logger.info("\n[Step 3/3] キャリブレーション品質評価")
            
            val_pred = self.layer1.predict(X_val, groups_val)
            val_probs = self.layer2.transform(
                val_pred['ability_score'].values,
                groups_val.values
            )
            
            # 勝敗ラベル
            actual_wins = (y_val == 1).astype(int)
            
            self.calibration_metrics = evaluate_calibration(
                val_probs['win_prob'].values,
                actual_wins.values
            )
            
            logger.info(f"  Brier Score: {self.calibration_metrics['brier_score']:.4f} "
                       f"({'PASS' if self.calibration_metrics['passed_brier'] else 'FAIL'})")
            logger.info(f"  ECE: {self.calibration_metrics['ece']:.4f} "
                       f"({'PASS' if self.calibration_metrics['passed_ece'] else 'FAIL'})")
        
        self.is_trained = True
        
        logger.info("\n" + "=" * 70)
        logger.info("HybridHorseRacingAI 学習完了")
        logger.info("=" * 70)
        
        return self
    
    def predict_race(
        self, 
        X: pd.DataFrame, 
        groups: pd.Series,
        odds_dict: Dict[str, Dict]
    ) -> Tuple[List[BetTicket], Dict]:
        """
        レース予測と推奨馬券生成
        
        Args:
            X: レースの特徴量
            groups: レースID
            odds_dict: {bet_type: {combination: odds}}
        
        Returns:
            (推奨馬券リスト, 予測詳細)
        """
        if not self.is_trained:
            raise ValueError("モデルが学習されていません")
        
        # Layer 1: 能力スコア
        l1_pred = self.layer1.predict(X, groups)
        
        # Layer 2: 確率変換
        l2_pred = self.layer2.transform(
            l1_pred['ability_score'].values,
            groups.values
        )
        
        win_probs = l2_pred['win_prob'].values
        
        # Layer 3: 全馬券種確率
        race_probs = self.layer3.simulate_race(win_probs)
        
        # Layer 4: EV最適化 → 推奨馬券
        tickets = self.layer4.select_tickets(race_probs, odds_dict)
        tickets = self.layer4.optimize_portfolio(tickets)
        
        # 予測詳細
        detail = {
            'ability_scores': l1_pred['ability_score'].values,
            'ability_ranks': l1_pred['ability_rank'].values,
            'win_probs': win_probs,
            'race_probs': race_probs,
            'expected_returns': self.layer4.calculate_expected_returns(tickets)
        }
        
        return tickets, detail
    
    def backtest(
        self, 
        test_df: pd.DataFrame,
        feature_cols: List[str],
        race_id_col: str = 'race_id',
        rank_col: str = 'finish_position',
        odds_col: str = 'win_odds',
        horse_number_col: str = 'horse_number',
        popularity_col: str = 'popularity'
    ) -> BacktestResult:
        """
        バックテスト実行（効率化版）
        
        Args:
            test_df: テストデータ
            feature_cols: 特徴量カラム
            race_id_col: レースIDカラム
            rank_col: 着順カラム
            odds_col: オッズカラム
            horse_number_col: 馬番カラム
            popularity_col: 人気カラム
        
        Returns:
            BacktestResult
        """
        try:
            from tqdm import tqdm
        except ImportError:
            # tqdmがない場合はダミー関数を使用
            def tqdm(iterable, **kwargs):
                return iterable
        
        logger.info("\n" + "=" * 70)
        logger.info("バックテスト開始（効率化版）")
        logger.info("=" * 70)
        
        race_ids = test_df[race_id_col].unique()
        n_races = len(race_ids)
        logger.info(f"  対象レース数: {n_races}")
        
        # ========================================
        # Step 1: 全データで一括推論（高速化のキー）
        # ========================================
        logger.info("\n[Step 1/2] 全データで一括推論中...")
        
        X_all = test_df[feature_cols].copy()
        groups_all = test_df[race_id_col]
        
        # Layer 1: 能力スコア（一括）
        l1_pred = self.layer1.predict(X_all, groups_all)
        
        # Layer 2: 確率変換（一括）
        l2_pred = self.layer2.transform(
            l1_pred['ability_score'].values,
            groups_all.values
        )
        
        # 結果をDataFrameに追加
        test_df = test_df.copy()
        test_df['_ability_score'] = l1_pred['ability_score'].values
        test_df['_ability_rank'] = l1_pred['ability_rank'].values
        test_df['_win_prob'] = l2_pred['win_prob'].values
        
        logger.info(f"  一括推論完了: {len(test_df)}行")
        
        # ========================================
        # Step 2: レースごとの評価（EV計算・的中判定のみ）
        # ========================================
        logger.info("\n[Step 2/2] レースごとの評価中...")
        
        total_bet = 0
        total_payout = 0
        n_bets = 0
        n_hits = 0
        favorite_selections = 0
        total_selections = 0
        monthly_results = {}
        
        for race_id in tqdm(race_ids, desc="バックテスト進行中", unit="races"):
            race_data = test_df[test_df[race_id_col] == race_id]
            
            if len(race_data) < 3:
                continue
            
            # このレースの勝率と馬番を取得
            win_probs = race_data['_win_prob'].values
            horse_numbers = race_data[horse_number_col].values
            odds_values = race_data[odds_col].values
            pop_values = race_data[popularity_col].values if popularity_col in race_data.columns else None
            
            # 有効なオッズを持つ馬だけ処理
            valid_mask = pd.notna(odds_values)
            if not valid_mask.any():
                continue
            
            # 単勝のEV計算（Layer 3をスキップ、直接計算）
            tickets = []
            for i in range(len(race_data)):
                if not valid_mask[i]:
                    continue
                    
                prob = win_probs[i]
                odds = odds_values[i]
                horse_num = int(horse_numbers[i])
                
                # EV = P(win) × Odds × PayoutRate(0.8)
                ev = prob * odds * 0.8
                
                if ev >= self.layer4.ev_threshold and prob >= self.layer4.prob_threshold:
                    # 推奨馬券として追加
                    kelly = min(self.layer4.kelly_fraction * (ev - 1) / (odds - 1), 0.1) if odds > 1 else 0
                    amount = max(100, int(self.layer4.bankroll * kelly / 100) * 100)
                    
                    tickets.append(BetTicket(
                        bet_type='win',
                        combination=(horse_num,),
                        model_prob=prob,
                        odds=odds,
                        ev=ev,
                        kelly_fraction=kelly,
                        amount=amount
                    ))
            
            if not tickets:
                continue
            
            # 実際の結果
            sorted_by_finish = race_data.sort_values(rank_col)
            winner_horse_num = int(sorted_by_finish[horse_number_col].iloc[0])
            actual_result = tuple(sorted_by_finish[horse_number_col].iloc[:3].astype(int))
            
            # 的中判定と払戻計算（直接計算）
            bet_amount = 0
            payout = 0
            hit_count = 0
            
            for ticket in tickets:
                bet_amount += ticket.amount
                horse_num = ticket.combination[0]
                
                if horse_num == winner_horse_num:
                    # 的中：払戻 = ベット額 × オッズ
                    payout += ticket.amount * ticket.odds
                    hit_count += 1
                    
                # 1番人気選択率
                total_selections += 1
                if pop_values is not None:
                    idx = np.where(horse_numbers == horse_num)[0]
                    if len(idx) > 0 and pop_values[idx[0]] == 1:
                        favorite_selections += 1
            
            total_bet += bet_amount
            total_payout += int(payout)
            n_bets += len(tickets)
            n_hits += hit_count
            
            # 月別集計
            race_month = str(race_id)[:6]
            if race_month not in monthly_results:
                monthly_results[race_month] = {'bet': 0, 'payout': 0}
            monthly_results[race_month]['bet'] += bet_amount
            monthly_results[race_month]['payout'] += int(payout)
        
        # 結果集計
        roi = (total_payout / total_bet * 100) if total_bet > 0 else 0
        hit_rate = (n_hits / n_bets * 100) if n_bets > 0 else 0
        favorite_rate = (favorite_selections / total_selections * 100) if total_selections > 0 else 0
        
        monthly_rois = {
            m: (r['payout'] / r['bet'] * 100) if r['bet'] > 0 else 0
            for m, r in monthly_results.items()
        }
        
        result = BacktestResult(
            n_races=n_races,
            n_bets=n_bets,
            total_bet=total_bet,
            total_payout=total_payout,
            roi=roi,
            hit_rate=hit_rate,
            favorite_selection_rate=favorite_rate,
            monthly_rois=monthly_rois
        )
        
        logger.info("\n" + "-" * 50)
        logger.info("バックテスト結果")
        logger.info("-" * 50)
        logger.info(f"  レース数: {n_races}")
        logger.info(f"  ベット数: {n_bets}")
        logger.info(f"  総投資: ¥{total_bet:,}")
        logger.info(f"  総払戻: ¥{total_payout:,}")
        logger.info(f"  ROI: {roi:.1f}%")
        logger.info(f"  的中率: {hit_rate:.1f}%")
        logger.info(f"  1番人気選択率: {favorite_rate:.1f}%")
        logger.info("-" * 50)
        
        return result
    
    def save(self, filepath: str):
        """モデルを保存"""
        import joblib
        
        output_dir = Path(filepath)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Layer 1 保存
        self.layer1.save(str(output_dir / "layer1"))
        
        # Layer 2 保存
        joblib.dump(self.layer2, output_dir / "layer2.pkl")
        
        # 設定保存
        import json
        
        # calibration_metricsをJSON化可能に変換
        cal_metrics = None
        if self.calibration_metrics:
            cal_metrics = {}
            for k, v in self.calibration_metrics.items():
                if hasattr(v, 'item'):  # numpy型
                    cal_metrics[k] = v.item()
                elif isinstance(v, (bool, int, float, str)):
                    cal_metrics[k] = v
                else:
                    cal_metrics[k] = str(v)
        
        config = {
            'calibration_method': str(self.layer2.method),
            'simulation_method': str(self.layer3.method),
            'ev_threshold': float(self.layer4.ev_threshold),
            'kelly_fraction': float(self.layer4.kelly_fraction),
            'calibration_metrics': cal_metrics
        }
        with open(output_dir / "config.json", "w") as f:
            json.dump(config, f, indent=2)
        
        logger.info(f"Model saved to {output_dir}")
    
    def load(self, filepath: str) -> 'HybridHorseRacingAI':
        """モデルを読み込み"""
        import joblib
        import json
        
        model_dir = Path(filepath)
        
        # Layer 1 読み込み
        self.layer1.load(str(model_dir / "layer1"))
        
        # Layer 2 読み込み
        self.layer2 = joblib.load(model_dir / "layer2.pkl")
        
        # 設定読み込み
        with open(model_dir / "config.json", "r") as f:
            config = json.load(f)
        
        self.layer3.method = config['simulation_method']
        self.layer4.ev_threshold = config['ev_threshold']
        self.layer4.kelly_fraction = config['kelly_fraction']
        self.calibration_metrics = config.get('calibration_metrics')
        
        self.is_trained = True
        
        logger.info(f"Model loaded from {model_dir}")
        
        return self

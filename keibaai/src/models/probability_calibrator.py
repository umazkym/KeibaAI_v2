"""
Layer 2: Probability Calibrator (確率変換層)

能力スコアを正確な勝率に変換し、レース内で合計100%になるよう正規化する。

Methods:
- softmax: 単純なSoftmax変換
- platt: Platt Scaling（ロジスティック回帰）
- isotonic: Isotonic Regression
- isotonic_softmax: ハイブリッド（推奨）

Quality Metrics:
- Brier Score: < 0.2 が合格基準
- ECE (Expected Calibration Error): < 0.05 が合格基準
"""

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import calibration_curve
from typing import Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class Layer2_ProbabilityCalibrator:
    """
    Layer 2: 確率変換層
    
    ランク学習のスコアを「真の勝率」に変換し、
    レース内で合計100%になるよう正規化する。
    """
    
    def __init__(self, method: str = 'isotonic_softmax'):
        """
        Args:
            method: キャリブレーション方式
                - 'softmax': 単純なSoftmax変換
                - 'platt': Platt Scaling
                - 'isotonic': Isotonic Regression
                - 'isotonic_softmax': ハイブリッド（推奨）
        """
        self.method = method
        self.calibrator = None
        self.is_fitted = False
        
    def fit(
        self, 
        scores: np.ndarray, 
        labels: np.ndarray, 
        groups: np.ndarray
    ) -> 'Layer2_ProbabilityCalibrator':
        """
        キャリブレーターを学習
        
        Args:
            scores: モデルの出力スコア
            labels: 実際の着順（1着=1, 2着=2, ...）
            groups: レースID
        """
        logger.info(f"\n[Layer 2] 確率キャリブレーションの学習 (method={self.method})")
        
        # 1着かどうかのバイナリラベル
        binary_labels = (labels == 1).astype(int)
        
        if self.method == 'isotonic':
            # Isotonic Regression: スコアと勝率の単調関係を学習
            self.calibrator = IsotonicRegression(
                y_min=0.001, 
                y_max=0.999, 
                out_of_bounds='clip'
            )
            self.calibrator.fit(scores, binary_labels)
            
        elif self.method == 'platt':
            # Platt Scaling: ロジスティック回帰
            self.calibrator = LogisticRegression(solver='lbfgs', max_iter=1000)
            self.calibrator.fit(scores.reshape(-1, 1), binary_labels)
            
        elif self.method in ['softmax', 'isotonic_softmax']:
            # Softmax: 学習不要（スコアを直接Softmax変換）
            # 最適な温度パラメータを推定
            self.temperature = self._estimate_temperature(scores, binary_labels, groups)
            logger.info(f"  Estimated temperature: {self.temperature:.4f}")
            
        else:
            raise ValueError(f"Unknown method: {self.method}")
        
        self.is_fitted = True
        logger.info(f"  → キャリブレーション完了")
        
        return self
    
    def _estimate_temperature(self, scores, labels, groups):
        """
        最適な温度パラメータを推定（NLL最小化）
        
        [2-1改修] スコアの標準偏差による粗い推定から、
        検証データ上のNegative Log-Likelihoodを最小化する温度を
        勾配降下法で求める方式に変更。
        これにより確率のキャリブレーションが改善され、
        EV計算→投資判断に連鎖的に影響する。
        """
        from scipy.optimize import minimize_scalar
        
        unique_groups = np.unique(groups)
        
        def nll(T):
            """Negative Log-Likelihood at temperature T"""
            if T <= 0:
                return 1e10
            total_nll = 0.0
            n_races = 0
            for race_id in unique_groups:
                mask = groups == race_id
                s = scores[mask] / T
                lab = labels[mask]
                
                # 勝馬がいないレースはスキップ
                if lab.sum() == 0:
                    continue
                
                # 数値安定性のために最大値を引く
                s_stable = s - np.max(s)
                exp_s = np.exp(s_stable)
                sum_exp = np.sum(exp_s)
                probs = exp_s / sum_exp
                
                # 勝馬のインデックス
                winner_indices = np.where(lab == 1)[0]
                for wi in winner_indices:
                    total_nll -= np.log(probs[wi] + 1e-10)
                n_races += 1
            
            return total_nll / max(n_races, 1)
        
        try:
            result = minimize_scalar(nll, bounds=(0.1, 10.0), method='bounded')
            optimal_T = result.x
            logger.info(f"  NLL最適化温度: T={optimal_T:.4f}, NLL={result.fun:.4f}")
            return float(np.clip(optimal_T, 0.1, 10.0))
        except Exception as e:
            logger.warning(f"  温度最適化失敗, フォールバック使用: {e}")
            # フォールバック: 標準偏差ベース
            score_std = np.std(scores)
            if score_std > 0:
                return max(0.5, min(2.0, score_std))
            return 1.0
    
    def transform(
        self, 
        scores: np.ndarray, 
        groups: np.ndarray
    ) -> pd.DataFrame:
        """
        スコアを確率に変換
        
        Args:
            scores: 能力スコア
            groups: レースID
        
        Returns:
            DataFrame with: group_id, ability_score, win_prob
        """
        result = pd.DataFrame({
            'group_id': groups,
            'ability_score': scores
        })
        
        if self.method == 'softmax':
            # 単純なSoftmax変換（レースごと）
            result['win_prob'] = result.groupby('group_id')['ability_score'].transform(
                lambda x: self._group_softmax(x.values)
            )
            
        elif self.method in ['isotonic', 'platt']:
            # キャリブレーション後、レース内で正規化
            if self.method == 'isotonic':
                raw_probs = self.calibrator.transform(scores)
            else:
                raw_probs = self.calibrator.predict_proba(scores.reshape(-1, 1))[:, 1]
            
            result['raw_prob'] = raw_probs
            
            # レース内正規化
            result['win_prob'] = result.groupby('group_id')['raw_prob'].transform(
                lambda x: x / x.sum()
            )
            
        elif self.method == 'isotonic_softmax':
            # 改良版: スコアを直接Softmax変換（温度パラメータ付き）
            temperature = getattr(self, 'temperature', 1.0)
            
            result['win_prob'] = result.groupby('group_id')['ability_score'].transform(
                lambda x: self._group_softmax(x.values, temperature)
            )
        
        return result
    
    def _group_softmax(self, scores: np.ndarray, temperature: float = 1.0) -> np.ndarray:
        """
        グループ内でSoftmax変換
        
        Args:
            scores: スコア配列
            temperature: 温度パラメータ（高いほど均等に近づく）
        """
        scaled = scores / temperature
        exp_scores = np.exp(scaled - np.max(scaled))  # オーバーフロー防止
        return exp_scores / exp_scores.sum()
    
    def _calibrated_softmax(self, probs: np.ndarray) -> np.ndarray:
        """
        キャリブレーション済み確率を正規化
        
        注意：以前のlogit変換は確率が小さすぎると不適切だったので、
        単純な正規化に変更
        """
        eps = 1e-10
        probs = np.clip(probs, eps, None)  # 負の値防止
        return probs / probs.sum()
    
    def calculate_top3_prob(
        self, 
        win_probs: np.ndarray, 
        groups: np.ndarray
    ) -> np.ndarray:
        """
        3着以内確率を計算（ハービル法の応用）
        
        P(i in top3) ≈ P(i wins) + P(i 2nd) + P(i 3rd)
        """
        result = np.zeros(len(win_probs))
        
        df = pd.DataFrame({
            'group': groups,
            'win_prob': win_probs
        })
        
        for group_id in df['group'].unique():
            mask = df['group'] == group_id
            probs = df.loc[mask, 'win_prob'].values
            n = len(probs)
            
            top3_probs = np.zeros(n)
            
            for i in range(n):
                p_i = probs[i]
                
                # P(i wins)
                p_1st = p_i
                
                # P(i 2nd) = Σ P(j wins) × P(i | j won)
                p_2nd = 0.0
                for j in range(n):
                    if j != i:
                        p_j = probs[j]
                        p_2nd += p_j * (p_i / (1 - p_j + 1e-10))
                
                # P(i 3rd) - 近似計算
                p_3rd = 0.0
                for j in range(n):
                    if j != i:
                        p_j = probs[j]
                        remaining_after_j = 1 - p_j
                        for k in range(n):
                            if k != i and k != j:
                                p_k = probs[k]
                                p_k_given_j = p_k / (remaining_after_j + 1e-10)
                                remaining_after_jk = remaining_after_j - p_k
                                p_i_given_jk = p_i / (remaining_after_jk + 1e-10)
                                p_3rd += p_j * p_k_given_j * p_i_given_jk
                
                top3_probs[i] = min(p_1st + p_2nd + p_3rd, 1.0)
            
            result[mask] = top3_probs
        
        return result


def evaluate_calibration(
    predicted_probs: np.ndarray, 
    actual_wins: np.ndarray, 
    n_bins: int = 10
) -> Dict[str, float]:
    """
    キャリブレーション品質を評価
    
    Args:
        predicted_probs: 予測確率
        actual_wins: 実際の勝敗（1=勝ち, 0=負け）
        n_bins: ECE計算用のビン数
    
    Returns:
        dict with brier_score, ece, max_calibration_error
    """
    # Brier Score: 確率予測の精度（低いほど良い）
    brier = float(np.mean((predicted_probs - actual_wins) ** 2))
    
    # [2-2改修] Brier Skill Score (BSS): ベースラインに対する改善度
    # ベースライン = 常に平均確率を予測するモデル
    baseline_prob = float(np.mean(actual_wins))
    brier_baseline = float(np.mean((baseline_prob - actual_wins) ** 2))
    if brier_baseline > 0:
        brier_skill_score = 1.0 - (brier / brier_baseline)
    else:
        brier_skill_score = 0.0
    
    # Expected Calibration Error (ECE)
    ece = float(_calculate_ece(predicted_probs, actual_wins, n_bins))
    
    # Maximum Calibration Error
    mce = float(_calculate_mce(predicted_probs, actual_wins, n_bins))
    
    return {
        'brier_score': brier,
        'brier_skill_score': brier_skill_score,  # v5.2追加: BSS > 0 ならベースラインより良い
        'brier_baseline': brier_baseline,         # v5.2追加: ベースラインのBrier
        'ece': ece,
        'mce': mce,
        'passed_brier': bool(brier < 0.2),
        'passed_brier_skill': bool(brier_skill_score > 0),  # v5.2追加
        'passed_ece': bool(ece < 0.05)
    }


def _calculate_ece(
    predicted_probs: np.ndarray, 
    actual_wins: np.ndarray, 
    n_bins: int = 10
) -> float:
    """
    Expected Calibration Error (ECE) を計算
    
    予測確率と実際の勝率のズレを測定
    """
    bins = np.linspace(0, 1, n_bins + 1)
    bin_indices = np.digitize(predicted_probs, bins) - 1
    bin_indices = np.clip(bin_indices, 0, n_bins - 1)
    
    ece = 0.0
    n_samples = len(predicted_probs)
    
    for i in range(n_bins):
        mask = bin_indices == i
        if mask.sum() > 0:
            avg_prob = predicted_probs[mask].mean()
            avg_true = actual_wins[mask].mean()
            ece += (mask.sum() / n_samples) * abs(avg_prob - avg_true)
    
    return ece


def _calculate_mce(
    predicted_probs: np.ndarray, 
    actual_wins: np.ndarray, 
    n_bins: int = 10
) -> float:
    """
    Maximum Calibration Error (MCE) を計算
    
    最大のキャリブレーション誤差
    """
    bins = np.linspace(0, 1, n_bins + 1)
    bin_indices = np.digitize(predicted_probs, bins) - 1
    bin_indices = np.clip(bin_indices, 0, n_bins - 1)
    
    mce = 0.0
    
    for i in range(n_bins):
        mask = bin_indices == i
        if mask.sum() > 0:
            avg_prob = predicted_probs[mask].mean()
            avg_true = actual_wins[mask].mean()
            mce = max(mce, abs(avg_prob - avg_true))
    
    return mce


def plot_calibration_curve(
    predicted_probs: np.ndarray, 
    actual_wins: np.ndarray, 
    n_bins: int = 10,
    title: str = "Calibration Curve"
) -> Tuple:
    """
    キャリブレーションカーブをプロット用データとして返す
    
    Returns:
        (prob_true, prob_pred, bins)
    """
    prob_true, prob_pred = calibration_curve(
        actual_wins, predicted_probs, n_bins=n_bins, strategy='uniform'
    )
    
    return prob_true, prob_pred

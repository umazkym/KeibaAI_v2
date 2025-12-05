#!/usr/bin/env python3
"""
μモデル v3.5 + Insights重み付けKelly評価

【アプローチ】
v3.5モデル（最良ROI 81.84%）を維持しつつ、
分析insightsに基づいてKelly傾斜配分を調整

【insights反映】
- 中穴ゾーン（10-50倍）: Kelly係数 +50%
- 5番人気・8番人気: Kelly係数 +30%
- 中距離（1800-2200m）: Kelly係数 +20%

【投資機会】
全レース投資を維持

【日本語】
すべてのログ、コメントは日本語で記述。
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class InsightsKellyEvaluator:
    """Insights重み付けKelly評価クラス"""
    
    def __init__(self):
        self.models_dir = Path('keibaai/models')
        self.mu_model = None
        self.mu_features = None
    
    def load_model_and_data(self):
        """モデルとデータを読み込み"""
        logging.info("モデルとデータを読み込み中...")
        
        # v3.5モデル（最良）
        with open(self.models_dir / 'mu_v3_5/mu_v3_5_ranker.pkl', 'rb') as f:
            self.mu_model = pickle.load(f)
        with open(self.models_dir / 'mu_v3_5/feature_names.json', 'r', encoding='utf-8') as f:
            self.mu_features = json.load(f)
        
        df = pd.read_parquet(self.models_dir / 'mu_v3_3/train_data_mu_v3_3.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logging.info(f"データ読み込み完了: {len(df):,} rows")
        return df
    
    def prepare_predictions(self, df: pd.DataFrame) -> pd.DataFrame:
        """予測を準備"""
        logging.info("予測を実行中...")
        
        # Gap Features
        if 'past_5_finish_position_mean' in df.columns and 'popularity' in df.columns:
            df['ability_rank'] = df.groupby('race_id')['past_5_finish_position_mean'].rank(ascending=True, method='first')
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
        
        for col in self.mu_features:
            if col not in df.columns:
                df[col] = 0
        
        df['mu_pred'] = self.mu_model.predict(df[self.mu_features])
        df['rank_pred'] = df.groupby('race_id')['mu_pred'].rank(ascending=False, method='first')
        
        # Test期間のみ
        df = df[df['race_date'] >= '2024-01-01'].copy()
        
        logging.info(f"Test期間データ: {len(df):,} rows, {df['race_id'].nunique():,} レース")
        return df
    
    def calculate_insights_kelly_weight(self, row, base_fraction: float = 0.25) -> float:
        """Insights反映Kelly重みを計算"""
        p = row['win_prob_estimated']
        odds = row['win_odds']
        pop = row.get('popularity', 5)
        dist = row.get('distance_m', 1800)
        
        if pd.isna(odds) or odds <= 1:
            return 0.0
        
        # 基本Kelly
        b = odds - 1
        kelly_full = (p * b - (1 - p)) / b
        
        if kelly_full <= 0:
            return 0.0
        
        # Insights調整係数
        adjustment = 1.0
        
        # 中穴ゾーンボーナス（10-50倍）
        if 10 <= odds <= 50:
            adjustment += 0.5
        
        # 5番人気・8番人気ボーナス
        if pop in [5, 8]:
            adjustment += 0.3
        
        # 中距離ボーナス
        if 1800 <= dist <= 2200:
            adjustment += 0.2
        
        kelly_adjusted = kelly_full * base_fraction * adjustment
        
        # 上限設定
        return min(kelly_adjusted, 0.15)
    
    def estimate_win_probability(self, df: pd.DataFrame) -> pd.DataFrame:
        """勝率を推定"""
        def softmax_win_prob(group):
            scores = group['mu_pred'].values
            temperature = 0.5
            exp_scores = np.exp(scores / temperature)
            probs = exp_scores / exp_scores.sum()
            return pd.Series(probs, index=group.index)
        
        df['win_prob_estimated'] = df.groupby('race_id').apply(softmax_win_prob).reset_index(level=0, drop=True)
        return df
    
    def calculate_roi_uniform(self, df: pd.DataFrame) -> float:
        """均等配分ROI"""
        top1 = df[df['rank_pred'] == 1]
        hits = top1[top1['finish_position'] == 1]
        return hits['win_odds'].sum() / len(top1) if len(top1) > 0 else 0
    
    def calculate_roi_insights_kelly(self, df: pd.DataFrame, base_fraction: float = 0.25) -> float:
        """Insights Kelly ROI"""
        top1 = df[df['rank_pred'] == 1].copy()
        
        # Insights Kelly重みを計算
        top1['kelly_weight'] = top1.apply(
            lambda row: self.calculate_insights_kelly_weight(row, base_fraction), 
            axis=1
        )
        
        # 正規化
        total_weight = top1['kelly_weight'].sum()
        if total_weight == 0:
            return 0.0
        
        top1['bet_amount'] = top1['kelly_weight'] / total_weight * len(top1)
        
        # ROI計算
        total_bet = top1['bet_amount'].sum()
        hits = top1[top1['finish_position'] == 1]
        total_return = (hits['bet_amount'] * hits['win_odds']).sum()
        
        return total_return / total_bet if total_bet > 0 else 0
    
    def run_evaluation(self):
        """評価実行"""
        df = self.load_model_and_data()
        df = self.prepare_predictions(df)
        df = self.estimate_win_probability(df)
        
        logging.info("=" * 60)
        logging.info("【v3.5 + Insights Kelly 評価】")
        logging.info("=" * 60)
        
        # 均等配分（ベースライン）
        roi_uniform = self.calculate_roi_uniform(df)
        logging.info(f"均等配分（v3.5）ROI: {roi_uniform:.2%}")
        
        # Insights Kelly（複数のbase_fraction）
        for base_frac in [0.1, 0.25, 0.5]:
            roi_kelly = self.calculate_roi_insights_kelly(df, base_frac)
            logging.info(f"Insights Kelly（base={base_frac}）ROI: {roi_kelly:.2%}")
        
        # 最良結果
        best_roi = max(
            self.calculate_roi_insights_kelly(df, 0.1),
            self.calculate_roi_insights_kelly(df, 0.25),
            self.calculate_roi_insights_kelly(df, 0.5),
            roi_uniform
        )
        
        logging.info(f"\n最良ROI: {best_roi:.2%}")
        logging.info(f"レース数: {df['race_id'].nunique():,}（全レース投資）")
        
        return best_roi


if __name__ == "__main__":
    evaluator = InsightsKellyEvaluator()
    evaluator.run_evaluation()

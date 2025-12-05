#!/usr/bin/env python3
"""
Kelly傾斜配分 + Gap Feature拡張評価スクリプト

【方針】
投資機会を維持しながらROIを改善する:
1. 全レースに投資（フィルタリングなし）
2. Kelly傾斜配分で高確信レースに多く投資
3. Gap Feature拡張でモデル予測精度向上

【日本語】
すべてのログ、コメントは日本語で記述。
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import json
import logging
import optuna
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class KellyWeightedEvaluator:
    """Kelly傾斜配分評価クラス"""
    
    def __init__(self):
        self.models_dir = Path('keibaai/models')
        self.mu_model = None
        self.mu_features = None
    
    def load_models(self):
        """モデル読み込み"""
        logging.info("モデルを読み込み中...")
        
        with open(self.models_dir / 'mu_v3_3/mu_v3_3_ranker.pkl', 'rb') as f:
            self.mu_model = pickle.load(f)
        with open(self.models_dir / 'mu_v3_3/feature_names.json', 'r', encoding='utf-8') as f:
            self.mu_features = json.load(f)
        
        logging.info("モデル読み込み完了")
    
    def load_data(self):
        """データ読み込み"""
        logging.info("データを読み込み中...")
        df = pd.read_parquet(self.models_dir / 'mu_v3_3/train_data_mu_v3_3.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        logging.info(f"データ読み込み完了: {len(df):,} rows")
        return df
    
    def add_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gap Feature拡張"""
        logging.info("Gap Features を拡張中...")
        
        # 既存の gap_ability_popularity を生成
        ability_col = 'past_5_finish_position_mean'
        if ability_col in df.columns and 'popularity' in df.columns:
            df['ability_rank'] = df.groupby('race_id')[ability_col].rank(ascending=True, method='first')
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
        
        # 新規Gap Features
        # 1. 騎手勝率 vs 人気
        if 'jockey_win_rate' in df.columns:
            df['jockey_rank'] = df.groupby('race_id')['jockey_win_rate'].rank(ascending=False, method='first')
            df['gap_jockey_popularity'] = df['popularity'] - df['jockey_rank']
        
        # 2. 調教師勝率 vs 人気
        if 'trainer_win_rate' in df.columns:
            df['trainer_rank'] = df.groupby('race_id')['trainer_win_rate'].rank(ascending=False, method='first')
            df['gap_trainer_popularity'] = df['popularity'] - df['trainer_rank']
        
        # 3. 血統スコア vs 人気
        if 'sire_win_rate' in df.columns:
            df['pedigree_rank'] = df.groupby('race_id')['sire_win_rate'].rank(ascending=False, method='first')
            df['gap_pedigree_popularity'] = df['popularity'] - df['pedigree_rank']
        
        # 4. 馬体重Zスコア vs 人気（新規）
        if 'horse_weight_zscore' in df.columns:
            df['weight_rank'] = df.groupby('race_id')['horse_weight_zscore'].rank(ascending=False, method='first')
            df['gap_weight_popularity'] = df['popularity'] - df['weight_rank']
        
        # 5. ペース適性 vs 人気（新規）
        if 'pace_fit_score' in df.columns:
            df['pace_rank'] = df.groupby('race_id')['pace_fit_score'].rank(ascending=False, method='first')
            df['gap_pace_popularity'] = df['popularity'] - df['pace_rank']
        
        # 6. コンボ成績 vs 人気（新規）
        if 'combo_avg_finish' in df.columns:
            df['combo_rank'] = df.groupby('race_id')['combo_avg_finish'].rank(ascending=True, method='first')
            df['gap_combo_popularity'] = df['popularity'] - df['combo_rank']
        
        logging.info("Gap Features 拡張完了")
        return df
    
    def predict_with_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """予測実行"""
        logging.info("予測を実行中...")
        
        # 欠損特徴量を0で埋める
        for col in self.mu_features:
            if col not in df.columns:
                df[col] = 0
        
        df['mu_pred'] = self.mu_model.predict(df[self.mu_features])
        df['rank_pred'] = df.groupby('race_id')['mu_pred'].rank(ascending=False, method='first')
        
        return df
    
    def estimate_win_probability(self, df: pd.DataFrame) -> pd.DataFrame:
        """勝率を推定（ソフトマックス変換）"""
        def softmax_win_prob(group):
            scores = group['mu_pred'].values
            # 温度パラメータでスケーリング
            temperature = 0.5
            exp_scores = np.exp(scores / temperature)
            probs = exp_scores / exp_scores.sum()
            return pd.Series(probs, index=group.index)
        
        df['win_prob_estimated'] = df.groupby('race_id').apply(softmax_win_prob).reset_index(level=0, drop=True)
        return df
    
    def calculate_kelly_weights(self, df: pd.DataFrame, fraction: float = 0.25) -> pd.DataFrame:
        """Kelly傾斜配分の重みを計算"""
        logging.info(f"Kelly傾斜配分（fraction={fraction}）を計算中...")
        
        def kelly_weight(row):
            p = row['win_prob_estimated']
            odds = row['win_odds']
            
            if pd.isna(odds) or odds <= 1:
                return 0.0
            
            b = odds - 1
            # Kelly fraction: (p * b - (1-p)) / b
            kelly_full = (p * b - (1 - p)) / b
            
            # 負の場合は0、正の場合はfractionでスケーリング
            kelly_scaled = max(0, kelly_full * fraction)
            
            # 上限設定（最大10%）
            return min(kelly_scaled, 0.1)
        
        df['kelly_weight'] = df.apply(kelly_weight, axis=1)
        
        # レース内で正規化（合計1にする）
        df['kelly_weight_normalized'] = df.groupby('race_id')['kelly_weight'].transform(
            lambda x: x / x.sum() if x.sum() > 0 else 1 / len(x)
        )
        
        return df
    
    def calculate_roi_uniform(self, df: pd.DataFrame) -> float:
        """均等配分ROI（従来方式）"""
        top1 = df[df['rank_pred'] == 1]
        hits = top1[top1['finish_position'] == 1]
        return hits['win_odds'].sum() / len(top1) if len(top1) > 0 else 0
    
    def calculate_roi_kelly(self, df: pd.DataFrame) -> float:
        """Kelly傾斜配分ROI"""
        # 各レースでTop1予測馬に投資
        top1 = df[df['rank_pred'] == 1].copy()
        
        # Kelly重みに基づく投資額（合計を揃える）
        top1['bet_amount'] = top1['kelly_weight_normalized'] * len(top1)
        
        # リターン計算
        total_bet = top1['bet_amount'].sum()
        hits = top1[top1['finish_position'] == 1]
        total_return = (hits['bet_amount'] * hits['win_odds']).sum()
        
        return total_return / total_bet if total_bet > 0 else 0
    
    def run_evaluation(self):
        """評価実行"""
        self.load_models()
        df = self.load_data()
        df = self.add_gap_features(df)
        df = self.predict_with_gap_features(df)
        df = self.estimate_win_probability(df)
        df = self.calculate_kelly_weights(df)
        
        # Test期間のみ評価
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        
        logging.info("=" * 60)
        logging.info("【ROI比較（全レース投資 - 投資機会維持）】")
        logging.info("=" * 60)
        
        # 均等配分
        roi_uniform = self.calculate_roi_uniform(test_df)
        logging.info(f"均等配分 ROI: {roi_uniform:.2%}")
        
        # Kelly傾斜配分（複数のfraction）
        for fraction in [0.1, 0.25, 0.5]:
            test_df = self.calculate_kelly_weights(test_df, fraction=fraction)
            roi_kelly = self.calculate_roi_kelly(test_df)
            logging.info(f"Kelly傾斜（fraction={fraction}） ROI: {roi_kelly:.2%}")
        
        # レース数確認
        n_races = test_df['race_id'].nunique()
        logging.info(f"\nTest期間レース数: {n_races:,}（全レース投資）")
        
        # 結果保存
        results = {
            'test_races': n_races,
            'roi_uniform': roi_uniform,
            'roi_kelly_0.1': self.calculate_roi_kelly(self.calculate_kelly_weights(test_df, 0.1)),
            'roi_kelly_0.25': self.calculate_roi_kelly(self.calculate_kelly_weights(test_df, 0.25)),
            'roi_kelly_0.5': self.calculate_roi_kelly(self.calculate_kelly_weights(test_df, 0.5)),
        }
        
        with open('keibaai/models/kelly_evaluation_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logging.info("\n結果を保存: keibaai/models/kelly_evaluation_results.json")
        
        return results


if __name__ == "__main__":
    evaluator = KellyWeightedEvaluator()
    results = evaluator.run_evaluation()

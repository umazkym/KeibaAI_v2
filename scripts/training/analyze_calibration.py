#!/usr/bin/env python3
"""
確率キャリブレーション分析

【計画書Phase 5】
> IsotonicCalibration適用 → EV計算 → SafeKelly実装
→ まだ適用していない！

【仮説】
- v11.0のsoftmax確率は「真の勝率」とずれている可能性
- キャリブレーションでEdge Score精度が向上する可能性

【検証】
- キャリブレーション前後のBrier Score比較
- キャリブレーション後のEdge ScoreでROI再評価
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
from sklearn.metrics import brier_score_loss

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class CalibrationAnalyzer:
    """確率キャリブレーション分析"""
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/calibration')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_data(self):
        """データ読み込み"""
        logging.info("=" * 60)
        logging.info("データ読み込み")
        logging.info("=" * 60)
        
        with open(self.model_dir / 'mu_v11_0_ranker.pkl', 'rb') as f:
            self.model = pickle.load(f)
        with open(self.model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            self.feature_names = json.load(f)
        
        base_data = pd.read_parquet('keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet')
        base_data['race_date'] = pd.to_datetime(base_data['race_date'])
        base_data['race_id'] = base_data['race_id'].astype(str)
        base_data['horse_id'] = base_data['horse_id'].astype(str)
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        races_df['horse_number'] = pd.to_numeric(races_df['horse_number'], errors='coerce')
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        returns_df['race_id'] = returns_df['race_id'].astype(str)
        
        logging.info(f"  学習データ: {len(base_data):,}件")
        
        return base_data, races_df, returns_df
    
    def generate_predictions(self, df, races_df):
        """v11.0で予測"""
        logging.info("\n予測生成中...")
        
        # 特徴量準備
        for f in self.feature_names:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        preds = self.model.predict(df[self.feature_names])
        df['raw_score'] = preds
        df['rank_in_race'] = df.groupby('race_id')['raw_score'].rank(ascending=False, method='first')
        
        # softmax確率
        def softmax_prob(x):
            exp_x = np.exp(x - x.max())
            return exp_x / exp_x.sum()
        
        df['model_prob'] = df.groupby('race_id')['raw_score'].transform(softmax_prob)
        
        # 結果マージ
        for col in ['finish_position', 'horse_number', 'win_odds']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'finish_position', 'horse_number', 'win_odds']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        # is_win
        df['is_win'] = (df['finish_position'].fillna(99) == 1).astype(int)
        
        return df
    
    def analyze_calibration(self, df):
        """キャリブレーション分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【キャリブレーション分析】")
        logging.info("=" * 60)
        
        # Train期間でキャリブレータを学習
        train_df = df[df['race_date'] < '2023-01-01'].dropna(subset=['model_prob', 'is_win'])
        
        logging.info(f"  Train: {len(train_df):,}件")
        
        # キャリブレーション前のBrier Score
        brier_before_train = brier_score_loss(train_df['is_win'], train_df['model_prob'])
        logging.info(f"  Train Brier Score (before): {brier_before_train:.4f}")
        
        # IsotonicRegressionでキャリブレータを学習
        calibrator = IsotonicRegression(y_min=0.001, y_max=0.999, out_of_bounds='clip')
        calibrator.fit(train_df['model_prob'], train_df['is_win'])
        
        # 全期間にキャリブレーションを適用
        df['calibrated_prob'] = calibrator.predict(df['model_prob'].clip(0.001, 0.999))
        
        # Valid/Testで評価
        for period_name, date_filter in [('Valid', ('2023-01-01', '2024-01-01')), ('Test', ('2024-01-01', '2100-01-01'))]:
            period_df = df[(df['race_date'] >= date_filter[0]) & (df['race_date'] < date_filter[1])].copy()
            period_df = period_df.dropna(subset=['model_prob', 'calibrated_prob', 'is_win'])
            
            brier_before = brier_score_loss(period_df['is_win'], period_df['model_prob'])
            brier_after = brier_score_loss(period_df['is_win'], period_df['calibrated_prob'])
            
            improvement = (brier_before - brier_after) / brier_before * 100
            
            logging.info(f"\n  {period_name}期間 ({len(period_df):,}件)")
            logging.info(f"    Brier Score (before): {brier_before:.4f}")
            logging.info(f"    Brier Score (after):  {brier_after:.4f}")
            logging.info(f"    改善: {improvement:.1f}%")
            
            # 確率分布の変化
            logging.info(f"    model_prob:      平均{period_df['model_prob'].mean():.3f}, 最大{period_df['model_prob'].max():.3f}")
            logging.info(f"    calibrated_prob: 平均{period_df['calibrated_prob'].mean():.3f}, 最大{period_df['calibrated_prob'].max():.3f}")
        
        return df, calibrator
    
    def evaluate_calibrated_edge_score(self, df, returns_df):
        """キャリブレーション後のEdge ScoreでROI評価"""
        logging.info("\n" + "=" * 60)
        logging.info("【キャリブレーション後 Edge Score ROI】")
        logging.info("=" * 60)
        
        # オッズ暗示確率
        df['odds_prob'] = 1 / df['win_odds'].clip(lower=1.1)
        df['odds_prob_normalized'] = df.groupby('race_id')['odds_prob'].transform(lambda x: x / x.sum() * 0.8)
        
        # キャリブレーション前後のEdge Score
        df['edge_score_raw'] = df['model_prob'] / df['odds_prob_normalized'].clip(lower=0.01)
        df['edge_score_calibrated'] = df['calibrated_prob'] / df['odds_prob_normalized'].clip(lower=0.01)
        
        # 配当データ
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        top1 = test_df[test_df['rank_in_race'] == 1].copy()
        top1 = top1.merge(
            tansho.rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        results = []
        
        for edge_type, edge_col in [('Raw', 'edge_score_raw'), ('Calibrated', 'edge_score_calibrated')]:
            logging.info(f"\n  【{edge_type} Edge Score】")
            for edge_thresh in [1.0, 1.5, 2.0]:
                subset = top1[top1[edge_col] >= edge_thresh]
                
                if len(subset) < 30:
                    continue
                
                payout = subset['tansho_payout'].fillna(0).sum()
                roi = payout / (len(subset) * 100)
                hits = (subset['tansho_payout'] > 0).sum()
                hit_rate = hits / len(subset)
                
                status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
                logging.info(f"    Edge≥{edge_thresh:.1f} | ベット{len(subset):5} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
                
                results.append({
                    'type': edge_type,
                    'edge_thresh': edge_thresh,
                    'n_bets': len(subset),
                    'hit_rate': hit_rate,
                    'roi': roi,
                })
        
        return pd.DataFrame(results)
    
    def run(self):
        """メイン実行"""
        base_data, races_df, returns_df = self.load_data()
        
        df = self.generate_predictions(base_data, races_df)
        
        df, calibrator = self.analyze_calibration(df)
        
        results = self.evaluate_calibrated_edge_score(df, returns_df)
        results.to_csv(self.output_dir / 'calibration_results.csv', index=False)
        
        # 結論
        logging.info("\n" + "=" * 60)
        logging.info("【結論】")
        logging.info("=" * 60)
        
        if len(results) > 0:
            raw_best = results[results['type'] == 'Raw']['roi'].max()
            calibrated_best = results[results['type'] == 'Calibrated']['roi'].max()
            
            if calibrated_best > raw_best:
                logging.info(f"  ✅ キャリブレーションでROI改善: {raw_best:.1%} → {calibrated_best:.1%}")
            else:
                logging.info(f"  ⚠️ キャリブレーションでROI改善せず")
        
        return results


if __name__ == "__main__":
    analyzer = CalibrationAnalyzer()
    analyzer.run()

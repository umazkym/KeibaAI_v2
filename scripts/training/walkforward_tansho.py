#!/usr/bin/env python3
"""
単勝Edge Score ウォークフォワード検証

【目的】
単勝 Edge≥2.0 + Conf≥0.1 で Test ROI 108.1%だったが、
複数期間での安定性を確認する。
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class TanshoWalkForwardValidator:
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
    
    def load_data(self):
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
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        returns_df['race_id'] = returns_df['race_id'].astype(str)
        
        logging.info(f"学習データ: {len(base_data):,}件")
        return base_data, races_df, returns_df
    
    def generate_predictions(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        for f in self.feature_names:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        preds = self.model.predict(df[self.feature_names])
        df['raw_score'] = preds
        df['rank_in_race'] = df.groupby('race_id')['raw_score'].rank(ascending=False, method='first')
        
        def softmax_prob(x):
            exp_x = np.exp(x - x.max())
            return exp_x / exp_x.sum()
        
        df['main_prob'] = df.groupby('race_id')['raw_score'].transform(softmax_prob)
        
        df['score_diff'] = df.groupby('race_id')['raw_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        
        for col in ['horse_number', 'win_odds']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'horse_number', 'win_odds']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        df['odds_implied_prob'] = (1 / df['win_odds'].clip(lower=1.1)) * 0.8
        df['edge_score'] = df['main_prob'] / df['odds_implied_prob'].clip(lower=0.01)
        
        return df
    
    def run(self):
        base_data, races_df, returns_df = self.load_data()
        
        logging.info("予測生成中...")
        pred_df = self.generate_predictions(base_data, races_df)
        
        tansho_df = returns_df[returns_df['bet_type'] == 'tansho']
        
        score_diff_max = pred_df['score_diff'].max()
        
        periods = [
            ('2021-01', '2021-07'),
            ('2021-07', '2022-01'),
            ('2022-01', '2022-07'),
            ('2022-07', '2023-01'),
            ('2023-01', '2023-07'),
            ('2023-07', '2024-01'),
            ('2024-01', '2024-07'),
            ('2024-07', '2025-01'),
        ]
        
        logging.info("\n" + "=" * 60)
        logging.info("【単勝 Edge Score ウォークフォワード検証】")
        logging.info("=" * 60)
        
        results = []
        
        for edge_thresh in [1.5, 2.0, 2.5]:
            for conf_thresh in [0.05, 0.1]:
                period_rois = []
                
                for start, end in periods:
                    period_pred = pred_df[
                        (pred_df['race_date'] >= start) & (pred_df['race_date'] < end)
                    ]
                    
                    top1 = period_pred[period_pred['rank_in_race'] == 1].copy()
                    filtered = top1[
                        (top1['edge_score'] >= edge_thresh) & 
                        (top1['score_diff'] >= conf_thresh * score_diff_max)
                    ]
                    
                    if len(filtered) < 10:
                        continue
                    
                    period_tansho = tansho_df[tansho_df['race_id'].isin(filtered['race_id'])]
                    
                    merged = filtered.merge(
                        period_tansho[['race_id', 'horse_number', 'payout']],
                        on=['race_id', 'horse_number'],
                        how='left'
                    )
                    
                    payout = merged['payout'].fillna(0).sum()
                    roi = payout / (len(filtered) * 100)
                    period_rois.append(roi)
                
                if len(period_rois) >= 4:
                    avg_roi = np.mean(period_rois)
                    std_roi = np.std(period_rois)
                    above_100 = sum(1 for r in period_rois if r > 1.0) / len(period_rois)
                    
                    results.append({
                        'filter': f'E{edge_thresh}_C{conf_thresh}',
                        'avg_roi': avg_roi,
                        'std_roi': std_roi,
                        'above_100': above_100,
                        'periods': len(period_rois),
                    })
                    
                    status = '✅' if avg_roi >= 1.03 else '⚠️'
                    logging.info(
                        f"  {results[-1]['filter']:>12} | 平均ROI{avg_roi:6.1%} ± {std_roi:.0%} | "
                        f"ROI>100%: {above_100:.0%} {status}"
                    )
        
        if results:
            best = max(results, key=lambda x: x['avg_roi'])
            logging.info(f"\n【最良条件】: {best['filter']}")
            logging.info(f"  平均ROI: {best['avg_roi']:.1%}")
            
            if best['avg_roi'] >= 1.03:
                logging.info("  ✅ 安定してROI 103%達成")
            else:
                logging.info("  ⚠️ ROI 103%未達（さらなる検証必要）")
        
        return results


if __name__ == "__main__":
    validator = TanshoWalkForwardValidator()
    validator.run()

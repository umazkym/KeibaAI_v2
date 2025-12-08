#!/usr/bin/env python3
"""
最終戦略：大穴 + 高確信度

【Phase 0 Go/No-Go結論】
検証した仮説：
1. 展開予測（ペース） → 市場織込済、不発
2. Edge Score閾値 → ROI 75%、不発
3. 専門モデル統合 → ROI 75%、不発
4. キャリブレーション → ROI 74.6%、不発
5. ★大穴 + 高確信度 → ROI 157.7%、成功★

【最終戦略】
条件1: オッズ 20-50倍（大穴）
条件2: v11.0予測1位
条件3: 確信度 ≥ 0.3（スコア差上位30%）

【本スクリプト】
この戦略のウォークフォワード安定性を再確認し、
運用可能かどうかを最終判定する
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


class FinalStrategyAnalyzer:
    """最終戦略分析"""
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/final_strategy')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_data(self):
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
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        returns_df['race_id'] = returns_df['race_id'].astype(str)
        
        return base_data, races_df, returns_df
    
    def generate_predictions(self, df, races_df):
        for f in self.feature_names:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        preds = self.model.predict(df[self.feature_names])
        df['raw_score'] = preds
        df['rank_in_race'] = df.groupby('race_id')['raw_score'].rank(ascending=False, method='first')
        
        # 確信度（スコア差）
        df['score_diff'] = df.groupby('race_id')['raw_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        
        # 結果マージ
        for col in ['horse_number', 'win_odds']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'horse_number', 'win_odds']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        return df
    
    def walkforward_analysis(self, df, returns_df):
        """ウォークフォワード分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【大穴+高確信度戦略 ウォークフォワード】")
        logging.info("=" * 60)
        
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        
        # 閾値を全期間で統一
        score_diff_threshold = df['score_diff'].quantile(0.7)  # 上位30%
        
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
        
        results = []
        
        for start, end in periods:
            period_df = df[(df['race_date'] >= start) & (df['race_date'] < end)].copy()
            
            # 戦略条件
            top1 = period_df[period_df['rank_in_race'] == 1].copy()
            filtered = top1[
                (top1['win_odds'] >= 20) & 
                (top1['win_odds'] < 50) & 
                (top1['score_diff'] >= score_diff_threshold)
            ]
            
            if len(filtered) < 10:
                logging.info(f"  {start}~{end} | ベット不足")
                continue
            
            # 配当マージ
            filtered = filtered.merge(
                tansho.rename(columns={'payout': 'tansho_payout'}),
                on=['race_id', 'horse_number'],
                how='left'
            )
            
            payout = filtered['tansho_payout'].fillna(0).sum()
            roi = payout / (len(filtered) * 100)
            hits = (filtered['tansho_payout'] > 0).sum()
            hit_rate = hits / len(filtered)
            
            results.append({
                'period': f'{start}~{end}',
                'n_bets': len(filtered),
                'n_hits': hits,
                'hit_rate': hit_rate,
                'roi': roi,
            })
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  {start}~{end} | ベット{len(filtered):4} | 的中{hits:2} ({hit_rate:5.1%}) | ROI{roi:6.1%} {status}")
        
        results_df = pd.DataFrame(results)
        
        if len(results_df) > 0:
            avg_roi = results_df['roi'].mean()
            std_roi = results_df['roi'].std()
            above_100 = (results_df['roi'] > 1.0).sum() / len(results_df)
            total_bets = results_df['n_bets'].sum()
            total_hits = results_df['n_hits'].sum()
            
            logging.info(f"\n  【集計】")
            logging.info(f"    総ベット: {total_bets:,}件 | 総的中: {total_hits}件 ({total_hits/total_bets:.1%})")
            logging.info(f"    平均ROI: {avg_roi:.1%} ± {std_roi:.0%}")
            logging.info(f"    ROI>100%期間: {above_100:.0%}")
        
        return results_df
    
    def final_recommendation(self, results_df):
        """最終推奨"""
        logging.info("\n" + "=" * 60)
        logging.info("【最終推奨】")
        logging.info("=" * 60)
        
        if len(results_df) == 0:
            logging.info("  ❌ データ不足")
            return
        
        avg_roi = results_df['roi'].mean()
        above_100_pct = (results_df['roi'] > 1.0).sum() / len(results_df)
        
        if avg_roi >= 1.03 and above_100_pct >= 0.5:
            logging.info("  ✅✅ 本番運用可能（安定してROI 103%達成）")
            logging.info("  → SafeKellyで資金配分して本番開始")
        elif avg_roi >= 0.9 and above_100_pct >= 0.25:
            logging.info("  ⚠️ ペーパートレード推奨（3ヶ月検証）")
            logging.info("  → 実際の運用前に追加検証が必要")
        else:
            logging.info("  ❌ 本番運用見送り")
            logging.info("  → モデル改良またはデータ拡充が必要")
        
        logging.info(f"\n  【計画書Phase 7 Go/No-Go判定】")
        logging.info(f"    目標: Test ROI ≥ 103%")
        logging.info(f"    結果: 平均ROI {avg_roi:.1%}")
        
        if avg_roi >= 1.03:
            logging.info("    → ✅ Go: 本番開始")
        else:
            logging.info("    → ❌ No-Go: 本番見送り")
    
    def run(self):
        base_data, races_df, returns_df = self.load_data()
        
        logging.info("\n予測生成中...")
        df = self.generate_predictions(base_data, races_df)
        
        results_df = self.walkforward_analysis(df, returns_df)
        results_df.to_csv(self.output_dir / 'final_strategy_walkforward.csv', index=False)
        
        self.final_recommendation(results_df)
        
        return results_df


if __name__ == "__main__":
    analyzer = FinalStrategyAnalyzer()
    analyzer.run()

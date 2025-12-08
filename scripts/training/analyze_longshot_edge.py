#!/usr/bin/env python3
"""
大穴（20-50倍）エッジの深掘り分析

【発見】
- 予測1位がオッズ20-50倍の場合、ROI 95.0%（311ベット）
- 他のどの条件よりも高い
- 計画書の「本命バイアス」理論と一致

【本スクリプト】
- 大穴ゾーンのROI安定性を検証
- さらなるフィルタ条件を探索してROI 103%達成を狙う
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


class LongShotEdgeAnalyzer:
    """大穴エッジ分析"""
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/longshot_edge')
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
        
        return base_data, races_df, returns_df
    
    def generate_predictions(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """v11.0モデルで予測"""
        df = df.copy()
        
        for f in self.feature_names:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        preds = self.model.predict(df[self.feature_names])
        df['raw_score'] = preds
        df['rank_in_race'] = df.groupby('race_id')['raw_score'].rank(ascending=False, method='first')
        
        # 確信度
        df['score_diff'] = df.groupby('race_id')['raw_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        
        # 結果マージ
        for col in ['finish_position', 'horse_number', 'win_odds', 'popularity']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'finish_position', 'horse_number', 'win_odds', 'popularity']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        return df
    
    def analyze_longshot_walkforward(self, pred_df, returns_df):
        """大穴ゾーンのウォークフォワード検証"""
        logging.info("\n" + "=" * 60)
        logging.info("【大穴ゾーン（20-50倍）ウォークフォワード検証】")
        logging.info("=" * 60)
        
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        
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
            period_pred = pred_df[(pred_df['race_date'] >= start) & (pred_df['race_date'] < end)].copy()
            
            # 予測1位 + オッズ20-50倍
            top1 = period_pred[period_pred['rank_in_race'] == 1].copy()
            longshot = top1[(top1['win_odds'] >= 20) & (top1['win_odds'] < 50)]
            
            if len(longshot) < 20:
                continue
            
            # 配当マージ
            longshot = longshot.merge(
                tansho.rename(columns={'payout': 'tansho_payout'}),
                on=['race_id', 'horse_number'],
                how='left'
            )
            
            payout = longshot['tansho_payout'].fillna(0).sum()
            roi = payout / (len(longshot) * 100)
            hits = (longshot['tansho_payout'] > 0).sum()
            hit_rate = hits / len(longshot)
            
            results.append({
                'period': f'{start}~{end}',
                'n_bets': len(longshot),
                'n_hits': hits,
                'hit_rate': hit_rate,
                'roi': roi,
            })
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  {start}~{end} | ベット{len(longshot):4} | 的中{hits:2} ({hit_rate:5.1%}) | ROI{roi:6.1%} {status}")
        
        results_df = pd.DataFrame(results)
        
        if len(results_df) > 0:
            avg_roi = results_df['roi'].mean()
            std_roi = results_df['roi'].std()
            above_100 = (results_df['roi'] > 1.0).sum() / len(results_df)
            
            logging.info(f"\n  平均ROI: {avg_roi:.1%} ± {std_roi:.0%}")
            logging.info(f"  ROI>100%期間: {above_100:.0%}")
        
        return results_df
    
    def find_additional_filters(self, pred_df, returns_df):
        """追加フィルタ条件の探索"""
        logging.info("\n" + "=" * 60)
        logging.info("【追加フィルタ探索（大穴ゾーン内）】")
        logging.info("=" * 60)
        
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        
        test_df = pred_df[pred_df['race_date'] >= '2024-01-01'].copy()
        top1 = test_df[test_df['rank_in_race'] == 1].copy()
        longshot = top1[(top1['win_odds'] >= 20) & (top1['win_odds'] < 50)]
        
        longshot = longshot.merge(
            tansho.rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        # 確信度別
        score_diff_max = longshot['score_diff'].max()
        logging.info("\n【確信度フィルタ】")
        for conf_thresh in [0.0, 0.1, 0.2, 0.3]:
            subset = longshot[longshot['score_diff'] >= conf_thresh * score_diff_max]
            if len(subset) < 20:
                continue
            
            payout = subset['tansho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['tansho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  Conf≥{conf_thresh:.1f} | ベット{len(subset):4} | 的中{hits:2} ({hit_rate:5.1%}) | ROI{roi:6.1%} {status}")
        
        # オッズ細分化
        logging.info("\n【オッズ細分化】")
        for odds_min, odds_max in [(20, 30), (30, 40), (40, 50), (50, 100)]:
            subset = top1[(top1['win_odds'] >= odds_min) & (top1['win_odds'] < odds_max)]
            subset = subset.merge(
                tansho.rename(columns={'payout': 'tansho_payout'}),
                on=['race_id', 'horse_number'],
                how='left'
            )
            
            if len(subset) < 20:
                continue
            
            payout = subset['tansho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['tansho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  {odds_min}-{odds_max}倍 | ベット{len(subset):4} | 的中{hits:2} ({hit_rate:5.1%}) | ROI{roi:6.1%} {status}")
        
        # 人気順位フィルタ
        logging.info("\n【人気順位フィルタ（大穴ゾーン）】")
        for pop_range in [(5, 7), (7, 10), (10, 15)]:
            subset = longshot[(longshot['popularity'] >= pop_range[0]) & (longshot['popularity'] < pop_range[1])]
            
            if len(subset) < 20:
                continue
            
            payout = subset['tansho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['tansho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  人気{pop_range[0]}-{pop_range[1]}位 | ベット{len(subset):4} | 的中{hits:2} ({hit_rate:5.1%}) | ROI{roi:6.1%} {status}")
    
    def run(self):
        """メイン実行"""
        base_data, races_df, returns_df = self.load_data()
        
        logging.info("\n予測生成中...")
        pred_df = self.generate_predictions(base_data, races_df)
        
        results_df = self.analyze_longshot_walkforward(pred_df, returns_df)
        
        self.find_additional_filters(pred_df, returns_df)
        
        logging.info("\n" + "=" * 60)
        logging.info("【結論】")
        logging.info("=" * 60)
        
        if len(results_df) > 0:
            avg_roi = results_df['roi'].mean()
            if avg_roi >= 1.03:
                logging.info("  ✅ 大穴戦略が安定してROI 103%達成！")
            elif avg_roi >= 0.9:
                logging.info("  ⚠️ 大穴戦略は有望だがさらなる調整必要")
            else:
                logging.info("  ❌ 大穴戦略は安定せず")
        
        return results_df


if __name__ == "__main__":
    analyzer = LongShotEdgeAnalyzer()
    analyzer.run()

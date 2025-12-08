#!/usr/bin/env python3
"""
Phase 6: 複勝拡張分析

【計画書】
> Phase 6: 複勝拡張
> - 複勝オッズ推定の検証
> - 複勝モデル
> - 単勝+複勝統合評価

【なぜ複勝が有望か】
1. 的中率が高い（3着以内で的中）→ サンプル数が増える
2. 計画書のEV閾値が低い（単勝1.05、複勝1.03）
3. ウォークフォワードでベット不足を解消できる可能性

【本スクリプト】
- v11.0予測Top3（3着以内予測）のROI分析
- 複勝配当データを使用した正確なROI計算
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


class FukushoAnalyzer:
    """複勝分析"""
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/fukusho')
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
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        returns_df['race_id'] = returns_df['race_id'].astype(str)
        returns_df['horse_number'] = pd.to_numeric(returns_df['horse_number'], errors='coerce')
        
        # 複勝配当の確認
        fukusho = returns_df[returns_df['bet_type'] == 'fukusho']
        logging.info(f"  複勝配当データ: {len(fukusho):,}件")
        
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
        
        # 確信度
        df['score_diff'] = df.groupby('race_id')['raw_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        
        # 結果マージ
        for col in ['finish_position', 'horse_number', 'win_odds']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'finish_position', 'horse_number', 'win_odds']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        return df
    
    def analyze_fukusho_basic(self, df, returns_df):
        """複勝基本分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【複勝基本分析（Test期間2024年）】")
        logging.info("=" * 60)
        
        fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']]
        
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        
        results = []
        
        # 予測順位別
        for rank in [1, 2, 3]:
            subset = test_df[test_df['rank_in_race'] == rank].copy()
            subset = subset.merge(
                fukusho.rename(columns={'payout': 'fukusho_payout'}),
                on=['race_id', 'horse_number'],
                how='left'
            )
            
            payout = subset['fukusho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['fukusho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  予測{rank}位 | ベット{len(subset):5} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
            
            results.append({
                'rank': rank,
                'n_bets': len(subset),
                'hit_rate': hit_rate,
                'roi': roi,
            })
        
        # Top3全体
        top3 = test_df[test_df['rank_in_race'] <= 3].copy()
        top3 = top3.merge(
            fukusho.rename(columns={'payout': 'fukusho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        payout = top3['fukusho_payout'].fillna(0).sum()
        roi = payout / (len(top3) * 100)
        hits = (top3['fukusho_payout'] > 0).sum()
        hit_rate = hits / len(top3)
        
        status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
        logging.info(f"\n  Top3全体 | ベット{len(top3):5} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
        
        return pd.DataFrame(results)
    
    def analyze_fukusho_with_filters(self, df, returns_df):
        """フィルタ付き複勝分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【複勝フィルタ分析（Test期間2024年）】")
        logging.info("=" * 60)
        
        fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']]
        
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        top1 = test_df[test_df['rank_in_race'] == 1].copy()
        top1 = top1.merge(
            fukusho.rename(columns={'payout': 'fukusho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        results = []
        
        # オッズ帯別
        logging.info("\n【オッズ帯別（予測1位の複勝）】")
        for odds_min, odds_max in [(1, 5), (5, 10), (10, 20), (20, 50)]:
            subset = top1[(top1['win_odds'] >= odds_min) & (top1['win_odds'] < odds_max)]
            
            if len(subset) < 30:
                continue
            
            payout = subset['fukusho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['fukusho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  {odds_min}-{odds_max}倍 | ベット{len(subset):4} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
            
            results.append({
                'filter': f'odds_{odds_min}_{odds_max}',
                'n_bets': len(subset),
                'hit_rate': hit_rate,
                'roi': roi,
            })
        
        # 確信度別
        logging.info("\n【確信度別（予測1位の複勝）】")
        score_diff_max = test_df['score_diff'].max()
        for conf_thresh in [0.0, 0.1, 0.2]:
            subset = top1[top1['score_diff'] >= conf_thresh * score_diff_max]
            
            if len(subset) < 30:
                continue
            
            payout = subset['fukusho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['fukusho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  Conf≥{conf_thresh:.1f} | ベット{len(subset):4} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
            
            results.append({
                'filter': f'conf_{conf_thresh}',
                'n_bets': len(subset),
                'hit_rate': hit_rate,
                'roi': roi,
            })
        
        # 複合条件：大穴 + 複勝
        logging.info("\n【大穴条件（20-50倍）の複勝】")
        longshot = top1[(top1['win_odds'] >= 20) & (top1['win_odds'] < 50)]
        
        if len(longshot) >= 30:
            payout = longshot['fukusho_payout'].fillna(0).sum()
            roi = payout / (len(longshot) * 100)
            hits = (longshot['fukusho_payout'] > 0).sum()
            hit_rate = hits / len(longshot)
            
            avg_payout = longshot[longshot['fukusho_payout'] > 0]['fukusho_payout'].mean() if hits > 0 else 0
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  大穴全体 | ベット{len(longshot):4} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
            logging.info(f"            平均配当{avg_payout:.0f}円")
        
        return pd.DataFrame(results)
    
    def run(self):
        base_data, races_df, returns_df = self.load_data()
        
        logging.info("\n予測生成中...")
        df = self.generate_predictions(base_data, races_df)
        
        basic_results = self.analyze_fukusho_basic(df, returns_df)
        filter_results = self.analyze_fukusho_with_filters(df, returns_df)
        
        # 結論
        logging.info("\n" + "=" * 60)
        logging.info("【結論】")
        logging.info("=" * 60)
        
        all_results = pd.concat([basic_results, filter_results], ignore_index=True)
        
        if len(all_results) > 0 and 'roi' in all_results.columns:
            best = all_results.loc[all_results['roi'].idxmax()]
            logging.info(f"  最高複勝ROI: {best.get('roi', 0):.1%}")
            
            if best.get('roi', 0) >= 1.03:
                logging.info("  ✅ 複勝でROI 103%達成条件を発見！")
            else:
                logging.info("  ❌ 複勝でもROI 103%未達")
        
        return all_results


if __name__ == "__main__":
    analyzer = FukushoAnalyzer()
    analyzer.run()

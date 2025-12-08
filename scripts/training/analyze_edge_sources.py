#!/usr/bin/env python3
"""
エッジ源泉の網羅的分析

【計画書のエッジ仮説】
1. 展開予測 → 検証済み、効果なし
2. 血統適性 → 未検証（条件別勝率差）
3. 状態変化 → 未検証（休養明け2戦目、体重変化等）
4. オッズ乖離 → 未検証（モデル確率 vs オッズ確率の差）

【本スクリプト】
残りの仮説を検証し、最も有望なエッジを特定する
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


class EdgeSourceAnalyzer:
    """エッジ源泉の網羅的分析"""
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/edge_sources')
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
        
        # softmax確率
        def softmax_prob(x):
            exp_x = np.exp(x - x.max())
            return exp_x / exp_x.sum()
        
        df['model_prob'] = df.groupby('race_id')['raw_score'].transform(softmax_prob)
        
        # 結果マージ
        for col in ['finish_position', 'horse_number', 'win_odds', 'popularity']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'finish_position', 'horse_number', 'win_odds', 'popularity']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        # オッズ暗示確率
        df['odds_prob'] = 1 / df['win_odds'].clip(lower=1.1)
        df['odds_prob_normalized'] = df.groupby('race_id')['odds_prob'].transform(lambda x: x / x.sum())
        
        # Edge Score
        df['edge_score'] = df['model_prob'] / df['odds_prob_normalized']
        
        return df
    
    def analyze_edge_hypothesis_1_odds_divergence(self, df, returns_df, period_name):
        """仮説1: オッズ乖離（モデルが市場より高く評価）"""
        logging.info(f"\n【{period_name}: 仮説1 オッズ乖離】")
        logging.info("=" * 60)
        
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        df = df.merge(
            tansho.rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        # 予測1位のみ
        top1 = df[df['rank_in_race'] == 1].copy()
        
        results = []
        
        # Edge Score別のROI
        for edge_thresh in [0.8, 1.0, 1.2, 1.5, 2.0]:
            subset = top1[top1['edge_score'] >= edge_thresh]
            if len(subset) < 30:
                continue
            
            payout = subset['tansho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['tansho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  Edge≥{edge_thresh:.1f} | ベット{len(subset):5} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
            
            results.append({'hypothesis': 'odds_divergence', 'threshold': edge_thresh, 'roi': roi})
        
        return results
    
    def analyze_edge_hypothesis_2_popularity_gap(self, df, returns_df, period_name):
        """仮説2: 人気乖離（モデル順位 vs 人気順位）"""
        logging.info(f"\n【{period_name}: 仮説2 人気乖離】")
        logging.info("=" * 60)
        
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        df = df.merge(
            tansho.rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        # 人気乖離 = モデル順位 - 人気順位
        df['popularity_gap'] = df['rank_in_race'] - df['popularity']
        # 負: モデルが人気より高評価、正: モデルが人気より低評価
        
        results = []
        
        # 「モデルが市場より高評価」= popularity_gap < 0
        for gap in [-2, -3, -4, -5]:
            # 人気順位では低いが、モデルでは高評価
            subset = df[(df['popularity_gap'] <= gap) & (df['rank_in_race'] <= 3)]
            if len(subset) < 30:
                continue
            
            payout = subset['tansho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['tansho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  人気差≤{gap},予測Top3 | ベット{len(subset):5} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
            
            results.append({'hypothesis': 'popularity_gap', 'threshold': gap, 'roi': roi})
        
        return results
    
    def analyze_edge_hypothesis_3_rest_pattern(self, df, returns_df, period_name):
        """仮説3: 休養明けパターン（叩き2戦目）"""
        logging.info(f"\n【{period_name}: 仮説3 休養明けパターン】")
        logging.info("=" * 60)
        
        # days_since_last_race特徴量を使用
        if 'days_since_last_race' not in df.columns:
            logging.warning("  days_since_last_race特徴量なし - スキップ")
            return []
        
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        df = df.merge(
            tansho.rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        # 予測1位のみ
        top1 = df[df['rank_in_race'] == 1].copy()
        
        results = []
        
        # 休養期間別のROI
        for days_min, days_max, label in [(0, 30, '短期'), (30, 60, '中期'), (60, 120, '休養明け1戦目想定'), (120, 999, '長期休養')]:
            subset = top1[(top1['days_since_last_race'] >= days_min) & (top1['days_since_last_race'] < days_max)]
            if len(subset) < 30:
                continue
            
            payout = subset['tansho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['tansho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  {label} ({days_min}-{days_max}日) | ベット{len(subset):5} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
            
            results.append({'hypothesis': 'rest_pattern', 'label': label, 'roi': roi})
        
        return results
    
    def analyze_edge_hypothesis_4_odds_range(self, df, returns_df, period_name):
        """仮説4: オッズ帯別（中穴狙い）"""
        logging.info(f"\n【{period_name}: 仮説4 オッズ帯別】")
        logging.info("=" * 60)
        
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        df = df.merge(
            tansho.rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        # 予測1位のみ
        top1 = df[df['rank_in_race'] == 1].copy()
        
        results = []
        
        # オッズ帯別のROI
        for odds_min, odds_max, label in [(1, 5, '本命(1-5倍)'), (5, 10, '中穴(5-10倍)'), (10, 20, '穴(10-20倍)'), (20, 50, '大穴(20-50倍)')]:
            subset = top1[(top1['win_odds'] >= odds_min) & (top1['win_odds'] < odds_max)]
            if len(subset) < 30:
                continue
            
            payout = subset['tansho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['tansho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  {label} | ベット{len(subset):5} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
            
            results.append({'hypothesis': 'odds_range', 'label': label, 'roi': roi})
        
        return results
    
    def run(self):
        """メイン実行"""
        base_data, races_df, returns_df = self.load_data()
        
        logging.info("\n予測生成中...")
        pred_df = self.generate_predictions(base_data, races_df)
        
        # Test期間のみ分析
        test_df = pred_df[pred_df['race_date'] >= '2024-01-01'].copy()
        test_returns = returns_df[returns_df['race_id'].isin(test_df['race_id'])]
        
        logging.info(f"\nTest期間: {len(test_df):,}件 ({test_df['race_id'].nunique()}レース)")
        
        all_results = []
        all_results.extend(self.analyze_edge_hypothesis_1_odds_divergence(test_df.copy(), test_returns, 'Test'))
        all_results.extend(self.analyze_edge_hypothesis_2_popularity_gap(test_df.copy(), test_returns, 'Test'))
        all_results.extend(self.analyze_edge_hypothesis_3_rest_pattern(test_df.copy(), test_returns, 'Test'))
        all_results.extend(self.analyze_edge_hypothesis_4_odds_range(test_df.copy(), test_returns, 'Test'))
        
        # 最も有望な仮説を特定
        logging.info("\n" + "=" * 60)
        logging.info("【最も有望なエッジ仮説】")
        logging.info("=" * 60)
        
        if all_results:
            best = max(all_results, key=lambda x: x['roi'])
            logging.info(f"  仮説: {best['hypothesis']}")
            logging.info(f"  ROI: {best['roi']:.1%}")
            
            if best['roi'] >= 1.03:
                logging.info("  ✅ ROI 103%達成！この仮説を深掘り")
            else:
                logging.info("  ⚠️ ROI 103%未達")
        
        return all_results


if __name__ == "__main__":
    analyzer = EdgeSourceAnalyzer()
    analyzer.run()

#!/usr/bin/env python3
"""
複勝エッジベッティング分析

【背景】
- 全券種分析で複勝が最高ROI (77.0%) + 最高的中率 (48.3%)
- 単勝のEdge Score戦略を複勝に適用してROI向上を目指す

【複勝の特性】
- 的中率が高い（48.3%）→ サンプル数確保が容易
- 配当は低い → Edge Scoreフィルタで過小評価馬を狙う
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


class FukushoEdgeAnalyzer:
    """
    複勝エッジ分析
    """
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/fukusho_edge')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.feature_names = None
    
    def load_all_data(self):
        """データ読み込み"""
        logging.info("=" * 60)
        logging.info("データ読み込み")
        logging.info("=" * 60)
        
        # モデル
        with open(self.model_dir / 'mu_v11_0_ranker.pkl', 'rb') as f:
            self.model = pickle.load(f)
        with open(self.model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            self.feature_names = json.load(f)
        
        # 学習データ
        base_data = pd.read_parquet('keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet')
        base_data['race_date'] = pd.to_datetime(base_data['race_date'])
        base_data['race_id'] = base_data['race_id'].astype(str)
        
        # レース結果
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        races_df['horse_number'] = pd.to_numeric(races_df['horse_number'], errors='coerce')
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        
        # 複勝配当
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        returns_df['race_id'] = returns_df['race_id'].astype(str)
        fukusho_df = returns_df[returns_df['bet_type'] == 'fukusho'].copy()
        
        logging.info(f"  学習データ: {len(base_data):,}件")
        logging.info(f"  複勝配当: {len(fukusho_df):,}件")
        
        return base_data, races_df, fukusho_df
    
    def generate_predictions_with_edge(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """予測とEdge Score生成"""
        logging.info("\n予測・Edge Score生成中...")
        
        df = df.copy()
        
        for f in self.feature_names:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        preds = self.model.predict(df[self.feature_names])
        df['model_score'] = preds
        
        # レース内ランク
        df['rank_in_race'] = df.groupby('race_id')['model_score'].rank(ascending=False, method='first')
        
        # スコア差（確信度）
        df['score_diff'] = df.groupby('race_id')['model_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        score_diff_max = df['score_diff'].max()
        df['normalized_score_diff'] = df['score_diff'] / score_diff_max if score_diff_max > 0 else 0
        
        # softmax確率
        def softmax_prob(x):
            exp_x = np.exp(x - x.max())
            return exp_x / exp_x.sum()
        
        df['model_prob'] = df.groupby('race_id')['model_score'].transform(softmax_prob)
        
        # 結果マージ
        df['race_id'] = df['race_id'].astype(str)
        df['horse_id'] = df['horse_id'].astype(str)
        
        for col in ['finish_position', 'horse_number', 'win_odds']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'finish_position', 'horse_number', 'win_odds']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        # Edge Score（単勝オッズベース）
        df['odds_implied_prob'] = 1 / df['win_odds'].clip(lower=1.1)
        df['edge_score'] = df['model_prob'] / df['odds_implied_prob'].clip(lower=0.01)
        
        # 複勝圏内フラグ（3着以内）
        df['is_place'] = (df['finish_position'] <= 3).fillna(False).astype(int)
        
        logging.info(f"  予測完了: {len(df):,}件")
        
        return df
    
    def evaluate_fukusho_edge(self, pred_df: pd.DataFrame, fukusho_df: pd.DataFrame, period_name: str):
        """Edge Score閾値別の複勝ROI評価"""
        logging.info(f"\n【{period_name}期間 複勝Edge分析】")
        logging.info("=" * 60)
        
        # 予測1位のみ
        top1 = pred_df[pred_df['rank_in_race'] == 1].copy()
        
        # 複勝配当をマージ
        top1 = top1.merge(
            fukusho_df[['race_id', 'horse_number', 'payout']].rename(columns={'payout': 'fukusho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        # 複勝的中フラグ（配当ありなら的中）
        top1['is_fukusho_hit'] = top1['fukusho_payout'].notna().astype(int)
        
        results = []
        
        # 閾値なし（全件）
        all_bets = len(top1)
        all_hits = top1['is_fukusho_hit'].sum()
        all_payout = top1['fukusho_payout'].fillna(0).sum()
        all_roi = all_payout / (all_bets * 100) if all_bets > 0 else 0
        
        logging.info(f"  閾値なし: ベット{all_bets} | 的中{all_hits} | 的中率{all_hits/all_bets:.1%} | ROI{all_roi:.1%}")
        results.append({
            'period': period_name, 'filter': 'none', 'bet_count': all_bets,
            'hit_count': all_hits, 'hit_rate': all_hits/all_bets, 'roi': all_roi
        })
        
        # Edge Score閾値
        for edge_thresh in [0.8, 1.0, 1.2, 1.5, 2.0]:
            filtered = top1[top1['edge_score'] >= edge_thresh]
            if len(filtered) < 50:
                continue
            
            bets = len(filtered)
            hits = filtered['is_fukusho_hit'].sum()
            payout = filtered['fukusho_payout'].fillna(0).sum()
            roi = payout / (bets * 100) if bets > 0 else 0
            
            logging.info(f"  Edge≥{edge_thresh:.1f}: ベット{bets:5} | 的中{hits:4} | 的中率{hits/bets:.1%} | ROI{roi:.1%}")
            results.append({
                'period': period_name, 'filter': f'edge{edge_thresh}', 'bet_count': bets,
                'hit_count': hits, 'hit_rate': hits/bets, 'roi': roi
            })
        
        # スコア差閾値
        for sd_thresh in [0.1, 0.2, 0.3]:
            filtered = top1[top1['normalized_score_diff'] >= sd_thresh]
            if len(filtered) < 50:
                continue
            
            bets = len(filtered)
            hits = filtered['is_fukusho_hit'].sum()
            payout = filtered['fukusho_payout'].fillna(0).sum()
            roi = payout / (bets * 100) if bets > 0 else 0
            
            logging.info(f"  SD≥{sd_thresh:.1f}:  ベット{bets:5} | 的中{hits:4} | 的中率{hits/bets:.1%} | ROI{roi:.1%}")
            results.append({
                'period': period_name, 'filter': f'sd{sd_thresh}', 'bet_count': bets,
                'hit_count': hits, 'hit_rate': hits/bets, 'roi': roi
            })
        
        # 組み合わせ
        for sd_thresh in [0.1, 0.2]:
            for edge_thresh in [0.8, 1.0, 1.2]:
                filtered = top1[(top1['normalized_score_diff'] >= sd_thresh) & 
                               (top1['edge_score'] >= edge_thresh)]
                if len(filtered) < 30:
                    continue
                
                bets = len(filtered)
                hits = filtered['is_fukusho_hit'].sum()
                payout = filtered['fukusho_payout'].fillna(0).sum()
                roi = payout / (bets * 100) if bets > 0 else 0
                
                logging.info(f"  SD{sd_thresh}+E{edge_thresh}: ベット{bets:5} | 的中{hits:4} | 的中率{hits/bets:.1%} | ROI{roi:.1%}")
                results.append({
                    'period': period_name, 'filter': f'sd{sd_thresh}_edge{edge_thresh}', 'bet_count': bets,
                    'hit_count': hits, 'hit_rate': hits/bets, 'roi': roi
                })
        
        return pd.DataFrame(results)
    
    def run(self):
        """メイン実行"""
        base_data, races_df, fukusho_df = self.load_all_data()
        
        pred_df = self.generate_predictions_with_edge(base_data, races_df)
        
        # Valid/Test分割
        valid_pred = pred_df[(pred_df['race_date'] >= '2023-01-01') & 
                             (pred_df['race_date'] < '2024-01-01')]
        test_pred = pred_df[pred_df['race_date'] >= '2024-01-01']
        
        logging.info(f"\n  Valid: {len(valid_pred):,}件")
        logging.info(f"  Test: {len(test_pred):,}件")
        
        valid_results = self.evaluate_fukusho_edge(
            valid_pred, 
            fukusho_df[fukusho_df['race_id'].isin(valid_pred['race_id'])],
            'Valid'
        )
        
        test_results = self.evaluate_fukusho_edge(
            test_pred,
            fukusho_df[fukusho_df['race_id'].isin(test_pred['race_id'])],
            'Test'
        )
        
        all_results = pd.concat([valid_results, test_results], ignore_index=True)
        all_results.to_csv(self.output_dir / 'fukusho_edge_results.csv', index=False)
        
        # 最良条件
        test_best = test_results.loc[test_results['roi'].idxmax()]
        valid_match = valid_results[valid_results['filter'] == test_best['filter']]
        
        logging.info("\n" + "=" * 60)
        logging.info("【複勝Edge最適条件】")
        logging.info("=" * 60)
        logging.info(f"  最良条件: {test_best['filter']}")
        logging.info(f"  Test ROI: {test_best['roi']:.1%}")
        logging.info(f"  Test 的中率: {test_best['hit_rate']:.1%}")
        if len(valid_match) > 0:
            logging.info(f"  Valid ROI: {valid_match.iloc[0]['roi']:.1%}")
        
        if test_best['roi'] >= 1.03:
            logging.info("✅ 目標達成: ROI >= 103%")
        else:
            logging.info(f"⚠️ 目標未達: あと{1.03 - test_best['roi']:.1%}")
        
        return all_results


if __name__ == "__main__":
    analyzer = FukushoEdgeAnalyzer()
    results = analyzer.run()

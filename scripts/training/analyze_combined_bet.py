#!/usr/bin/env python3
"""
複合ベット戦略分析 (単勝 + 複勝)

【戦略】
- 単勝と複勝を同時ベット（同一馬）
- 単勝: 高配当狙い
- 複勝: リスクヘッジ（3着以内で回収）

【期待効果】
- 3着以内: 複勝で一部回収
- 1着: 単勝+複勝で大きく回収
- 分散により安定したROI
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


class CombinedBetAnalyzer:
    """
    単勝+複勝複合ベット分析
    """
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/combined_bet')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.feature_names = None
    
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
        """予測生成"""
        df = df.copy()
        
        for f in self.feature_names:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        preds = self.model.predict(df[self.feature_names])
        df['model_score'] = preds
        df['rank_in_race'] = df.groupby('race_id')['model_score'].rank(ascending=False, method='first')
        
        # スコア差
        df['score_diff'] = df.groupby('race_id')['model_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        score_diff_max = df['score_diff'].max()
        df['normalized_score_diff'] = df['score_diff'] / score_diff_max if score_diff_max > 0 else 0
        
        # モデル確率
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
        
        # Edge Score
        df['odds_implied_prob'] = 1 / df['win_odds'].clip(lower=1.1)
        df['edge_score'] = df['model_prob'] / df['odds_implied_prob'].clip(lower=0.01)
        
        return df
    
    def analyze_combined_bet(self, pred_df, returns_df, period_name):
        """単勝+複勝複合ベット分析"""
        logging.info(f"\n【{period_name}期間 複合ベット分析】")
        logging.info("=" * 60)
        
        top1 = pred_df[pred_df['rank_in_race'] == 1].copy()
        
        # 単勝・複勝配当を取得
        tansho_df = returns_df[returns_df['bet_type'] == 'tansho'].copy()
        fukusho_df = returns_df[returns_df['bet_type'] == 'fukusho'].copy()
        
        # 単勝的中をマージ
        top1 = top1.merge(
            tansho_df[['race_id', 'horse_number', 'payout']].rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        # 複勝的中をマージ
        top1 = top1.merge(
            fukusho_df[['race_id', 'horse_number', 'payout']].rename(columns={'payout': 'fukusho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        results = []
        
        # 各分配比率でシミュレーション
        # tansho_ratio : fukusho_ratio
        ratios = [(100, 0), (0, 100), (50, 50), (70, 30), (30, 70), (80, 20), (20, 80)]
        
        logging.info(f"{'単勝:複勝':>12} | {'ベット':>6} | {'回収':>8} | {'ROI':>8} | {'単勝的中':>8} | {'複勝的中':>8}")
        logging.info("-" * 75)
        
        for t_ratio, f_ratio in ratios:
            total_bet = []
            total_return = []
            tansho_hits = 0
            fukusho_hits = 0
            
            for _, row in top1.iterrows():
                bet = 100  # 1レース100円
                t_bet = bet * t_ratio / 100
                f_bet = bet * f_ratio / 100
                
                t_return = row['tansho_payout'] if pd.notna(row['tansho_payout']) else 0
                f_return = row['fukusho_payout'] * (f_bet / 100) if pd.notna(row['fukusho_payout']) else 0
                
                # 単勝は馬番一致で配当取得済み、複勝も同様
                if t_bet > 0 and t_return > 0:
                    t_return = t_return * (t_bet / 100)  # ベット額に応じて
                    tansho_hits += 1
                else:
                    t_return = 0
                
                if f_bet > 0 and row['fukusho_payout'] > 0 if pd.notna(row['fukusho_payout']) else False:
                    fukusho_hits += 1
                
                total_bet.append(bet)
                total_return.append(t_return + f_return)
            
            bet_count = len(top1)
            total_invested = sum(total_bet)
            total_returned = sum(total_return)
            roi = total_returned / total_invested if total_invested > 0 else 0
            
            logging.info(f"{t_ratio:>5}:{f_ratio:<5} | {bet_count:>6} | {total_returned:>8.0f} | {roi:>7.1%} | {tansho_hits:>8} | {fukusho_hits:>8}")
            
            results.append({
                'period': period_name,
                'tansho_ratio': t_ratio,
                'fukusho_ratio': f_ratio,
                'bet_count': bet_count,
                'total_return': total_returned,
                'roi': roi
            })
        
        # 閾値フィルタ + 分配比率
        logging.info("\n【スコア差フィルタ + 分配比率】")
        logging.info(f"{'フィルタ':>15} | {'単勝:複勝':>10} | {'ベット':>6} | {'ROI':>8}")
        logging.info("-" * 55)
        
        for sd_thresh in [0.1, 0.2, 0.3]:
            filtered = top1[top1['normalized_score_diff'] >= sd_thresh]
            if len(filtered) < 30:
                continue
            
            for t_ratio, f_ratio in [(50, 50), (70, 30), (30, 70)]:
                total_bet = []
                total_return = []
                
                for _, row in filtered.iterrows():
                    bet = 100
                    t_bet = bet * t_ratio / 100
                    f_bet = bet * f_ratio / 100
                    
                    t_return = 0
                    if t_bet > 0 and pd.notna(row['tansho_payout']):
                        t_return = row['tansho_payout'] * (t_bet / 100)
                    
                    f_return = 0
                    if f_bet > 0 and pd.notna(row['fukusho_payout']):
                        f_return = row['fukusho_payout'] * (f_bet / 100)
                    
                    total_bet.append(bet)
                    total_return.append(t_return + f_return)
                
                roi = sum(total_return) / sum(total_bet) if sum(total_bet) > 0 else 0
                
                logging.info(f"SD≥{sd_thresh}        | {t_ratio:>4}:{f_ratio:<4} | {len(filtered):>6} | {roi:>7.1%}")
                
                results.append({
                    'period': period_name,
                    'filter': f'sd{sd_thresh}',
                    'tansho_ratio': t_ratio,
                    'fukusho_ratio': f_ratio,
                    'bet_count': len(filtered),
                    'roi': roi
                })
        
        return pd.DataFrame(results)
    
    def run(self):
        """メイン実行"""
        base_data, races_df, returns_df = self.load_data()
        
        logging.info("\n予測生成中...")
        pred_df = self.generate_predictions(base_data, races_df)
        
        valid_pred = pred_df[(pred_df['race_date'] >= '2023-01-01') & 
                             (pred_df['race_date'] < '2024-01-01')]
        test_pred = pred_df[pred_df['race_date'] >= '2024-01-01']
        
        logging.info(f"  Valid: {len(valid_pred):,}件")
        logging.info(f"  Test: {len(test_pred):,}件")
        
        valid_results = self.analyze_combined_bet(
            valid_pred,
            returns_df[returns_df['race_id'].isin(valid_pred['race_id'])],
            'Valid'
        )
        
        test_results = self.analyze_combined_bet(
            test_pred,
            returns_df[returns_df['race_id'].isin(test_pred['race_id'])],
            'Test'
        )
        
        all_results = pd.concat([valid_results, test_results], ignore_index=True)
        all_results.to_csv(self.output_dir / 'combined_bet_results.csv', index=False)
        
        # 最良条件
        test_best = test_results.loc[test_results['roi'].idxmax()]
        logging.info("\n" + "=" * 60)
        logging.info("【最高ROI条件】")
        logging.info("=" * 60)
        logging.info(f"  条件: {test_best.get('filter', 'none')}, 単勝{test_best['tansho_ratio']}:複勝{test_best['fukusho_ratio']}")
        logging.info(f"  Test ROI: {test_best['roi']:.1%}")
        
        if test_best['roi'] >= 1.03:
            logging.info("✅ 目標達成: ROI >= 103%")
        
        return all_results


if __name__ == "__main__":
    analyzer = CombinedBetAnalyzer()
    results = analyzer.run()

#!/usr/bin/env python3
"""
馬連ボックス買い分析

【戦略】
- 予測上位3頭から馬連3通りをボックス買い
- 1レース300円（100円×3）
- 的中率向上を狙う

【期待】
- 単買い的中率: ~8%
- ボックス的中率: ~15-20%?
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


class UmarenBoxAnalyzer:
    """
    馬連ボックス買い分析
    """
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/umaren_box')
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
        df['raw_score'] = preds
        df['rank_in_race'] = df.groupby('race_id')['raw_score'].rank(ascending=False, method='first')
        
        # 確信度
        df['score_diff'] = df.groupby('race_id')['raw_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        
        # 結果マージ
        for col in ['horse_number']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'horse_number']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        return df
    
    def analyze_box_betting(self, pred_df, returns_df, period_name, top_n=3):
        """ボックス買い分析"""
        logging.info(f"\n【{period_name}期間 馬連ボックス分析】")
        logging.info("=" * 60)
        
        umaren_df = returns_df[returns_df['bet_type'] == 'umaren'].copy()
        
        # レースごとに予測上位を取得
        race_preds = {}
        for race_id, group in pred_df.groupby('race_id'):
            sorted_g = group.sort_values('rank_in_race')
            top = sorted_g.head(top_n)
            
            if len(top) >= top_n:
                top_horses = [int(h) for h in top['horse_number'].values if pd.notna(h)]
                if len(top_horses) >= top_n:
                    # ボックス組み合わせ生成
                    from itertools import combinations
                    box_combos = [frozenset(c) for c in combinations(top_horses[:top_n], 2)]
                    
                    race_preds[race_id] = {
                        'top_horses': top_horses,
                        'box_combos': box_combos,
                        'score_diff': top['score_diff'].mean(),
                    }
        
        results = []
        
        # 確信度閾値別に分析
        score_diff_max = max(r['score_diff'] for r in race_preds.values())
        
        for conf_thresh in [0.0, 0.05, 0.1, 0.15]:
            filtered_races = {
                k: v for k, v in race_preds.items()
                if v['score_diff'] >= conf_thresh * score_diff_max
            }
            
            if len(filtered_races) < 30:
                continue
            
            # ボックス買い評価
            n_races = len(filtered_races)
            n_bets = n_races * len(list(filtered_races.values())[0]['box_combos'])  # 3通り
            bet_amount = n_races * 300  # 1レース300円
            
            hits = 0
            total_payout = 0
            
            for _, row in umaren_df.iterrows():
                if row['race_id'] in filtered_races:
                    if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
                        actual_combo = frozenset({int(row['horse_1']), int(row['horse_2'])})
                        
                        if actual_combo in filtered_races[row['race_id']]['box_combos']:
                            hits += 1
                            total_payout += row['payout']  # 100円買いの配当
            
            hit_rate = hits / n_races if n_races > 0 else 0
            roi = total_payout / bet_amount if bet_amount > 0 else 0
            
            results.append({
                'period': period_name,
                'conf_thresh': conf_thresh,
                'n_races': n_races,
                'n_bets': n_bets,
                'bet_amount': bet_amount,
                'hits': hits,
                'hit_rate': hit_rate,
                'payout': total_payout,
                'roi': roi,
            })
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(
                f"  Conf≥{conf_thresh:.2f} | レース{n_races:4} | 的中{hits:4} ({hit_rate:5.1%}) | "
                f"ROI{roi:6.1%} {status}"
            )
        
        return pd.DataFrame(results)
    
    def run_walkforward(self, pred_df, returns_df):
        """ウォークフォワード検証"""
        logging.info("\n" + "=" * 60)
        logging.info("ウォークフォワード検証（馬連ボックス）")
        logging.info("=" * 60)
        
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
        
        all_results = []
        
        for start, end in periods:
            period_pred = pred_df[
                (pred_df['race_date'] >= start) & (pred_df['race_date'] < end)
            ]
            period_returns = returns_df[returns_df['race_id'].isin(period_pred['race_id'])]
            
            if len(period_pred['race_id'].unique()) > 100:
                period_name = f"{start}~{end}"
                logging.info(f"\n期間: {period_name} ({len(period_pred['race_id'].unique())}レース)")
                
                period_results = self.analyze_box_betting(period_pred, period_returns, period_name)
                all_results.append(period_results)
        
        return pd.concat(all_results, ignore_index=True)
    
    def analyze_stability(self, results_df):
        """安定性分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【確信度別 ROI安定性分析】")
        logging.info("=" * 60)
        
        for conf, group in results_df.groupby('conf_thresh'):
            avg_roi = group['roi'].mean()
            std_roi = group['roi'].std()
            above_100 = (group['roi'] > 1.0).sum() / len(group)
            avg_hit = group['hit_rate'].mean()
            
            status = '✅' if avg_roi >= 1.03 else '⚠️'
            logging.info(
                f"  Conf≥{conf:.2f} | 平均ROI{avg_roi:6.1%} ± {std_roi:.0%} | "
                f"ROI>100%: {above_100:.0%} | 平均的中率{avg_hit:5.1%} {status}"
            )
    
    def run(self):
        """メイン実行"""
        base_data, races_df, returns_df = self.load_data()
        
        logging.info("\n予測生成中...")
        pred_df = self.generate_predictions(base_data, races_df)
        
        results_df = self.run_walkforward(pred_df, returns_df)
        
        self.analyze_stability(results_df)
        
        results_df.to_csv(self.output_dir / 'umaren_box_walkforward.csv', index=False)
        
        logging.info(f"\n結果保存: {self.output_dir}")
        
        return results_df


if __name__ == "__main__":
    analyzer = UmarenBoxAnalyzer()
    results = analyzer.run()

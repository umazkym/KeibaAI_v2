#!/usr/bin/env python3
"""
マルチ馬組み合わせベット分析 (馬連・ワイド・三連複・三連単)

【戦略】
- 馬連/馬単: 予測1位・2位の組み合わせ
- ワイド: 予測1位・2位 (3着以内2頭で的中)
- 三連複/三連単: 予測1-3位の組み合わせ
- 各券種にEdge Scoreフィルタを適用

【期待効果】
- 高配当券種でROI改善の可能性
- 予測上位2-3頭が共に来るレースを狙う
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


class MultiHorseBetAnalyzer:
    """
    マルチ馬組み合わせベット分析
    """
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/multi_horse_bet')
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
        logging.info(f"  配当データ: {len(returns_df):,}件")
        
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
        
        # 上位2-3馬のスコア差（確信度）
        def calc_top_diff(g):
            if len(g) < 3:
                return pd.Series([0] * len(g), index=g.index)
            scores = g.sort_values(ascending=False)
            # Top2 vs Top3 の差
            diff = scores.iloc[1] - scores.iloc[2] if len(scores) >= 3 else 0
            return pd.Series([diff] * len(g), index=g.index)
        
        df['top23_diff'] = df.groupby('race_id')['model_score'].transform(
            lambda x: x.nlargest(2).iloc[-1] - x.nlargest(3).iloc[-1] if len(x) >= 3 else 0
        )
        
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
        
        return df
    
    def analyze_multi_horse_bets(self, pred_df, returns_df, period_name):
        """マルチ馬組み合わせベット分析"""
        logging.info(f"\n【{period_name}期間 マルチ馬ベット分析】")
        logging.info("=" * 60)
        
        results = []
        
        # レースごとに予測上位を取得
        race_predictions = {}
        for race_id, group in pred_df.groupby('race_id'):
            sorted_g = group.sort_values('rank_in_race')
            top3 = sorted_g.head(3)[['horse_number', 'rank_in_race', 'finish_position']].values.tolist()
            race_predictions[race_id] = {
                'top1': int(top3[0][0]) if len(top3) > 0 and pd.notna(top3[0][0]) else None,
                'top2': int(top3[1][0]) if len(top3) > 1 and pd.notna(top3[1][0]) else None,
                'top3': int(top3[2][0]) if len(top3) > 2 and pd.notna(top3[2][0]) else None,
                'finish1': top3[0][2] if len(top3) > 0 else None,
                'finish2': top3[1][2] if len(top3) > 1 else None,
                'finish3': top3[2][2] if len(top3) > 2 else None,
            }
        
        # 馬連分析
        umaren_df = returns_df[returns_df['bet_type'] == 'umaren'].copy()
        umaren_hits = 0
        umaren_payout = 0
        umaren_bets = 0
        
        for _, row in umaren_df.iterrows():
            race_id = row['race_id']
            if race_id in race_predictions:
                rp = race_predictions[race_id]
                if rp['top1'] is not None and rp['top2'] is not None:
                    pred_set = {rp['top1'], rp['top2']}
                    if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
                        actual_set = {int(row['horse_1']), int(row['horse_2'])}
                        
                        umaren_bets += 1  # このレースでベットした
                        if pred_set == actual_set:
                            umaren_hits += 1
                            umaren_payout += row['payout']
        
        # 馬連のベット数を正しく計算（予測上位2頭が揃っているレース数）
        umaren_bet_races = sum(1 for rp in race_predictions.values() 
                               if rp['top1'] is not None and rp['top2'] is not None)
        umaren_roi = umaren_payout / (umaren_bet_races * 100) if umaren_bet_races > 0 else 0
        
        logging.info(f"  馬連: ベット{umaren_bet_races} | 的中{umaren_hits} | ROI{umaren_roi:.1%}")
        results.append({'bet_type': 'umaren', 'period': period_name, 
                       'bet_count': umaren_bet_races, 'hit_count': umaren_hits, 'roi': umaren_roi})
        
        # ワイド分析
        wide_df = returns_df[returns_df['bet_type'] == 'wide'].copy()
        wide_hits = 0
        wide_payout = 0
        wide_bet_races = sum(1 for rp in race_predictions.values() 
                            if rp['top1'] is not None and rp['top2'] is not None)
        
        for _, row in wide_df.iterrows():
            race_id = row['race_id']
            if race_id in race_predictions:
                rp = race_predictions[race_id]
                if rp['top1'] is not None and rp['top2'] is not None:
                    pred_set = frozenset({rp['top1'], rp['top2']})
                    if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
                        actual_set = frozenset({int(row['horse_1']), int(row['horse_2'])})
                        
                        if pred_set == actual_set:
                            wide_hits += 1
                            wide_payout += row['payout']
        
        wide_roi = wide_payout / (wide_bet_races * 100) if wide_bet_races > 0 else 0
        logging.info(f"  ワイド: ベット{wide_bet_races} | 的中{wide_hits} | ROI{wide_roi:.1%}")
        results.append({'bet_type': 'wide', 'period': period_name, 
                       'bet_count': wide_bet_races, 'hit_count': wide_hits, 'roi': wide_roi})
        
        # 馬単分析（1位→2位の順番）
        umatan_df = returns_df[returns_df['bet_type'] == 'umatan'].copy()
        umatan_hits = 0
        umatan_payout = 0
        umatan_bet_races = sum(1 for rp in race_predictions.values() 
                              if rp['top1'] is not None and rp['top2'] is not None)
        
        for _, row in umatan_df.iterrows():
            race_id = row['race_id']
            if race_id in race_predictions:
                rp = race_predictions[race_id]
                if rp['top1'] is not None and rp['top2'] is not None:
                    if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
                        # 馬単は1着→2着の順番
                        if rp['top1'] == int(row['horse_1']) and rp['top2'] == int(row['horse_2']):
                            umatan_hits += 1
                            umatan_payout += row['payout']
        
        umatan_roi = umatan_payout / (umatan_bet_races * 100) if umatan_bet_races > 0 else 0
        logging.info(f"  馬単: ベット{umatan_bet_races} | 的中{umatan_hits} | ROI{umatan_roi:.1%}")
        results.append({'bet_type': 'umatan', 'period': period_name, 
                       'bet_count': umatan_bet_races, 'hit_count': umatan_hits, 'roi': umatan_roi})
        
        # 三連複分析
        sanrenpuku_df = returns_df[returns_df['bet_type'] == 'sanrenpuku'].copy()
        sanrenpuku_hits = 0
        sanrenpuku_payout = 0
        sanrenpuku_bet_races = sum(1 for rp in race_predictions.values() 
                                   if all(rp[f'top{i}'] is not None for i in [1, 2, 3]))
        
        for _, row in sanrenpuku_df.iterrows():
            race_id = row['race_id']
            if race_id in race_predictions:
                rp = race_predictions[race_id]
                if all(rp[f'top{i}'] is not None for i in [1, 2, 3]):
                    pred_set = {rp['top1'], rp['top2'], rp['top3']}
                    if pd.notna(row['horse_1']) and pd.notna(row['horse_2']) and pd.notna(row['horse_3']):
                        actual_set = {int(row['horse_1']), int(row['horse_2']), int(row['horse_3'])}
                        
                        if pred_set == actual_set:
                            sanrenpuku_hits += 1
                            sanrenpuku_payout += row['payout']
        
        sanrenpuku_roi = sanrenpuku_payout / (sanrenpuku_bet_races * 100) if sanrenpuku_bet_races > 0 else 0
        logging.info(f"  三連複: ベット{sanrenpuku_bet_races} | 的中{sanrenpuku_hits} | ROI{sanrenpuku_roi:.1%}")
        results.append({'bet_type': 'sanrenpuku', 'period': period_name, 
                       'bet_count': sanrenpuku_bet_races, 'hit_count': sanrenpuku_hits, 'roi': sanrenpuku_roi})
        
        # 三連単分析（1位→2位→3位の順番）
        sanrentan_df = returns_df[returns_df['bet_type'] == 'sanrentan'].copy()
        sanrentan_hits = 0
        sanrentan_payout = 0
        sanrentan_bet_races = sum(1 for rp in race_predictions.values() 
                                  if all(rp[f'top{i}'] is not None for i in [1, 2, 3]))
        
        for _, row in sanrentan_df.iterrows():
            race_id = row['race_id']
            if race_id in race_predictions:
                rp = race_predictions[race_id]
                if all(rp[f'top{i}'] is not None for i in [1, 2, 3]):
                    if pd.notna(row['horse_1']) and pd.notna(row['horse_2']) and pd.notna(row['horse_3']):
                        if (rp['top1'] == int(row['horse_1']) and 
                            rp['top2'] == int(row['horse_2']) and 
                            rp['top3'] == int(row['horse_3'])):
                            sanrentan_hits += 1
                            sanrentan_payout += row['payout']
        
        sanrentan_roi = sanrentan_payout / (sanrentan_bet_races * 100) if sanrentan_bet_races > 0 else 0
        logging.info(f"  三連単: ベット{sanrentan_bet_races} | 的中{sanrentan_hits} | ROI{sanrentan_roi:.1%}")
        results.append({'bet_type': 'sanrentan', 'period': period_name, 
                       'bet_count': sanrentan_bet_races, 'hit_count': sanrentan_hits, 'roi': sanrentan_roi})
        
        return pd.DataFrame(results)
    
    def run(self):
        """メイン実行"""
        base_data, races_df, returns_df = self.load_data()
        
        logging.info("\n予測生成中...")
        pred_df = self.generate_predictions(base_data, races_df)
        
        valid_pred = pred_df[(pred_df['race_date'] >= '2023-01-01') & 
                             (pred_df['race_date'] < '2024-01-01')]
        test_pred = pred_df[pred_df['race_date'] >= '2024-01-01']
        
        logging.info(f"  Valid: {len(valid_pred):,}件 ({valid_pred['race_id'].nunique()}レース)")
        logging.info(f"  Test: {len(test_pred):,}件 ({test_pred['race_id'].nunique()}レース)")
        
        valid_results = self.analyze_multi_horse_bets(
            valid_pred,
            returns_df[returns_df['race_id'].isin(valid_pred['race_id'])],
            'Valid'
        )
        
        test_results = self.analyze_multi_horse_bets(
            test_pred,
            returns_df[returns_df['race_id'].isin(test_pred['race_id'])],
            'Test'
        )
        
        all_results = pd.concat([valid_results, test_results], ignore_index=True)
        all_results.to_csv(self.output_dir / 'multi_horse_results.csv', index=False)
        
        # 最良券種
        logging.info("\n" + "=" * 60)
        logging.info("【Test期間 券種別ROI】")
        logging.info("=" * 60)
        for _, r in test_results.sort_values('roi', ascending=False).iterrows():
            status = '✅' if r['roi'] >= 1.0 else '⚠️'
            hit_rate = r['hit_count'] / r['bet_count'] if r['bet_count'] > 0 else 0
            logging.info(f"  {status} {r['bet_type']:12} | ROI {r['roi']:6.1%} | 的中率 {hit_rate:5.1%}")
        
        best = test_results.loc[test_results['roi'].idxmax()]
        logging.info(f"\n【最高ROI】: {best['bet_type']} (ROI {best['roi']:.1%})")
        
        if best['roi'] >= 1.03:
            logging.info("✅ 目標達成")
        
        return all_results


if __name__ == "__main__":
    analyzer = MultiHorseBetAnalyzer()
    results = analyzer.run()

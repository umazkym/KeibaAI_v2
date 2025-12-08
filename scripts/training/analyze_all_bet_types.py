#!/usr/bin/env python3
"""
Phase 6: 全券種ROI分析スクリプト

【目的】
単勝に限らず、以下の全券種でEdge Score戦略のROIを検証する：
- 単勝 (tansho)
- 複勝 (fukusho) 
- 馬連 (umaren)
- ワイド (wide)
- 馬単 (umatan)
- 三連複 (sanrenpuku)
- 三連単 (sanrentan)

【データ】
returns.parquet: 実際の配当データ（240,333レコード）

【戦略】
1. 単勝・複勝: モデル予測1位の馬をベット
2. 馬連・ワイド: モデル予測1位・2位の組み合わせ
3. 馬単: 1位→2位の順番
4. 三連複・三連単: 1位・2位・3位の組み合わせ
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


class MultiBetTypeAnalyzer:
    """
    全券種ROI分析
    """
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/multi_bet_analysis')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.feature_names = None
    
    def load_all_data(self):
        """全データ読み込み"""
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
        base_data['horse_id'] = base_data['horse_id'].astype(str)
        
        # レース結果
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        races_df['horse_number'] = pd.to_numeric(races_df['horse_number'], errors='coerce')
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        
        # 配当データ
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        returns_df['race_id'] = returns_df['race_id'].astype(str)
        
        logging.info(f"  学習データ: {len(base_data):,}件")
        logging.info(f"  レース結果: {len(races_df):,}件")
        logging.info(f"  配当データ: {len(returns_df):,}件")
        
        # 券種別レコード数
        bet_counts = returns_df['bet_type'].value_counts()
        logging.info("\n  券種別レコード数:")
        for bet_type, count in bet_counts.items():
            logging.info(f"    {bet_type}: {count:,}")
        
        return base_data, races_df, returns_df
    
    def generate_predictions(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """予測生成"""
        logging.info("\n予測生成中...")
        
        df = df.copy()
        
        for f in self.feature_names:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        preds = self.model.predict(df[self.feature_names])
        df['model_score'] = preds
        
        # レース内ランク（1=最高スコア）
        df['rank_in_race'] = df.groupby('race_id')['model_score'].rank(ascending=False, method='first')
        
        # 結果マージ
        for col in ['finish_position', 'horse_number']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'finish_position', 'horse_number']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        logging.info(f"  予測完了: {len(df):,}件")
        
        return df
    
    def analyze_tansho(self, pred_df: pd.DataFrame, returns_df: pd.DataFrame, period_name: str) -> dict:
        """単勝分析"""
        # 予測1位を選択
        top1 = pred_df[pred_df['rank_in_race'] == 1].copy()
        
        # 単勝配当を取得
        tansho = returns_df[returns_df['bet_type'] == 'tansho'].copy()
        tansho = tansho.rename(columns={'horse_number': 'winning_horse'})
        
        # マージ
        merged = top1.merge(
            tansho[['race_id', 'winning_horse', 'payout']],
            left_on=['race_id', 'horse_number'],
            right_on=['race_id', 'winning_horse'],
            how='inner'
        )
        
        if len(merged) == 0:
            return {'bet_type': 'tansho', 'period': period_name, 'bet_count': 0, 'roi': 0}
        
        bet_count = len(top1)
        win_count = len(merged)
        total_payout = merged['payout'].sum()
        roi = total_payout / (bet_count * 100)  # 100円単位
        
        return {
            'bet_type': 'tansho',
            'period': period_name,
            'bet_count': bet_count,
            'win_count': win_count,
            'hit_rate': win_count / bet_count,
            'avg_payout': total_payout / win_count if win_count > 0 else 0,
            'roi': roi
        }
    
    def analyze_fukusho(self, pred_df: pd.DataFrame, returns_df: pd.DataFrame, period_name: str) -> dict:
        """複勝分析"""
        # 予測1位を選択
        top1 = pred_df[pred_df['rank_in_race'] == 1].copy()
        
        # 複勝配当を取得
        fukusho = returns_df[returns_df['bet_type'] == 'fukusho'].copy()
        
        # マージ（複勝は3着以内）
        merged = top1.merge(
            fukusho[['race_id', 'horse_number', 'payout']],
            on=['race_id', 'horse_number'],
            how='inner'
        )
        
        bet_count = len(top1)
        win_count = len(merged)
        total_payout = merged['payout'].sum() if len(merged) > 0 else 0
        roi = total_payout / (bet_count * 100) if bet_count > 0 else 0
        
        return {
            'bet_type': 'fukusho',
            'period': period_name,
            'bet_count': bet_count,
            'win_count': win_count,
            'hit_rate': win_count / bet_count if bet_count > 0 else 0,
            'avg_payout': total_payout / win_count if win_count > 0 else 0,
            'roi': roi
        }
    
    def analyze_umaren(self, pred_df: pd.DataFrame, returns_df: pd.DataFrame, period_name: str) -> dict:
        """馬連分析（予測1位・2位の組み合わせ）"""
        # 予測1位・2位を取得
        top2 = pred_df[pred_df['rank_in_race'].isin([1, 2])].copy()
        
        # レースごとに1位・2位の馬番を取得
        race_top2 = top2.groupby('race_id').apply(
            lambda g: set(g['horse_number'].dropna().astype(int).tolist()) if len(g) >= 2 else set()
        ).to_dict()
        
        # 馬連配当
        umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
        
        results = []
        for _, row in umaren.iterrows():
            race_id = row['race_id']
            if race_id in race_top2:
                pred_set = race_top2[race_id]
                actual_set = {int(row['horse_1']), int(row['horse_2'])} if pd.notna(row['horse_1']) and pd.notna(row['horse_2']) else set()
                
                if pred_set == actual_set and len(pred_set) == 2:
                    results.append({'race_id': race_id, 'payout': row['payout']})
        
        bet_count = len([r for r in race_top2.values() if len(r) == 2])
        win_count = len(results)
        total_payout = sum(r['payout'] for r in results)
        roi = total_payout / (bet_count * 100) if bet_count > 0 else 0
        
        return {
            'bet_type': 'umaren',
            'period': period_name,
            'bet_count': bet_count,
            'win_count': win_count,
            'hit_rate': win_count / bet_count if bet_count > 0 else 0,
            'avg_payout': total_payout / win_count if win_count > 0 else 0,
            'roi': roi
        }
    
    def analyze_wide(self, pred_df: pd.DataFrame, returns_df: pd.DataFrame, period_name: str) -> dict:
        """ワイド分析（予測1位・2位の組み合わせ、3着以内で的中）"""
        # 予測1位・2位を取得
        top2 = pred_df[pred_df['rank_in_race'].isin([1, 2])].copy()
        
        # レースごとに1位・2位の馬番を取得
        race_top2 = top2.groupby('race_id').apply(
            lambda g: tuple(sorted(g['horse_number'].dropna().astype(int).tolist()[:2])) if len(g) >= 2 else ()
        ).to_dict()
        
        # ワイド配当
        wide = returns_df[returns_df['bet_type'] == 'wide'].copy()
        
        results = []
        for _, row in wide.iterrows():
            race_id = row['race_id']
            if race_id in race_top2 and len(race_top2[race_id]) == 2:
                pred_tuple = race_top2[race_id]
                if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
                    actual_tuple = tuple(sorted([int(row['horse_1']), int(row['horse_2'])]))
                    
                    if pred_tuple == actual_tuple:
                        results.append({'race_id': race_id, 'payout': row['payout']})
        
        bet_count = len([r for r in race_top2.values() if len(r) == 2])
        win_count = len(results)
        total_payout = sum(r['payout'] for r in results)
        roi = total_payout / (bet_count * 100) if bet_count > 0 else 0
        
        return {
            'bet_type': 'wide',
            'period': period_name,
            'bet_count': bet_count,
            'win_count': win_count,
            'hit_rate': win_count / bet_count if bet_count > 0 else 0,
            'avg_payout': total_payout / win_count if win_count > 0 else 0,
            'roi': roi
        }
    
    def analyze_sanrenpuku(self, pred_df: pd.DataFrame, returns_df: pd.DataFrame, period_name: str) -> dict:
        """三連複分析（予測1-3位の組み合わせ）"""
        # 予測1-3位を取得
        top3 = pred_df[pred_df['rank_in_race'].isin([1, 2, 3])].copy()
        
        # レースごとに1-3位の馬番を取得
        race_top3 = top3.groupby('race_id').apply(
            lambda g: set(g['horse_number'].dropna().astype(int).tolist()[:3]) if len(g) >= 3 else set()
        ).to_dict()
        
        # 三連複配当
        sanrenpuku = returns_df[returns_df['bet_type'] == 'sanrenpuku'].copy()
        
        results = []
        for _, row in sanrenpuku.iterrows():
            race_id = row['race_id']
            if race_id in race_top3:
                pred_set = race_top3[race_id]
                if pd.notna(row['horse_1']) and pd.notna(row['horse_2']) and pd.notna(row['horse_3']):
                    actual_set = {int(row['horse_1']), int(row['horse_2']), int(row['horse_3'])}
                    
                    if pred_set == actual_set and len(pred_set) == 3:
                        results.append({'race_id': race_id, 'payout': row['payout']})
        
        bet_count = len([r for r in race_top3.values() if len(r) == 3])
        win_count = len(results)
        total_payout = sum(r['payout'] for r in results)
        roi = total_payout / (bet_count * 100) if bet_count > 0 else 0
        
        return {
            'bet_type': 'sanrenpuku',
            'period': period_name,
            'bet_count': bet_count,
            'win_count': win_count,
            'hit_rate': win_count / bet_count if bet_count > 0 else 0,
            'avg_payout': total_payout / win_count if win_count > 0 else 0,
            'roi': roi
        }
    
    def run(self):
        """メイン実行"""
        # データ読み込み
        base_data, races_df, returns_df = self.load_all_data()
        
        # 予測生成
        pred_df = self.generate_predictions(base_data, races_df)
        
        # Valid/Test分割
        valid_pred = pred_df[(pred_df['race_date'] >= '2023-01-01') & 
                             (pred_df['race_date'] < '2024-01-01')]
        test_pred = pred_df[pred_df['race_date'] >= '2024-01-01']
        
        logging.info(f"\n  Valid: {len(valid_pred):,}件 ({valid_pred['race_id'].nunique()}レース)")
        logging.info(f"  Test: {len(test_pred):,}件 ({test_pred['race_id'].nunique()}レース)")
        
        all_results = []
        
        for period_name, pred, ret in [
            ('Valid', valid_pred, returns_df[returns_df['race_id'].isin(valid_pred['race_id'])]),
            ('Test', test_pred, returns_df[returns_df['race_id'].isin(test_pred['race_id'])])
        ]:
            logging.info(f"\n【{period_name}期間分析】")
            logging.info("=" * 60)
            
            # 各券種分析
            for analyze_func, name in [
                (self.analyze_tansho, '単勝'),
                (self.analyze_fukusho, '複勝'),
                (self.analyze_umaren, '馬連'),
                (self.analyze_wide, 'ワイド'),
                (self.analyze_sanrenpuku, '三連複'),
            ]:
                result = analyze_func(pred, ret, period_name)
                all_results.append(result)
                
                logging.info(f"  {name:6} | ベット{result['bet_count']:5} | 的中{result['win_count']:4} | "
                           f"的中率{result['hit_rate']:5.1%} | ROI{result['roi']:6.1%}")
        
        # 結果保存
        results_df = pd.DataFrame(all_results)
        results_df.to_csv(self.output_dir / 'multi_bet_results.csv', index=False)
        
        # 最良券種特定
        logging.info("\n" + "=" * 60)
        logging.info("【券種別ROI比較】")
        logging.info("=" * 60)
        
        test_results = results_df[results_df['period'] == 'Test']
        test_results = test_results.sort_values('roi', ascending=False)
        
        for _, r in test_results.iterrows():
            status = '✅' if r['roi'] >= 1.0 else '⚠️'
            logging.info(f"  {status} {r['bet_type']:12} | ROI {r['roi']:6.1%} | 的中率 {r['hit_rate']:5.1%}")
        
        best = test_results.iloc[0] if len(test_results) > 0 else None
        if best is not None:
            logging.info(f"\n【最高ROI券種】: {best['bet_type']} (ROI {best['roi']:.1%})")
            if best['roi'] >= 1.03:
                logging.info("✅ 目標達成: ROI >= 103%")
        
        logging.info(f"\n結果保存: {self.output_dir}")
        
        return results_df


if __name__ == "__main__":
    analyzer = MultiBetTypeAnalyzer()
    results_df = analyzer.run()

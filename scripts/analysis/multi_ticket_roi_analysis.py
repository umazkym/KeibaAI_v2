#!/usr/bin/env python3
"""
他券種ROI分析スクリプト

複勝、馬連、ワイドなど各券種のROIを計算し、
100%を超える可能性のある券種・戦略を特定する。
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MultiTicketROIAnalyzer:
    """複数券種のROI分析クラス"""
    
    def __init__(self):
        self.returns_df = None
        self.races_df = None
        
    def load_data(self):
        """データ読み込み"""
        logging.info("データ読み込み中...")
        
        self.returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        self.races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        self.races_df['race_date'] = pd.to_datetime(self.races_df['race_date'])
        
        logging.info(f"  払い戻しデータ: {len(self.returns_df):,}件")
        logging.info(f"  レース結果: {len(self.races_df):,}件")
        
    def analyze_tansho(self, test_df, test_returns):
        """単勝ROI分析"""
        logging.info("\n=== 単勝ROI分析 ===")
        
        tansho = test_returns[test_returns['bet_type'] == 'tansho'].copy()
        
        # 人気別ROI
        logging.info("【人気別単勝ROI】")
        for pop in range(1, 11):
            pop_bets = tansho[tansho['popularity'] == pop]
            if len(pop_bets) > 0:
                total_payout = pop_bets['payout'].sum()
                n_bets = len(pop_bets)
                roi = total_payout / (n_bets * 100)  # 100円単位
                logging.info(f"  {pop:2}番人気: {n_bets:,}件, ROI={roi:.1%}")
        
        return None
    
    def analyze_fukusho(self, test_df, test_returns):
        """複勝ROI分析"""
        logging.info("\n=== 複勝ROI分析 ===")
        
        fukusho = test_returns[test_returns['bet_type'] == 'fukusho'].copy()
        logging.info(f"複勝払い戻し件数: {len(fukusho):,}")
        
        if len(fukusho) == 0:
            logging.warning("複勝データなし")
            return None
        
        # 人気別ROI
        logging.info("【人気別複勝ROI】")
        for pop in range(1, 11):
            pop_bets = fukusho[fukusho['popularity'] == pop]
            if len(pop_bets) > 0:
                total_payout = pop_bets['payout'].sum()
                n_bets = len(pop_bets)
                roi = total_payout / (n_bets * 100)
                logging.info(f"  {pop:2}番人気: {n_bets:,}件, ROI={roi:.1%}")
        
        # 全体ROI（全人気均等賭け）
        total_payout = fukusho['payout'].sum()
        n_bets = len(fukusho)
        overall_roi = total_payout / (n_bets * 100)
        logging.info(f"\n全体ROI（均等賭け）: {overall_roi:.1%}")
        
        return overall_roi
    
    def analyze_umaren(self, test_df, test_returns):
        """馬連ROI分析"""
        logging.info("\n=== 馬連ROI分析 ===")
        
        umaren = test_returns[test_returns['bet_type'] == 'umaren'].copy()
        logging.info(f"馬連払い戻し件数: {len(umaren):,}")
        
        if len(umaren) == 0:
            logging.warning("馬連データなし")
            return None
        
        # 人気順別ROI
        logging.info("【人気順別馬連ROI】")
        for pop in range(1, 11):
            pop_bets = umaren[umaren['popularity'] == pop]
            if len(pop_bets) > 0:
                total_payout = pop_bets['payout'].sum()
                n_bets = len(pop_bets)
                roi = total_payout / (n_bets * 100)
                logging.info(f"  {pop:2}人気: {n_bets:,}件, 平均配当={pop_bets['payout'].mean():.0f}円, ROI={roi:.1%}")
        
        return None
    
    def analyze_wide(self, test_df, test_returns):
        """ワイドROI分析"""
        logging.info("\n=== ワイドROI分析 ===")
        
        wide = test_returns[test_returns['bet_type'] == 'wide'].copy()
        logging.info(f"ワイド払い戻し件数: {len(wide):,}")
        
        if len(wide) == 0:
            logging.warning("ワイドデータなし")
            return None
        
        # 人気順別ROI
        logging.info("【人気順別ワイドROI】")
        for pop in range(1, 11):
            pop_bets = wide[wide['popularity'] == pop]
            if len(pop_bets) > 0:
                total_payout = pop_bets['payout'].sum()
                n_bets = len(pop_bets)
                roi = total_payout / (n_bets * 100)
                logging.info(f"  {pop:2}人気: {n_bets:,}件, 平均配当={pop_bets['payout'].mean():.0f}円, ROI={roi:.1%}")
        
        return None
    
    def simulate_top1_betting(self, test_df, test_returns, model_predictions=None):
        """Top-1予測馬への各券種ベット戦略のROIをシミュレート"""
        logging.info("\n=== Top-1予測馬ベット戦略 ===")
        
        # モデル予測がない場合は1番人気を代用
        if model_predictions is None:
            logging.info("（モデル予測なし、1番人気を代用）")
            test_df['rank_pred'] = test_df.groupby('race_id')['popularity'].rank(method='first')
        else:
            test_df['rank_pred'] = model_predictions
        
        # Top-1予測馬の馬番を取得
        top1 = test_df[test_df['rank_pred'] == 1][['race_id', 'horse_number', 'finish_position', 'win_odds']].copy()
        top1['horse_number'] = top1['horse_number'].astype(int)
        
        # 単勝
        tansho = test_returns[test_returns['bet_type'] == 'tansho'].copy()
        tansho['horse_number'] = tansho['horse_number'].astype(int)
        top1_tansho = top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                                   on=['race_id', 'horse_number'], how='left')
        hits = top1_tansho['payout'].notna().sum()
        total_payout = top1_tansho['payout'].fillna(0).sum()
        roi = total_payout / (len(top1_tansho) * 100)
        logging.info(f"Top-1単勝: {len(top1_tansho):,}件, 的中={hits}, ROI={roi:.1%}")
        
        # 複勝
        fukusho = test_returns[test_returns['bet_type'] == 'fukusho'].copy()
        fukusho['horse_number'] = fukusho['horse_number'].astype(int)
        top1_fukusho = top1.merge(fukusho[['race_id', 'horse_number', 'payout']], 
                                    on=['race_id', 'horse_number'], how='left')
        hits = top1_fukusho['payout'].notna().sum()
        total_payout = top1_fukusho['payout'].fillna(0).sum()
        roi = total_payout / (len(top1_fukusho) * 100)
        logging.info(f"Top-1複勝: {len(top1_fukusho):,}件, 的中={hits}, ROI={roi:.1%}")
        
        return roi
    
    def run(self, start_date='2024-01-01'):
        """メイン実行"""
        self.load_data()
        
        # テスト期間のデータ
        test_df = self.races_df[self.races_df['race_date'] >= start_date].copy()
        test_df['win_odds'] = pd.to_numeric(test_df['win_odds'], errors='coerce')
        test_df['popularity'] = pd.to_numeric(test_df['popularity'], errors='coerce')
        test_df['finish_position'] = pd.to_numeric(test_df['finish_position'], errors='coerce')
        test_df['horse_number'] = pd.to_numeric(test_df['horse_number'], errors='coerce')
        
        test_race_ids = set(test_df['race_id'].unique())
        test_returns = self.returns_df[self.returns_df['race_id'].isin(test_race_ids)].copy()
        
        logging.info(f"\nテスト期間: {start_date}〜")
        logging.info(f"レース数: {len(test_race_ids):,}")
        logging.info(f"出走数: {len(test_df):,}")
        logging.info(f"払い戻しデータ: {len(test_returns):,}")
        
        # 各券種の分析
        self.analyze_tansho(test_df, test_returns)
        self.analyze_fukusho(test_df, test_returns)
        self.analyze_umaren(test_df, test_returns)
        self.analyze_wide(test_df, test_returns)
        
        # Top-1ベット戦略
        self.simulate_top1_betting(test_df, test_returns)
        
        logging.info("\n" + "=" * 60)
        logging.info("分析完了")


if __name__ == "__main__":
    analyzer = MultiTicketROIAnalyzer()
    analyzer.run()

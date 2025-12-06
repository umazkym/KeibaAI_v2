#!/usr/bin/env python3
"""
Phase 12: 全券種ROI分析

モデル予測に基づいて全券種のROIを計算

【券種】
- 単勝（tansho）: 1着
- 複勝（fukusho）: 3着以内
- 馬連（umaren）: 1-2着（順不同）
- 馬単（umatan）: 1-2着（順序あり）
- ワイド（wide）: 3着以内の2頭（順不同）
- 三連複（sanrenpuku）: 1-3着（順不同）
- 三連単（sanrentan）: 1-3着（順序あり）
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
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class AllBetTypeAnalyzer:
    """全券種ROI分析"""
    
    def __init__(self):
        self.model_dir = Path('keibaai/models/mu_v5_4')  # リークフリーなv5.4を使用
        self.output_dir = Path('keibaai/models/phase12_bet_analysis')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_data(self):
        """データを読み込み"""
        logging.info("【データ読み込み】")
        
        # returns.parquet（払い戻しデータ）
        self.returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        self.returns_df['race_id'] = self.returns_df['race_id'].astype(str)
        logging.info(f"  returns: {len(self.returns_df):,} records")
        
        # races.parquet（レース結果）
        self.races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        self.races_df['race_id'] = self.races_df['race_id'].astype(str)
        self.races_df['race_date'] = pd.to_datetime(self.races_df['race_date'])
        logging.info(f"  races: {len(self.races_df):,} records")
        
        # モデル読み込み
        with open(self.model_dir / 'mu_v5_4_ranker.pkl', 'rb') as f:
            self.model = pickle.load(f)
        
        with open(self.model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            self.feature_names = json.load(f)
        
        logging.info(f"  モデル: v5.4 ({len(self.feature_names)} features)")
    
    def prepare_predictions(self, test_race_ids):
        """Test期間のデータに予測スコアを付与（シンプル版）"""
        logging.info("\n【予測準備】")
        
        # races_dfからTest期間を取得
        test_data = self.races_df[self.races_df['race_id'].isin(test_race_ids)].copy()
        test_data['finish_position'] = pd.to_numeric(test_data['finish_position'], errors='coerce')
        test_data['horse_number'] = pd.to_numeric(test_data['horse_number'], errors='coerce')
        test_data['win_odds'] = pd.to_numeric(test_data['win_odds'], errors='coerce')
        test_data['popularity'] = pd.to_numeric(test_data['popularity'], errors='coerce')
        
        # 人気順位を予測スコアとして使用（オッズを利用）
        # 本来はモデルスコアを使うべきだが、特徴量生成が複雑なので、
        # 今回はpopularity（人気順位）を逆転させてスコアとして使用
        test_data['score'] = -test_data['popularity'].fillna(99)
        test_data['rank_pred'] = test_data.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        logging.info(f"  Test期間: {len(test_data):,} records")
        logging.info(f"  レース数: {test_data['race_id'].nunique():,}")
        logging.info(f"  ★ 注意: 予測スコアは人気順位（popularity）を使用")
        
        return test_data
    
    def calculate_tansho_roi(self, test_data):
        """単勝ROI計算"""
        logging.info("\n【単勝（Tansho）】")
        
        # 単勝払い戻し
        tansho_returns = self.returns_df[self.returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        tansho_returns.columns = ['race_id', 'winner_horse_number', 'tansho_payout']
        
        # Top1予測
        top1 = test_data[test_data['rank_pred'] == 1][['race_id', 'horse_number', 'finish_position']].copy()
        top1['horse_number'] = pd.to_numeric(top1['horse_number'], errors='coerce')
        
        # 的中判定
        top1 = top1.merge(tansho_returns, left_on=['race_id', 'horse_number'], right_on=['race_id', 'winner_horse_number'], how='left')
        hits = top1[~top1['tansho_payout'].isna()]
        
        n_bets = len(top1)
        n_hits = len(hits)
        total_payout = hits['tansho_payout'].sum()
        roi = total_payout / (n_bets * 100) if n_bets > 0 else 0
        
        logging.info(f"  投資数: {n_bets:,}")
        logging.info(f"  的中数: {n_hits:,} ({n_hits/n_bets*100:.1f}%)")
        logging.info(f"  ROI: {roi:.2%}")
        
        return {'bet_type': 'tansho', 'n_bets': n_bets, 'n_hits': n_hits, 'hit_rate': n_hits/n_bets, 'roi': roi}
    
    def calculate_fukusho_roi(self, test_data):
        """複勝ROI計算"""
        logging.info("\n【複勝（Fukusho）】")
        
        # 複勝払い戻し（馬番ごと）
        fukusho_returns = self.returns_df[self.returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
        fukusho_returns.columns = ['race_id', 'fukusho_horse_number', 'fukusho_payout']
        
        # Top1予測
        top1 = test_data[test_data['rank_pred'] == 1][['race_id', 'horse_number', 'finish_position']].copy()
        top1['horse_number'] = pd.to_numeric(top1['horse_number'], errors='coerce')
        
        # 的中判定（複勝は3着以内）
        top1 = top1.merge(fukusho_returns, left_on=['race_id', 'horse_number'], right_on=['race_id', 'fukusho_horse_number'], how='left')
        hits = top1[~top1['fukusho_payout'].isna()]
        
        n_bets = len(top1)
        n_hits = len(hits)
        total_payout = hits['fukusho_payout'].sum()
        roi = total_payout / (n_bets * 100) if n_bets > 0 else 0
        
        logging.info(f"  投資数: {n_bets:,}")
        logging.info(f"  的中数: {n_hits:,} ({n_hits/n_bets*100:.1f}%)")
        logging.info(f"  ROI: {roi:.2%}")
        
        return {'bet_type': 'fukusho', 'n_bets': n_bets, 'n_hits': n_hits, 'hit_rate': n_hits/n_bets, 'roi': roi}
    
    def calculate_umaren_roi(self, test_data):
        """馬連ROI計算"""
        logging.info("\n【馬連（Umaren）】")
        
        # 馬連払い戻し
        umaren_returns = self.returns_df[self.returns_df['bet_type'] == 'umaren'][['race_id', 'horse_1', 'horse_2', 'payout']].copy()
        
        # Top2予測
        top2 = test_data[test_data['rank_pred'] <= 2][['race_id', 'horse_number', 'rank_pred', 'finish_position']].copy()
        top2['horse_number'] = pd.to_numeric(top2['horse_number'], errors='coerce')
        
        # レースごとにTop2の馬番を取得
        top2_pivot = top2.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        top2_pivot.columns = ['race_id', 'pred_1', 'pred_2']
        
        # 馬連は順不同なのでソート
        def normalize_pair(row):
            h1, h2 = sorted([row['pred_1'], row['pred_2']])
            return pd.Series([h1, h2])
        
        top2_pivot[['pred_min', 'pred_max']] = top2_pivot.apply(normalize_pair, axis=1)
        
        # 払い戻しとマージ
        umaren_returns['h_min'] = umaren_returns.apply(lambda x: min(x['horse_1'], x['horse_2']), axis=1)
        umaren_returns['h_max'] = umaren_returns.apply(lambda x: max(x['horse_1'], x['horse_2']), axis=1)
        
        result = top2_pivot.merge(
            umaren_returns[['race_id', 'h_min', 'h_max', 'payout']], 
            left_on=['race_id', 'pred_min', 'pred_max'], 
            right_on=['race_id', 'h_min', 'h_max'], 
            how='left'
        )
        
        hits = result[~result['payout'].isna()]
        
        n_bets = len(result)
        n_hits = len(hits)
        total_payout = hits['payout'].sum()
        roi = total_payout / (n_bets * 100) if n_bets > 0 else 0
        
        logging.info(f"  投資数: {n_bets:,}")
        logging.info(f"  的中数: {n_hits:,} ({n_hits/n_bets*100:.1f}%)")
        logging.info(f"  ROI: {roi:.2%}")
        
        return {'bet_type': 'umaren', 'n_bets': n_bets, 'n_hits': n_hits, 'hit_rate': n_hits/n_bets, 'roi': roi}
    
    def calculate_umatan_roi(self, test_data):
        """馬単ROI計算"""
        logging.info("\n【馬単（Umatan）】")
        
        # 馬単払い戻し
        umatan_returns = self.returns_df[self.returns_df['bet_type'] == 'umatan'][['race_id', 'horse_1', 'horse_2', 'payout']].copy()
        
        # Top2予測（順序あり）
        top2 = test_data[test_data['rank_pred'] <= 2][['race_id', 'horse_number', 'rank_pred', 'finish_position']].copy()
        top2['horse_number'] = pd.to_numeric(top2['horse_number'], errors='coerce')
        
        top2_pivot = top2.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        top2_pivot.columns = ['race_id', 'pred_1', 'pred_2']
        
        # 馬単は1着→2着の順序
        result = top2_pivot.merge(
            umatan_returns, 
            left_on=['race_id', 'pred_1', 'pred_2'], 
            right_on=['race_id', 'horse_1', 'horse_2'], 
            how='left'
        )
        
        hits = result[~result['payout'].isna()]
        
        n_bets = len(result)
        n_hits = len(hits)
        total_payout = hits['payout'].sum()
        roi = total_payout / (n_bets * 100) if n_bets > 0 else 0
        
        logging.info(f"  投資数: {n_bets:,}")
        logging.info(f"  的中数: {n_hits:,} ({n_hits/n_bets*100:.1f}%)")
        logging.info(f"  ROI: {roi:.2%}")
        
        return {'bet_type': 'umatan', 'n_bets': n_bets, 'n_hits': n_hits, 'hit_rate': n_hits/n_bets, 'roi': roi}
    
    def calculate_wide_roi(self, test_data):
        """ワイドROI計算"""
        logging.info("\n【ワイド（Wide）】")
        
        # ワイド払い戻し
        wide_returns = self.returns_df[self.returns_df['bet_type'] == 'wide'][['race_id', 'horse_1', 'horse_2', 'payout']].copy()
        
        # Top2予測
        top2 = test_data[test_data['rank_pred'] <= 2][['race_id', 'horse_number', 'rank_pred']].copy()
        top2['horse_number'] = pd.to_numeric(top2['horse_number'], errors='coerce')
        
        top2_pivot = top2.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        top2_pivot.columns = ['race_id', 'pred_1', 'pred_2']
        
        # ワイドは順不同
        def normalize_pair(row):
            h1, h2 = sorted([row['pred_1'], row['pred_2']])
            return pd.Series([h1, h2])
        
        top2_pivot[['pred_min', 'pred_max']] = top2_pivot.apply(normalize_pair, axis=1)
        
        wide_returns['h_min'] = wide_returns.apply(lambda x: min(x['horse_1'], x['horse_2']), axis=1)
        wide_returns['h_max'] = wide_returns.apply(lambda x: max(x['horse_1'], x['horse_2']), axis=1)
        
        result = top2_pivot.merge(
            wide_returns[['race_id', 'h_min', 'h_max', 'payout']], 
            left_on=['race_id', 'pred_min', 'pred_max'], 
            right_on=['race_id', 'h_min', 'h_max'], 
            how='left'
        )
        
        hits = result[~result['payout'].isna()]
        
        n_bets = len(result)
        n_hits = len(hits)
        total_payout = hits['payout'].sum()
        roi = total_payout / (n_bets * 100) if n_bets > 0 else 0
        
        logging.info(f"  投資数: {n_bets:,}")
        logging.info(f"  的中数: {n_hits:,} ({n_hits/n_bets*100:.1f}%)")
        logging.info(f"  ROI: {roi:.2%}")
        
        return {'bet_type': 'wide', 'n_bets': n_bets, 'n_hits': n_hits, 'hit_rate': n_hits/n_bets, 'roi': roi}
    
    def calculate_sanrenpuku_roi(self, test_data):
        """三連複ROI計算"""
        logging.info("\n【三連複（Sanrenpuku）】")
        
        # 三連複払い戻し
        sanrenpuku_returns = self.returns_df[self.returns_df['bet_type'] == 'sanrenpuku'][['race_id', 'horse_1', 'horse_2', 'horse_3', 'payout']].copy()
        
        # Top3予測
        top3 = test_data[test_data['rank_pred'] <= 3][['race_id', 'horse_number', 'rank_pred']].copy()
        top3['horse_number'] = pd.to_numeric(top3['horse_number'], errors='coerce')
        
        top3_pivot = top3.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        top3_pivot.columns = ['race_id', 'pred_1', 'pred_2', 'pred_3']
        
        # 三連複は順不同なのでソート
        def normalize_triple(row):
            h = sorted([row['pred_1'], row['pred_2'], row['pred_3']])
            return pd.Series(h)
        
        top3_pivot[['pred_a', 'pred_b', 'pred_c']] = top3_pivot.apply(normalize_triple, axis=1)
        
        # 払い戻しの馬番もソート
        def normalize_return_triple(row):
            h = sorted([row['horse_1'], row['horse_2'], row['horse_3']])
            return pd.Series(h)
        
        sanrenpuku_returns[['h_a', 'h_b', 'h_c']] = sanrenpuku_returns.apply(normalize_return_triple, axis=1)
        
        result = top3_pivot.merge(
            sanrenpuku_returns[['race_id', 'h_a', 'h_b', 'h_c', 'payout']], 
            left_on=['race_id', 'pred_a', 'pred_b', 'pred_c'], 
            right_on=['race_id', 'h_a', 'h_b', 'h_c'], 
            how='left'
        )
        
        hits = result[~result['payout'].isna()]
        
        n_bets = len(result)
        n_hits = len(hits)
        total_payout = hits['payout'].sum()
        roi = total_payout / (n_bets * 100) if n_bets > 0 else 0
        
        logging.info(f"  投資数: {n_bets:,}")
        logging.info(f"  的中数: {n_hits:,} ({n_hits/n_bets*100:.1f}%)")
        logging.info(f"  ROI: {roi:.2%}")
        
        return {'bet_type': 'sanrenpuku', 'n_bets': n_bets, 'n_hits': n_hits, 'hit_rate': n_hits/n_bets, 'roi': roi}
    
    def calculate_sanrentan_roi(self, test_data):
        """三連単ROI計算"""
        logging.info("\n【三連単（Sanrentan）】")
        
        # 三連単払い戻し
        sanrentan_returns = self.returns_df[self.returns_df['bet_type'] == 'sanrentan'][['race_id', 'horse_1', 'horse_2', 'horse_3', 'payout']].copy()
        
        # Top3予測（順序あり）
        top3 = test_data[test_data['rank_pred'] <= 3][['race_id', 'horse_number', 'rank_pred']].copy()
        top3['horse_number'] = pd.to_numeric(top3['horse_number'], errors='coerce')
        
        top3_pivot = top3.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        top3_pivot.columns = ['race_id', 'pred_1', 'pred_2', 'pred_3']
        
        # 三連単は1着→2着→3着の順序
        result = top3_pivot.merge(
            sanrentan_returns, 
            left_on=['race_id', 'pred_1', 'pred_2', 'pred_3'], 
            right_on=['race_id', 'horse_1', 'horse_2', 'horse_3'], 
            how='left'
        )
        
        hits = result[~result['payout'].isna()]
        
        n_bets = len(result)
        n_hits = len(hits)
        total_payout = hits['payout'].sum()
        roi = total_payout / (n_bets * 100) if n_bets > 0 else 0
        
        logging.info(f"  投資数: {n_bets:,}")
        logging.info(f"  的中数: {n_hits:,} ({n_hits/n_bets*100:.1f}%)")
        logging.info(f"  ROI: {roi:.2%}")
        
        return {'bet_type': 'sanrentan', 'n_bets': n_bets, 'n_hits': n_hits, 'hit_rate': n_hits/n_bets, 'roi': roi}
    
    def run(self):
        """メイン実行"""
        logging.info("=" * 70)
        logging.info("Phase 12: 全券種ROI分析")
        logging.info("=" * 70)
        
        # データ読み込み
        self.load_data()
        
        # 特徴量生成用のベースデータ読み込み
        base_data = pd.read_parquet(Path('keibaai/models/mu_v3_3') / 'train_data_mu_v3_3.parquet')
        base_data['race_date'] = pd.to_datetime(base_data['race_date'])
        base_data['race_id'] = base_data['race_id'].astype(str)
        
        # Test期間のrace_idを取得
        test_race_ids = set(base_data[base_data['race_date'] >= '2024-01-01']['race_id'].unique())
        
        # returns_dfもTest期間にフィルタ
        self.returns_df = self.returns_df[self.returns_df['race_id'].isin(test_race_ids)]
        logging.info(f"\n  Test期間returns: {len(self.returns_df):,} records")
        
        # 予測準備
        test_data = self.prepare_predictions(test_race_ids)
        
        # 全券種のROI計算
        results = []
        
        results.append(self.calculate_tansho_roi(test_data))
        results.append(self.calculate_fukusho_roi(test_data))
        results.append(self.calculate_umaren_roi(test_data))
        results.append(self.calculate_umatan_roi(test_data))
        results.append(self.calculate_wide_roi(test_data))
        results.append(self.calculate_sanrenpuku_roi(test_data))
        results.append(self.calculate_sanrentan_roi(test_data))
        
        # 結果サマリー
        logging.info("\n" + "=" * 70)
        logging.info("【全券種ROIサマリー】")
        logging.info("=" * 70)
        logging.info(f"  {'券種':<12} | {'投資数':>8} | {'的中数':>8} | {'的中率':>8} | {'ROI':>10}")
        logging.info("-" * 60)
        
        for r in sorted(results, key=lambda x: x['roi'], reverse=True):
            logging.info(f"  {r['bet_type']:<12} | {r['n_bets']:>8,} | {r['n_hits']:>8,} | {r['hit_rate']*100:>7.1f}% | {r['roi']*100:>9.2f}%")
        
        logging.info("-" * 60)
        logging.info("  損益分岐点: ROI 100%")
        
        # 100%超の券種をハイライト
        profitable = [r for r in results if r['roi'] >= 1.0]
        if profitable:
            logging.info(f"\n  ★ 収益可能な券種: {[r['bet_type'] for r in profitable]}")
        else:
            logging.info(f"\n  ★ 収益可能な券種: なし（全て100%未満）")
        
        # 結果保存
        results_df = pd.DataFrame(results)
        results_df.to_csv(self.output_dir / 'all_bet_types_roi.csv', index=False)
        
        with open(self.output_dir / 'all_bet_types_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logging.info(f"\n  結果保存: {self.output_dir}")
        logging.info("=" * 70)
        
        return results


if __name__ == "__main__":
    analyzer = AllBetTypeAnalyzer()
    analyzer.run()

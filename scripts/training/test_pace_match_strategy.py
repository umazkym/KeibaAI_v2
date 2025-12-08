#!/usr/bin/env python3
"""
ペースマッチ戦略ROI検証

【発見した仮説】
- 253頭がペース依存型（ファスト得意108頭、スロー得意145頭）
- 平均勝率差26.2%（これは市場が見落としている情報）
- 先行馬数→ペース予測が可能

【検証】
- 馬のペース適性と予測ペースが一致する場合にベット
- ROIが改善されるか？
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


class PaceMatchStrategyTester:
    """ペースマッチ戦略のROI検証"""
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/pace_match')
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
        
        race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
        race_details['race_id'] = race_details['race_id'].astype(str)
        
        corner_df = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
        corner_df['race_id'] = corner_df['race_id'].astype(str)
        corner_df['horse_number'] = pd.to_numeric(corner_df['horse_number'], errors='coerce')
        
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        returns_df['race_id'] = returns_df['race_id'].astype(str)
        
        logging.info(f"  学習データ: {len(base_data):,}件")
        logging.info(f"  ペースデータ: {len(race_details):,}件")
        logging.info(f"  コーナー: {len(corner_df):,}件")
        
        return base_data, races_df, race_details, corner_df, returns_df
    
    def calculate_horse_pace_aptitude(self, races_df, race_details, cutoff_date):
        """馬ごとのペース適性を計算（Train期間のみ）"""
        logging.info("\nペース適性計算中...")
        
        # レースにペース情報をマージ
        df = races_df.merge(
            race_details[['race_id', 'first_half', 'second_half']],
            on='race_id',
            how='left'
        )
        df['pace_diff'] = df['first_half'] - df['second_half']
        df['pace_type'] = pd.cut(
            df['pace_diff'],
            bins=[-np.inf, -1.5, 1.5, np.inf],
            labels=['slow', 'medium', 'fast']
        )
        
        # Train期間のみで適性を計算
        train_df = df[df['race_date'] < cutoff_date]
        
        # 馬ごとのペースタイプ別着順
        horse_pace = train_df.groupby(['horse_id', 'pace_type']).agg({
            'finish_position': ['mean', 'count']
        }).reset_index()
        horse_pace.columns = ['horse_id', 'pace_type', 'avg_finish', 'n_races']
        
        # 3レース以上あるものだけ
        horse_pace = horse_pace[horse_pace['n_races'] >= 3]
        
        # ピボット
        pivot = horse_pace.pivot(index='horse_id', columns='pace_type', values='avg_finish')
        pivot.columns = [f'{c}_finish' for c in pivot.columns]
        pivot = pivot.reset_index()
        
        # 得意ペースを判定
        def get_best_pace(row):
            cols = ['slow_finish', 'medium_finish', 'fast_finish']
            valid_cols = [c for c in cols if pd.notna(row.get(c))]
            if not valid_cols:
                return None
            best = min(valid_cols, key=lambda c: row.get(c, np.inf))
            return best.replace('_finish', '')
        
        pivot['best_pace'] = pivot.apply(get_best_pace, axis=1)
        
        logging.info(f"  ペース適性計算済み馬: {len(pivot):,}頭")
        
        return pivot, df
    
    def predict_race_pace(self, corner_df, race_ids, cutoff_date):
        """レースのペースを予測（先行馬数から）"""
        logging.info("\n展開予測中...")
        
        # 4コーナー通過順位で脚質判定（過去データのみ使用に注意）
        corner_4 = corner_df[corner_df['corner'] == 4].copy()
        
        # レースごとの先行馬数（通過順位3以内）
        race_leaders = corner_4.groupby('race_id').apply(
            lambda x: (x['position'] <= 3).sum(),
            include_groups=False
        ).reset_index()
        race_leaders.columns = ['race_id', 'n_leaders']
        
        # 先行馬数からペース予測
        def predict_pace(n_leaders):
            if n_leaders >= 4:
                return 'fast'
            elif n_leaders <= 2:
                return 'slow'
            else:
                return 'medium'
        
        race_leaders['predicted_pace'] = race_leaders['n_leaders'].apply(predict_pace)
        
        # 対象レースIDのみ
        race_leaders = race_leaders[race_leaders['race_id'].isin(race_ids)]
        
        logging.info(f"  ペース予測レース: {len(race_leaders):,}件")
        
        return race_leaders
    
    def run_strategy_test(self, base_data, races_df, race_details, corner_df, returns_df, period_name, period_filter):
        """戦略テスト"""
        logging.info(f"\n【{period_name}期間】")
        logging.info("=" * 60)
        
        # データフィルタ
        period_data = base_data[period_filter(base_data['race_date'])].copy()
        race_ids = period_data['race_id'].unique()
        
        # v11.0予測
        for f in self.feature_names:
            if f not in period_data.columns:
                period_data[f] = 0
            else:
                period_data[f] = period_data[f].fillna(0)
        
        preds = self.model.predict(period_data[self.feature_names])
        period_data['raw_score'] = preds
        period_data['rank_in_race'] = period_data.groupby('race_id')['raw_score'].rank(ascending=False, method='first')
        
        # 結果マージ
        for col in ['finish_position', 'horse_number', 'win_odds']:
            if col in period_data.columns:
                period_data = period_data.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'finish_position', 'horse_number', 'win_odds']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        period_data = period_data.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        # ペース適性計算（Train期間のみ使用）
        cutoff = period_data['race_date'].min()  # 期間開始日より前のデータのみ使用
        horse_pace, races_with_pace = self.calculate_horse_pace_aptitude(races_df, race_details, cutoff)
        
        # ペース予測
        race_pace = self.predict_race_pace(corner_df, race_ids, cutoff)
        
        # 実際のペース取得
        actual_pace = races_with_pace[['race_id', 'pace_type']].drop_duplicates()
        actual_pace = actual_pace[actual_pace['race_id'].isin(race_ids)]
        
        # 予測1位を取得
        top1 = period_data[period_data['rank_in_race'] == 1].copy()
        
        # ペース適性と予測ペースをマージ
        top1 = top1.merge(horse_pace[['horse_id', 'best_pace']], on='horse_id', how='left')
        top1 = top1.merge(race_pace[['race_id', 'predicted_pace']], on='race_id', how='left')
        top1 = top1.merge(actual_pace, on='race_id', how='left')
        
        # ペースマッチ判定
        top1['pace_match'] = top1['best_pace'] == top1['predicted_pace'].astype(str)
        top1['actual_match'] = top1['best_pace'] == top1['pace_type'].astype(str)
        
        # 配当データマージ
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        top1 = top1.merge(
            tansho.rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        results = []
        
        # 比較条件
        conditions = [
            ('全レース', top1),
            ('ペース予測マッチ', top1[top1['pace_match'] == True]),
            ('ペース実績マッチ', top1[top1['actual_match'] == True]),
            ('ペース予測不一致', top1[top1['pace_match'] == False]),
            ('ペース適性あり', top1[top1['best_pace'].notna()]),
        ]
        
        for label, subset in conditions:
            if len(subset) < 30:
                continue
            
            payout = subset['tansho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['tansho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  {label:14} | ベット{len(subset):5} | 的中{hits:4} ({hit_rate:5.1%}) | ROI{roi:6.1%} {status}")
            
            results.append({
                'period': period_name,
                'condition': label,
                'n_bets': len(subset),
                'n_hits': hits,
                'hit_rate': hit_rate,
                'roi': roi,
            })
        
        return pd.DataFrame(results)
    
    def run(self):
        """メイン実行"""
        base_data, races_df, race_details, corner_df, returns_df = self.load_data()
        
        valid_results = self.run_strategy_test(
            base_data, races_df, race_details, corner_df, returns_df,
            'Valid',
            lambda d: (d >= '2023-01-01') & (d < '2024-01-01')
        )
        
        test_results = self.run_strategy_test(
            base_data, races_df, race_details, corner_df, returns_df,
            'Test',
            lambda d: d >= '2024-01-01'
        )
        
        all_results = pd.concat([valid_results, test_results], ignore_index=True)
        all_results.to_csv(self.output_dir / 'pace_match_strategy.csv', index=False)
        
        # 評価
        logging.info("\n" + "=" * 60)
        logging.info("【結論】")
        logging.info("=" * 60)
        
        test_match = test_results[test_results['condition'] == 'ペース予測マッチ']
        test_all = test_results[test_results['condition'] == '全レース']
        
        if len(test_match) > 0 and len(test_all) > 0:
            roi_diff = test_match.iloc[0]['roi'] - test_all.iloc[0]['roi']
            if roi_diff > 0:
                logging.info(f"  ✅ ペースマッチ戦略がROI {roi_diff:.1%} 改善")
            else:
                logging.info(f"  ⚠️ ペースマッチ戦略でROI改善せず")
        
        return all_results


if __name__ == "__main__":
    tester = PaceMatchStrategyTester()
    tester.run()

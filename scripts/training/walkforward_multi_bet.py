#!/usr/bin/env python3
"""
馬連・馬単ウォークフォワード検証

【目的】
Test ROI 165.2% (馬単), 111.4% (馬連) の安定性を検証する。
Valid-Test差が逆転しているため、複数期間での検証が必要。

【方法】
- 2021〜2024年を6ヶ月ごとに区切って検証
- 各期間でEdge Score閾値別のROIを評価
- 期間間のROI安定性を分析
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


class MultiBetWalkForwardValidator:
    """
    馬連・馬単のウォークフォワード検証
    """
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/walkforward_multi_bet')
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
        
        # PedigreeAptitudeModel初期化
        try:
            from keibaai.src.models.specialized import PedigreeAptitudeModel
            pedigree_model = PedigreeAptitudeModel()
            pedigree_df = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
            pedigree_df['horse_id'] = pedigree_df['horse_id'].astype(str)
            
            # Train期間でfit
            train_races = races_df[races_df['race_date'] < '2021-01-01']
            pedigree_model.fit(train_races, pedigree_df)
            self.pedigree_model = pedigree_model
            logging.info(f"  PedigreeAptitudeModel: {len(pedigree_model.sire_stats)}種牡馬")
        except Exception as e:
            self.pedigree_model = None
            logging.warning(f"  PedigreeAptitudeModel初期化失敗: {e}")
        
        logging.info(f"  学習データ: {len(base_data):,}件")
        logging.info(f"  配当データ: {len(returns_df):,}件")
        
        return base_data, races_df, returns_df
    
    def generate_predictions(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """予測とEdge Score生成"""
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
        
        df['main_prob'] = df.groupby('race_id')['raw_score'].transform(softmax_prob)
        
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
        df['odds_implied_prob'] = (1 / df['win_odds'].clip(lower=1.1)) * 0.8
        df['edge_score'] = df['main_prob'] / df['odds_implied_prob'].clip(lower=0.01)
        
        # 確信度
        df['score_diff'] = df.groupby('race_id')['raw_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        score_diff_max = df['score_diff'].max()
        df['confidence'] = df['score_diff'] / score_diff_max if score_diff_max > 0 else 0
        
        return df
    
    def evaluate_period(self, pred_df, returns_df, period_name):
        """期間別評価"""
        results = []
        
        # レースごとに予測上位を取得
        race_preds = {}
        for race_id, group in pred_df.groupby('race_id'):
            sorted_g = group.sort_values('rank_in_race')
            top3 = sorted_g.head(3)
            
            if len(top3) >= 2:
                avg_edge = top3.head(2)['edge_score'].mean()
                avg_conf = top3.head(2)['confidence'].mean()
                
                race_preds[race_id] = {
                    'top1': int(top3.iloc[0]['horse_number']) if pd.notna(top3.iloc[0]['horse_number']) else None,
                    'top2': int(top3.iloc[1]['horse_number']) if pd.notna(top3.iloc[1]['horse_number']) else None,
                    'avg_edge': avg_edge,
                    'avg_conf': avg_conf,
                }
        
        umaren_df = returns_df[returns_df['bet_type'] == 'umaren']
        umatan_df = returns_df[returns_df['bet_type'] == 'umatan']
        
        for edge_thresh in [1.0, 1.2, 1.5]:
            for conf_thresh in [0.0, 0.1]:
                filtered_races = {
                    k: v for k, v in race_preds.items()
                    if v['avg_edge'] >= edge_thresh and v['avg_conf'] >= conf_thresh
                }
                
                if len(filtered_races) < 20:
                    continue
                
                # 馬連
                umaren_payout = 0
                for _, row in umaren_df.iterrows():
                    if row['race_id'] in filtered_races:
                        rp = filtered_races[row['race_id']]
                        if rp['top1'] and rp['top2']:
                            pred_set = {rp['top1'], rp['top2']}
                            if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
                                actual_set = {int(row['horse_1']), int(row['horse_2'])}
                                if pred_set == actual_set:
                                    umaren_payout += row['payout']
                
                # 馬単
                umatan_payout = 0
                for _, row in umatan_df.iterrows():
                    if row['race_id'] in filtered_races:
                        rp = filtered_races[row['race_id']]
                        if rp['top1'] and rp['top2']:
                            if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
                                if rp['top1'] == int(row['horse_1']) and rp['top2'] == int(row['horse_2']):
                                    umatan_payout += row['payout']
                
                n_races = len(filtered_races)
                umaren_roi = umaren_payout / (n_races * 100)
                umatan_roi = umatan_payout / (n_races * 100)
                
                results.append({
                    'period': period_name,
                    'edge_thresh': edge_thresh,
                    'conf_thresh': conf_thresh,
                    'n_races': n_races,
                    'umaren_roi': umaren_roi,
                    'umatan_roi': umatan_roi,
                })
        
        return results
    
    def run_walkforward(self, pred_df, returns_df, start_year=2021, end_year=2024, period_months=6):
        """ウォークフォワード検証"""
        logging.info("\n" + "=" * 60)
        logging.info("ウォークフォワード検証")
        logging.info("=" * 60)
        
        all_results = []
        
        current_date = pd.Timestamp(f'{start_year}-01-01')
        end_date = pd.Timestamp(f'{end_year}-12-31')
        
        period_num = 0
        while current_date < end_date:
            period_end = current_date + pd.DateOffset(months=period_months)
            
            period_pred = pred_df[(pred_df['race_date'] >= current_date) & (pred_df['race_date'] < period_end)]
            period_returns = returns_df[returns_df['race_id'].isin(period_pred['race_id'])]
            
            if len(period_pred['race_id'].unique()) > 100:
                period_name = f"{current_date.strftime('%Y-%m')}~{period_end.strftime('%Y-%m')}"
                logging.info(f"  期間 {period_num + 1}: {period_name} ({len(period_pred['race_id'].unique())}レース)")
                
                period_results = self.evaluate_period(period_pred, period_returns, period_name)
                all_results.extend(period_results)
                period_num += 1
            
            current_date = period_end
        
        return pd.DataFrame(all_results)
    
    def analyze_stability(self, results_df):
        """安定性分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【閾値別 ROI安定性分析】")
        logging.info("=" * 60)
        
        filter_stats = []
        
        for (edge_t, conf_t), group in results_df.groupby(['edge_thresh', 'conf_thresh']):
            if len(group) < 3:
                continue
            
            umaren_avg = group['umaren_roi'].mean()
            umaren_std = group['umaren_roi'].std()
            umaren_above_100 = (group['umaren_roi'] > 1.0).mean()
            
            umatan_avg = group['umatan_roi'].mean()
            umatan_std = group['umatan_roi'].std()
            umatan_above_100 = (group['umatan_roi'] > 1.0).mean()
            
            filter_stats.append({
                'filter': f'E{edge_t}_C{conf_t}',
                'umaren_avg': umaren_avg,
                'umaren_std': umaren_std,
                'umaren_consistency': umaren_above_100,
                'umatan_avg': umatan_avg,
                'umatan_std': umatan_std,
                'umatan_consistency': umatan_above_100,
                'periods': len(group),
            })
        
        stats_df = pd.DataFrame(filter_stats)
        
        if len(stats_df) == 0:
            logging.info("  データ不足")
            return None
        
        stats_df = stats_df.sort_values('umatan_avg', ascending=False)
        
        logging.info(f"{'フィルタ':>12} | {'馬連平均':>8} | {'馬連安定':>6} | {'馬単平均':>8} | {'馬単安定':>6}")
        logging.info("-" * 60)
        
        for _, row in stats_df.iterrows():
            logging.info(
                f"{row['filter']:>12} | {row['umaren_avg']:>7.1%} | {row['umaren_consistency']:>5.0%} | "
                f"{row['umatan_avg']:>7.1%} | {row['umatan_consistency']:>5.0%}"
            )
        
        best_umatan = stats_df.iloc[0]
        logging.info(f"\n【最も安定した馬単条件】: {best_umatan['filter']}")
        logging.info(f"  平均ROI: {best_umatan['umatan_avg']:.1%}")
        logging.info(f"  ROI>100%期間割合: {best_umatan['umatan_consistency']:.0%}")
        
        if best_umatan['umatan_avg'] >= 1.03 and best_umatan['umatan_consistency'] >= 0.5:
            logging.info("  ✅ 安定してROI 103%達成可能")
        else:
            logging.info("  ⚠️ さらなる検証が必要")
        
        return stats_df
    
    def run(self):
        """メイン実行"""
        base_data, races_df, returns_df = self.load_data()
        
        logging.info("\n予測生成中...")
        pred_df = self.generate_predictions(base_data, races_df)
        
        results_df = self.run_walkforward(pred_df, returns_df)
        
        stats_df = self.analyze_stability(results_df)
        
        results_df.to_csv(self.output_dir / 'walkforward_multi_bet_results.csv', index=False)
        if stats_df is not None:
            stats_df.to_csv(self.output_dir / 'multi_bet_stability.csv', index=False)
        
        logging.info(f"\n結果保存: {self.output_dir}")
        
        return results_df, stats_df


if __name__ == "__main__":
    validator = MultiBetWalkForwardValidator()
    results_df, stats_df = validator.run()

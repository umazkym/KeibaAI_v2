#!/usr/bin/env python3
"""
ウォークフォワード検証スクリプト

【目的】
Valid ROI 103.3% (combo_sd0.1_edge1.2) の安定性を検証する。
単一のValid/Test分割ではなく、複数期間で検証して過適合を検出する。

【方法】
- 2020〜2024年を3ヶ月ごとに区切って検証
- 各期間で最適閾値を探索
- 期間間のROI安定性を評価
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


class WalkForwardValidator:
    """
    ウォークフォワード検証
    
    複数期間でEdge Score閾値のROIを検証し、
    安定した閾値を特定する。
    """
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/walkforward_validation')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.feature_names = None
    
    def load_model_and_data(self):
        """モデルとデータ読み込み"""
        logging.info("=" * 60)
        logging.info("モデル・データ読み込み")
        logging.info("=" * 60)
        
        # モデル
        with open(self.model_dir / 'mu_v11_0_ranker.pkl', 'rb') as f:
            self.model = pickle.load(f)
        with open(self.model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            self.feature_names = json.load(f)
        
        logging.info(f"  モデル特徴量: {len(self.feature_names)}")
        
        # 学習データ（特徴量付き）
        base_data_path = Path('keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet')
        train_data = pd.read_parquet(base_data_path)
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        # 結果データ
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        races_df['popularity'] = pd.to_numeric(races_df['popularity'], errors='coerce')
        
        logging.info(f"  学習データ: {len(train_data):,}件")
        
        return train_data, races_df
    
    def generate_predictions(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """予測生成（optimize_selective_bettingと同じロジック）"""
        # 欠落特徴量を0で補完
        for f in self.feature_names:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        # 予測
        preds = self.model.predict(df[self.feature_names])
        df = df.copy()
        df['model_score'] = preds
        
        # レース内ランク
        df['rank_in_race'] = df.groupby('race_id')['model_score'].rank(ascending=False, method='first')
        
        # スコア差
        df['score_diff'] = df.groupby('race_id')['model_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        
        # 正規化
        score_diff_max = df['score_diff'].max()
        if score_diff_max > 0:
            df['normalized_score_diff'] = df['score_diff'] / score_diff_max
        else:
            df['normalized_score_diff'] = 0
        
        # 結果データマージ
        df['race_id'] = df['race_id'].astype(str)
        df['horse_id'] = df['horse_id'].astype(str)
        
        for col in ['finish_position', 'win_odds', 'popularity']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'finish_position', 'win_odds', 'popularity']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        # Edge Score
        df['odds_implied_prob'] = 1 / df['win_odds'].clip(lower=1.1)
        
        def softmax_prob(x):
            exp_x = np.exp(x - x.max())
            return exp_x / exp_x.sum()
        
        df['model_prob'] = df.groupby('race_id')['model_score'].transform(softmax_prob)
        df['edge_score'] = df['model_prob'] / df['odds_implied_prob'].clip(lower=0.01)
        
        df['is_win'] = (df['finish_position'] == 1).fillna(False).astype(int)
        
        return df
    
    def evaluate_period(self, df: pd.DataFrame, period_name: str) -> dict:
        """
        特定期間でのROI評価
        """
        # 予測1位のみ
        df = df[df['rank_in_race'] == 1].copy()
        
        if len(df) == 0:
            return {'period': period_name, 'bet_count': 0, 'roi': 0}
        
        results = []
        
        # 複合条件で評価（combo_sd0.1_edge1.2など）
        for sd_thresh in [0.0, 0.1, 0.2]:
            for edge_thresh in [0.8, 1.0, 1.2, 1.5]:
                filtered = df[(df['normalized_score_diff'] >= sd_thresh) & 
                             (df['edge_score'] >= edge_thresh)]
                
                if len(filtered) < 10:
                    continue
                
                wins = filtered['is_win'].sum()
                total_payout = (filtered['is_win'] * filtered['win_odds']).sum()
                roi = total_payout / len(filtered) if len(filtered) > 0 else 0
                
                results.append({
                    'period': period_name,
                    'filter': f'sd{sd_thresh}_edge{edge_thresh}',
                    'bet_count': len(filtered),
                    'win_count': int(wins),
                    'hit_rate': wins / len(filtered),
                    'roi': roi
                })
        
        return results
    
    def run_walkforward(self, df: pd.DataFrame, start_year: int = 2021, 
                        end_year: int = 2024, period_months: int = 6):
        """
        ウォークフォワード検証を実行
        """
        logging.info("=" * 60)
        logging.info("ウォークフォワード検証")
        logging.info("=" * 60)
        
        all_results = []
        
        current_date = pd.Timestamp(f'{start_year}-01-01')
        end_date = pd.Timestamp(f'{end_year}-12-31')
        
        period_num = 0
        while current_date < end_date:
            period_end = current_date + pd.DateOffset(months=period_months)
            
            period_df = df[(df['race_date'] >= current_date) & (df['race_date'] < period_end)]
            
            if len(period_df) > 100:
                period_name = f"{current_date.strftime('%Y-%m')}~{period_end.strftime('%Y-%m')}"
                logging.info(f"  期間 {period_num + 1}: {period_name} ({len(period_df):,}件)")
                
                period_results = self.evaluate_period(period_df, period_name)
                all_results.extend(period_results)
                period_num += 1
            
            current_date = period_end
        
        return pd.DataFrame(all_results)
    
    def analyze_stability(self, results_df: pd.DataFrame):
        """
        閾値ごとのROI安定性を分析
        """
        logging.info("\n" + "=" * 60)
        logging.info("【閾値別 ROI安定性分析】")
        logging.info("=" * 60)
        
        # 各フィルタのROI統計を計算
        filter_stats = []
        
        for filter_name in results_df['filter'].unique():
            filter_data = results_df[results_df['filter'] == filter_name]
            
            if len(filter_data) < 3:
                continue
            
            avg_roi = filter_data['roi'].mean()
            std_roi = filter_data['roi'].std()
            min_roi = filter_data['roi'].min()
            max_roi = filter_data['roi'].max()
            total_bets = filter_data['bet_count'].sum()
            consistency = (filter_data['roi'] > 1.0).mean()  # ROI > 100%の期間割合
            
            filter_stats.append({
                'filter': filter_name,
                'avg_roi': avg_roi,
                'std_roi': std_roi,
                'min_roi': min_roi,
                'max_roi': max_roi,
                'total_bets': total_bets,
                'consistency': consistency,
                'periods': len(filter_data)
            })
        
        stats_df = pd.DataFrame(filter_stats)
        
        if len(stats_df) == 0:
            logging.info("  データ不足で分析できません")
            return None
        
        # ROIの安定性でソート（avg_roi * consistency を指標に）
        stats_df['stability_score'] = stats_df['avg_roi'] * stats_df['consistency']
        stats_df = stats_df.sort_values('stability_score', ascending=False)
        
        logging.info(f"{'フィルタ':>20} | {'平均ROI':>8} | {'標準偏差':>8} | {'安定性':>6} | {'総ベット':>8}")
        logging.info("-" * 70)
        
        for _, row in stats_df.head(10).iterrows():
            logging.info(f"{row['filter']:>20} | {row['avg_roi']:>7.1%} | {row['std_roi']:>7.1%} | {row['consistency']:>5.0%} | {row['total_bets']:>8}")
        
        # 最も安定したフィルタを特定
        if len(stats_df) > 0:
            best = stats_df.iloc[0]
            logging.info(f"\n【最も安定したフィルタ】: {best['filter']}")
            logging.info(f"  平均ROI: {best['avg_roi']:.1%}")
            logging.info(f"  ROI標準偏差: {best['std_roi']:.1%}")
            logging.info(f"  安定性（ROI>100%期間割合）: {best['consistency']:.0%}")
            logging.info(f"  総ベット数: {best['total_bets']}")
            
            if best['avg_roi'] >= 1.0 and best['consistency'] >= 0.5:
                logging.info("  ✅ 実運用に十分な安定性")
            else:
                logging.info("  ⚠️ さらなる改善が必要")
        
        return stats_df
    
    def run(self):
        """メイン実行"""
        # データ読み込み
        train_data, races_df = self.load_model_and_data()
        
        # 予測生成
        logging.info("\n予測生成中...")
        train_data = self.generate_predictions(train_data, races_df)
        
        # ウォークフォワード検証
        results_df = self.run_walkforward(train_data, start_year=2021, end_year=2024, period_months=6)
        
        # 安定性分析
        stats_df = self.analyze_stability(results_df)
        
        # 結果保存
        results_df.to_csv(self.output_dir / 'walkforward_results.csv', index=False)
        if stats_df is not None:
            stats_df.to_csv(self.output_dir / 'filter_stability.csv', index=False)
        
        logging.info(f"\n結果保存: {self.output_dir}")
        
        return results_df, stats_df


if __name__ == "__main__":
    validator = WalkForwardValidator()
    results_df, stats_df = validator.run()

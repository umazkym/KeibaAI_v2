#!/usr/bin/env python3
"""
Insights Kelly戦略のリーク/過学習検証スクリプト

【検証ポイント】
1. Test期間（2024年）のデータで「insights」を得た
2. そのinsightsを同じTest期間に適用した
→ これは「テストデータへの過学習」に相当

【正しい検証方法】
- Validation期間（2023年）でinsightsを導出
- Test期間（2024年）で検証
- insightsが時間を超えて持続するか確認

【日本語】
すべてのログ、コメントは日本語で記述。
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class LeakageValidator:
    """リーク/過学習検証クラス"""
    
    def __init__(self):
        self.models_dir = Path('keibaai/models')
        self.mu_model = None
        self.mu_features = None
    
    def load_model_and_data(self):
        """モデルとデータを読み込み"""
        logging.info("モデルとデータを読み込み中...")
        
        with open(self.models_dir / 'mu_v3_5/mu_v3_5_ranker.pkl', 'rb') as f:
            self.mu_model = pickle.load(f)
        with open(self.models_dir / 'mu_v3_5/feature_names.json', 'r', encoding='utf-8') as f:
            self.mu_features = json.load(f)
        
        df = pd.read_parquet(self.models_dir / 'mu_v3_3/train_data_mu_v3_3.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logging.info(f"データ読み込み完了: {len(df):,} rows")
        return df
    
    def prepare_predictions(self, df: pd.DataFrame) -> pd.DataFrame:
        """予測を準備"""
        if 'past_5_finish_position_mean' in df.columns and 'popularity' in df.columns:
            df['ability_rank'] = df.groupby('race_id')['past_5_finish_position_mean'].rank(ascending=True, method='first')
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
        
        for col in self.mu_features:
            if col not in df.columns:
                df[col] = 0
        
        df['mu_pred'] = self.mu_model.predict(df[self.mu_features])
        df['rank_pred'] = df.groupby('race_id')['mu_pred'].rank(ascending=False, method='first')
        
        return df
    
    def analyze_period(self, df: pd.DataFrame, period_name: str):
        """期間別の詳細分析"""
        logging.info(f"\n{'=' * 60}")
        logging.info(f"【{period_name}の詳細分析】")
        logging.info(f"{'=' * 60}")
        
        top1 = df[df['rank_pred'] == 1].copy()
        
        # オッズ帯別ROI
        logging.info("\n【オッズ帯別ROI】")
        odds_bins = [0, 2, 3, 5, 10, 20, 50, 100, 1000]
        odds_labels = ['~2倍', '2-3倍', '3-5倍', '5-10倍', '10-20倍', '20-50倍', '50-100倍', '100倍~']
        top1['odds_range'] = pd.cut(top1['win_odds'], bins=odds_bins, labels=odds_labels, right=False)
        
        for odds_range in odds_labels:
            subset = top1[top1['odds_range'] == odds_range]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                logging.info(f"  {odds_range}: ROI {roi:.1%} ({len(subset)}件)")
        
        # 人気別ROI
        logging.info("\n【人気別ROI】")
        for pop in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
            subset = top1[top1['popularity'] == pop]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                logging.info(f"  {pop}番人気: ROI {roi:.1%} ({len(subset)}件)")
        
        # 全体ROI
        hits = top1[top1['finish_position'] == 1]
        overall_roi = hits['win_odds'].sum() / len(top1) if len(top1) > 0 else 0
        logging.info(f"\n全体ROI: {overall_roi:.2%}")
        
        return top1
    
    def validate_insights_generalization(self, df: pd.DataFrame):
        """Insightsの汎化性を検証"""
        logging.info("\n" + "=" * 80)
        logging.info("【重要】Insights Kelly戦略のリーク/過学習検証")
        logging.info("=" * 80)
        
        logging.info("""
■ 問題点:
  1. 「10-50倍がROI高い」「5番/8番人気がROI高い」は
     2024年のTest期間を分析して得た知見（insights）
  
  2. その知見を同じ2024年に適用してROI 101.54%を達成
  
  3. これは「テストデータに対する過学習」と同じ問題
     → 将来のデータで同じ傾向が続く保証がない
        """)
        
        # Validation期間（2023年）の分析
        valid_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')].copy()
        valid_df = self.prepare_predictions(valid_df)
        
        logging.info("\n■ Validation期間（2023年）の傾向を分析:")
        self.analyze_period(valid_df, "Validation期間（2023年）")
        
        # Test期間（2024年）の分析
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        test_df = self.prepare_predictions(test_df)
        
        logging.info("\n■ Test期間（2024年）の傾向を分析:")
        self.analyze_period(test_df, "Test期間（2024年）")
        
        # 傾向の比較
        logging.info("\n" + "=" * 80)
        logging.info("【傾向の比較：2023年 vs 2024年】")
        logging.info("=" * 80)
        
        # 2023年の高ROIゾーンを特定
        logging.info("\n■ 2023年から導出すべきだったinsights:")
        
        valid_top1 = valid_df[valid_df['rank_pred'] == 1].copy()
        
        # オッズ帯別（2023年）
        logging.info("\n【2023年オッズ帯別】")
        for lower, upper in [(10, 20), (20, 50), (3, 5), (5, 10)]:
            subset = valid_top1[(valid_top1['win_odds'] >= lower) & (valid_top1['win_odds'] < upper)]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                logging.info(f"  {lower}-{upper}倍: ROI {roi:.1%}")
        
        # 人気別（2023年）
        logging.info("\n【2023年人気別（高ROI）】")
        for pop in [5, 8, 2, 3]:
            subset = valid_top1[valid_top1['popularity'] == pop]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                logging.info(f"  {pop}番人気: ROI {roi:.1%}")
        
        return valid_df, test_df
    
    def test_with_validation_insights(self, valid_df: pd.DataFrame, test_df: pd.DataFrame):
        """Validation期間のインサイトでTest期間を評価"""
        logging.info("\n" + "=" * 80)
        logging.info("【正しい検証】Validation(2023年)のinsightsでTest(2024年)を評価")
        logging.info("=" * 80)
        
        valid_top1 = valid_df[valid_df['rank_pred'] == 1].copy()
        test_top1 = test_df[test_df['rank_pred'] == 1].copy()
        
        # 2023年で高ROIだったゾーンを特定
        valid_roi_by_pop = {}
        for pop in range(1, 11):
            subset = valid_top1[valid_top1['popularity'] == pop]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                valid_roi_by_pop[pop] = hits['win_odds'].sum() / len(subset)
        
        # 2023年で最もROIが高かった人気順
        best_pops_2023 = sorted(valid_roi_by_pop.items(), key=lambda x: x[1], reverse=True)[:3]
        logging.info(f"\n2023年で高ROIだった人気: {[p for p, r in best_pops_2023]}")
        logging.info(f"  ROI: {[f'{r:.1%}' for p, r in best_pops_2023]}")
        
        # 2023年のinsightsで2024年を評価
        logging.info("\n■ 2023年insightsを2024年に適用した結果:")
        
        def calculate_insights_kelly_2023(row, base_fraction=0.25):
            """2023年のinsightsを使用"""
            p = row.get('win_prob_estimated', 0.2)
            odds = row['win_odds']
            pop = row.get('popularity', 5)
            
            if pd.isna(odds) or odds <= 1:
                return 0.0
            
            b = odds - 1
            kelly_full = (p * b - (1 - p)) / b
            
            if kelly_full <= 0:
                return 0.0
            
            # 2023年のinsightsに基づく調整（仮に2023年で高ROIだった条件を使う）
            adjustment = 1.0
            
            # 2023年で高ROIだった人気にボーナス
            best_pop_2023 = [p for p, r in best_pops_2023]
            if pop in best_pop_2023:
                adjustment += 0.3
            
            return min(kelly_full * base_fraction * adjustment, 0.15)
        
        # 勝率推定
        def softmax_win_prob(group):
            scores = group['mu_pred'].values
            temperature = 0.5
            exp_scores = np.exp(scores / temperature)
            probs = exp_scores / exp_scores.sum()
            return pd.Series(probs, index=group.index)
        
        test_df_copy = test_df.copy()
        test_df_copy['win_prob_estimated'] = test_df_copy.groupby('race_id').apply(softmax_win_prob).reset_index(level=0, drop=True)
        test_top1 = test_df_copy[test_df_copy['rank_pred'] == 1].copy()
        
        # 均等配分ROI
        hits = test_top1[test_top1['finish_position'] == 1]
        uniform_roi = hits['win_odds'].sum() / len(test_top1)
        logging.info(f"\n均等配分ROI（2024年）: {uniform_roi:.2%}")
        
        # 2023年insightsを適用
        test_top1['kelly_weight'] = test_top1.apply(
            lambda row: calculate_insights_kelly_2023(row, 0.25), 
            axis=1
        )
        
        total_weight = test_top1['kelly_weight'].sum()
        if total_weight > 0:
            test_top1['bet_amount'] = test_top1['kelly_weight'] / total_weight * len(test_top1)
            total_bet = test_top1['bet_amount'].sum()
            hits = test_top1[test_top1['finish_position'] == 1]
            kelly_roi = (hits['bet_amount'] * hits['win_odds']).sum() / total_bet
            logging.info(f"2023年insightsベースKelly ROI（2024年）: {kelly_roi:.2%}")
        
        return uniform_roi
    
    def run_validation(self):
        """検証実行"""
        df = self.load_model_and_data()
        valid_df, test_df = self.validate_insights_generalization(df)
        self.test_with_validation_insights(valid_df, test_df)
        
        logging.info("\n" + "=" * 80)
        logging.info("【結論】")
        logging.info("=" * 80)
        logging.info("""
■ リスク評価:

  1. 【データリーク】: なし
     - オッズ、人気、距離はレース前に確定する情報
     - 予測にはレース結果を使用していない
  
  2. 【過学習リスク】: 高い ⚠️
     - 「10-50倍がROI高い」「5番/8番人気がROI高い」は
       2024年Test期間の特徴であり、将来も続く保証がない
     - 2023年と2024年で「高ROIゾーン」は異なる可能性
  
  3. 【正しいアプローチ】:
     - Validation期間（2023年）でinsightsを導出
     - Test期間（2024年）で検証
     - または、insightsを使わず均等配分に留める
  
■ 推奨:
  - **101.54%は過信しないこと**
  - **v3.5均等配分（81.84%）を基準値とすべき**
  - Kelly配分を使う場合も、過去1年のinsightsで調整
        """)


if __name__ == "__main__":
    validator = LeakageValidator()
    validator.run_validation()

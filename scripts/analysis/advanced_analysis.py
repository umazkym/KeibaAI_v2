#!/usr/bin/env python3
"""
高度多次元分析スクリプト (advanced_analysis.py)

【目的】
保存されたall_predictions.parquetを使って、さらに深い分析を実施

【分析項目】
1. 多次元クロス分析（騎手勝率×オッズ帯×レースクラス）
2. 時系列分析（年ごとのパターン変化）
3. ROI・期待値シミュレーション
4. 特徴量間の相互作用分析
5. 予測確率分布と実際の勝率の乖離分析
6. 距離・馬場・開催場別分析
7. 具体的なケーススタディ

Author: KeibaAI Development Team
Date: 2026-01-10
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AdvancedAnalyzer:
    """高度分析クラス"""
    
    def __init__(self, data_path: str = 'keibaai/models/turf_dirt_separate/deep_analysis/all_predictions.parquet'):
        self.data_path = Path(data_path)
        self.output_dir = self.data_path.parent
        
        logger.info("データ読み込み中...")
        self.df = pd.read_parquet(self.data_path)
        logger.info(f"データ: {len(self.df):,}件")
        
        # 追加カラム
        self.df['jockey_tier'] = pd.cut(
            self.df['jockey_win_rate'].fillna(0), 
            bins=[0, 0.05, 0.10, 0.15, 0.20, 1.0],
            labels=['0-5%', '5-10%', '10-15%', '15-20%', '20%+']
        )
        
        self.df['pred_prob_tier'] = pd.cut(
            self.df['pred_prob'], 
            bins=[0, 0.05, 0.10, 0.15, 0.20, 0.30, 1.0],
            labels=['0-5%', '5-10%', '10-15%', '15-20%', '20-30%', '30%+']
        )
        
    def analyze_jockey_odds_cross(self):
        """1. 騎手勝率×オッズ帯のクロス分析"""
        logger.info("1. 騎手勝率×オッズ帯クロス分析")
        
        # 騎手勝率×オッズ帯での勝率・過大評価率
        cross = self.df.groupby(['jockey_tier', 'odds_tier'], observed=True).agg({
            'is_winner': ['mean', 'sum', 'count'],
            'is_overvalued': 'mean',
            'is_undervalued': 'mean',
            'pred_prob': 'mean',
        }).round(4)
        cross.columns = ['actual_win_rate', 'wins', 'count', 'overvalued_rate', 
                        'undervalued_rate', 'mean_pred_prob']
        
        # 期待値計算（単勝購入時のROI）
        roi_data = []
        for (jt, ot), group in self.df.groupby(['jockey_tier', 'odds_tier'], observed=True):
            winners = group[group['is_winner']]
            total_bet = len(group)
            total_return = winners['win_odds'].sum() if len(winners) > 0 else 0
            roi = total_return / total_bet if total_bet > 0 else 0
            roi_data.append({
                'jockey_tier': jt,
                'odds_tier': ot,
                'roi': roi,
                'count': total_bet
            })
        
        roi_df = pd.DataFrame(roi_data)
        
        print("\n" + "="*100)
        print("【騎手勝率×オッズ帯 クロス分析】")
        print("="*100)
        print("\n各セルの勝率とROI:")
        
        # ピボットテーブル形式で表示
        pivot_win = self.df.pivot_table(
            values='is_winner', 
            index='jockey_tier', 
            columns='odds_tier', 
            aggfunc='mean',
            observed=True
        ).round(4) * 100
        
        print("\n勝率(%):")
        print(pivot_win.to_string())
        
        pivot_roi = roi_df.pivot_table(
            values='roi', 
            index='jockey_tier', 
            columns='odds_tier',
            observed=True
        ).round(2) * 100
        
        print("\nROI(%):")
        print(pivot_roi.to_string())
        
        # 最も期待値が高い/低いセルを特定
        roi_df_sorted = roi_df.sort_values('roi', ascending=False)
        print("\n【期待値が高いセル】")
        for _, row in roi_df_sorted.head(5).iterrows():
            print(f"  騎手{row['jockey_tier']} × オッズ{row['odds_tier']}: ROI {row['roi']*100:.1f}% (n={row['count']:,})")
        
        print("\n【期待値が低いセル】")
        for _, row in roi_df_sorted.tail(5).iterrows():
            print(f"  騎手{row['jockey_tier']} × オッズ{row['odds_tier']}: ROI {row['roi']*100:.1f}% (n={row['count']:,})")
        
        return cross, roi_df
    
    def analyze_prediction_calibration(self):
        """2. 予測確率のキャリブレーション分析"""
        logger.info("2. 予測確率キャリブレーション分析")
        
        print("\n" + "="*100)
        print("【予測確率 vs 実際の勝率 キャリブレーション】")
        print("="*100)
        
        # 予測確率を10%刻みでビン分け
        self.df['pred_prob_bin'] = pd.cut(
            self.df['pred_prob'], 
            bins=[0, 0.02, 0.05, 0.10, 0.15, 0.20, 0.30, 0.50, 1.0],
            labels=['0-2%', '2-5%', '5-10%', '10-15%', '15-20%', '20-30%', '30-50%', '50%+']
        )
        
        calib = self.df.groupby('pred_prob_bin', observed=True).agg({
            'is_winner': ['mean', 'sum', 'count'],
            'pred_prob': 'mean',
            'win_odds': 'mean',
        })
        calib.columns = ['actual_win_rate', 'wins', 'count', 'mean_pred_prob', 'mean_odds']
        
        print("\n予測確率帯 | 予測勝率 | 実際勝率 | 乖離 | 平均オッズ | 件数")
        print("-" * 80)
        for idx, row in calib.iterrows():
            gap = (row['actual_win_rate'] - row['mean_pred_prob']) * 100
            gap_str = f"{gap:+.2f}%" if not pd.isna(gap) else "N/A"
            print(f"  {idx:8} | {row['mean_pred_prob']*100:6.2f}% | {row['actual_win_rate']*100:6.2f}% | {gap_str:>7} | {row['mean_odds']:7.1f}倍 | {int(row['count']):,}")
        
        # キャリブレーションの問題点を特定
        print("\n【キャリブレーション問題の特定】")
        for idx, row in calib.iterrows():
            gap = row['actual_win_rate'] - row['mean_pred_prob']
            if abs(gap) > 0.02:  # 2%以上の乖離
                if gap > 0:
                    print(f"  ⚠ {idx}: 予測{row['mean_pred_prob']*100:.1f}% → 実際{row['actual_win_rate']*100:.1f}% (過小評価)")
                else:
                    print(f"  ⚠ {idx}: 予測{row['mean_pred_prob']*100:.1f}% → 実際{row['actual_win_rate']*100:.1f}% (過大評価)")
        
        return calib
    
    def analyze_temporal_trends(self):
        """3. 時系列分析（年ごとのパターン変化）"""
        logger.info("3. 時系列分析")
        
        print("\n" + "="*100)
        print("【年ごとのパターン変化】")
        print("="*100)
        
        yearly = self.df.groupby('year').agg({
            'is_winner': 'mean',
            'is_undervalued': 'mean',
            'is_overvalued': 'mean',
            'race_id': 'nunique',
            'horse_number': 'count',
        })
        yearly.columns = ['win_rate', 'undervalued_rate', 'overvalued_rate', 'n_races', 'n_horses']
        
        # 年ごとのROI計算
        yearly_roi = []
        for year, group in self.df.groupby('year'):
            # Top1予測の馬のROI
            top1 = group[group['pred_rank'] == 1]
            if len(top1) > 0:
                winners = top1[top1['is_winner']]
                roi_top1 = winners['win_odds'].sum() / len(top1) if len(top1) > 0 else 0
            else:
                roi_top1 = 0
            
            # 人気1番のROI
            pop1 = group[group['odds_rank'] == 1]
            if len(pop1) > 0:
                pop1_winners = pop1[pop1['is_winner']]
                roi_pop1 = pop1_winners['win_odds'].sum() / len(pop1) if len(pop1) > 0 else 0
            else:
                roi_pop1 = 0
            
            yearly_roi.append({
                'year': year,
                'roi_pred_top1': roi_top1,
                'roi_odds_top1': roi_pop1,
            })
        
        yearly_roi_df = pd.DataFrame(yearly_roi).set_index('year')
        yearly = yearly.join(yearly_roi_df)
        
        print("\n年 | 過小評価率 | 過大評価率 | 予測Top1 ROI | 人気1 ROI | レース数")
        print("-" * 80)
        for idx, row in yearly.iterrows():
            print(f"  {idx} | {row['undervalued_rate']*100:7.2f}% | {row['overvalued_rate']*100:7.2f}% | {row['roi_pred_top1']*100:9.1f}% | {row['roi_odds_top1']*100:7.1f}% | {int(row['n_races']):,}")
        
        return yearly
    
    def analyze_distance_venue(self):
        """4. 距離・会場別分析"""
        logger.info("4. 距離・会場別分析")
        
        print("\n" + "="*100)
        print("【距離帯別分析】")
        print("="*100)
        
        # 距離帯
        self.df['distance_tier'] = pd.cut(
            self.df['distance'].fillna(0), 
            bins=[0, 1200, 1600, 2000, 2400, 9999],
            labels=['短距離(~1200)', 'マイル(~1600)', '中距離(~2000)', '中長距離(~2400)', '長距離(2400+)']
        )
        
        dist = self.df.groupby('distance_tier', observed=True).agg({
            'is_winner': 'mean',
            'is_undervalued': 'mean',
            'is_overvalued': 'mean',
            'horse_number': 'count',
        })
        dist.columns = ['win_rate', 'undervalued_rate', 'overvalued_rate', 'count']
        
        print("\n距離帯 | 過小評価率 | 過大評価率 | 件数")
        print("-" * 60)
        for idx, row in dist.iterrows():
            print(f"  {idx:15} | {row['undervalued_rate']*100:7.2f}% | {row['overvalued_rate']*100:7.2f}% | {int(row['count']):,}")
        
        return dist
    
    def analyze_feature_interactions(self):
        """5. 特徴量間の相互作用分析"""
        logger.info("5. 特徴量相互作用分析")
        
        print("\n" + "="*100)
        print("【特徴量の相互作用分析】")
        print("="*100)
        
        # 騎手勝率 × 馬過去成績
        self.df['horse_quality'] = pd.cut(
            self.df['horse_avg_finish'].fillna(99), 
            bins=[0, 4, 6, 8, 99],
            labels=['優秀(~4着)', '良好(~6着)', '普通(~8着)', '低調(8着+)']
        )
        
        cross_jockey_horse = self.df.groupby(['jockey_tier', 'horse_quality'], observed=True).agg({
            'is_winner': 'mean',
            'is_overvalued': 'mean',
            'horse_number': 'count',
        }).round(4)
        cross_jockey_horse.columns = ['win_rate', 'overvalued_rate', 'count']
        
        print("\n【騎手勝率帯 × 馬過去成績 の過大評価率】")
        pivot = self.df.pivot_table(
            values='is_overvalued', 
            index='jockey_tier', 
            columns='horse_quality', 
            aggfunc='mean',
            observed=True
        )
        print((pivot * 100).round(2).to_string())
        
        print("\n【同上の勝率】")
        pivot_win = self.df.pivot_table(
            values='is_winner', 
            index='jockey_tier', 
            columns='horse_quality', 
            aggfunc='mean',
            observed=True
        )
        print((pivot_win * 100).round(2).to_string())
        
        # 最も問題のある組み合わせ
        print("\n【問題パターンの特定】")
        for (jt, hq), row in cross_jockey_horse.iterrows():
            if row['overvalued_rate'] > 0.15 and row['count'] > 100:
                print(f"  ⚠ 騎手{jt} × 馬{hq}: 過大評価率{row['overvalued_rate']*100:.1f}%, 実勝率{row['win_rate']*100:.1f}% (n={int(row['count'])})")
        
        return cross_jockey_horse
    
    def analyze_roi_optimization(self):
        """6. ROI最適化分析（どの条件で賭けると得か）"""
        logger.info("6. ROI最適化分析")
        
        print("\n" + "="*100)
        print("【ROI最適化分析】")
        print("="*100)
        
        # 予測順位×オッズ帯のROI
        roi_by_pred_odds = []
        for pred_rank in [1, 2, 3]:
            for odds_tier in ['1-3', '3-10', '10-50', '50-150', '150+']:
                subset = self.df[(self.df['pred_rank'] == pred_rank) & (self.df['odds_tier'] == odds_tier)]
                if len(subset) > 0:
                    winners = subset[subset['is_winner']]
                    roi = winners['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                    win_rate = len(winners) / len(subset)
                    roi_by_pred_odds.append({
                        'pred_rank': pred_rank,
                        'odds_tier': odds_tier,
                        'roi': roi,
                        'win_rate': win_rate,
                        'count': len(subset)
                    })
        
        roi_pred_odds_df = pd.DataFrame(roi_by_pred_odds)
        
        print("\n【予測順位×オッズ帯のROI】")
        pivot_roi = roi_pred_odds_df.pivot_table(
            values='roi', 
            index='pred_rank', 
            columns='odds_tier'
        )
        print((pivot_roi * 100).round(1).to_string())
        
        print("\n【高ROIの購入条件】（ROI > 90%）")
        good = roi_pred_odds_df[roi_pred_odds_df['roi'] > 0.9].sort_values('roi', ascending=False)
        for _, row in good.iterrows():
            print(f"  ✓ 予測{int(row['pred_rank'])}位 × オッズ{row['odds_tier']}: ROI {row['roi']*100:.1f}%, 勝率{row['win_rate']*100:.1f}% (n={int(row['count'])})")
        
        print("\n【低ROIの購入条件】（ROI < 70%）")
        bad = roi_pred_odds_df[roi_pred_odds_df['roi'] < 0.7].sort_values('roi')
        for _, row in bad.head(5).iterrows():
            print(f"  ✗ 予測{int(row['pred_rank'])}位 × オッズ{row['odds_tier']}: ROI {row['roi']*100:.1f}%, 勝率{row['win_rate']*100:.1f}% (n={int(row['count'])})")
        
        # 騎手勝率でフィルタした場合のROI
        print("\n【騎手勝率フィルタ後のROI】")
        for jt in ['0-5%', '5-10%', '10-15%', '15-20%', '20%+']:
            subset = self.df[(self.df['pred_rank'] == 1) & (self.df['jockey_tier'] == jt)]
            if len(subset) > 0:
                winners = subset[subset['is_winner']]
                roi = winners['win_odds'].sum() / len(subset)
                print(f"  予測1位×騎手{jt}: ROI {roi*100:.1f}% (n={len(subset):,})")
        
        return roi_pred_odds_df
    
    def analyze_specific_cases(self):
        """7. 具体的なケーススタディ"""
        logger.info("7. ケーススタディ")
        
        print("\n" + "="*100)
        print("【具体的なケーススタディ】")
        print("="*100)
        
        # 過大評価が顕著な例（高騎手勝率×低オッズ×低着順）
        overvalued_cases = self.df[
            (self.df['jockey_win_rate'] >= 0.15) & 
            (self.df['win_odds'] <= 5) & 
            (self.df['finish_position'] >= 6) &
            (self.df['pred_rank'] <= 3)
        ].copy()
        
        print(f"\n【過大評価の典型例】")
        print(f"条件: 騎手勝率15%+, オッズ5倍以下, 予測Top3, 実着順6位以下")
        print(f"該当数: {len(overvalued_cases):,}件")
        
        if len(overvalued_cases) > 0:
            sample = overvalued_cases.sample(min(5, len(overvalued_cases)), random_state=42)
            for _, row in sample.iterrows():
                print(f"  レース{row['race_id']}: 予測{int(row['pred_rank'])}位, オッズ{row['win_odds']:.1f}倍 → 実着{int(row['finish_position'])}位")
                print(f"    騎手勝率={row['jockey_win_rate']*100:.1f}%, 馬勝率={row['horse_win_rate']*100:.1f}%, 前走={row['prev_finish']}")
        
        # 過小評価が顕著な例（低騎手勝率×中オッズ×好着順）
        undervalued_cases = self.df[
            (self.df['jockey_win_rate'] <= 0.06) & 
            (self.df['win_odds'] >= 5) & 
            (self.df['win_odds'] <= 20) & 
            (self.df['finish_position'] <= 3) &
            (self.df['pred_rank'] >= 6)
        ].copy()
        
        print(f"\n【過小評価の典型例】")
        print(f"条件: 騎手勝率6%以下, オッズ5-20倍, 予測6位以下, 実着順3位以内")
        print(f"該当数: {len(undervalued_cases):,}件")
        
        if len(undervalued_cases) > 0:
            sample = undervalued_cases.sample(min(5, len(undervalued_cases)), random_state=42)
            for _, row in sample.iterrows():
                print(f"  レース{row['race_id']}: 予測{int(row['pred_rank'])}位, オッズ{row['win_odds']:.1f}倍 → 実着{int(row['finish_position'])}位")
                print(f"    騎手勝率={row['jockey_win_rate']*100:.1f}%, 馬勝率={row['horse_win_rate']*100:.1f}% if notNaN else N/A, 前走={row['prev_finish']}")
        
        return overvalued_cases, undervalued_cases
    
    def analyze_improvement_potential(self):
        """8. 改善ポテンシャル分析"""
        logger.info("8. 改善ポテンシャル分析")
        
        print("\n" + "="*100)
        print("【改善ポテンシャル分析】")
        print("="*100)
        
        # 現状のROI（予測1位を購入）
        top1 = self.df[self.df['pred_rank'] == 1]
        current_roi = top1[top1['is_winner']]['win_odds'].sum() / len(top1)
        
        print(f"\n現状: 予測1位を全購入 → ROI {current_roi*100:.1f}%")
        
        # フィルタ1: 騎手勝率20%+を除外
        filtered1 = self.df[(self.df['pred_rank'] == 1) & (self.df['jockey_win_rate'] < 0.20)]
        roi1 = filtered1[filtered1['is_winner']]['win_odds'].sum() / len(filtered1) if len(filtered1) > 0 else 0
        print(f"改善案1: 騎手勝率20%+を除外 → ROI {roi1*100:.1f}% (対象{len(filtered1):,}件)")
        
        # フィルタ2: オッズ3倍以下を除外
        filtered2 = self.df[(self.df['pred_rank'] == 1) & (self.df['win_odds'] > 3)]
        roi2 = filtered2[filtered2['is_winner']]['win_odds'].sum() / len(filtered2) if len(filtered2) > 0 else 0
        print(f"改善案2: オッズ3倍以下を除外 → ROI {roi2*100:.1f}% (対象{len(filtered2):,}件)")
        
        # フィルタ3: 騎手勝率5-10%帯に限定
        filtered3 = self.df[(self.df['pred_rank'] == 1) & (self.df['jockey_tier'] == '5-10%')]
        roi3 = filtered3[filtered3['is_winner']]['win_odds'].sum() / len(filtered3) if len(filtered3) > 0 else 0
        print(f"改善案3: 騎手勝率5-10%帯に限定 → ROI {roi3*100:.1f}% (対象{len(filtered3):,}件)")
        
        # フィルタ4: 予測1位かつオッズ10-50倍
        filtered4 = self.df[(self.df['pred_rank'] == 1) & (self.df['odds_tier'] == '10-50')]
        roi4 = filtered4[filtered4['is_winner']]['win_odds'].sum() / len(filtered4) if len(filtered4) > 0 else 0
        print(f"改善案4: 予測1位×オッズ10-50倍 → ROI {roi4*100:.1f}% (対象{len(filtered4):,}件)")
        
        # 複合フィルタ
        filtered5 = self.df[
            (self.df['pred_rank'] <= 3) & 
            (self.df['odds_tier'].isin(['10-50', '50-150'])) &
            (self.df['jockey_tier'].isin(['5-10%', '0-5%']))
        ]
        roi5 = filtered5[filtered5['is_winner']]['win_odds'].sum() / len(filtered5) if len(filtered5) > 0 else 0
        print(f"改善案5: 予測Top3×オッズ10-150倍×騎手勝率10%以下 → ROI {roi5*100:.1f}% (対象{len(filtered5):,}件)")
        
        return {
            'current': current_roi,
            'excluding_top_jockeys': roi1,
            'excluding_low_odds': roi2,
            'mid_jockeys_only': roi3,
            'pred1_mid_odds': roi4,
            'complex_filter': roi5,
        }
    
    def run_all(self):
        """全分析実行"""
        print("\n" + "="*100)
        print("高度多次元分析レポート")
        print(f"データ: {len(self.df):,}頭")
        print("="*100)
        
        results = {}
        results['jockey_odds_cross'] = self.analyze_jockey_odds_cross()
        results['calibration'] = self.analyze_prediction_calibration()
        results['temporal'] = self.analyze_temporal_trends()
        results['distance'] = self.analyze_distance_venue()
        results['feature_interactions'] = self.analyze_feature_interactions()
        results['roi_optimization'] = self.analyze_roi_optimization()
        results['case_studies'] = self.analyze_specific_cases()
        results['improvement'] = self.analyze_improvement_potential()
        
        logger.info("全分析完了")
        return results


if __name__ == "__main__":
    analyzer = AdvancedAnalyzer()
    results = analyzer.run_all()

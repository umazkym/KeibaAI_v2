#!/usr/bin/env python3
"""
包括的多因子分析スクリプト (comprehensive_factor_analysis.py)

【目的】
騎手以外の多くの因子を分析し、予測精度向上のヒントを得る

【分析因子】
1. 調教師成績
2. 馬の年齢
3. 前走からの距離変化
4. 休養期間
5. 馬体重変化
6. 頭数（多頭数vs少頭数）
7. 馬場状態
8. レース番号（早い/遅い）
9. 前走着順
10. 枠番
11. リーク検証

Author: KeibaAI Development Team
Date: 2026-01-10
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ComprehensiveFactorAnalyzer:
    """包括的多因子分析"""
    
    def __init__(self, data_path: str = 'keibaai/models/turf_dirt_separate/deep_analysis/all_predictions.parquet'):
        self.data_path = Path(data_path)
        
        logger.info("データ読み込み中...")
        self.df = pd.read_parquet(self.data_path)
        logger.info(f"データ: {len(self.df):,}件")
        
        # 追加データの読み込み（元データから）
        races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
        self.full_races = pd.read_parquet(races_path)
        
        # 追加カラムのマージ
        self._add_extra_columns()
    
    def _add_extra_columns(self):
        """追加分析用カラムの追加"""
        # 年齢
        if 'age' not in self.df.columns:
            self.df = self.df.merge(
                self.full_races[['race_id', 'horse_number', 'age', 'sex', 'bracket_number', 'horse_weight']],
                on=['race_id', 'horse_number'],
                how='left',
                suffixes=('', '_full')
            )
        
        # 調教師勝率帯
        self.df['trainer_tier'] = pd.cut(
            self.df['trainer_win_rate'].fillna(0), 
            bins=[0, 0.05, 0.08, 0.12, 1.0],
            labels=['0-5%', '5-8%', '8-12%', '12%+']
        )
        
        # 前走着順帯
        self.df['prev_finish_tier'] = pd.cut(
            self.df['prev_finish'].fillna(99), 
            bins=[0, 3, 6, 10, 99],
            labels=['好走(1-3)', '中間(4-6)', '凡走(7-10)', '大敗(10+)']
        )
        
        # 休養期間帯
        self.df['rest_tier'] = pd.cut(
            self.df['days_since_last'].fillna(999), 
            bins=[0, 30, 60, 120, 999],
            labels=['短期(~30日)', '中期(~60日)', '長期(~120日)', '超長期(120日+)']
        )
        
        # 頭数帯
        self.df['field_tier'] = pd.cut(
            self.df['field_size'].fillna(0), 
            bins=[0, 10, 14, 18, 99],
            labels=['少頭数(~10)', '中頭数(~14)', '多頭数(~18)', '超多頭数']
        )
    
    def verify_no_leakage(self):
        """リーク検証"""
        logger.info("リーク検証")
        
        print("\n" + "="*100)
        print("【リーク検証】")
        print("="*100)
        
        # 1. 予測確率が異常に高い馬を調査
        high_prob = self.df[self.df['pred_prob'] > 0.5]
        print(f"\n予測確率50%超: {len(high_prob):,}件 ({len(high_prob)/len(self.df)*100:.2f}%)")
        if len(high_prob) > 0:
            win_rate = high_prob['is_winner'].mean()
            print(f"  実際の勝率: {win_rate*100:.1f}%")
            print(f"  → 予測50%>に対して実際{win_rate*100:.1f}%なら適正")
        
        # 2. 予測1位のオッズ分布
        top1 = self.df[self.df['pred_rank'] == 1]
        print(f"\n予測1位のオッズ分布:")
        print(f"  平均: {top1['win_odds'].mean():.1f}倍")
        print(f"  中央値: {top1['win_odds'].median():.1f}倍")
        print(f"  1-3倍: {len(top1[top1['win_odds'] <= 3]):,}件 ({len(top1[top1['win_odds'] <= 3])/len(top1)*100:.1f}%)")
        print(f"  10倍超: {len(top1[top1['win_odds'] > 10]):,}件 ({len(top1[top1['win_odds'] > 10])/len(top1)*100:.1f}%)")
        
        # 3. 予測と人気の不一致ケース
        disagree = self.df[(self.df['pred_rank'] <= 3) & (self.df['odds_rank'] >= 6)]
        print(f"\n予測Top3 かつ 人気6位以下: {len(disagree):,}件")
        if len(disagree) > 0:
            win_rate = disagree['is_winner'].mean()
            print(f"  実際の勝率: {win_rate*100:.2f}%")
            print(f"  → これが異常に高ければリーク疑い")
        
        # 4. 年ごとの予測精度が一定か
        print(f"\n年ごとの予測1位勝率:")
        for year in sorted(self.df['year'].unique()):
            year_top1 = self.df[(self.df['year'] == year) & (self.df['pred_rank'] == 1)]
            win_rate = year_top1['is_winner'].mean()
            print(f"  {year}: {win_rate*100:.1f}%")
    
    def analyze_trainer_patterns(self):
        """調教師パターン分析"""
        logger.info("調教師分析")
        
        print("\n" + "="*100)
        print("【調教師勝率帯別分析】")
        print("="*100)
        
        trainer = self.df.groupby('trainer_tier', observed=True).agg({
            'is_winner': 'mean',
            'is_undervalued': 'mean',
            'is_overvalued': 'mean',
            'horse_number': 'count',
        })
        trainer.columns = ['win_rate', 'undervalued', 'overvalued', 'count']
        
        # ROI計算
        for tier in trainer.index:
            subset = self.df[(self.df['trainer_tier'] == tier) & (self.df['pred_rank'] == 1)]
            if len(subset) > 0:
                winners = subset[subset['is_winner']]
                roi = winners['win_odds'].sum() / len(subset)
                trainer.loc[tier, 'pred1_roi'] = roi
        
        print("\n調教師勝率帯 | 実勝率 | 過小評価 | 過大評価 | 予測1位ROI | 件数")
        print("-" * 80)
        for idx, row in trainer.iterrows():
            roi_str = f"{row['pred1_roi']*100:.1f}%" if pd.notna(row.get('pred1_roi')) else 'N/A'
            print(f"  {idx:10} | {row['win_rate']*100:5.2f}% | {row['undervalued']*100:6.2f}% | {row['overvalued']*100:6.2f}% | {roi_str:>10} | {int(row['count']):,}")
        
        return trainer
    
    def analyze_prev_finish_patterns(self):
        """前走着順パターン分析"""
        logger.info("前走着順分析")
        
        print("\n" + "="*100)
        print("【前走着順帯別分析】")
        print("="*100)
        
        prev = self.df.groupby('prev_finish_tier', observed=True).agg({
            'is_winner': 'mean',
            'is_undervalued': 'mean',
            'is_overvalued': 'mean',
            'horse_number': 'count',
        })
        prev.columns = ['win_rate', 'undervalued', 'overvalued', 'count']
        
        for tier in prev.index:
            subset = self.df[(self.df['prev_finish_tier'] == tier) & (self.df['pred_rank'] == 1)]
            if len(subset) > 0:
                winners = subset[subset['is_winner']]
                roi = winners['win_odds'].sum() / len(subset)
                prev.loc[tier, 'pred1_roi'] = roi
        
        print("\n前走着順帯 | 実勝率 | 過小評価 | 過大評価 | 予測1位ROI | 件数")
        print("-" * 80)
        for idx, row in prev.iterrows():
            roi_str = f"{row['pred1_roi']*100:.1f}%" if pd.notna(row.get('pred1_roi')) else 'N/A'
            print(f"  {idx:12} | {row['win_rate']*100:5.2f}% | {row['undervalued']*100:6.2f}% | {row['overvalued']*100:6.2f}% | {roi_str:>10} | {int(row['count']):,}")
        
        return prev
    
    def analyze_rest_patterns(self):
        """休養期間パターン分析"""
        logger.info("休養期間分析")
        
        print("\n" + "="*100)
        print("【休養期間帯別分析】")
        print("="*100)
        
        rest = self.df.groupby('rest_tier', observed=True).agg({
            'is_winner': 'mean',
            'is_undervalued': 'mean',
            'is_overvalued': 'mean',
            'horse_number': 'count',
        })
        rest.columns = ['win_rate', 'undervalued', 'overvalued', 'count']
        
        for tier in rest.index:
            subset = self.df[(self.df['rest_tier'] == tier) & (self.df['pred_rank'] == 1)]
            if len(subset) > 0:
                winners = subset[subset['is_winner']]
                roi = winners['win_odds'].sum() / len(subset)
                rest.loc[tier, 'pred1_roi'] = roi
        
        print("\n休養期間帯 | 実勝率 | 過小評価 | 過大評価 | 予測1位ROI | 件数")
        print("-" * 80)
        for idx, row in rest.iterrows():
            roi_str = f"{row['pred1_roi']*100:.1f}%" if pd.notna(row.get('pred1_roi')) else 'N/A'
            print(f"  {idx:15} | {row['win_rate']*100:5.2f}% | {row['undervalued']*100:6.2f}% | {row['overvalued']*100:6.2f}% | {roi_str:>10} | {int(row['count']):,}")
        
        return rest
    
    def analyze_field_size_patterns(self):
        """頭数パターン分析"""
        logger.info("頭数分析")
        
        print("\n" + "="*100)
        print("【頭数帯別分析】")
        print("="*100)
        
        field = self.df.groupby('field_tier', observed=True).agg({
            'is_winner': 'mean',
            'is_undervalued': 'mean',
            'is_overvalued': 'mean',
            'horse_number': 'count',
        })
        field.columns = ['win_rate', 'undervalued', 'overvalued', 'count']
        
        for tier in field.index:
            subset = self.df[(self.df['field_tier'] == tier) & (self.df['pred_rank'] == 1)]
            if len(subset) > 0:
                winners = subset[subset['is_winner']]
                roi = winners['win_odds'].sum() / len(subset)
                field.loc[tier, 'pred1_roi'] = roi
        
        print("\n頭数帯 | 実勝率 | 過小評価 | 過大評価 | 予測1位ROI | 件数")
        print("-" * 80)
        for idx, row in field.iterrows():
            roi_str = f"{row['pred1_roi']*100:.1f}%" if pd.notna(row.get('pred1_roi')) else 'N/A'
            print(f"  {idx:15} | {row['win_rate']*100:5.2f}% | {row['undervalued']*100:6.2f}% | {row['overvalued']*100:6.2f}% | {roi_str:>10} | {int(row['count']):,}")
        
        return field
    
    def analyze_track_condition(self):
        """馬場状態分析"""
        logger.info("馬場状態分析")
        
        print("\n" + "="*100)
        print("【馬場状態別分析】")
        print("="*100)
        
        track = self.df.groupby('track_condition', observed=True).agg({
            'is_winner': 'mean',
            'is_undervalued': 'mean',
            'is_overvalued': 'mean',
            'horse_number': 'count',
        })
        track.columns = ['win_rate', 'undervalued', 'overvalued', 'count']
        
        for cond in track.index:
            subset = self.df[(self.df['track_condition'] == cond) & (self.df['pred_rank'] == 1)]
            if len(subset) > 0:
                winners = subset[subset['is_winner']]
                roi = winners['win_odds'].sum() / len(subset)
                track.loc[cond, 'pred1_roi'] = roi
        
        print("\n馬場状態 | 実勝率 | 過小評価 | 過大評価 | 予測1位ROI | 件数")
        print("-" * 80)
        for idx, row in track.iterrows():
            if idx is None:
                continue
            roi_str = f"{row['pred1_roi']*100:.1f}%" if pd.notna(row.get('pred1_roi')) else 'N/A'
            print(f"  {idx:8} | {row['win_rate']*100:5.2f}% | {row['undervalued']*100:6.2f}% | {row['overvalued']*100:6.2f}% | {roi_str:>10} | {int(row['count']):,}")
        
        return track
    
    def analyze_multi_factor_cross(self):
        """多因子クロス分析"""
        logger.info("多因子クロス分析")
        
        print("\n" + "="*100)
        print("【多因子クロス分析】")
        print("="*100)
        
        # 前走着順 × 休養期間
        print("\n【前走着順 × 休養期間 の過小評価率】")
        pivot = self.df.pivot_table(
            values='is_undervalued', 
            index='prev_finish_tier', 
            columns='rest_tier', 
            aggfunc='mean',
            observed=True
        )
        print((pivot * 100).round(2).to_string())
        
        # 前走着順 × 頭数
        print("\n【前走着順 × 頭数 の勝率】")
        pivot2 = self.df.pivot_table(
            values='is_winner', 
            index='prev_finish_tier', 
            columns='field_tier', 
            aggfunc='mean',
            observed=True
        )
        print((pivot2 * 100).round(2).to_string())
        
        # 調教師 × 前走着順
        print("\n【調教師勝率帯 × 前走着順 の過大評価率】")
        pivot3 = self.df.pivot_table(
            values='is_overvalued', 
            index='trainer_tier', 
            columns='prev_finish_tier', 
            aggfunc='mean',
            observed=True
        )
        print((pivot3 * 100).round(2).to_string())
    
    def find_high_value_conditions(self):
        """高価値条件の発見"""
        logger.info("高価値条件探索")
        
        print("\n" + "="*100)
        print("【高価値条件の発見】")
        print("予測1位で購入した場合のROI > 120%の条件")
        print("="*100)
        
        conditions = []
        
        # 単一条件でのROI計算
        for factor, tiers in [
            ('trainer_tier', self.df['trainer_tier'].unique()),
            ('prev_finish_tier', self.df['prev_finish_tier'].unique()),
            ('rest_tier', self.df['rest_tier'].unique()),
            ('field_tier', self.df['field_tier'].unique()),
            ('track_condition', self.df['track_condition'].unique()),
            ('odds_tier', self.df['odds_tier'].unique()),
            ('race_class', self.df['race_class'].unique()),
        ]:
            for tier in tiers:
                if pd.isna(tier):
                    continue
                subset = self.df[(self.df[factor] == tier) & (self.df['pred_rank'] == 1)]
                if len(subset) >= 50:  # 最低50件
                    winners = subset[subset['is_winner']]
                    roi = winners['win_odds'].sum() / len(subset)
                    win_rate = len(winners) / len(subset)
                    if roi > 1.0:  # ROI > 100%
                        conditions.append({
                            'factor': factor,
                            'value': tier,
                            'roi': roi,
                            'win_rate': win_rate,
                            'count': len(subset)
                        })
        
        # ROIでソート
        conditions.sort(key=lambda x: x['roi'], reverse=True)
        
        print("\n【単一条件での高ROI】")
        for c in conditions[:15]:
            print(f"  {c['factor']}={c['value']}: ROI {c['roi']*100:.1f}%, 勝率{c['win_rate']*100:.1f}% (n={c['count']})")
        
        # 複合条件
        print("\n【複合条件での高ROI探索】")
        complex_conditions = []
        
        for odds in ['10-50', '3-10']:
            for prev in ['好走(1-3)', '中間(4-6)']:
                for rest in ['短期(~30日)', '中期(~60日)']:
                    subset = self.df[
                        (self.df['odds_tier'] == odds) &
                        (self.df['prev_finish_tier'] == prev) &
                        (self.df['rest_tier'] == rest) &
                        (self.df['pred_rank'] == 1)
                    ]
                    if len(subset) >= 20:
                        winners = subset[subset['is_winner']]
                        roi = winners['win_odds'].sum() / len(subset)
                        win_rate = len(winners) / len(subset)
                        if roi > 1.0:
                            complex_conditions.append({
                                'condition': f"オッズ{odds} × 前走{prev} × 休養{rest}",
                                'roi': roi,
                                'win_rate': win_rate,
                                'count': len(subset)
                            })
        
        complex_conditions.sort(key=lambda x: x['roi'], reverse=True)
        for c in complex_conditions[:10]:
            print(f"  {c['condition']}: ROI {c['roi']*100:.1f}%, 勝率{c['win_rate']*100:.1f}% (n={c['count']})")
        
        return conditions, complex_conditions
    
    def analyze_prediction_miss_patterns(self):
        """予測ミスパターンの詳細分析"""
        logger.info("予測ミスパターン分析")
        
        print("\n" + "="*100)
        print("【予測ミスパターンの深層分析】")
        print("="*100)
        
        # 1. 予測1位が負けたケース
        top1_lost = self.df[(self.df['pred_rank'] == 1) & (~self.df['is_winner'])]
        print(f"\n予測1位が負けたケース: {len(top1_lost):,}件")
        
        # このケースの特徴
        print("\n  【負けた予測1位の特徴量分布】")
        for col in ['horse_win_rate', 'jockey_win_rate', 'trainer_win_rate', 'prev_finish', 'days_since_last']:
            if col in top1_lost.columns:
                mean_val = top1_lost[col].mean()
                miss_rate = top1_lost[col].isna().mean()
                print(f"    {col:20}: 平均={mean_val:.3f}, 欠損率={miss_rate*100:.1f}%")
        
        # 2. 予測1位が負けた時の実際の勝者
        print("\n  【予測1位が負けた時、実際の勝者は予測何位だったか】")
        top1_lost_races = top1_lost['race_id'].unique()
        actual_winners_in_those_races = self.df[
            (self.df['race_id'].isin(top1_lost_races)) & 
            (self.df['is_winner'])
        ]
        winner_pred_rank = actual_winners_in_those_races['pred_rank'].value_counts().sort_index()
        total_lost = len(top1_lost_races)
        for rank, count in winner_pred_rank.head(10).items():
            print(f"    予測{int(rank)}位が勝利: {count}回 ({count/total_lost*100:.1f}%)")
        
        # 3. 穴馬（オッズ10倍超）が勝ったケース
        longshot_winners = self.df[(self.df['is_winner']) & (self.df['win_odds'] > 10)]
        print(f"\n穴馬（10倍超）勝利: {len(longshot_winners):,}件")
        print("\n  【穴馬勝利ケースの予測順位分布】")
        pred_rank_dist = longshot_winners['pred_rank'].value_counts().sort_index()
        for rank, count in pred_rank_dist.head(10).items():
            print(f"    予測{int(rank)}位: {count}回 ({count/len(longshot_winners)*100:.1f}%)")
        
        # 4. 穴馬勝利時の特徴量
        print("\n  【穴馬勝利ケースの特徴量】")
        for col in ['horse_win_rate', 'jockey_win_rate', 'trainer_win_rate', 'prev_finish', 'days_since_last']:
            if col in longshot_winners.columns:
                mean_val = longshot_winners[col].mean()
                miss_rate = longshot_winners[col].isna().mean()
                print(f"    {col:20}: 平均={mean_val:.3f}, 欠損率={miss_rate*100:.1f}%")
    
    def run_all(self):
        """全分析実行"""
        print("\n" + "="*100)
        print("包括的多因子分析レポート")
        print(f"データ: {len(self.df):,}頭")
        print("="*100)
        
        self.verify_no_leakage()
        self.analyze_trainer_patterns()
        self.analyze_prev_finish_patterns()
        self.analyze_rest_patterns()
        self.analyze_field_size_patterns()
        self.analyze_track_condition()
        self.analyze_multi_factor_cross()
        self.find_high_value_conditions()
        self.analyze_prediction_miss_patterns()
        
        logger.info("全分析完了")


if __name__ == "__main__":
    analyzer = ComprehensiveFactorAnalyzer()
    analyzer.run_all()

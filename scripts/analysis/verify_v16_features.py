#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
V16特徴量エンジニアの検証スクリプト

1. 特徴量計算テスト
2. リーク検証
3. 年度別効果検証
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import logging
import warnings
warnings.filterwarnings('ignore')

# パス設定
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16

BASE_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")
OUTPUT_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\outputs\analysis")

def calc_roi(df):
    if len(df) == 0:
        return 0, 0, 0
    win_rate = df['is_win'].mean()
    winners = df[df['is_win'] == 1]
    total_return = (winners['win_odds'] * 100).sum()
    total_bet = len(df) * 100
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    return win_rate, roi, len(df)

def main():
    print("=" * 80)
    print("V16特徴量エンジニア検証")
    print("=" * 80)
    
    # データ読み込み
    print("\nデータ読み込み中...")
    races = pd.read_parquet(BASE_PATH / "races" / "races.parquet")
    corners = pd.read_parquet(BASE_PATH / "corners" / "corner_positions.parquet")
    pedigrees = pd.read_parquet(BASE_PATH / "pedigrees" / "pedigrees.parquet")
    
    print(f"レースデータ: {len(races):,}件")
    
    # 特徴量エンジニアリング
    print("\n特徴量エンジニアリング実行中...")
    fe = LeakFreeFeatureEngineerV16()
    fe.fit(races, pedigrees_df=pedigrees, corners_df=corners)
    
    # 全データでtransform
    result = fe.transform(races)
    result['year'] = pd.to_datetime(result['race_date']).dt.year
    
    print("\n" + "=" * 80)
    print("V16新特徴量の統計")
    print("=" * 80)
    
    for col in LeakFreeFeatureEngineerV16.V16_NEW_FEATURES:
        if col in result.columns:
            if result[col].dtype in ['float64', 'int64', 'int32']:
                non_null = result[col].notna().sum()
                print(f"{col:25s}: 非欠損 {non_null:,}件 ({non_null/len(result)*100:.1f}%), 平均 {result[col].mean():.4f}")
            else:
                non_null = result[col].notna().sum()
                print(f"{col:25s}: 非欠損 {non_null:,}件 ({non_null/len(result)*100:.1f}%)")
    
    # ========================================================================
    print("\n" + "=" * 80)
    print("リーク検証")
    print("=" * 80)
    # ========================================================================
    
    # 時系列ソート
    result_sorted = result.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    # 最初のレースでNaNになっているか確認
    first_race_mask = result_sorted.groupby('horse_id').cumcount() == 0
    first_races = result_sorted[first_race_mask]
    
    print("\n最初のレース（初出走）での特徴量がNaNになっているか:")
    
    features_to_check = ['horse_prev_winrate', 'prev_finish', 'prev_3f_avg']
    all_pass = True
    for col in features_to_check:
        if col in first_races.columns:
            nan_rate = first_races[col].isna().mean()
            status = "✅ PASS" if nan_rate > 0.99 else "❌ FAIL"
            print(f"  {col}: NaN率 {nan_rate*100:.1f}% {status}")
            if nan_rate <= 0.99:
                all_pass = False
    
    print(f"\nリーク検証: {'✅ 全て合格' if all_pass else '❌ 要確認'}")
    
    # ========================================================================
    print("\n" + "=" * 80)
    print("年度別効果検証（騎手累積勝率）")
    print("=" * 80)
    # ========================================================================
    
    print("\n騎手Top25% vs Bottom25%のROI差:")
    
    yearly_diffs = []
    for year in range(2019, 2025):
        year_data = result[(result['year'] == year) & result['jockey_prev_winrate'].notna()]
        
        if len(year_data) > 1000:
            q75 = year_data['jockey_prev_winrate'].quantile(0.75)
            q25 = year_data['jockey_prev_winrate'].quantile(0.25)
            
            top = year_data[year_data['jockey_prev_winrate'] >= q75]
            bottom = year_data[year_data['jockey_prev_winrate'] <= q25]
            
            top_roi = calc_roi(top)[1]
            bottom_roi = calc_roi(bottom)[1]
            diff = top_roi - bottom_roi
            yearly_diffs.append(diff)
            
            mark = "✅" if diff > 0 else "❌"
            print(f"  {year}年: Top25% ROI {top_roi:.1f}% vs Bottom25% ROI {bottom_roi:.1f}% | 差: {diff:+.1f}pt {mark}")
    
    if yearly_diffs:
        print(f"\n  平均差: {np.mean(yearly_diffs):+.1f}pt, プラス年: {sum(1 for d in yearly_diffs if d > 0)}/{len(yearly_diffs)}年")
    
    # ========================================================================
    print("\n" + "=" * 80)
    print("年度別効果検証（過去上がり3F）")
    print("=" * 80)
    # ========================================================================
    
    print("\n上がりTop25%（速い）vs Bottom25%（遅い）のROI差:")
    
    yearly_diffs_3f = []
    for year in range(2019, 2025):
        year_data = result[(result['year'] == year) & result['prev_3f_avg'].notna()]
        
        if len(year_data) > 1000:
            q25 = year_data['prev_3f_avg'].quantile(0.25)  # 低い=速い
            q75 = year_data['prev_3f_avg'].quantile(0.75)
            
            fast = year_data[year_data['prev_3f_avg'] <= q25]
            slow = year_data[year_data['prev_3f_avg'] >= q75]
            
            fast_roi = calc_roi(fast)[1]
            slow_roi = calc_roi(slow)[1]
            diff = fast_roi - slow_roi
            yearly_diffs_3f.append(diff)
            
            mark = "✅" if diff > 0 else "❌"
            print(f"  {year}年: Top25% ROI {fast_roi:.1f}% vs Bottom25% ROI {slow_roi:.1f}% | 差: {diff:+.1f}pt {mark}")
    
    if yearly_diffs_3f:
        print(f"\n  平均差: {np.mean(yearly_diffs_3f):+.1f}pt, プラス年: {sum(1 for d in yearly_diffs_3f if d > 0)}/{len(yearly_diffs_3f)}年")
    
    print("\n" + "=" * 80)
    print("検証完了")
    print("=" * 80)

if __name__ == "__main__":
    main()

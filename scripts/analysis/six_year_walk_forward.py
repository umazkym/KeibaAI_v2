#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
6年Walk-forward検証スクリプト

リーク防止特徴量エンジニアリングモジュールで作成した特徴量が
複数年にわたって安定した効果を持つことを検証する。

検証期間: 2019-2024年（6年間）
検証方法: 各年をテスト期間として、その前年までのデータで計算した特徴量の効果を検証
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

# 特徴量モジュールをインポート
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from keibaai.features.leak_free_features import LeakFreeFeatureEngineer

BASE_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")
OUTPUT_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\outputs\analysis")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

def calc_roi(df, win_col='is_win', odds_col='win_odds'):
    if len(df) == 0:
        return 0, 0, 0
    win_rate = df[win_col].mean()
    winners = df[df[win_col] == 1]
    total_return = (winners[odds_col] * 100).sum()
    total_bet = len(df) * 100
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    return win_rate, roi, len(df)

def main():
    results = []
    
    def log(msg):
        results.append(msg)
        print(msg)
    
    log("=" * 100)
    log("6年Walk-forward検証")
    log("リーク防止特徴量の効果検証")
    log("=" * 100)
    log("")
    
    # データ読み込み
    log("データ読み込み中...")
    races = pd.read_parquet(BASE_PATH / "races" / "races.parquet")
    corners = pd.read_parquet(BASE_PATH / "corners" / "corner_positions.parquet")
    
    log(f"レースデータ: {len(races):,}件")
    
    # 特徴量エンジニアリング
    log("\n特徴量エンジニアリング実行中...")
    fe = LeakFreeFeatureEngineer(verbose=False)
    df = fe.transform(races, corners)
    
    df['year'] = df['race_date'].dt.year
    
    log(f"特徴量追加後: {len(df):,}件")
    log(f"年度範囲: {df['year'].min()}-{df['year'].max()}")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【特徴量1: 過去上がり3F統計 (prev_3f_avg)】")
    log("=" * 100)
    # ========================================================================
    
    log("\n上がり速い馬（Top25%）vs 遅い馬（Bottom25%）のROI差:")
    log(f"{'年度':>6} | {'Top25% ROI':>14} | {'Bottom25% ROI':>16} | {'差':>10} | {'評価':>6}")
    log("-" * 70)
    
    yearly_diffs_3f = []
    for year in range(2019, 2025):
        year_data = df[(df['year'] == year) & df['prev_3f_avg'].notna()]
        
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
            log(f"{year:>6} | ROI {fast_roi:>8.1f}% | ROI {slow_roi:>10.1f}% | {diff:>+8.1f}pt | {mark}")
    
    log(f"\n平均差: {np.mean(yearly_diffs_3f):+.1f}pt, 標準偏差: {np.std(yearly_diffs_3f):.1f}pt")
    log(f"プラス年: {sum(1 for d in yearly_diffs_3f if d > 0)}/{len(yearly_diffs_3f)}年")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【特徴量2: 過去脚質 (prev_fr_rate)】")
    log("=" * 100)
    # ========================================================================
    
    log("\n過去先行率高い馬（50%+）vs 低い馬（30%未満）のROI差:")
    log(f"{'年度':>6} | {'先行率高 ROI':>14} | {'先行率低 ROI':>16} | {'差':>10} | {'評価':>6}")
    log("-" * 70)
    
    yearly_diffs_fr = []
    for year in range(2019, 2025):
        year_data = df[(df['year'] == year) & df['prev_fr_rate'].notna()]
        
        if len(year_data) > 1000:
            high_fr = year_data[year_data['prev_fr_rate'] >= 0.5]
            low_fr = year_data[year_data['prev_fr_rate'] < 0.3]
            
            if len(high_fr) > 100 and len(low_fr) > 100:
                high_roi = calc_roi(high_fr)[1]
                low_roi = calc_roi(low_fr)[1]
                diff = high_roi - low_roi
                yearly_diffs_fr.append(diff)
                
                mark = "✅" if diff > 0 else "❌"
                log(f"{year:>6} | ROI {high_roi:>8.1f}% | ROI {low_roi:>10.1f}% | {diff:>+8.1f}pt | {mark}")
    
    if yearly_diffs_fr:
        log(f"\n平均差: {np.mean(yearly_diffs_fr):+.1f}pt, 標準偏差: {np.std(yearly_diffs_fr):.1f}pt")
        log(f"プラス年: {sum(1 for d in yearly_diffs_fr if d > 0)}/{len(yearly_diffs_fr)}年")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【特徴量3: 馬累積勝率 (horse_prev_winrate)】")
    log("=" * 100)
    # ========================================================================
    
    log("\n勝利経験あり vs 未勝利のROI差:")
    log(f"{'年度':>6} | {'勝利経験あり':>14} | {'未勝利':>14} | {'差':>10} | {'評価':>6}")
    log("-" * 70)
    
    yearly_diffs_hw = []
    for year in range(2019, 2025):
        year_data = df[(df['year'] == year) & df['horse_prev_winrate'].notna()]
        
        if len(year_data) > 1000:
            winner = year_data[year_data['horse_prev_winrate'] > 0]
            maiden = year_data[year_data['horse_prev_winrate'] == 0]
            
            winner_roi = calc_roi(winner)[1]
            maiden_roi = calc_roi(maiden)[1]
            diff = winner_roi - maiden_roi
            yearly_diffs_hw.append(diff)
            
            mark = "✅" if diff > 0 else "❌"
            log(f"{year:>6} | ROI {winner_roi:>8.1f}% | ROI {maiden_roi:>8.1f}% | {diff:>+8.1f}pt | {mark}")
    
    log(f"\n平均差: {np.mean(yearly_diffs_hw):+.1f}pt, 標準偏差: {np.std(yearly_diffs_hw):.1f}pt")
    log(f"プラス年: {sum(1 for d in yearly_diffs_hw if d > 0)}/{len(yearly_diffs_hw)}年")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【特徴量4: 騎手累積勝率 (jockey_prev_winrate)】")
    log("=" * 100)
    # ========================================================================
    
    log("\n騎手Top25% vs Bottom25%のROI差:")
    log(f"{'年度':>6} | {'Top25%':>14} | {'Bottom25%':>14} | {'差':>10} | {'評価':>6}")
    log("-" * 70)
    
    yearly_diffs_jw = []
    for year in range(2019, 2025):
        year_data = df[(df['year'] == year) & df['jockey_prev_winrate'].notna()]
        
        if len(year_data) > 1000:
            q75 = year_data['jockey_prev_winrate'].quantile(0.75)
            q25 = year_data['jockey_prev_winrate'].quantile(0.25)
            
            top = year_data[year_data['jockey_prev_winrate'] >= q75]
            bottom = year_data[year_data['jockey_prev_winrate'] <= q25]
            
            top_roi = calc_roi(top)[1]
            bottom_roi = calc_roi(bottom)[1]
            diff = top_roi - bottom_roi
            yearly_diffs_jw.append(diff)
            
            mark = "✅" if diff > 0 else "❌"
            log(f"{year:>6} | ROI {top_roi:>8.1f}% | ROI {bottom_roi:>8.1f}% | {diff:>+8.1f}pt | {mark}")
    
    log(f"\n平均差: {np.mean(yearly_diffs_jw):+.1f}pt, 標準偏差: {np.std(yearly_diffs_jw):.1f}pt")
    log(f"プラス年: {sum(1 for d in yearly_diffs_jw if d > 0)}/{len(yearly_diffs_jw)}年")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【特徴量5: R8-12レース (is_late_race)】")
    log("=" * 100)
    # ========================================================================
    
    log("\n後半レース(R8-12) vs 前半レース(R1-7)のROI差:")
    log(f"{'年度':>6} | {'R8-12':>14} | {'R1-7':>14} | {'差':>10} | {'評価':>6}")
    log("-" * 70)
    
    yearly_diffs_late = []
    for year in range(2019, 2025):
        year_data = df[df['year'] == year]
        
        late = year_data[year_data['is_late_race'] == 1]
        early = year_data[year_data['is_late_race'] == 0]
        
        late_roi = calc_roi(late)[1]
        early_roi = calc_roi(early)[1]
        diff = late_roi - early_roi
        yearly_diffs_late.append(diff)
        
        mark = "✅" if diff > 0 else "❌"
        log(f"{year:>6} | ROI {late_roi:>8.1f}% | ROI {early_roi:>8.1f}% | {diff:>+8.1f}pt | {mark}")
    
    log(f"\n平均差: {np.mean(yearly_diffs_late):+.1f}pt, 標準偏差: {np.std(yearly_diffs_late):.1f}pt")
    log(f"プラス年: {sum(1 for d in yearly_diffs_late if d > 0)}/{len(yearly_diffs_late)}年")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【特徴量6: 前走着順 (prev_finish_cat)】")
    log("=" * 100)
    # ========================================================================
    
    log("\n前走Top3 vs 前走7着以下のROI差:")
    log(f"{'年度':>6} | {'前走Top3':>14} | {'前走7着以下':>14} | {'差':>10} | {'評価':>6}")
    log("-" * 70)
    
    yearly_diffs_pf = []
    for year in range(2019, 2025):
        year_data = df[(df['year'] == year) & df['prev_finish'].notna()]
        
        top3 = year_data[year_data['prev_finish'] <= 3]
        bottom = year_data[year_data['prev_finish'] >= 7]
        
        top_roi = calc_roi(top3)[1]
        bottom_roi = calc_roi(bottom)[1]
        diff = top_roi - bottom_roi
        yearly_diffs_pf.append(diff)
        
        mark = "✅" if diff > 0 else "❌"
        log(f"{year:>6} | ROI {top_roi:>8.1f}% | ROI {bottom_roi:>8.1f}% | {diff:>+8.1f}pt | {mark}")
    
    log(f"\n平均差: {np.mean(yearly_diffs_pf):+.1f}pt, 標準偏差: {np.std(yearly_diffs_pf):.1f}pt")
    log(f"プラス年: {sum(1 for d in yearly_diffs_pf if d > 0)}/{len(yearly_diffs_pf)}年")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【総合評価】")
    log("=" * 100)
    # ========================================================================
    
    log("""
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        6年Walk-forward検証結果                                    │
├──────────────────────────────────────────────────────────────────────────────────┤
""")
    
    features = [
        ('過去上がり3F統計', yearly_diffs_3f),
        ('過去脚質（先行率）', yearly_diffs_fr),
        ('馬累積勝率', yearly_diffs_hw),
        ('騎手累積勝率', yearly_diffs_jw),
        ('R8-12レース', yearly_diffs_late),
        ('前走着順', yearly_diffs_pf),
    ]
    
    for name, diffs in features:
        if diffs:
            pos_years = sum(1 for d in diffs if d > 0)
            mean_diff = np.mean(diffs)
            std_diff = np.std(diffs)
            
            if pos_years >= 5 and std_diff < 5:
                rating = "★★★★★"
            elif pos_years >= 4:
                rating = "★★★★☆"
            elif pos_years >= 3:
                rating = "★★★☆☆"
            else:
                rating = "★★☆☆☆"
            
            log(f"│ {name:20s}: {rating} {pos_years}/6年プラス, 平均{mean_diff:+.1f}pt, σ{std_diff:.1f}pt")
    
    log("""│                                                                                   │
└──────────────────────────────────────────────────────────────────────────────────┘
""")
    
    # 結果をファイルに保存
    output_file = OUTPUT_PATH / "six_year_walk_forward.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(results))
    
    log(f"\n結果を {output_file} に保存しました。")

if __name__ == "__main__":
    main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
追加検討項目の5年Walk-forward検証

新たに検討価値のある項目:
1. 過去上がり3F統計
2. R8-12レース絞り込み
3. 過去脚質からのハイペース確率
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

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
    log("追加検討項目の5年Walk-forward検証")
    log("=" * 100)
    log("")
    
    # データ読み込み
    log("データ読み込み中...")
    races = pd.read_parquet(BASE_PATH / "races" / "races.parquet")
    corners = pd.read_parquet(BASE_PATH / "corners" / "corner_positions.parquet")
    
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['is_win'] = (races['finish_position'] == 1).astype(int)
    races = races.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    races['year'] = races['race_date'].dt.year
    races['race_number'] = races['race_id'].astype(str).str[-2:].astype(int)
    
    log(f"総レコード数: {len(races):,}件")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【検証1: 過去上がり3F統計（リークなし）】")
    log("=" * 100)
    # ========================================================================
    
    # 過去上がり3Fの累積平均を計算
    # まずNaNを0として扱い、有効なカウントを別途計算
    races['last_3f_notna'] = races['last_3f_time'].notna().astype(float)
    races['prev_3f_sum'] = races.groupby('horse_id')['last_3f_time'].transform(
        lambda x: x.fillna(0).cumsum()
    ).shift(1)
    races['prev_3f_count'] = races.groupby('horse_id')['last_3f_notna'].cumsum().shift(1)
    races['prev_3f_avg'] = races['prev_3f_sum'] / races['prev_3f_count'].replace(0, np.nan)
    
    log("\n過去上がり3F平均と今走勝率の関係（年度別）:")
    log(f"{'年度':>6} | {'上がりTop25%':>14} | {'上がりBottom25%':>16} | {'差':>10}")
    log("-" * 60)
    
    yearly_diffs_3f = []
    for year in [2020, 2021, 2022, 2023, 2024]:
        year_data = races[(races['year'] == year) & races['prev_3f_avg'].notna()]
        
        if len(year_data) > 1000:
            q25 = year_data['prev_3f_avg'].quantile(0.25)  # 低い方が速い
            q75 = year_data['prev_3f_avg'].quantile(0.75)
            
            fast = year_data[year_data['prev_3f_avg'] <= q25]  # 過去上がりが速い
            slow = year_data[year_data['prev_3f_avg'] >= q75]  # 過去上がりが遅い
            
            fast_roi = calc_roi(fast)[1]
            slow_roi = calc_roi(slow)[1]
            diff = fast_roi - slow_roi
            yearly_diffs_3f.append(diff)
            
            log(f"{year:>6} | ROI {fast_roi:>8.1f}% | ROI {slow_roi:>10.1f}% | {diff:>+8.1f}pt")
    
    if yearly_diffs_3f:
        log(f"\n平均差: {np.mean(yearly_diffs_3f):+.1f}pt, 標準偏差: {np.std(yearly_diffs_3f):.1f}pt")
        log(f"全年度でプラス: {sum(1 for d in yearly_diffs_3f if d > 0)}/{len(yearly_diffs_3f)}年")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【検証2: R8-12レース絞り込み（年度安定性）】")
    log("=" * 100)
    # ========================================================================
    
    log("\nR8-12 vs R1-7 のROI差（年度別）:")
    log(f"{'年度':>6} | {'R1-7':>10} | {'R8-12':>10} | {'差':>10} | {'評価':>8}")
    log("-" * 60)
    
    yearly_diffs_race = []
    for year in [2020, 2021, 2022, 2023, 2024]:
        year_data = races[races['year'] == year]
        
        early = year_data[year_data['race_number'] <= 7]
        late = year_data[(year_data['race_number'] >= 8) & (year_data['race_number'] <= 12)]
        
        early_roi = calc_roi(early)[1]
        late_roi = calc_roi(late)[1]
        diff = late_roi - early_roi
        yearly_diffs_race.append(diff)
        
        eval_mark = "✅" if diff > 0 else "❌"
        log(f"{year:>6} | ROI {early_roi:>5.1f}% | ROI {late_roi:>5.1f}% | {diff:>+6.1f}pt | {eval_mark}")
    
    log(f"\n平均差: {np.mean(yearly_diffs_race):+.1f}pt, 標準偏差: {np.std(yearly_diffs_race):.1f}pt")
    log(f"後半レース優位な年: {sum(1 for d in yearly_diffs_race if d > 0)}/{len(yearly_diffs_race)}年")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【検証3: 過去脚質からのハイペース確率】")
    log("=" * 100)
    # ========================================================================
    
    # 4コーナーデータを結合
    c4 = corners[corners['corner'] == 4].copy()
    c4 = c4.merge(races[['race_id', 'horse_number', 'horse_id', 'race_date', 'is_win', 'win_odds', 'year']], 
                  on=['race_id', 'horse_number'], how='inner')
    c4 = c4.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    # 過去の先行率を累積計算
    c4['is_frontrunner'] = (c4['position'] <= 2).astype(int)
    c4['prev_fr_sum'] = c4.groupby('horse_id')['is_frontrunner'].cumsum().shift(1).fillna(0)
    c4['prev_fr_count'] = c4.groupby('horse_id').cumcount()
    c4['prev_fr_rate'] = c4['prev_fr_sum'] / c4['prev_fr_count'].replace(0, np.nan)
    
    log("\n過去先行率（先行傾向）と今走勝率の関係（年度別）:")
    log(f"{'年度':>6} | {'過去先行率高':>14} | {'過去先行率低':>14} | {'差':>10}")
    log("-" * 60)
    
    yearly_diffs_fr = []
    for year in [2020, 2021, 2022, 2023, 2024]:
        year_data = c4[(c4['year'] == year) & c4['prev_fr_rate'].notna()]
        
        if len(year_data) > 1000:
            high_fr = year_data[year_data['prev_fr_rate'] >= 0.5]  # 過去に50%以上先行
            low_fr = year_data[year_data['prev_fr_rate'] < 0.3]   # 過去に30%未満先行
            
            if len(high_fr) > 100 and len(low_fr) > 100:
                high_roi = calc_roi(high_fr)[1]
                low_roi = calc_roi(low_fr)[1]
                diff = high_roi - low_roi
                yearly_diffs_fr.append(diff)
                
                log(f"{year:>6} | ROI {high_roi:>8.1f}% | ROI {low_roi:>8.1f}% | {diff:>+8.1f}pt")
    
    if yearly_diffs_fr:
        log(f"\n平均差: {np.mean(yearly_diffs_fr):+.1f}pt, 標準偏差: {np.std(yearly_diffs_fr):.1f}pt")
        log(f"過去先行馬が優位な年: {sum(1 for d in yearly_diffs_fr if d > 0)}/{len(yearly_diffs_fr)}年")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【総合評価: 追加検討項目の採用判断】")
    log("=" * 100)
    # ========================================================================
    
    log("""
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         追加検討項目の採用判断                                    │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
""")
    
    # 評価
    items = [
        ('過去上がり3F統計', yearly_diffs_3f if yearly_diffs_3f else []),
        ('R8-12レース絞り込み', yearly_diffs_race),
        ('過去脚質（先行率）', yearly_diffs_fr if yearly_diffs_fr else []),
    ]
    
    for name, diffs in items:
        if diffs:
            pos_years = sum(1 for d in diffs if d > 0)
            mean_diff = np.mean(diffs)
            std_diff = np.std(diffs)
            
            if pos_years >= 4 and std_diff < 10:
                rating = "★★★★☆ 採用推奨"
            elif pos_years >= 3:
                rating = "★★★☆☆ 検討価値あり"
            else:
                rating = "★★☆☆☆ 見送り"
            
            log(f"│ {name:20s}: {rating} ({pos_years}/5年プラス, 平均{mean_diff:+.1f}pt)")
    
    log("""│                                                                                   │
└──────────────────────────────────────────────────────────────────────────────────┘
""")
    
    # 結果をファイルに保存
    output_file = OUTPUT_PATH / "additional_features_validation.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(results))
    
    log(f"\n結果を {output_file} に保存しました。")

if __name__ == "__main__":
    main()

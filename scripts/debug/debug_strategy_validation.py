"""
複合条件戦略の詳細検証

発見された有望な戦略:
1. 前走2着馬 × 20-50倍オッズ → ROI 134%
2. 前走4-5着馬 → ROI 80%

より詳細な条件を検証する
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*80)
    print("複合条件戦略の詳細検証")
    print("="*80)
    
    # データ読み込み
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[races['race_date'] >= '2020-01-01']
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    # 時系列分割
    train = races[races['race_date'] < '2024-07-01'].copy()
    valid = races[(races['race_date'] >= '2024-07-01') & (races['race_date'] < '2025-01-01')].copy()
    test = races[races['race_date'] >= '2025-01-01'].copy()
    
    print(f"\nTrain: {len(train):,}件 ({train['race_date'].min().date()} - {train['race_date'].max().date()})")
    print(f"Valid: {len(valid):,}件 ({valid['race_date'].min().date()} - {valid['race_date'].max().date()})")
    print(f"Test: {len(test):,}件 ({test['race_date'].min().date()} - {test['race_date'].max().date()})")
    
    # 前走着順を計算
    def add_prev_features(df):
        df = df.sort_values(['horse_id', 'race_date', 'race_id'])
        df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
        df['is_win'] = (df['finish_position'] == 1).astype(int)
        return df
    
    train = add_prev_features(train)
    valid = add_prev_features(valid)
    test = add_prev_features(test)
    
    # =================================================================
    # 戦略1: 前走2着馬 × 高オッズ帯
    # =================================================================
    print("\n" + "="*80)
    print("戦略1: 前走2着馬 × オッズ帯")
    print("="*80)
    
    for dataset_name, df in [('Train', train), ('Valid', valid), ('Test', test)]:
        print(f"\n[{dataset_name}]")
        prev2 = df[df['prev_finish'] == 2].copy()
        prev2['odds_band'] = pd.cut(prev2['win_odds'], 
                                    bins=[0, 5, 10, 20, 50, 100, 999],
                                    labels=['1-5x', '5-10x', '10-20x', '20-50x', '50-100x', '100x+'])
        
        for band in ['1-5x', '5-10x', '10-20x', '20-50x', '50-100x', '100x+']:
            subset = prev2[prev2['odds_band'] == band]
            if len(subset) < 10: continue
            wins = subset[subset['is_win'] == 1]
            win_rate = subset['is_win'].mean()
            roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
            avg_odds = subset['win_odds'].mean()
            print(f"  {band}: n={len(subset):,}, 勝率={win_rate:.1%}, 平均オッズ={avg_odds:.1f}倍, ROI={roi:.1f}%")
    
    # =================================================================
    # 戦略2: 前走4-5着馬 × オッズ帯
    # =================================================================
    print("\n" + "="*80)
    print("戦略2: 前走4-5着馬 × オッズ帯")
    print("="*80)
    
    for dataset_name, df in [('Train', train), ('Valid', valid), ('Test', test)]:
        print(f"\n[{dataset_name}]")
        prev45 = df[df['prev_finish'].isin([4, 5])].copy()
        prev45['odds_band'] = pd.cut(prev45['win_odds'], 
                                     bins=[0, 5, 10, 20, 50, 100, 999],
                                     labels=['1-5x', '5-10x', '10-20x', '20-50x', '50-100x', '100x+'])
        
        for band in ['1-5x', '5-10x', '10-20x', '20-50x', '50-100x', '100x+']:
            subset = prev45[prev45['odds_band'] == band]
            if len(subset) < 10: continue
            wins = subset[subset['is_win'] == 1]
            win_rate = subset['is_win'].mean()
            roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
            avg_odds = subset['win_odds'].mean()
            print(f"  {band}: n={len(subset):,}, 勝率={win_rate:.1%}, 平均オッズ={avg_odds:.1f}倍, ROI={roi:.1f}%")
    
    # =================================================================
    # 戦略3: 前走上位（1-3着）× クラス降級
    # =================================================================
    print("\n" + "="*80)
    print("戦略3: 前走上位（1-3着）× クラス降級")
    print("="*80)
    
    def parse_class(name):
        if pd.isna(name): return np.nan
        name = str(name)
        if 'G1' in name or 'GⅠ' in name: return 10
        if 'G2' in name or 'GⅡ' in name: return 9
        if 'G3' in name or 'GⅢ' in name: return 8
        if '(L)' in name or 'リステッド' in name: return 7
        if 'オープン' in name or 'OP' in name: return 6
        if '3勝' in name or '1600万' in name: return 5
        if '2勝' in name or '1000万' in name: return 4
        if '1勝' in name or '500万' in name: return 3
        if '未勝利' in name: return 2
        if '新馬' in name: return 1
        return np.nan
    
    for dataset_name, df in [('Train', train), ('Valid', valid), ('Test', test)]:
        df['race_class'] = df['race_name'].apply(parse_class)
        df['prev_race_class'] = df.groupby('horse_id')['race_class'].shift(1)
        df['class_change'] = df['race_class'] - df['prev_race_class']
    
    for dataset_name, df in [('Train', train), ('Valid', valid), ('Test', test)]:
        print(f"\n[{dataset_name}]")
        
        # 前走上位 + 降級
        subset = df[(df['prev_finish'] <= 3) & (df['class_change'] < 0)]
        if len(subset) >= 10:
            wins = subset[subset['is_win'] == 1]
            win_rate = subset['is_win'].mean()
            roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
            avg_odds = subset['win_odds'].mean()
            print(f"  前走1-3着 + 降級: n={len(subset):,}, 勝率={win_rate:.1%}, 平均オッズ={avg_odds:.1f}倍, ROI={roi:.1f}%")
        
        # 前走着外(4着以下) + 降級
        subset = df[(df['prev_finish'] >= 4) & (df['class_change'] < 0)]
        if len(subset) >= 10:
            wins = subset[subset['is_win'] == 1]
            win_rate = subset['is_win'].mean()
            roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
            avg_odds = subset['win_odds'].mean()
            print(f"  前走4着以下 + 降級: n={len(subset):,}, 勝率={win_rate:.1%}, 平均オッズ={avg_odds:.1f}倍, ROI={roi:.1f}%")
    
    # =================================================================
    # 戦略4: 人気と前走着順のギャップ
    # =================================================================
    print("\n" + "="*80)
    print("戦略4: 人気と前走着順のギャップ（穴馬発掘）")
    print("="*80)
    
    for dataset_name, df in [('Train', train), ('Valid', valid), ('Test', test)]:
        print(f"\n[{dataset_name}]")
        
        # popularity（人気順位）が存在する場合
        if 'popularity' in df.columns:
            df['pop_vs_prev'] = df['popularity'] - df['prev_finish']
            
            # 前走上位なのに人気がない馬（過小評価）
            underrated = df[(df['prev_finish'] <= 3) & (df['popularity'] >= 5)]
            if len(underrated) >= 10:
                wins = underrated[underrated['is_win'] == 1]
                win_rate = underrated['is_win'].mean()
                roi = wins['win_odds'].sum() * 100 / (len(underrated) * 100) * 100
                avg_odds = underrated['win_odds'].mean()
                print(f"  前走1-3着 & 人気5番以降（過小評価）: n={len(underrated):,}, 勝率={win_rate:.1%}, 平均オッズ={avg_odds:.1f}倍, ROI={roi:.1f}%")
    
    # =================================================================
    # 最有望戦略のサマリー
    # =================================================================
    print("\n" + "="*80)
    print("最有望戦略のTest期間サマリー")
    print("="*80)
    
    def evaluate_strategy(df, condition, name):
        subset = df[condition].copy()
        if len(subset) < 10:
            return None
        wins = subset[subset['is_win'] == 1]
        win_rate = subset['is_win'].mean()
        roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
        avg_odds = subset['win_odds'].mean()
        return {
            'name': name,
            'n': len(subset),
            'win_rate': win_rate,
            'avg_odds': avg_odds,
            'roi': roi
        }
    
    strategies = []
    
    # 前走2着 × 20-50倍
    test['odds_band'] = pd.cut(test['win_odds'], 
                               bins=[0, 5, 10, 20, 50, 100, 999],
                               labels=['1-5x', '5-10x', '10-20x', '20-50x', '50-100x', '100x+'])
    
    result = evaluate_strategy(test, (test['prev_finish'] == 2) & (test['odds_band'] == '20-50x'), '前走2着×20-50倍')
    if result: strategies.append(result)
    
    result = evaluate_strategy(test, (test['prev_finish'] == 2) & (test['odds_band'] == '10-20x'), '前走2着×10-20倍')
    if result: strategies.append(result)
    
    result = evaluate_strategy(test, (test['prev_finish'].isin([4, 5])) & (test['odds_band'] == '10-20x'), '前走4-5着×10-20倍')
    if result: strategies.append(result)
    
    result = evaluate_strategy(test, (test['prev_finish'] <= 3) & (test['class_change'] < 0), '前走1-3着×降級')
    if result: strategies.append(result)
    
    if 'popularity' in test.columns:
        result = evaluate_strategy(test, (test['prev_finish'] <= 3) & (test['popularity'] >= 5), '前走1-3着×人気5番以降')
        if result: strategies.append(result)
    
    # ソートして表示
    strategies_df = pd.DataFrame(strategies)
    strategies_df = strategies_df.sort_values('roi', ascending=False)
    
    print("\n戦略別ROIランキング:")
    print(f"{'戦略':<25} {'n':>6} {'勝率':>8} {'平均オッズ':>10} {'ROI':>8}")
    print("-" * 65)
    for _, row in strategies_df.iterrows():
        print(f"{row['name']:<25} {row['n']:>6} {row['win_rate']:>7.1%} {row['avg_odds']:>9.1f}倍 {row['roi']:>7.1f}%")


if __name__ == '__main__':
    main()

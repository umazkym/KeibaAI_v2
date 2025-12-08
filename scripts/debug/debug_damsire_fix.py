"""
母父取得バグ修正とROI効果検証

発見されたバグ:
- V9では generation=2 の2番目(nth(1))を母父としていた
- 実際は 3番目(index=2) が母父

血統構造:
  generation=2: 
    index=0: 父の父
    index=1: 父の母
    index=2: 母の父 ← 正しい母父
    index=3: 母の母
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
    print("母父統計の正しい計算とROI効果検証")
    print("="*80)
    
    # データ読み込み
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[races['race_date'] >= '2020-01-01']
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    
    print(f"\n使用レース数: {len(races):,}")
    
    # =================================================================
    # 正しい母父取得（index=2）
    # =================================================================
    print("\n" + "="*80)
    print("正しい母父取得（generation=2のindex=2）")
    print("="*80)
    
    gen2 = pedigrees[pedigrees['generation'] == 2].copy()
    gen2_sorted = gen2.sort_values(['horse_id', 'ancestor_id'])
    
    # 各馬のgeneration=2レコードからindex=2（3番目）を取得
    def get_damsire(group):
        if len(group) >= 3:
            return group.iloc[2]['ancestor_id']  # index=2が母父
        return None
    
    damsire_map = gen2_sorted.groupby('horse_id').apply(get_damsire).reset_index()
    damsire_map.columns = ['horse_id', 'damsire_id']
    damsire_map = damsire_map.dropna()
    print(f"母父マッピング: {len(damsire_map):,}頭")
    
    # レースデータにマージ
    races_with_damsire = races.merge(damsire_map, on='horse_id', how='left')
    races_with_damsire['is_win'] = (races_with_damsire['finish_position'] == 1).astype(int)
    
    print(f"母父あり: {races_with_damsire['damsire_id'].notna().sum():,} ({races_with_damsire['damsire_id'].notna().mean()*100:.1f}%)")
    
    # =================================================================
    # Train/Testで母父勝率を計算しROI効果を検証
    # =================================================================
    print("\n" + "="*80)
    print("母父勝率とROI効果の検証")
    print("="*80)
    
    train = races_with_damsire[races_with_damsire['race_date'] < '2024-07-01'].copy()
    test = races_with_damsire[races_with_damsire['race_date'] >= '2025-01-01'].copy()
    
    print(f"Train: {len(train):,}件")
    print(f"Test: {len(test):,}件")
    
    # Train期間で母父統計を計算
    damsire_stats = train.groupby('damsire_id').agg(
        damsire_races=('is_win', 'count'),
        damsire_win_rate=('is_win', 'mean')
    ).reset_index()
    
    # 最低10レース以上
    damsire_stats = damsire_stats[damsire_stats['damsire_races'] >= 10]
    print(f"有効な母父統計: {len(damsire_stats):,}頭")
    
    # 上位・下位母父を分析
    top_damsires = damsire_stats.nlargest(20, 'damsire_win_rate')
    bottom_damsires = damsire_stats.nsmallest(20, 'damsire_win_rate')
    
    print(f"\n上位20母父（勝率）:")
    for _, row in top_damsires.iterrows():
        print(f"  {row['damsire_id']}: 勝率={row['damsire_win_rate']:.1%}, n={row['damsire_races']:.0f}")
    
    # Testデータに母父勝率をマージ
    test = test.merge(damsire_stats[['damsire_id', 'damsire_win_rate']], on='damsire_id', how='left')
    
    # 母父勝率の四分位で分析
    test['damsire_quartile'] = pd.qcut(test['damsire_win_rate'].fillna(0), 4, labels=['Q1(低)', 'Q2', 'Q3', 'Q4(高)'])
    
    print(f"\nTest期間での母父四分位別勝率とROI:")
    for q in ['Q1(低)', 'Q2', 'Q3', 'Q4(高)']:
        subset = test[test['damsire_quartile'] == q]
        wins = subset[subset['is_win'] == 1]
        win_rate = subset['is_win'].mean()
        roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100 if len(subset) > 0 else 0
        print(f"  {q}: n={len(subset):,}, 勝率={win_rate:.1%}, ROI={roi:.1f}%")
    
    # =================================================================
    # 前走着順の効果をより詳細に分析
    # =================================================================
    print("\n" + "="*80)
    print("前走着順とオッズの関係分析")
    print("="*80)
    
    test_sorted = test.sort_values(['horse_id', 'race_date'])
    test_sorted['prev_finish'] = test_sorted.groupby('horse_id')['finish_position'].shift(1)
    
    # 前走着順別の分析
    print("\n前走着順別の詳細分析（Test期間）:")
    for prev in range(1, 11):
        subset = test_sorted[test_sorted['prev_finish'] == prev]
        wins = subset[subset['is_win'] == 1]
        win_rate = subset['is_win'].mean() if len(subset) > 0 else 0
        avg_odds = subset['win_odds'].mean() if len(subset) > 0 else 0
        roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100 if len(subset) > 0 else 0
        print(f"  前走{prev:2d}着: n={len(subset):,}, 勝率={win_rate:.1%}, 平均オッズ={avg_odds:.1f}倍, ROI={roi:.1f}%")
    
    # 前走2着馬の詳細（勝率最高）
    print("\n前走2着馬の詳細（勝率最高）:")
    prev2_subset = test_sorted[test_sorted['prev_finish'] == 2]
    
    # オッズ帯別
    prev2_subset['odds_band'] = pd.cut(prev2_subset['win_odds'], 
                                        bins=[0, 3, 5, 10, 20, 50, 100, 999],
                                        labels=['1-3x', '3-5x', '5-10x', '10-20x', '20-50x', '50-100x', '100x+'])
    
    print("  オッズ帯別:")
    for band in ['1-3x', '3-5x', '5-10x', '10-20x', '20-50x', '50-100x', '100x+']:
        subset = prev2_subset[prev2_subset['odds_band'] == band]
        if len(subset) == 0: continue
        wins = subset[subset['is_win'] == 1]
        win_rate = subset['is_win'].mean()
        roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
        print(f"    {band}: n={len(subset):,}, 勝率={win_rate:.1%}, ROI={roi:.1f}%")
    
    # =================================================================
    # 複合条件でのROI効果
    # =================================================================
    print("\n" + "="*80)
    print("複合条件でのROI効果")
    print("="*80)
    
    # 昇級 + 前走上位
    test_sorted['class_change'] = test_sorted.groupby('horse_id')['finish_position'].transform(
        lambda x: x.shift(1)
    )  # 一旦前走着順をclass_changeとして計算
    
    # 実際のクラス変動を計算
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
    
    test_sorted['race_class'] = test_sorted['race_name'].apply(parse_class)
    test_sorted['prev_race_class'] = test_sorted.groupby('horse_id')['race_class'].shift(1)
    test_sorted['real_class_change'] = test_sorted['race_class'] - test_sorted['prev_race_class']
    
    # 昇級馬（class_change > 0）かつ前走1-3着
    upgrade_prev_top = test_sorted[
        (test_sorted['real_class_change'] > 0) & 
        (test_sorted['prev_finish'] <= 3)
    ]
    if len(upgrade_prev_top) > 0:
        wins = upgrade_prev_top[upgrade_prev_top['is_win'] == 1]
        win_rate = upgrade_prev_top['is_win'].mean()
        roi = wins['win_odds'].sum() * 100 / (len(upgrade_prev_top) * 100) * 100
        avg_odds = upgrade_prev_top['win_odds'].mean()
        print(f"\n昇級馬 + 前走1-3着:")
        print(f"  n={len(upgrade_prev_top):,}, 勝率={win_rate:.1%}, 平均オッズ={avg_odds:.1f}倍, ROI={roi:.1f}%")
    
    # 降級馬（class_change < 0）
    downgrade = test_sorted[test_sorted['real_class_change'] < 0]
    if len(downgrade) > 0:
        wins = downgrade[downgrade['is_win'] == 1]
        win_rate = downgrade['is_win'].mean()
        roi = wins['win_odds'].sum() * 100 / (len(downgrade) * 100) * 100
        avg_odds = downgrade['win_odds'].mean()
        print(f"\n降級馬:")
        print(f"  n={len(downgrade):,}, 勝率={win_rate:.1%}, 平均オッズ={avg_odds:.1f}倍, ROI={roi:.1f}%")


if __name__ == '__main__':
    main()

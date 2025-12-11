# -*- coding: utf-8 -*-
"""
Phase 1 データ深層分析スクリプト
複勝・単勝データの傾向を多角的に分析
"""
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

def main():
    # データ読み込み
    print("データ読み込み中...")
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    returns = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print("=" * 60)
    print("【1】複勝データの深層分析")
    print("=" * 60)
    
    # 複勝払戻データ
    fukusho = returns[returns['bet_type'] == 'fukusho'].copy()
    print(f"複勝レコード数: {len(fukusho):,}")
    print(f"レース数: {fukusho['race_id'].nunique():,}")
    print()
    
    # 複勝払戻金の分布
    print("【複勝払戻金の分布】")
    print(fukusho['payout'].describe())
    print()
    
    # 3着以内馬と複勝払戻をマージ
    races_top3 = races[races['finish_position'] <= 3][['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity', 'race_date']].copy()
    merged_place = races_top3.merge(fukusho[['race_id', 'horse_number', 'payout']], on=['race_id', 'horse_number'], how='left')
    print(f"3着以内馬数: {len(races_top3):,}")
    print(f"払戻マッチ数: {merged_place['payout'].notna().sum():,}")
    print()
    
    # 人気別の複勝ROI分析
    print("【人気別・複勝理論ROI（3着以内馬のみ）】")
    for pop in range(1, 8):
        pop_data = merged_place[merged_place['popularity'] == pop]
        if len(pop_data) > 100:
            total_payout = pop_data['payout'].sum()
            total_bets = len(pop_data) * 100
            roi = total_payout / total_bets * 100 if total_bets > 0 else 0
            avg_payout = pop_data['payout'].mean()
            print(f"  {pop}番人気: {len(pop_data):,}件, 平均払戻={avg_payout:.0f}円, ROI={roi:.1f}%")
    print()
    
    # オッズ帯別の複勝ROI分析
    print("【オッズ帯別・3着以内馬の複勝ROI】")
    odds_bins = [(1, 3), (3, 5), (5, 10), (10, 20), (20, 50), (50, 100)]
    for low, high in odds_bins:
        mask = (merged_place['win_odds'] >= low) & (merged_place['win_odds'] < high)
        data = merged_place[mask]
        if len(data) > 100:
            total_payout = data['payout'].sum()
            total_bets = len(data) * 100
            roi = total_payout / total_bets * 100 if total_bets > 0 else 0
            avg_payout = data['payout'].mean()
            print(f"  {low}-{high}倍: {len(data):,}件, 平均払戻={avg_payout:.0f}円, ROI={roi:.1f}%")
    print()
    
    print("=" * 60)
    print("【2】単勝データの深層分析")
    print("=" * 60)
    
    # 単勝払戻データ
    tansho = returns[returns['bet_type'] == 'tansho'].copy()
    print(f"単勝レコード数: {len(tansho):,}")
    
    # 1着馬と単勝払戻をマージ
    races_1st = races[races['finish_position'] == 1][['race_id', 'horse_number', 'win_odds', 'popularity', 'race_date']].copy()
    merged_win = races_1st.merge(tansho[['race_id', 'horse_number', 'payout']], on=['race_id', 'horse_number'], how='left')
    print(f"1着馬数: {len(races_1st):,}")
    print(f"払戻マッチ数: {merged_win['payout'].notna().sum():,}")
    print()
    
    # オッズと払戻金の関係
    print("【オッズ vs 払戻金の関係（1着馬）】")
    merged_win['ratio'] = merged_win['payout'] / (merged_win['win_odds'] * 100)
    print(merged_win[['win_odds', 'payout', 'ratio']].describe())
    print()
    
    # 人気別の単勝的中率・ROI
    print("【人気別・単勝的中率とROI（全馬）】")
    for pop in range(1, 8):
        pop_all = races[races['popularity'] == pop]
        pop_wins = pop_all[pop_all['finish_position'] == 1]
        if len(pop_all) > 100:
            hit_rate = len(pop_wins) / len(pop_all) * 100
            # ROI計算: 当たった馬のオッズ合計 / ベット数
            total_odds = pop_wins['win_odds'].sum()
            roi = total_odds / len(pop_all) * 100
            print(f"  {pop}番人気: 的中率={hit_rate:.1f}%, ROI={roi:.1f}%")
    print()
    
    print("=" * 60)
    print("【3】期間別分析（Test期間2024年以降）")
    print("=" * 60)
    
    # 2024年以降のTest期間
    test_df = races[races['race_date'] >= '2024-01-01'].copy()
    test_fukusho = fukusho[fukusho['race_id'].isin(test_df['race_id'].unique())]
    
    test_top3 = test_df[test_df['finish_position'] <= 3][['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity']].copy()
    test_merged = test_top3.merge(fukusho[['race_id', 'horse_number', 'payout']], on=['race_id', 'horse_number'], how='left')
    
    print(f"Test期間レース数: {test_df['race_id'].nunique():,}")
    print(f"Test期間3着以内馬: {len(test_top3):,}")
    print()
    
    # Test期間の人気別複勝ROI
    print("【Test期間 人気別 複勝ROI】")
    for pop in range(1, 6):
        pop_data = test_merged[test_merged['popularity'] == pop]
        if len(pop_data) > 50:
            total_payout = pop_data['payout'].sum()
            total_bets = len(pop_data) * 100
            roi = total_payout / total_bets * 100 if total_bets > 0 else 0
            print(f"  {pop}番人気: {len(pop_data):,}件, ROI={roi:.1f}%")
    print()
    
    print("=" * 60)
    print("【4】複勝モデル向け重要分析")
    print("=" * 60)
    
    # 複勝で重要: 2着・3着馬の特性
    print("【着順別の複勝払戻金】")
    for pos in [1, 2, 3]:
        pos_data = merged_place[merged_place['finish_position'] == pos]
        avg_payout = pos_data['payout'].mean()
        print(f"  {pos}着: {len(pos_data):,}件, 平均払戻={avg_payout:.0f}円")
    print()
    
    # 人気と着順のクロス分析
    print("【人気×着順 件数分布（Top5人気×Top3着順）】")
    cross = pd.crosstab(merged_place['popularity'].clip(upper=5), merged_place['finish_position'])
    print(cross)
    print()

if __name__ == "__main__":
    main()

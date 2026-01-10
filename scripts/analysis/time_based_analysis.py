# -*- coding: utf-8 -*-
"""
タイムベース特徴量の分析スクリプト

順位だけでなく、タイム（レース内相対パフォーマンス）を分析
"""
import pandas as pd
import numpy as np
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent

def main():
    print("=" * 80)
    print("タイムベース分析")
    print("=" * 80)
    
    r = pd.read_parquet(project_root / "keibaai/data/parsed/parquet/races/races.parquet")
    r = r[r['finish_position'].notna()].copy()
    r['race_date'] = pd.to_datetime(r['race_date'])
    
    print(f"総レコード数: {len(r):,}")
    
    # タイム関連カラムの確認
    print("\n=== タイム関連カラムの統計 ===")
    
    print("\n1. finish_time_seconds (走破タイム秒):")
    print(r['finish_time_seconds'].describe())
    
    print("\n2. margin_seconds (着差秒):")
    print(r['margin_seconds'].describe())
    
    print("\n3. last_3f_time (上がり3ハロン):")
    print(r['last_3f_time'].describe())
    
    # レース内での相対タイム分析
    print("\n=== レース内相対パフォーマンス分析 ===")
    
    # 1着との着差を計算
    first_time = r[r['finish_position'] == 1][['race_id', 'finish_time_seconds']].copy()
    first_time.columns = ['race_id', 'winner_time']
    r = r.merge(first_time, on='race_id', how='left')
    r['time_behind_winner'] = r['finish_time_seconds'] - r['winner_time']
    
    print("\n順位別の1着とのタイム差（秒）:")
    for pos in range(1, 11):
        subset = r[r['finish_position'] == pos]
        if len(subset) > 1000:
            mean_diff = subset['time_behind_winner'].mean()
            std_diff = subset['time_behind_winner'].std()
            print(f"  {pos}着: 平均 {mean_diff:.2f}秒, σ={std_diff:.2f}")
    
    # 上がり3ハロンの分析
    print("\n=== 上がり3ハロン分析 ===")
    r['last3f_rank'] = r.groupby('race_id')['last_3f_time'].rank(method='first')
    
    corr = r[['last3f_rank', 'finish_position']].corr().iloc[0,1]
    print(f"上がり順位と着順の相関係数: {corr:.3f}")
    
    # 上がり最速で勝利した割合
    fastest_last3f = r.groupby('race_id')['last_3f_time'].idxmin()
    r['is_fastest_last3f'] = r.index.isin(fastest_last3f)
    fastest_winners = r[(r['is_fastest_last3f']) & (r['finish_position'] == 1)]
    total_races = r['race_id'].nunique()
    print(f"上がり最速馬が勝った割合: {len(fastest_winners)/total_races*100:.1f}%")
    
    # 上がり順位別の勝率
    print("\n上がり順位別の勝率:")
    for rank in range(1, 6):
        subset = r[r['last3f_rank'] == rank]
        wins = subset[subset['finish_position'] == 1]
        win_rate = len(wins) / len(subset) * 100 if len(subset) > 0 else 0
        print(f"  上がり{rank}位: 勝率 {win_rate:.1f}% ({len(subset):,}件)")
    
    # === 「惜敗」パターンの分析 ===
    print("\n=== 惜敗パターン分析（僅差で2着だった馬） ===")
    
    # 2着馬で着差0.2秒以内（僅差）
    close_second = r[(r['finish_position'] == 2) & (r['time_behind_winner'] <= 0.2)]
    far_second = r[(r['finish_position'] == 2) & (r['time_behind_winner'] > 0.5)]
    
    print(f"僅差2着（0.2秒以内）: {len(close_second):,}件")
    print(f"大差2着（0.5秒超）: {len(far_second):,}件")
    
    # 惜敗馬の次走成績
    print("\n=== 「惜敗後の次走」分析 ===")
    r_sorted = r.sort_values(['horse_id', 'race_date'])
    r['prev_margin'] = r_sorted.groupby('horse_id')['time_behind_winner'].shift(1)
    r['prev_finish'] = r_sorted.groupby('horse_id')['finish_position'].shift(1)
    
    # 前走が僅差2着だった馬の今走成績
    close_loss_prev = r[(r['prev_finish'] == 2) & (r['prev_margin'] <= 0.2) & (r['prev_margin'].notna())]
    far_loss_prev = r[(r['prev_finish'] == 2) & (r['prev_margin'] > 0.5) & (r['prev_margin'].notna())]
    
    if len(close_loss_prev) > 100:
        win_rate_close = (close_loss_prev['finish_position'] == 1).mean() * 100
        print(f"前走が僅差2着だった馬の勝率: {win_rate_close:.1f}% ({len(close_loss_prev):,}件)")
    
    if len(far_loss_prev) > 100:
        win_rate_far = (far_loss_prev['finish_position'] == 1).mean() * 100
        print(f"前走が大差2着だった馬の勝率: {win_rate_far:.1f}% ({len(far_loss_prev):,}件)")
    
    # === タイムベース「真の強さ」指標 ===
    print("\n=== レース内相対強さ指標の分析 ===")
    
    # レース内での標準化タイム（z-score）
    r['race_mean_time'] = r.groupby('race_id')['finish_time_seconds'].transform('mean')
    r['race_std_time'] = r.groupby('race_id')['finish_time_seconds'].transform('std')
    r['time_zscore'] = (r['finish_time_seconds'] - r['race_mean_time']) / (r['race_std_time'] + 0.001)
    
    print("\n順位別のレース内z-score:")
    for pos in range(1, 6):
        subset = r[r['finish_position'] == pos]
        if len(subset) > 1000:
            mean_z = subset['time_zscore'].mean()
            print(f"  {pos}着: 平均z-score {mean_z:.2f}")
    
    # 過去の「タイム強さ」と今走の関係
    print("\n=== 過去タイムパフォーマンスと今走成績 ===")
    
    # 累積z-scoreを計算
    r_sorted = r.sort_values(['horse_id', 'race_date'])
    r['prev_zscore'] = r_sorted.groupby('horse_id')['time_zscore'].shift(1)
    r['prev3_zscore_avg'] = r_sorted.groupby('horse_id')['time_zscore'].transform(
        lambda x: x.shift(1).rolling(3, min_periods=1).mean()
    )
    
    # z-scoreのQuartile別の今走勝率
    r['prev_zscore_quartile'] = pd.qcut(r['prev_zscore'].dropna(), 4, labels=['Q1(最弱)', 'Q2', 'Q3', 'Q4(最強)'])
    
    print("\n前走z-score四分位別の今走勝率:")
    for q in ['Q1(最弱)', 'Q2', 'Q3', 'Q4(最強)']:
        subset = r[r['prev_zscore_quartile'] == q]
        if len(subset) > 1000:
            win_rate = (subset['finish_position'] == 1).mean() * 100
            print(f"  {q}: 勝率 {win_rate:.1f}% ({len(subset):,}件)")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()

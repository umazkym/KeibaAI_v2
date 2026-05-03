#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
タイムデータの問題を定量的に診断するスクリプト

問題仮説:
1. 季節性ノイズ - 月ごとの予測可能な変動が除去されていない
2. 開催スケジュールバイアス - 夏場開催の競馬場と通年開催の混在
3. 距離別サンプル数不均衡 - 特定距離の月にデータが0件
4. 長期高速化トレンド - 全期間平均では最近が過大評価
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

PARQUET_PATH = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"

def main():
    print("=" * 80)
    print("📊 タイムデータ診断レポート")
    print("=" * 80)

    df = pd.read_parquet(PARQUET_PATH, columns=[
        'race_date', 'venue', 'distance_m', 'track_surface',
        'track_condition', 'finish_position', 'finish_time_seconds', 'last_3f_time'
    ])
    df['race_date'] = pd.to_datetime(df['race_date'])
    df['finish_time_seconds'] = pd.to_numeric(df['finish_time_seconds'], errors='coerce')
    df['last_3f_time'] = pd.to_numeric(df['last_3f_time'], errors='coerce')
    df = df.dropna(subset=['finish_time_seconds', 'track_surface', 'venue', 'distance_m'])
    df = df[df['track_surface'].isin(['芝', 'ダート'])].copy()
    df_top3 = df[df['finish_position'].isin([1, 2, 3])].copy()
    df_top3['year'] = df_top3['race_date'].dt.year
    df_top3['month'] = df_top3['race_date'].dt.month
    df_top3['year_month'] = df_top3['race_date'].dt.to_period('M').astype(str)

    # ===== 1. 競馬場別の月次開催有無 =====
    print("\n" + "=" * 80)
    print("【1】競馬場別 月次開催パターン（2024年）")
    print("=" * 80)
    df_2024 = df_top3[df_top3['year'] == 2024]
    venue_month = df_2024.groupby(['venue', 'month']).size().unstack(fill_value=0)
    print(venue_month.to_string())
    
    # 通年開催vs限定開催
    print("\n  → 通年開催数 vs データなし月:")
    for venue in sorted(df_top3['venue'].unique()):
        v_data = venue_month.loc[venue] if venue in venue_month.index else pd.Series(dtype=int)
        months_with_data = (v_data > 0).sum()
        months_without = 12 - months_with_data
        print(f"    {venue}: {months_with_data}ヶ月開催, {months_without}ヶ月休止")

    # ===== 2. 月次のZスコアの季節性パターン =====
    print("\n" + "=" * 80)
    print("【2】月別 平均Zスコア（全期間, 季節性の可視化）")
    print("=" * 80)
    
    # 距離別標準化
    group_stats = df_top3.groupby(['track_surface', 'distance_m']).agg(
        time_mean=('finish_time_seconds', 'mean'),
        time_std=('finish_time_seconds', 'std')
    ).reset_index()
    df_top3 = df_top3.merge(group_stats, on=['track_surface', 'distance_m'], how='left')
    df_top3['time_zscore'] = (df_top3['finish_time_seconds'] - df_top3['time_mean']) / df_top3['time_std'].clip(lower=0.5)

    monthly_seasonal = df_top3.groupby('month')['time_zscore'].agg(['mean', 'std', 'count']).round(4)
    monthly_seasonal.columns = ['平均Z', 'σ', 'N']
    print(monthly_seasonal.to_string())
    
    # 季節性のインパクト
    z_range = monthly_seasonal['平均Z'].max() - monthly_seasonal['平均Z'].min()
    print(f"\n  → 月別Zスコアの振幅: {z_range:.4f}")
    print(f"  → 最も速い月: {monthly_seasonal['平均Z'].idxmin()}月 (Z={monthly_seasonal['平均Z'].min():.4f})")
    print(f"  → 最も遅い月: {monthly_seasonal['平均Z'].idxmax()}月 (Z={monthly_seasonal['平均Z'].max():.4f})")

    # ===== 3. 競馬場別の月別Zスコアパターン =====
    print("\n" + "=" * 80)
    print("【3】競馬場×月 Zスコア分布（開催月の偏りの可視化）")
    print("=" * 80)
    
    venue_month_z = df_top3.groupby(['venue', 'month'])['time_zscore'].agg(['mean', 'count']).round(4)
    venue_month_z.columns = ['平均Z', 'N']
    
    # 主要4場のみ表示
    for venue in ['東京', '中山', '阪神', '京都', '札幌', '函館']:
        if venue in venue_month_z.index.get_level_values(0):
            v_data = venue_month_z.loc[venue]
            print(f"\n  【{venue}】")
            print(f"    {'月':<5} {'平均Z':<10} {'データ数':<10}")
            for month in range(1, 13):
                if month in v_data.index:
                    row = v_data.loc[month]
                    print(f"    {month:<5} {row['平均Z']:<10.4f} {int(row['N']):<10}")
                else:
                    print(f"    {month:<5} {'---':<10} {'0':<10}")

    # ===== 4. 年次トレンド（高速化の検証）=====
    print("\n" + "=" * 80)
    print("【4】年次 平均Zスコア推移（長期高速化トレンド）")
    print("=" * 80)
    
    yearly_z = df_top3.groupby('year')['time_zscore'].agg(['mean', 'count']).round(4)
    yearly_z.columns = ['平均Z', 'N']
    print(yearly_z.to_string())
    
    # 線形フィット
    years = yearly_z.index.values
    zvals = yearly_z['平均Z'].values
    slope, intercept = np.polyfit(years, zvals, 1)
    print(f"\n  → 線形フィット: Z = {slope:.6f} × 年 + {intercept:.4f}")
    print(f"  → 年間変化率: {slope:.6f} (年間 {abs(slope) * 10:.4f} Zスコア/10年)")
    if slope < 0:
        print(f"  → 傾向: 年々高速化 (10年で {abs(slope*10):.4f} Zスコア改善)")
    else:
        print(f"  → 傾向: 年々低速化")

    # ===== 5. 距離別の月次データ存在率 =====
    print("\n" + "=" * 80)
    print("【5】主要距離×月のデータ有無（芝, 2024年）")
    print("=" * 80)
    
    df_2024_turf = df_2024[df_2024['track_surface'] == '芝']
    dist_month = df_2024_turf.groupby(['distance_m', 'month']).size().unstack(fill_value=0)
    print(dist_month.to_string())
    
    # 特定距離で著しくギャップがある月を検出
    print("\n  → データ欠損率（12ヶ月中何ヶ月データなし）:")
    for dist in sorted(dist_month.index):
        missing = (dist_month.loc[dist] == 0).sum()
        total = dist_month.loc[dist].sum()
        print(f"    芝{dist}m: {missing}ヶ月欠損, 年間{total}レコード")

    # ===== 6. 「全体合算」の構成比変動 =====
    print("\n" + "=" * 80)
    print("【6】月次の競馬場構成比（全体平均に影響する顔ぶれの変動）")
    print("=" * 80)
    
    month_venue_pct = df_top3.groupby(['month', 'venue']).size().unstack(fill_value=0)
    month_totals = month_venue_pct.sum(axis=1)
    month_venue_pct_norm = month_venue_pct.div(month_totals, axis=0) * 100
    print(month_venue_pct_norm.round(1).to_string())
    
    # 夏場（7-8月）の構成比
    print("\n  → 夏場(7-8月) vs 冬場(1-2月) の構成比変動:")
    summer = month_venue_pct_norm.loc[[7, 8]].mean()
    winter = month_venue_pct_norm.loc[[1, 2]].mean()
    diff = summer - winter
    for venue in sorted(diff.index, key=lambda x: abs(diff[x]), reverse=True):
        if abs(diff[venue]) > 1:
            print(f"    {venue}: 夏{summer[venue]:.1f}% vs 冬{winter[venue]:.1f}% (差{diff[venue]:+.1f}%)")

    print("\n" + "=" * 80)
    print("✅ 診断完了")
    print("=" * 80)

if __name__ == "__main__":
    main()

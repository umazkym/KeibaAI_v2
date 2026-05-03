#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PKL補正の網羅的検証スクリプト

全競馬場×全距離×全コース×全馬場状態で:
1. PKL統計値の妥当性（サンプル数、異常値）
2. 季節補正値の妥当性（振幅が常識的か）
3. venue補正値の妥当性（実データとの整合性）
4. 補正前後のT指数シミュレーション比較
"""

import sys, pickle
from pathlib import Path
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PARQUET_PATH = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
PKL_DIR = PROJECT_ROOT / "keibaai" / "data" / "processed"


def load_pkl(name):
    path = PKL_DIR / name
    if not path.exists():
        print(f"  ❌ {name} NOT FOUND")
        return {}
    with open(path, 'rb') as f:
        return pickle.load(f)


def main():
    print("=" * 80)
    print("🔬 PKL補正 網羅的検証")
    print("=" * 80)

    global_stats = load_pkl("global_stats_lookup.pkl")
    venue_adj = load_pkl("venue_adjustment.pkl")
    seasonal_adj = load_pkl("seasonal_adjustment.pkl")
    stats_lookup = load_pkl("stats_lookup.pkl")

    # Load raw data for verification
    df = pd.read_parquet(PARQUET_PATH, columns=[
        'race_date', 'venue', 'distance_m', 'track_surface',
        'track_condition', 'finish_position', 'finish_time_seconds', 'last_3f_time'
    ])
    df['race_date'] = pd.to_datetime(df['race_date'])
    df['finish_time_seconds'] = pd.to_numeric(df['finish_time_seconds'], errors='coerce')
    df['last_3f_time'] = pd.to_numeric(df['last_3f_time'], errors='coerce')
    df = df.dropna(subset=['finish_time_seconds', 'track_surface', 'venue', 'distance_m'])
    df = df[df['track_surface'].isin(['芝', 'ダート'])]
    df_top3 = df[df['finish_position'].isin([1, 2, 3])]

    issues = []

    # ===== 1. global_stats_lookup.pkl 検証 =====
    print("\n" + "=" * 80)
    print("【1】global_stats_lookup.pkl 検証")
    print("=" * 80)
    print(f"  条件数: {len(global_stats)}")
    
    source_counts = {}
    for k, v in global_stats.items():
        src = v.get('source', 'unknown')
        source_counts[src] = source_counts.get(src, 0) + 1
    for src, cnt in sorted(source_counts.items()):
        print(f"  ソース '{src}': {cnt} 条件")
    
    # 異常値チェック
    for key, val in global_stats.items():
        dist, surf, cond = key
        if val['time_std'] == 0.5:  # Minimum clippingされたもの
            issues.append(f"global_stats: {surf}{dist}m({cond}) stdがクリップ(0.5)")
        if val['count'] < 20:
            issues.append(f"global_stats: {surf}{dist}m({cond}) サンプル少(N={val['count']})")

    # ===== 2. stats_lookup.pkl 検証 =====
    print(f"\n{'=' * 80}")
    print("【2】stats_lookup.pkl 検証")
    print("=" * 80)
    print(f"  条件数: {len(stats_lookup)}")
    
    source_counts2 = {}
    for k, v in stats_lookup.items():
        src = v.get('source', 'unknown')
        source_counts2[src] = source_counts2.get(src, 0) + 1
    for src, cnt in sorted(source_counts2.items()):
        print(f"  ソース '{src}': {cnt} 条件")

    # ===== 3. venue_adjustment.pkl 検証 =====
    print(f"\n{'=' * 80}")
    print("【3】venue_adjustment.pkl 検証")
    print("=" * 80)
    
    full_key_count = sum(1 for k in venue_adj if len(k) == 4)
    simple_key_count = sum(1 for k in venue_adj if len(k) == 3)
    print(f"  フルキー(venue,dist,surf,cond): {full_key_count}")
    print(f"  簡略キー(venue,dist,surf): {simple_key_count}")
    
    # 極端な補正値チェック
    print(f"\n  補正値が大きい(>3秒)条件:")
    for key, val in venue_adj.items():
        if len(key) == 4 and abs(val['time_adjustment']) > 3.0:
            venue, dist, surf, cond = key
            issues.append(f"venue_adj: {venue}{surf}{dist}m({cond}) 補正={val['time_adjustment']:+.2f}秒 (極端)")
            print(f"    ⚠️ {venue} {surf}{dist}m({cond}): {val['time_adjustment']:+.2f}秒 (N={val['count']})")

    # ===== 4. seasonal_adjustment.pkl 検証 =====
    print(f"\n{'=' * 80}")
    print("【4】seasonal_adjustment.pkl 検証")
    print("=" * 80)
    print(f"  条件数: {len(seasonal_adj)}")

    # 各(venue, dist, surf)で何月分のデータがあるか
    combos = {}
    for (venue, dist, surf, month), val in seasonal_adj.items():
        k = (venue, dist, surf)
        if k not in combos:
            combos[k] = {}
        combos[k][month] = val

    print(f"  ユニーク(venue,dist,surf): {len(combos)}")

    # 季節振幅チェック
    print(f"\n  季節振幅(月間最大-最小)が大きい上位10条件:")
    swings = []
    for combo, months in combos.items():
        adjs = [v['time_adj'] for v in months.values()]
        swing = max(adjs) - min(adjs)
        swings.append((combo, swing, len(months), months))
    swings.sort(key=lambda x: -x[1])
    
    for combo, swing, n_months, months in swings[:10]:
        venue, dist, surf = combo
        best_m = min(months, key=lambda m: months[m]['time_adj'])
        worst_m = max(months, key=lambda m: months[m]['time_adj'])
        print(f"    {venue} {surf}{dist}m: 振幅={swing:.2f}秒 ({n_months}ヶ月)")
        print(f"      最速{best_m}月({months[best_m]['time_adj']:+.2f}秒, N={months[best_m]['count']}) "
              f"最遅{worst_m}月({months[worst_m]['time_adj']:+.2f}秒, N={months[worst_m]['count']})")
        if swing > 5.0:
            issues.append(f"seasonal: {venue}{surf}{dist}m 振幅={swing:.2f}秒 (異常に大きい)")

    # N<20の季節補正
    small_n = [(k, v) for k, v in seasonal_adj.items() if v['count'] < 20]
    print(f"\n  サンプル数 < 20 の季節補正: {len(small_n)}件")
    for (venue, dist, surf, month), val in small_n[:5]:
        print(f"    {venue} {surf}{dist}m {month}月: N={val['count']}, 補正={val['time_adj']:+.2f}秒")

    # ===== 5. T指数シミュレーション: 補正前vs補正後 =====
    print(f"\n{'=' * 80}")
    print("【5】T指数シミュレーション（同じ馬が異なる条件で走った場合）")
    print("=" * 80)
    
    test_cases = [
        # (場所, 距離, コース, 馬場, 月, 実タイム, 説明)
        ('中山', 2000, '芝', '良', 1, 121.5, '冬の中山芝2000m'),
        ('中山', 2000, '芝', '良', 4, 121.5, '春の中山芝2000m'),
        ('中山', 2000, '芝', '良', 9, 121.5, '秋の中山芝2000m'),
        ('東京', 2000, '芝', '良', 5, 121.5, '春の東京芝2000m'),
        ('京都', 2000, '芝', '良', 10, 121.5, '秋の京都芝2000m'),
        ('中山', 1200, '芝', '良', 1, 69.5, '冬の中山芝1200m'),
        ('中山', 1200, '芝', '良', 9, 69.5, '秋の中山芝1200m'),
        ('中山', 1800, 'ダート', '良', 1, 114.0, '冬の中山ダ1800m'),
        ('中山', 1800, 'ダート', '良', 9, 114.0, '秋の中山ダ1800m'),
        ('札幌', 1200, '芝', '良', 7, 69.5, '夏の札幌芝1200m'),
    ]
    
    print(f"\n  {'条件':<22} {'実タイム':>7} {'v1 T指数':>8} {'v2 T指数':>8} {'差':>6} {'補正詳細'}")
    print("  " + "-" * 80)
    
    for venue, dist, surf, cond, month, time_sec, desc in test_cases:
        global_key = (dist, surf, cond)
        venue_key = (venue, dist, surf, cond)
        season_key = (venue, dist, surf, month)
        
        g_stats = global_stats.get(global_key)
        v_adj = venue_adj.get(venue_key)
        s_adj = seasonal_adj.get(season_key)
        
        if not g_stats or not v_adj:
            print(f"  {desc:<22} {time_sec:>7.1f}   データなし")
            continue
        
        time_adj = v_adj['time_adjustment']
        season_time_adj = s_adj['time_adj'] if s_adj else 0.0
        g_mean = g_stats['time_mean']
        g_std = g_stats['time_std']
        
        # v1: venue補正のみ
        adj_time_v1 = time_sec - time_adj
        t_idx_v1 = 50 + 10 * (g_mean - adj_time_v1) / g_std
        t_idx_v1 = max(20, min(80, t_idx_v1))
        
        # v2: venue + seasonal補正
        adj_time_v2 = time_sec - time_adj - season_time_adj
        t_idx_v2 = 50 + 10 * (g_mean - adj_time_v2) / g_std
        t_idx_v2 = max(20, min(80, t_idx_v2))
        
        diff = t_idx_v2 - t_idx_v1
        detail = f"会場{time_adj:+.2f}, 季節{season_time_adj:+.2f}"
        print(f"  {desc:<22} {time_sec:>7.1f}   {t_idx_v1:>6.1f}   {t_idx_v2:>6.1f}  {diff:>+5.1f}  {detail}")

    # ===== 6. カバレッジ: どの条件がPKLに存在するか =====
    print(f"\n{'=' * 80}")
    print("【6】カバレッジ分析（レースが行われた全条件のうちPKLに存在する割合）")
    print("=" * 80)
    
    cutoff = pd.Timestamp.now() - pd.DateOffset(years=3)
    df_recent = df_top3[df_top3['race_date'] >= cutoff]
    
    # 実際に存在する条件
    real_conditions = df_recent.groupby(['venue', 'distance_m', 'track_surface', 'track_condition']).size().reset_index(name='count')
    real_conditions = real_conditions[real_conditions['count'] >= 3]
    
    total = len(real_conditions)
    covered_global = sum(1 for _, r in real_conditions.iterrows() 
                        if (int(r['distance_m']), r['track_surface'], r['track_condition']) in global_stats)
    covered_venue = sum(1 for _, r in real_conditions.iterrows()
                       if (r['venue'], int(r['distance_m']), r['track_surface'], r['track_condition']) in venue_adj)
    covered_stats = sum(1 for _, r in real_conditions.iterrows()
                       if (r['venue'], int(r['distance_m']), r['track_surface'], r['track_condition']) in stats_lookup)
    
    print(f"  直近3年の実条件数: {total}")
    print(f"  global_stats カバー: {covered_global}/{total} ({covered_global/total*100:.1f}%)")
    print(f"  venue_adj カバー: {covered_venue}/{total} ({covered_venue/total*100:.1f}%)")
    print(f"  stats_lookup カバー: {covered_stats}/{total} ({covered_stats/total*100:.1f}%)")
    
    # 未カバーの条件
    print(f"\n  global_statsに未登録の条件:")
    for _, r in real_conditions.iterrows():
        gk = (int(r['distance_m']), r['track_surface'], r['track_condition'])
        if gk not in global_stats:
            print(f"    {r['venue']} {r['track_surface']}{int(r['distance_m'])}m({r['track_condition']}) N={r['count']}")

    # ===== 7. 総合判定 =====
    print(f"\n{'=' * 80}")
    print("【7】総合判定")
    print("=" * 80)
    
    if issues:
        print(f"\n  ⚠️ 発見された問題: {len(issues)}件")
        for i, issue in enumerate(issues, 1):
            print(f"    {i}. {issue}")
    else:
        print("  ✅ 重大な問題は検出されませんでした")
    
    print(f"\n{'=' * 80}")
    print("✅ 網羅的検証完了")
    print("=" * 80)


if __name__ == "__main__":
    main()

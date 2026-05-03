#!/usr/bin/env python3
"""L指数が20にクリップされる原因を調査"""
import pickle, sys
from pathlib import Path

PKL_DIR = Path(r"keibaai\data\processed")

# Load all PKLs
with open(PKL_DIR / "global_stats_lookup.pkl", 'rb') as f:
    global_stats = pickle.load(f)
with open(PKL_DIR / "venue_adjustment.pkl", 'rb') as f:
    venue_adj = pickle.load(f)
with open(PKL_DIR / "seasonal_adjustment.pkl", 'rb') as f:
    seasonal_adj = pickle.load(f)

# 阪神のテスト条件 (11R = 芝1400m 良)
test_cases = [
    # (venue, dist, surf, cond, month, 上り3F実測値)
    ('阪神', 1400, '芝', '良', 3, 34.5),
    ('阪神', 1400, '芝', '良', 3, 35.0),
    ('阪神', 1400, '芝', '良', 3, 35.5),
    ('阪神', 2000, '芝', '良', 3, 34.0),
    ('阪神', 1800, 'ダート', '良', 3, 37.0),
    # 中山の同条件（正常に動く）
    ('中山', 1800, '芝', '良', 2, 35.0),
    ('中山', 2000, '芝', '良', 2, 34.5),
]

print("=" * 100)
print("L指数デバッグ: 計算過程を可視化")
print("=" * 100)

for venue, dist, surf, cond, month, l3f_sec in test_cases:
    global_key = (dist, surf, cond)
    venue_key = (venue, dist, surf, cond)
    season_key = (venue, dist, surf, month)
    
    g_stats = global_stats.get(global_key)
    v_adj = venue_adj.get(venue_key)
    s_adj = seasonal_adj.get(season_key)
    
    print(f"\n--- {venue} {surf}{dist}m ({cond}) {month}月, 上り={l3f_sec}s ---")
    
    if not g_stats:
        print(f"  ❌ global_stats に {global_key} なし")
        continue
    if not v_adj:
        print(f"  ❌ venue_adj に {venue_key} なし")
        # Try simple key
        simple = venue_adj.get((venue, dist, surf))
        if simple:
            print(f"  → 簡略キー {(venue,dist,surf)} あり: {simple}")
        continue
    
    l3f_adj = v_adj.get('l3f_adjustment', 0.0)
    season_l3f_adj = s_adj.get('l3f_adj', 0.0) if s_adj else 0.0
    g_mean3f = g_stats['l3f_mean']
    g_std3f = g_stats['l3f_std']
    
    adj_l3f = l3f_sec - l3f_adj - season_l3f_adj
    g_idx3f = 50 + 10 * (g_mean3f - adj_l3f) / g_std3f
    g_idx3f_clipped = max(20.0, min(80.0, g_idx3f))
    
    print(f"  global_stats: l3f_mean={g_mean3f:.3f}, l3f_std={g_std3f:.3f}")
    print(f"  venue_adj (l3f): {l3f_adj:+.3f}")
    print(f"  seasonal_adj (l3f): {season_l3f_adj:+.3f}")
    print(f"  計算: adj_l3f = {l3f_sec} - ({l3f_adj:+.3f}) - ({season_l3f_adj:+.3f}) = {adj_l3f:.3f}")
    print(f"  L指数 = 50 + 10 * ({g_mean3f:.3f} - {adj_l3f:.3f}) / {g_std3f:.3f}")
    print(f"        = 50 + 10 * {g_mean3f - adj_l3f:.3f} / {g_std3f:.3f}")
    print(f"        = 50 + {10 * (g_mean3f - adj_l3f) / g_std3f:.1f}")
    print(f"        = {g_idx3f:.1f} → clipped: {g_idx3f_clipped:.1f}")

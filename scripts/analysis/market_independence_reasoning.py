#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深層推論：市場に引っ張られない改善策

問題:
- 騎手勝率(JW)が低い人気馬をAIが見逃している
- しかしオッズ/人気を使うと「市場の後追い」になる
- AIの独自性を維持しつつ改善したい

推論:
1. なぜ騎手勝率が「過大評価」されているのか？
2. 騎手勝率に頼らず「馬の実力」を測る方法はないか？
3. 条件付きで重みを調整する（市場ではなく他の条件で）
"""

import pandas as pd
import numpy as np
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent

preds = pd.read_csv(project_root / 'outputs/analysis/race_level_deep_analysis.csv')

# ============================================================
# 推論1: なぜJWが「過大評価」されるのか？
# ============================================================
print("="*70)
print("【推論1】なぜ騎手勝率が過大評価されるのか？")
print("="*70)

# 仮説: JWが低い馬は「実際に着順が悪い」傾向があり、
# モデルがそれを学習しすぎている

print("\n■ JW別の実際の平均着順（全馬）")
jw_bins = [(0, 0.05), (0.05, 0.10), (0.10, 0.15), (0.15, 0.20), (0.20, 1.0)]
for low, high in jw_bins:
    subset = preds[(preds['jockey_win_rate'] >= low) & (preds['jockey_win_rate'] < high)]
    if len(subset) > 500:
        avg_finish = subset['finish_position'].mean()
        first_rate = (subset['finish_position'] == 1).mean() * 100
        print(f"    JW {low:.2f}-{high:.2f}: 平均着順 {avg_finish:.2f}, 1着率 {first_rate:.1f}% (n={len(subset):,})")

print("""
考察:
- 確かにJWが高いほど着順は良い傾向がある
- しかし「人気馬」の場合はJWの影響が小さくなるはず
- モデルはその「条件付き」の関係を学習できていない可能性
""")

# ============================================================
# 推論2: 「人気帯」ではなく「馬の実力系特徴量」で分ける
# ============================================================
print("="*70)
print("【推論2】オッズ/人気を使わずに「馬の実力」を測る方法")
print("="*70)

print("""
■ 考えられる「馬の実力」を示す特徴量:
  1. win_rate, place_rate（過去の勝率・複勝率）
  2. avg_finish_position（過去の平均着順）
  3. 血統系の特徴量（horse_place_rate_sireなど）
  4. 近走のパフォーマンス（タイム、上がり3Fなど）

■ 仮説:
  「馬の実力（win_rate等）が高い場合、騎手の影響は相対的に小さい」
  → これは市場ではなく「馬のデータ」に基づく条件
""")

# ============================================================
# 推論3: win_rateが高い馬でのJWの影響を検証
# ============================================================
print("="*70)
print("【推論3】馬の勝率(win_rate)別にJWの影響を検証")
print("="*70)

# win_rateが存在するか確認
if 'win_rate' in preds.columns:
    # win_rateが高い馬（過去に勝っている）
    high_wr = preds[preds['win_rate'] >= 0.15]  # 勝率15%以上
    low_wr = preds[preds['win_rate'] < 0.05]  # 勝率5%未満
    
    print(f"\n■ 馬自身の勝率別分析")
    print(f"  win_rate >= 15%: {len(high_wr):,}頭")
    print(f"  win_rate < 5%: {len(low_wr):,}頭")
    
    # 高勝率馬でのJW別成績
    print("\n■ 高勝率馬(win_rate>=15%)でのJW別1着率:")
    for low, high in [(0, 0.10), (0.10, 0.15), (0.15, 1.0)]:
        subset = high_wr[(high_wr['jockey_win_rate'] >= low) & (high_wr['jockey_win_rate'] < high)]
        if len(subset) > 100:
            first_rate = (subset['finish_position'] == 1).mean() * 100
            print(f"    JW {low:.2f}-{high:.2f}: 1着率 {first_rate:.1f}% (n={len(subset)})")
    
    # 低勝率馬でのJW別成績
    print("\n■ 低勝率馬(win_rate<5%)でのJW別1着率:")
    for low, high in [(0, 0.10), (0.10, 0.15), (0.15, 1.0)]:
        subset = low_wr[(low_wr['jockey_win_rate'] >= low) & (low_wr['jockey_win_rate'] < high)]
        if len(subset) > 100:
            first_rate = (subset['finish_position'] == 1).mean() * 100
            print(f"    JW {low:.2f}-{high:.2f}: 1着率 {first_rate:.1f}% (n={len(subset)})")

# ============================================================
# 推論4: place_rateでも同様に検証
# ============================================================
print("\n" + "="*70)
print("【推論4】複勝率(place_rate)別にJWの影響を検証")
print("="*70)

if 'place_rate' in preds.columns:
    high_pr = preds[preds['place_rate'] >= 0.50]  # 複勝率50%以上
    low_pr = preds[preds['place_rate'] < 0.20]  # 複勝率20%未満
    
    print(f"\n■ 馬自身の複勝率別分析")
    print(f"  place_rate >= 50%: {len(high_pr):,}頭")
    print(f"  place_rate < 20%: {len(low_pr):,}頭")
    
    print("\n■ 高複勝率馬(place_rate>=50%)でのJW別1着率:")
    for low, high in [(0, 0.10), (0.10, 0.15), (0.15, 1.0)]:
        subset = high_pr[(high_pr['jockey_win_rate'] >= low) & (high_pr['jockey_win_rate'] < high)]
        if len(subset) > 100:
            first_rate = (subset['finish_position'] == 1).mean() * 100
            print(f"    JW {low:.2f}-{high:.2f}: 1着率 {first_rate:.1f}% (n={len(subset)})")

# ============================================================
# 推論5: 改善策のアイデア
# ============================================================
print("\n" + "="*70)
print("【推論5】市場に引っ張られない改善策")
print("="*70)

print("""
■ 案A: 条件付き交互作用項の追加
  - 「馬の勝率 × 騎手勝率」の交互作用項を追加
  - 馬の勝率が高い場合、JWの影響を自動的に下げる
  - 市場データ（オッズ）は使わない

■ 案B: 騎手勝率の「相対化」
  - JWを絶対値ではなく「そのレース内での相対順位」で使用
  - 例: レース内でJW何位か
  - これにより、全体的なJWの影響が抑制される

■ 案C: 馬の実力指標の強化
  - win_rate, place_rateの重みを上げる
  - JWの重みを直接下げる
  - より「馬本位」の予測になる

■ 案D: 近走パフォーマンスの活用
  - 近5走の着順変動、タイム変動を特徴量化
  - 「直近の調子」を捉える（これが市場が見ている情報かも）
  - 騎手ではなく馬のコンディションを重視

■ 案E: 騎手勝率の上限設定（クリッピング）
  - JWが0.20以上は全て0.20として扱う
  - 高JW騎手の過大評価を防ぐ
  - 逆にJWが低い馬の不当な低評価も緩和
""")

# ============================================================
# 各案の検証データを収集
# ============================================================
print("\n" + "="*70)
print("【データ収集】各案の効果を予測するためのデータ")
print("="*70)

# パターンA（見逃し）馬のwin_rate/place_rate分布
winners = preds[preds['finish_position'] == 1]
pattern_a = winners[(winners['popularity'] <= 3) & (winners['rank'] >= 6)]

print(f"\n■ パターンA馬（見逃し549頭）の馬自身の成績:")
if 'win_rate' in pattern_a.columns:
    print(f"    平均win_rate: {pattern_a['win_rate'].mean():.4f}")
if 'place_rate' in pattern_a.columns:
    print(f"    平均place_rate: {pattern_a['place_rate'].mean():.4f}")

# 正しく予測できた馬
correct = winners[(winners['popularity'] <= 3) & (winners['rank'] <= 3)]
print(f"\n■ 正しく予測した馬（1172頭）の馬自身の成績:")
if 'win_rate' in correct.columns:
    print(f"    平均win_rate: {correct['win_rate'].mean():.4f}")
if 'place_rate' in correct.columns:
    print(f"    平均place_rate: {correct['place_rate'].mean():.4f}")

print("""
考察:
- もしパターンAの馬も「馬自身の勝率」が高いなら、
  それを根拠にJWの影響を抑えることができる
- 市場（オッズ）に頼らず、馬のデータだけで改善可能
""")

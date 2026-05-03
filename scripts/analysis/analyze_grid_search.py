#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""グリッドサーチ結果の深い分析"""
import pandas as pd
import numpy as np

df = pd.read_csv('outputs/analysis/grid_search_results.csv')

print('='*80)
print('グリッドサーチ結果 深い分析')
print('='*80)

# ============================================================
# 1. V4.4ウェイトの影響分析
# ============================================================
print('\n【1. V4.4ウェイトの影響（iterations=50固定）】')
print('-'*60)

iter50 = df[df['iterations'] == 50]
for v44 in [0.0, 0.05, 0.1]:
    sub = iter50[iter50['v44_weight'] == v44]
    print(f"  V4.4={v44*100:.0f}%: win {sub['win_test_roi'].mean():.1f}%, ensemble {sub['ensemble_test_roi'].mean():.1f}%")

print('\n  ★ 観察: win_test_roiはV4.4ウェイトに依存しない')
print('     → ensembleのみ影響（V4.4成分が含まれるため）')
print('     → win/top2/placeモデル自体はV4.4ウェイトと独立')

# ============================================================
# 2. iterationsの影響分析
# ============================================================
print('\n【2. iterationsの影響（V4.4=0%固定）】')
print('-'*60)

v44_0 = df[df['v44_weight'] == 0.0]
for iters in [50, 100, 150]:
    sub = v44_0[v44_0['iterations'] == iters]
    print(f"  iter={iters}: win {sub['win_test_roi'].mean():.1f}% (Gap {sub['win_gap'].mean():.1f}%), Valid {sub['win_valid_roi'].mean():.1f}%")

print('\n  ★ 観察: iterationsが増えると')
print('     - Valid ROI: 上昇（過学習増加）')
print('     - Test ROI: 微増～横ばい')
print('     - Gap: 大幅増加')
print('     → iterations=50が最もGap効率が良い')

# ============================================================
# 3. 年別の詳細分析
# ============================================================
print('\n【3. 年別パフォーマンス（iterations=50, V4.4=0%）】')
print('-'*60)

best = df[(df['iterations'] == 50) & (df['v44_weight'] == 0.0)]
print(f"{'年':>6} | {'Valid':>8} | {'Test':>8} | {'Gap':>8} | {'評価'}")
print('-'*50)
for _, row in best.iterrows():
    gap = row['win_gap']
    rating = '✓良好' if gap < 60 else ('△中程度' if gap < 80 else '×高い')
    print(f"{int(row['year']):>6} | {row['win_valid_roi']:>7.1f}% | {row['win_test_roi']:>7.1f}% | {gap:>7.1f}% | {rating}")

# ============================================================
# 4. 過学習の根本原因分析
# ============================================================
print('\n【4. 過学習の根本原因分析】')
print('-'*60)

print('Valid ROI vs Test ROI の乖離パターン:')
for iters in [50, 100, 150]:
    sub = v44_0[v44_0['iterations'] == iters]
    valid_avg = sub['win_valid_roi'].mean()
    test_avg = sub['win_test_roi'].mean()
    ratio = valid_avg / test_avg
    print(f"  iter={iters}: Valid/Test比 = {ratio:.2f} ({valid_avg:.0f}% / {test_avg:.0f}%)")

print('\n  ★ 根本原因仮説:')
print('     1. Validationデータが小さすぎる（1年≒4万件）')
print('     2. Validationでの勝率推定が不安定')
print('     3. 年をまたぐ分布シフト')

# ============================================================
# 5. top2_probの安定性
# ============================================================
print('\n【5. top2_prob vs win_prob 安定性比較】')
print('-'*60)

for iters in [50, 100, 150]:
    sub = v44_0[v44_0['iterations'] == iters]
    win_gap = sub['win_gap'].mean()
    top2_gap = sub['top2_gap'].mean()
    print(f"  iter={iters}: win Gap {win_gap:.1f}%, top2 Gap {top2_gap:.1f}% (差 {win_gap-top2_gap:.1f}pt)")

print('\n  ★ 観察: top2_probはwin_probより常にGapが低い')
print('     → 2着以内の正例率が高く、推定が安定')

# ============================================================
# 6. 既存システムとの本質的な差
# ============================================================
print('\n【6. 既存System A+との本質的な差】')
print('-'*60)

print('既存System A+:')
print('  - 単一ターゲット（is_win）')
print('  - 全データで統一学習')
print('  - ROI 80.2%')
print()
print('今回のSystem E:')
print('  - 3ターゲット分離（win/top2/place）')
print('  - 芝/ダート分離学習')
print(f'  - ROI {best["win_test_roi"].mean():.1f}%')
print()
print('  ★ 差の原因仮説:')
print('     1. 3分離によりサンプル効率が低下')
print('     2. 芝/ダート分離でさらに半減')
print('        → 総サンプル: 25万件 → 芝12万/ダ12万 → 各指数4万件相当')
print('     3. 過学習リスク増大')

# ============================================================
# 7. 最終推奨
# ============================================================
print('\n【7. 最終推奨と深い考察】')
print('='*60)

print('\n推奨パラメータ: iterations=50, V4.4=0%')
print(f'  - ROI: {best["win_test_roi"].mean():.1f}%')
print(f'  - Gap: {best["win_gap"].mean():.1f}%')
print(f'  - 既存との差: {best["win_test_roi"].mean() - 80.2:+.1f}pt')

print('\n深い考察:')
print('  Q: なぜ既存System A+より-2.7pt低いのか？')
print()
print('  A1: サンプル効率の低下')
print('      - 3指数分離 → 各モデルのサンプル減少')
print('      - 芝/ダート分離 → さらに半減')
print('      - 結果: 過学習増加 + 汎化性能低下')
print()
print('  A2: ターゲット設計の違い')
print('      - 既存: is_win（シンプル）')
print('      - 今回: win_prob/top2_prob/place_prob（複雑）')
print('      - 複雑なターゲットは学習が困難')
print()
print('  A3: ensemble統合の非最適性')
print('      - 単純な線形加重では最適でない可能性')
print('      - スタッキングやメタ学習が必要かも')

print('\n次のステップ案:')
print('  1. 3指数統合をやめ、既存のis_winベースに戻す')
print('  2. top2_prob単独で馬連戦略のみ構築')
print('  3. サンプル効率を維持する別設計を検討')

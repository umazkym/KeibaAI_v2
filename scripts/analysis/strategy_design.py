#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深層戦略分析: ROI 100%達成のための具体的戦略設計
"""

import pandas as pd
import numpy as np

# 予測結果を読み込み
preds = pd.read_csv('outputs/analysis/comprehensive_analysis_preds.csv')

print('='*70)
print('【深層分析】ROI 100%達成のための戦略設計')
print('='*70)

# === 分析1: オッズ帯とpopularity乖離の組み合わせ ===
print('\n【戦略1】オッズ5-10倍 × AI予測1位 × 人気順≠1位')

top1 = preds[preds['rank'] == 1]
# オッズ5-10倍で1番人気でない
target = top1[(top1['win_odds'] >= 5) & (top1['win_odds'] < 10) & (top1['popularity'] != 1)]
bets = len(target)
hits = (target['finish_position'] == 1).sum()
returns = target[target['finish_position'] == 1]['win_odds'].sum()
roi = returns / bets * 100 if bets > 0 else 0
hit_rate = hits / bets * 100 if bets > 0 else 0
print(f'  件数: {bets}, 的中: {hits}, 的中率: {hit_rate:.1f}%, ROI: {roi:.1f}%')

# === 分析2: 馬連Top3-Top4をさらに深堀り ===
print('\n【戦略2】馬連Top3-Top4のオッズ合計別分析')

# Top3とTop4の情報を取得
top3 = preds[preds['rank'] == 3][['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
top3.columns = ['race_id', 'h3', 'odds3', 'pop3', 'pos3']
top4 = preds[preds['rank'] == 4][['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
top4.columns = ['race_id', 'h4', 'odds4', 'pop4', 'pos4']

combo = top3.merge(top4, on='race_id')
combo['both_top2'] = ((combo['pos3'] <= 2) & (combo['pos4'] <= 2))

total = len(combo)
hits = combo['both_top2'].sum()
print(f'  全件: {total}, 的中: {hits}件, 的中率: {hits/total*100:.2f}%')

# オッズ合計による分類
combo['odds_sum'] = combo['odds3'] + combo['odds4']
for low, high in [(0, 30), (30, 60), (60, 100), (100, 200), (200, 500)]:
    subset = combo[(combo['odds_sum'] >= low) & (combo['odds_sum'] < high)]
    hr = subset['both_top2'].mean() * 100 if len(subset) > 0 else 0
    n = len(subset)
    nh = subset['both_top2'].sum()
    print(f'  オッズ合計{low:3}-{high:3}: {n:4}件, 的中{nh:2}件, 的中率{hr:.1f}%')

# === 分析3: 人気順のフィルタリング ===
print('\n【戦略3】人気との乖離度分析（単勝Top1）')

# 人気順ごとにTop1のROI
for pop in range(1, 8):
    subset = top1[top1['popularity'] == pop]
    if len(subset) > 0:
        roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
        hr = (subset['finish_position'] == 1).mean() * 100
        print(f'  Top1が{pop}番人気: {len(subset):4}件, 的中率{hr:5.1f}%, ROI {roi:5.1f}%')

# === 分析4: オッズ × 人気 組み合わせ ===
print('\n【戦略4】オッズ帯 × 人気≠1のROI（単勝Top1）')

for low, high in [(3, 5), (5, 10), (10, 20), (20, 50)]:
    # 1番人気でない
    subset = top1[(top1['win_odds'] >= low) & (top1['win_odds'] < high) & (top1['popularity'] != 1)]
    if len(subset) > 0:
        roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
        hr = (subset['finish_position'] == 1).mean() * 100
        print(f'  オッズ{low:2}-{high:2}倍 & 人気≠1: {len(subset):4}件, 的中率{hr:5.1f}%, ROI {roi:6.1f}%')

# === 結論 ===
print('\n' + '='*70)
print('【ROI 100%達成への具体的戦略】')
print('='*70)
print('''
■ 戦略A: 単勝「中穴狙い」
  条件: AI Top1 & オッズ5-10倍 (& 人気≠1位の場合さらに絞れる)
  期待ROI: 89%前後
  改善案: stable/moderate条件フィルタ追加

■ 戦略B: 馬連「穴組み合わせ」
  条件: Top3-Top4の馬連
  期待ROI: 99.9%（2024年で100%近い）
  改善案: オッズ合計30-100帯で絞る

■ モデル改良の方向性
  1. 「人気≠AI予測」ケースの精度向上
     → 隠れた実力馬を見つける特徴量強化
  2. 中穴帯の的中率向上
     → 期待値モデルとの統合検討
  3. レース条件別の専門モデル
     → 芝/ダート/距離/クラス別チューニング
''')

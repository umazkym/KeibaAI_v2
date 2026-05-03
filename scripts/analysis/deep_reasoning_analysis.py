#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
なぜその傾向が起きるのか？を推論するための深層分析

目的:
- 「AIが見逃す勝ち馬」の具体的特徴を特定
- 「AIが過大評価する馬」の共通パターンを発見
- 改善のための具体的な仮説を構築

作成日: 2026-01-12
"""

import pandas as pd
import numpy as np
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent

preds = pd.read_csv(project_root / 'outputs/analysis/race_level_deep_analysis.csv')
print(f"データ: {len(preds):,}件")

# ============================================================
# 深層分析1: AIが見逃す勝ち馬の詳細プロファイル
# ============================================================
print("\n" + "="*80)
print("【深層分析1】AIが見逃す勝ち馬（AI予測6位以下→1着）の詳細")
print("="*80)

winners = preds[preds['finish_position'] == 1]
missed = winners[winners['rank'] >= 6]
caught = winners[winners['rank'] <= 3]

print(f"\n全1着馬: {len(winners)}頭")
print(f"  見逃し: {len(missed)}頭 ({len(missed)/len(winners)*100:.1f}%)")
print(f"  的中: {len(caught)}頭 ({len(caught)/len(winners)*100:.1f}%)")

# 詳細比較
print("\n■ 見逃し vs 的中 特徴量比較")
compare_cols = ['popularity', 'win_odds', 'jockey_win_rate', 'ensemble_score']
compare_cols = [c for c in compare_cols if c in preds.columns]

print(f"{'特徴量':25} {'見逃し':>12} {'的中':>12} {'差':>12}")
print("-" * 60)
for col in compare_cols:
    m_val = missed[col].mean()
    c_val = caught[col].mean()
    diff = m_val - c_val
    print(f"{col:25} {m_val:12.3f} {c_val:12.3f} {diff:+12.3f}")

# 見逃しパターンの細分化
print("\n■ 見逃しパターン細分化")

# パターンA: 1-3番人気なのにAI予測6位以下
pattern_a = missed[missed['popularity'] <= 3]
print(f"\n  [パターンA] 1-3番人気だがAI低評価: {len(pattern_a)}頭")
if len(pattern_a) > 0:
    print(f"    平均オッズ: {pattern_a['win_odds'].mean():.1f}倍")
    print(f"    騎手勝率: {pattern_a['jockey_win_rate'].mean():.3f}")
    print(f"    AIスコア: {pattern_a['ensemble_score'].mean():.3f}")
    
# パターンB: 4-6番人気でAI予測6位以下
pattern_b = missed[missed['popularity'].isin([4, 5, 6])]
print(f"\n  [パターンB] 4-6番人気でAI低評価: {len(pattern_b)}頭")
if len(pattern_b) > 0:
    print(f"    平均オッズ: {pattern_b['win_odds'].mean():.1f}倍")
    print(f"    騎手勝率: {pattern_b['jockey_win_rate'].mean():.3f}")
    print(f"    AIスコア: {pattern_b['ensemble_score'].mean():.3f}")

# パターンC: 7番人気以下の穴馬で1着
pattern_c = missed[missed['popularity'] >= 7]
print(f"\n  [パターンC] 7番人気以下の穴馬が激走: {len(pattern_c)}頭")
if len(pattern_c) > 0:
    print(f"    平均オッズ: {pattern_c['win_odds'].mean():.1f}倍")
    print(f"    騎手勝率: {pattern_c['jockey_win_rate'].mean():.3f}")
    print(f"    AIスコア: {pattern_c['ensemble_score'].mean():.3f}")

# ============================================================
# 深層分析2: 予測スコアが示す「確信度」と実際の結果
# ============================================================
print("\n" + "="*80)
print("【深層分析2】予測スコアの分析")
print("="*80)

if 'ensemble_score' in preds.columns:
    print("\n■ スコア分布と着順の関係（全馬）")
    
    # スコアを10分位に分割
    preds['score_decile'] = pd.qcut(preds['ensemble_score'], 10, labels=False, duplicates='drop')
    
    for d in range(10):
        subset = preds[preds['score_decile'] == d]
        avg_finish = subset['finish_position'].mean()
        first_rate = (subset['finish_position'] == 1).mean() * 100
        top3_rate = (subset['finish_position'] <= 3).mean() * 100
        avg_score = subset['ensemble_score'].mean()
        
        print(f"    分位{d+1:2}: スコア{avg_score:.3f}, 平均着順{avg_finish:.2f}, "
              f"1着率{first_rate:.1f}%, 3着内{top3_rate:.1f}%")

# ============================================================
# 深層分析3: 「堅いレース」vs「荒れるレース」の特徴
# ============================================================
print("\n" + "="*80)
print("【深層分析3】堅いレースと荒れるレースの特徴")
print("="*80)

# 1着馬の人気でレースを分類
race_winner_pop = preds[preds['finish_position'] == 1].groupby('race_id')['popularity'].first()

# 堅いレース（1-2番人気が勝利）
solid_races = race_winner_pop[race_winner_pop <= 2].index
# 荒れるレース（5番人気以下が勝利）
upset_races = race_winner_pop[race_winner_pop >= 5].index

print(f"\n堅いレース（1-2番人気勝利）: {len(solid_races)}レース")
print(f"荒れるレース（5番人気以下勝利）: {len(upset_races)}レース")

# 堅いレースでのTop1成績
solid_top1 = preds[(preds['race_id'].isin(solid_races)) & (preds['rank'] == 1)]
upset_top1 = preds[(preds['race_id'].isin(upset_races)) & (preds['rank'] == 1)]

print(f"\n■ 堅いレースでのTop1成績:")
solid_hit = (solid_top1['finish_position'] == 1).mean() * 100
solid_top3 = (solid_top1['finish_position'] <= 3).mean() * 100
print(f"    1着率: {solid_hit:.1f}%, 3着内: {solid_top3:.1f}%")
print(f"    平均人気: {solid_top1['popularity'].mean():.1f}番人気")

print(f"\n■ 荒れるレースでのTop1成績:")
upset_hit = (upset_top1['finish_position'] == 1).mean() * 100
upset_top3 = (upset_top1['finish_position'] <= 3).mean() * 100
print(f"    1着率: {upset_hit:.1f}%, 3着内: {upset_top3:.1f}%")
print(f"    平均人気: {upset_top1['popularity'].mean():.1f}番人気")

# 荒れるレースを予測できているか
print(f"\n■ 荒れるレースでの予測分析:")
upset_winners = preds[(preds['race_id'].isin(upset_races)) & (preds['finish_position'] == 1)]
avg_rank = upset_winners['rank'].mean()
top3_predicted = (upset_winners['rank'] <= 3).mean() * 100
top5_predicted = (upset_winners['rank'] <= 5).mean() * 100
print(f"    勝ち馬の平均AI順位: {avg_rank:.1f}位")
print(f"    Top3予測率: {top3_predicted:.1f}%")
print(f"    Top5予測率: {top5_predicted:.1f}%")

# ============================================================
# 深層分析4: 騎手要素の詳細分析
# ============================================================
print("\n" + "="*80)
print("【深層分析4】騎手要素の詳細分析")
print("="*80)

if 'jockey_win_rate' in preds.columns:
    # 騎手勝率と着順の関係
    print("\n■ 騎手勝率帯別の成績分布")
    
    jw_bins = [(0, 0.03), (0.03, 0.06), (0.06, 0.09), (0.09, 0.12), 
               (0.12, 0.15), (0.15, 0.20), (0.20, 0.30), (0.30, 1.0)]
    
    for low, high in jw_bins:
        subset = preds[(preds['jockey_win_rate'] >= low) & (preds['jockey_win_rate'] < high)]
        if len(subset) < 500:
            continue
        
        avg_finish = subset['finish_position'].mean()
        first_rate = (subset['finish_position'] == 1).mean() * 100
        top3_rate = (subset['finish_position'] <= 3).mean() * 100
        
        print(f"    JW {low:.2f}-{high:.2f}: 平均着順{avg_finish:.2f}, "
              f"1着率{first_rate:.1f}%, 3着内{top3_rate:.1f}% (n={len(subset):,})")
    
    # 「騎手勝率が低いが勝った馬」の分析
    winners_low_jw = winners[winners['jockey_win_rate'] < 0.05]
    winners_high_jw = winners[winners['jockey_win_rate'] >= 0.15]
    
    print(f"\n■ 騎手勝率と1着馬の関係")
    print(f"    JW < 5% の勝ち馬: {len(winners_low_jw)}頭")
    print(f"    JW >= 15% の勝ち馬: {len(winners_high_jw)}頭")
    
    if len(winners_low_jw) > 0:
        print(f"\n    [JW < 5% の勝ち馬プロファイル]")
        print(f"      平均人気: {winners_low_jw['popularity'].mean():.1f}番人気")
        print(f"      平均オッズ: {winners_low_jw['win_odds'].mean():.1f}倍")
        print(f"      平均AI順位: {winners_low_jw['rank'].mean():.1f}位")
        print(f"      Top3予測率: {(winners_low_jw['rank'] <= 3).mean()*100:.1f}%")

# ============================================================
# 深層分析5: スコアと人気の乖離分析
# ============================================================
print("\n" + "="*80)
print("【深層分析5】スコアと人気の乖離分析")
print("="*80)

if 'ensemble_score' in preds.columns:
    # レース内でのスコア順位と人気順位の比較
    preds['score_rank'] = preds.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    preds['rank_diff'] = preds['popularity'] - preds['score_rank']
    
    print("\n■ AI順位 vs 人気順位の乖離と着順")
    
    # AIが人気より高評価（rank_diff > 0）
    ai_higher = preds[preds['rank_diff'] >= 3]
    ai_lower = preds[preds['rank_diff'] <= -3]
    ai_match = preds[(preds['rank_diff'] > -2) & (preds['rank_diff'] < 2)]
    
    for name, subset in [('AIが高評価', ai_higher), ('AIが低評価', ai_lower), ('概ね一致', ai_match)]:
        avg_finish = subset['finish_position'].mean()
        first_rate = (subset['finish_position'] == 1).mean() * 100
        print(f"    {name}: 平均着順{avg_finish:.2f}, 1着率{first_rate:.1f}% (n={len(subset):,})")
    
    # AIが高評価したが外れたケース
    ai_high_fail = preds[(preds['rank'] <= 3) & (preds['popularity'] >= 5) & (preds['finish_position'] >= 8)]
    print(f"\n■ AIが高評価（Top3） × 人気外（5番人気以下） × 大敗（8着以下）")
    print(f"    件数: {len(ai_high_fail)}")
    if len(ai_high_fail) > 0 and 'jockey_win_rate' in ai_high_fail.columns:
        print(f"    平均騎手勝率: {ai_high_fail['jockey_win_rate'].mean():.3f}")
        print(f"    平均オッズ: {ai_high_fail['win_odds'].mean():.1f}倍")

# ============================================================
# まとめ: 改善の方向性
# ============================================================
print("\n" + "="*80)
print("【まとめ】分析から見えた改善の方向性")
print("="*80)

print("""
■ 発見1: AIが見逃す勝ち馬は「騎手勝率が低い」ケースが多い
  - 見逃し勝ち馬の平均JW: 0.072
  - 的中した勝ち馬の平均JW: 0.123
  
  → 仮説: AIは騎手勝率を「低評価の根拠」として使いすぎている
  → 改善案: 騎手勝率の影響を相対化する（人気馬との比較など）

■ 発見2: 1-3番人気がAI予測6位以下で勝つケースがある
  - このパターンが多数存在（分析1のパターンA）
  
  → 仮説: 市場は「直近の調子」や「適性」を正しく評価している
  → 改善案: 「直近の調子」を示す特徴量を強化（タイム変動など）

■ 発見3: 荒れるレースの勝ち馬をほとんど予測できていない
  - 荒れるレースでのTop1的中率は堅いレースより大幅に低い
  
  → 仮説: 「荒れ予測」と「勝ち馬予測」は別問題
  → 改善案: レースの荒れ具合を先に予測し、戦略を分ける

■ 発見4: 騎手勝率5%未満でも勝つ馬は存在する
  - これらの馬はAI順位が低くなりがち
  
  → 仮説: 騎手以外の要素（馬の実力、展開）が決定的な場合がある
  → 改善案: 騎手要素の重みを条件付きで調整

■ 全体の方向性
  1. 騎手勝率への過度な依存を見直す
  2. 「直近の調子」「適性」を測る特徴量を追加
  3. レースの「荒れ予測」を別途モデル化
  4. AIと市場の乖離が大きい場合の評価ロジックを見直す
""")

# 結果保存
output_dir = project_root / 'outputs/analysis'
preds.to_csv(output_dir / 'deep_reasoning_analysis.csv', index=False, encoding='utf-8-sig')
print(f"\n結果保存: deep_reasoning_analysis.csv")

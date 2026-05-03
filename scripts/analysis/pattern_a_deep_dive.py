#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
パターンA（人気馬をAIが低評価して負ける）の徹底分析

目的:
- 549頭の「1-3番人気だがAI予測6位以下で勝利」の詳細を分析
- なぜAIがこれらの馬を低評価したのかを特定
- 具体的な改善策を導出

作成日: 2026-01-12
"""

import pandas as pd
import numpy as np
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent

preds = pd.read_csv(project_root / 'outputs/analysis/race_level_deep_analysis.csv')
print(f"データ: {len(preds):,}件")

# パターンA: 1-3番人気だがAI予測6位以下で勝利
winners = preds[preds['finish_position'] == 1]
pattern_a = winners[(winners['popularity'] <= 3) & (winners['rank'] >= 6)]

print(f"\n【パターンA】1-3番人気 × AI予測6位以下 × 1着: {len(pattern_a)}頭")

# ============================================================
# 分析1: パターンAの詳細プロファイル
# ============================================================
print("\n" + "="*80)
print("【分析1】パターンAの詳細プロファイル")
print("="*80)

# 同じレースでAIがTop1に選んだ馬と比較
def get_ai_top1_for_race(race_id):
    race_df = preds[preds['race_id'] == race_id]
    top1 = race_df[race_df['rank'] == 1].iloc[0] if len(race_df[race_df['rank'] == 1]) > 0 else None
    return top1

print("\n■ パターンA馬 vs 同レースのAI Top1 比較（サンプル20件）")
print("-"*100)

compare_data = []
for idx, (_, row) in enumerate(pattern_a.sample(min(20, len(pattern_a))).iterrows()):
    ai_top1 = get_ai_top1_for_race(row['race_id'])
    if ai_top1 is None:
        continue
    
    compare_data.append({
        'race_id': row['race_id'],
        # 実際の勝ち馬（パターンA）
        'winner_name': row['horse_name'],
        'winner_pop': row['popularity'],
        'winner_jw': row.get('jockey_win_rate', np.nan),
        'winner_score': row.get('ensemble_score', np.nan),
        'winner_rank': row['rank'],
        # AIのTop1
        'ai_top1_name': ai_top1['horse_name'],
        'ai_top1_pop': ai_top1['popularity'],
        'ai_top1_jw': ai_top1.get('jockey_win_rate', np.nan),
        'ai_top1_score': ai_top1.get('ensemble_score', np.nan),
        'ai_top1_finish': ai_top1['finish_position'],
    })

compare_df = pd.DataFrame(compare_data)
if len(compare_df) > 0:
    print(f"{'勝馬':10} {'人気':>4} {'JW':>6} {'AIランク':>6} | "
          f"{'AITop1':10} {'人気':>4} {'JW':>6} {'着順':>4}")
    print("-"*80)
    for _, r in compare_df.iterrows():
        print(f"{str(r['winner_name'])[:10]:10} {int(r['winner_pop']):4} {r['winner_jw']:.3f} "
              f"{int(r['winner_rank']):6} | "
              f"{str(r['ai_top1_name'])[:10]:10} {int(r['ai_top1_pop']):4} {r['ai_top1_jw']:.3f} "
              f"{int(r['ai_top1_finish']):4}")

# ============================================================
# 分析2: パターンAの特徴量分布
# ============================================================
print("\n" + "="*80)
print("【分析2】パターンAの特徴量分布")
print("="*80)

# AIがTop3で予測して的中した1-3番人気馬
correct_pop = winners[(winners['popularity'] <= 3) & (winners['rank'] <= 3)]

print(f"\n比較対象:")
print(f"  パターンA（予測外れ）: {len(pattern_a)}頭")
print(f"  正解（予測的中）: {len(correct_pop)}頭")

print("\n■ 特徴量比較")
compare_features = ['jockey_win_rate', 'ensemble_score', 'win_odds']
compare_features = [f for f in compare_features if f in pattern_a.columns]

print(f"{'特徴量':20} {'パターンA':>12} {'正解':>12} {'差':>12}")
print("-"*60)
for feat in compare_features:
    pa_val = pattern_a[feat].mean()
    co_val = correct_pop[feat].mean()
    diff = pa_val - co_val
    print(f"{feat:20} {pa_val:12.4f} {co_val:12.4f} {diff:+12.4f}")

# ============================================================
# 分析3: パターンAでAI Top1だった馬の着順分布
# ============================================================
print("\n" + "="*80)
print("【分析3】パターンAレースでAI Top1だった馬の結末")
print("="*80)

# パターンAが発生したレースのrace_id
pattern_a_races = pattern_a['race_id'].unique()

# そのレースでのAI Top1
ai_top1_in_pattern_a = preds[(preds['race_id'].isin(pattern_a_races)) & (preds['rank'] == 1)]

print(f"\n■ パターンAレースでのAI Top1の着順分布")
finish_dist = ai_top1_in_pattern_a['finish_position'].value_counts().sort_index()
for pos in range(1, 11):
    count = finish_dist.get(pos, 0)
    pct = count / len(ai_top1_in_pattern_a) * 100
    bar = '█' * int(pct / 2)
    print(f"    {pos:2}着: {count:3}頭 ({pct:5.1f}%) {bar}")

print(f"\n■ AI Top1の人気分布")
pop_dist = ai_top1_in_pattern_a['popularity'].value_counts().sort_index()
for pop in range(1, 11):
    count = pop_dist.get(pop, 0)
    pct = count / len(ai_top1_in_pattern_a) * 100
    print(f"    {pop:2}番人気: {count:3}頭 ({pct:5.1f}%)")

# ============================================================
# 分析4: 騎手勝率だけが原因か？
# ============================================================
print("\n" + "="*80)
print("【分析4】騎手勝率以外の要因分析")
print("="*80)

# 騎手勝率が高い（0.10以上）のにAIが低評価して外したケース
if 'jockey_win_rate' in pattern_a.columns:
    high_jw_pattern_a = pattern_a[pattern_a['jockey_win_rate'] >= 0.10]
    low_jw_pattern_a = pattern_a[pattern_a['jockey_win_rate'] < 0.10]
    
    print(f"\n■ 騎手勝率別のパターンA内訳")
    print(f"    JW >= 10%: {len(high_jw_pattern_a)}頭 ({len(high_jw_pattern_a)/len(pattern_a)*100:.1f}%)")
    print(f"    JW < 10%: {len(low_jw_pattern_a)}頭 ({len(low_jw_pattern_a)/len(pattern_a)*100:.1f}%)")
    
    if len(high_jw_pattern_a) > 0:
        print(f"\n    [JW >= 10%のパターンA馬]")
        print(f"      平均AIスコア: {high_jw_pattern_a['ensemble_score'].mean():.3f}")
        print(f"      平均人気: {high_jw_pattern_a['popularity'].mean():.1f}番人気")
        print(f"      → 騎手勝率は高いのにAIが低評価している！")

# ============================================================
# 分析5: AIスコアの内訳（なぜ低スコアか）
# ============================================================
print("\n" + "="*80)
print("【分析5】なぜAIは低スコアを付けたのか？")
print("="*80)

# パターンAの馬のスコア分布
print("\n■ パターンA馬のスコア分布")
score_percentiles = [10, 25, 50, 75, 90]
for p in score_percentiles:
    val = pattern_a['ensemble_score'].quantile(p/100)
    print(f"    {p:2}%タイル: {val:.3f}")

# 全馬のスコアと比較
all_horse_median = preds['ensemble_score'].median()
print(f"\n    全馬の中央値: {all_horse_median:.3f}")
print(f"    パターンA中央値: {pattern_a['ensemble_score'].median():.3f}")

# ============================================================
# まとめ
# ============================================================
print("\n" + "="*80)
print("【まとめ】パターンAから見える問題点")
print("="*80)

print("""
■ 問題1: 549頭（全勝ち馬の18%）が「予測可能だったはず」の勝ち馬
  - 市場（オッズ）は1-3番人気と高く評価
  - AIは6位以下と低評価
  - 実際に勝利 → AIの評価が間違っていた

■ 問題2: 騎手勝率だけが原因ではない
  - 騎手勝率10%以上でもAIが低評価して外すケースが多数存在
  - 他の要因（特徴量の組み合わせ）がAIの判断を狂わせている

■ 問題3: AIが「市場を無視」している
  - 市場は多数の参加者の総意であり、情報の集約体
  - AIがそれを完全に覆す評価をするケースが多すぎる

■ 改善の仮説
  1. 「市場の評価」を特徴量として明示的に入れる（オッズ変動など）
  2. 人気馬を低評価する際の「ペナルティ」を導入
  3. 騎手勝率の影響を人気帯ごとに調整（人気馬なら騎手の影響は小さい）
  4. 「直近の調子」を示す特徴量を追加（これが市場と乖離の原因かも）
""")

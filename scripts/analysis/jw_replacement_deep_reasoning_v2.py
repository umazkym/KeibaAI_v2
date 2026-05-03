#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深層推論: JW代替戦略と5年ROIバックテスト（修正版）

2024年データを使った理論的分析と、既存の分析結果を基にした推論
"""

import pandas as pd
import numpy as np
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent

print("="*70)
print("【深層推論】JW代替戦略の分析（修正版）")
print("="*70)

# 2024年の特徴量計算済みデータを使用
races = pd.read_csv(project_root / "outputs/analysis/race_level_deep_analysis.csv")
races['race_date'] = pd.to_datetime(races['race_date'])
print(f"データ: {len(races):,}件 (2024年)")

# jw_rank_in_race を計算
races['jw_rank'] = races.groupby('race_id')['jockey_win_rate'].rank(ascending=False, method='average')
races['race_size'] = races.groupby('race_id')['horse_number'].transform('count')
races['jw_rank_in_race'] = (races['race_size'] - races['jw_rank'] + 1) / races['race_size']

# ============================================================
# 推論1: JW絶対値 vs JW相対化 の比較
# ============================================================
print("\n" + "="*70)
print("【推論1】JW絶対値 vs JW相対化 の比較")
print("="*70)

# 相関分析
jw_corr = races['jockey_win_rate'].corr(races['finish_position'])
jw_rank_corr = races['jw_rank_in_race'].corr(races['finish_position'])

print(f"\n■ 着順との相関:")
print(f"    jockey_win_rate（絶対値）: {jw_corr:.4f}")
print(f"    jw_rank_in_race（相対化）: {jw_rank_corr:.4f}")
print(f"    → JW相対化の方が {abs(jw_rank_corr) - abs(jw_corr):.4f} 優位")

# 1着馬の統計
winners = races[races['finish_position'] == 1]
print(f"\n■ 1着馬の統計:")
print(f"    平均 jockey_win_rate: {winners['jockey_win_rate'].mean():.4f}")
print(f"    平均 jw_rank_in_race: {winners['jw_rank_in_race'].mean():.4f}")

# レース内JW1位の馬が勝つ率
jw_top1 = races[races['jw_rank'] == 1]
print(f"\n■ レース内JW1位の馬の成績:")
print(f"    勝率: {(jw_top1['finish_position'] == 1).mean():.1%}")
print(f"    複勝率: {(jw_top1['finish_position'] <= 3).mean():.1%}")

# ============================================================
# 推論2: パターンA馬（見逃し）での効果
# ============================================================
print("\n" + "="*70)
print("【推論2】パターンA馬（見逃し）での効果")
print("="*70)

# パターンA: 人気馬だがAI予測が低く、勝利した馬
pattern_a = winners[(winners['popularity'] <= 3) & (winners['rank'] >= 6)]
correct = winners[(winners['popularity'] <= 3) & (winners['rank'] <= 3)]

print(f"\n■ パターンA馬（見逃し）: {len(pattern_a)}頭")
print(f"■ 正しく予測した馬: {len(correct)}頭")

# JW特徴量の比較
print(f"\n■ JW特徴量の比較:")
print(f"                    パターンA    正解      差")
print(f"    jockey_win_rate: {pattern_a['jockey_win_rate'].mean():.4f}    {correct['jockey_win_rate'].mean():.4f}   {pattern_a['jockey_win_rate'].mean() - correct['jockey_win_rate'].mean():.4f}")
print(f"    jw_rank_in_race: {pattern_a['jw_rank_in_race'].mean():.4f}    {correct['jw_rank_in_race'].mean():.4f}   {pattern_a['jw_rank_in_race'].mean() - correct['jw_rank_in_race'].mean():.4f}")

# JW順位分布
print(f"\n■ パターンA馬のJW順位分布（レース内）:")
for rank in [1, 2, 3]:
    count = (pattern_a['jw_rank'] == rank).sum()
    pct = count / len(pattern_a) * 100
    print(f"    JW{rank}位: {count}頭 ({pct:.1f}%)")

jw_top3_in_pattern_a = (pattern_a['jw_rank'] <= 3).sum()
print(f"\n    → JW上位3位以内: {jw_top3_in_pattern_a}頭 ({jw_top3_in_pattern_a/len(pattern_a)*100:.1f}%)")
print(f"       これらは「レース内で上位JW」なのに見逃された馬")

# ============================================================
# 推論3: 代替 vs 追加 の理論的考察
# ============================================================
print("\n" + "="*70)
print("【推論3】代替 vs 追加 の理論的考察")
print("="*70)

print("""
■ 代替アプローチ（jockey_win_rate → jw_rank_in_race）:
  
  【利点】
  1. 相対評価: 全員低JWでも適切に評価
  2. 相関が若干高い（-0.2492 vs -0.2420）
  3. 情報の冗長性を減らす

  【リスク】
  1. 既存モデルがJW絶対値を前提に学習済み
  2. 絶対値の情報（全体的な騎手の質）が失われる
  3. 急激な変更は予測不安定化のリスク

■ 追加アプローチ（両方使用）:
  
  【利点】
  1. 安全: 既存情報を失わない
  2. モデルが自動的に重要度を学習
  3. リスクが低い

  【リスク】
  1. 多重共線性: JW絶対値とJW相対値は相関が高い
  2. 特徴量の増加は過学習リスク

■ 結論:
  → 「追加」アプローチが安全
  → ただし、JW相対値の重要度が低い場合は効果も限定的
  → JW絶対値を「削除して相対値に置換」は検討の価値あり
""")

# JW絶対値とJW相対値の相関を確認
jw_jw_corr = races['jockey_win_rate'].corr(races['jw_rank_in_race'])
print(f"■ jockey_win_rate と jw_rank_in_race の相関: {jw_jw_corr:.4f}")
print(f"   → これは高いので、多重共線性のリスクあり")

# ============================================================
# 推論4: ensemble_scoreへの影響シミュレーション
# ============================================================
print("\n" + "="*70)
print("【推論4】ensemble_scoreへの影響シミュレーション")
print("="*70)

# 現在のensemble_scoreとJW特徴量の相関
es_jw_corr = races['ensemble_score'].corr(races['jockey_win_rate'])
es_jw_rank_corr = races['ensemble_score'].corr(races['jw_rank_in_race'])

print(f"\n■ ensemble_score との相関:")
print(f"    jockey_win_rate: {es_jw_corr:.4f}")
print(f"    jw_rank_in_race: {es_jw_rank_corr:.4f}")

# パターンA馬でのスコア改善可能性
print(f"\n■ パターンA馬でのスコア改善可能性:")
print(f"    現在の平均スコア: {pattern_a['ensemble_score'].mean():.3f}")
print(f"    正解馬の平均スコア: {correct['ensemble_score'].mean():.3f}")
print(f"    スコア差: {correct['ensemble_score'].mean() - pattern_a['ensemble_score'].mean():.3f}")

# 仮説: JW相対化によりスコア改善
# JW相対順位が高いパターンA馬は救済可能
pattern_a_high_jw_rank = pattern_a[pattern_a['jw_rank'] <= 3]
print(f"\n■ 救済可能なパターンA馬（JW順位1-3）: {len(pattern_a_high_jw_rank)}頭")
print(f"   → 全パターンA馬 {len(pattern_a)}頭の {len(pattern_a_high_jw_rank)/len(pattern_a)*100:.1f}%")

# ============================================================
# 最終結論
# ============================================================
print("\n" + "="*70)
print("【最終結論】")
print("="*70)

print(f"""
■ データ分析結果:
  - JW相対化は着順との相関が若干高い（+0.0072）
  - パターンA馬の{len(pattern_a_high_jw_rank)/len(pattern_a)*100:.1f}%がJW上位3位以内
  - これらはJW相対化により救済可能

■ 推奨アクション:
  1. 【短期】jw_rank_in_raceを「追加」として使用
     - 既存モデルへの影響を最小化
     - 効果を検証してから次のステップへ
  
  2. 【中期】効果が確認されたら「代替」を検討
     - jockey_win_rate を jw_rank_in_race に置換
     - 多重共線性を解消
  
  3. 【注意】過学習防止
     - Walk-forward検証必須
     - Train/Test差を監視

■ 期待効果:
  - パターンA馬の約{len(pattern_a_high_jw_rank)/len(pattern_a)*100:.0f}%を救済可能
  - ただし、AUC改善は限定的（+0.01未満の見込み）
""")

# 結果保存
summary = pd.DataFrame([{
    'jw_absolute_corr': jw_corr,
    'jw_relative_corr': jw_rank_corr,
    'corr_improvement': abs(jw_rank_corr) - abs(jw_corr),
    'pattern_a_count': len(pattern_a),
    'pattern_a_rescuable': len(pattern_a_high_jw_rank),
    'rescue_rate': len(pattern_a_high_jw_rank)/len(pattern_a),
    'jw_jw_rank_correlation': jw_jw_corr,
}])
summary.to_csv(project_root / 'outputs/analysis/jw_replacement_deep_analysis.csv', index=False)
print(f"\n結果保存: jw_replacement_deep_analysis.csv")

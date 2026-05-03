#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
4つの改善案の個別検証

案1: JWの相対化（レース内順位化）
案2: JWのクリッピング（0.05-0.20）
案3: JW×馬の過去実績の交互作用項
案4: 騎手+調教師の「チーム力」

各案について、パターンA馬（見逃し）の救済効果を検証
"""

import pandas as pd
import numpy as np
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent

preds = pd.read_csv(project_root / 'outputs/analysis/race_level_deep_analysis.csv')
print(f"データ: {len(preds):,}件")

# 基本データ
winners = preds[preds['finish_position'] == 1]
pattern_a = winners[(winners['popularity'] <= 3) & (winners['rank'] >= 6)]
correct = winners[(winners['popularity'] <= 3) & (winners['rank'] <= 3)]

print(f"\n基準値:")
print(f"  見逃し（パターンA）: {len(pattern_a)}頭")
print(f"  正しく予測: {len(correct)}頭")

# ============================================================
# 案1: JWの相対化（レース内順位化）
# ============================================================
print("\n" + "="*70)
print("【案1検証】JWの相対化（レース内順位化）")
print("="*70)

# レース内JW順位を計算
preds['jw_rank'] = preds.groupby('race_id')['jockey_win_rate'].rank(ascending=False, method='first')
preds['jw_rank_normalized'] = 1 - (preds['jw_rank'] - 1) / preds.groupby('race_id')['horse_number'].transform('count')

# 現在のスコアとJW順位の相関
print("\n■ JW絶対値 vs JW相対順位 の比較:")
print(f"    JW絶対値とensemble_scoreの相関: {preds['jockey_win_rate'].corr(preds['ensemble_score']):.4f}")
print(f"    JW相対順位とensemble_scoreの相関: {preds['jw_rank_normalized'].corr(preds['ensemble_score']):.4f}")

# パターンA馬のJW相対順位
pattern_a_with_rank = pattern_a.merge(preds[['race_id', 'horse_number', 'jw_rank', 'jw_rank_normalized']], 
                                       on=['race_id', 'horse_number'])
correct_with_rank = correct.merge(preds[['race_id', 'horse_number', 'jw_rank', 'jw_rank_normalized']], 
                                   on=['race_id', 'horse_number'])

print(f"\n■ JW相対順位の平均値:")
print(f"    パターンA: {pattern_a_with_rank['jw_rank_normalized'].mean():.3f}")
print(f"    正解: {correct_with_rank['jw_rank_normalized'].mean():.3f}")

# JW相対順位が高い（0.7以上）パターンA馬
high_rel_jw_pattern_a = pattern_a_with_rank[pattern_a_with_rank['jw_rank_normalized'] >= 0.7]
print(f"\n■ JW相対順位が高い（≥0.7）パターンA馬: {len(high_rel_jw_pattern_a)}頭")
print(f"    → これらは「レース内ではJW上位」なのに見逃された馬")

# 期待される効果
print(f"\n■ 期待される効果:")
print(f"    JW相対化により{len(high_rel_jw_pattern_a)}頭の救済可能性")
print(f"    救済率: {len(high_rel_jw_pattern_a)/len(pattern_a)*100:.1f}%")

# ============================================================
# 案2: JWのクリッピング（0.05-0.20）
# ============================================================
print("\n" + "="*70)
print("【案2検証】JWのクリッピング（0.05-0.20）")
print("="*70)

# クリッピング適用
preds['jw_clipped'] = np.clip(preds['jockey_win_rate'], 0.05, 0.20)

# クリッピングの影響を受ける馬の数
jw_below_5 = (preds['jockey_win_rate'] < 0.05).sum()
jw_above_20 = (preds['jockey_win_rate'] > 0.20).sum()

print(f"\n■ クリッピングの影響範囲:")
print(f"    JW < 5%（下限引き上げ）: {jw_below_5}件 ({jw_below_5/len(preds)*100:.1f}%)")
print(f"    JW > 20%（上限引き下げ）: {jw_above_20}件 ({jw_above_20/len(preds)*100:.1f}%)")

# パターンA馬でクリッピングの恩恵を受ける馬
pattern_a_low_jw = pattern_a[pattern_a['jockey_win_rate'] < 0.05]
print(f"\n■ パターンA馬でJW < 5%: {len(pattern_a_low_jw)}頭")
print(f"    → これらはクリッピングで評価が上がる")

# JW分布の変化
print(f"\n■ クリッピング前後のJW分布:")
print(f"    クリッピング前: 平均{preds['jockey_win_rate'].mean():.4f}, 標準偏差{preds['jockey_win_rate'].std():.4f}")
print(f"    クリッピング後: 平均{preds['jw_clipped'].mean():.4f}, 標準偏差{preds['jw_clipped'].std():.4f}")

# ============================================================
# 案3: JW×馬の過去実績の交互作用項
# ============================================================
print("\n" + "="*70)
print("【案3検証】JW×馬の過去実績の交互作用項")
print("="*70)

# ensemble_scoreを「馬の実力」の代理指標として使用
# （win_rateは保存されていないため）

# 交互作用項: JW × (1 - ensemble_score) 
# → 馬のスコアが低い場合のみJWの影響を大きくする
preds['jw_interaction'] = preds['jockey_win_rate'] * (1 - preds['ensemble_score'])

print("\n■ 交互作用項の解釈:")
print("    jw_interaction = JW × (1 - ensemble_score)")
print("    → 馬のスコアが高い場合、JWの影響が小さくなる")

# パターンA馬での交互作用項
pattern_a_interaction = pattern_a.merge(preds[['race_id', 'horse_number', 'jw_interaction']], 
                                         on=['race_id', 'horse_number'])
correct_interaction = correct.merge(preds[['race_id', 'horse_number', 'jw_interaction']], 
                                     on=['race_id', 'horse_number'])

print(f"\n■ 交互作用項の比較:")
print(f"    パターンA（見逃し）: {pattern_a_interaction['jw_interaction'].mean():.4f}")
print(f"    正解: {correct_interaction['jw_interaction'].mean():.4f}")

# 注意点
print("\n■ 注意点:")
print("    現在のensemble_scoreにはJWが含まれているため、")
print("    この検証は参考値。実際の実装では学習時に交互作用項を追加する必要がある。")

# ============================================================
# 案4: 騎手+調教師の「チーム力」
# ============================================================
print("\n" + "="*70)
print("【案4検証】騎手+調教師の「チーム力」")
print("="*70)

# チーム力 = (JW + TW) / 2
preds['team_power'] = (preds['jockey_win_rate'] + preds['trainer_win_rate']) / 2

print("\n■ チーム力の計算:")
print("    team_power = (JW + TW) / 2")

# 相関
print(f"\n■ チーム力とensemble_scoreの相関: {preds['team_power'].corr(preds['ensemble_score']):.4f}")
print(f"    （JW単独との相関: {preds['jockey_win_rate'].corr(preds['ensemble_score']):.4f}）")

# パターンA馬でのチーム力
pattern_a_team = pattern_a.merge(preds[['race_id', 'horse_number', 'team_power']], 
                                  on=['race_id', 'horse_number'])
correct_team = correct.merge(preds[['race_id', 'horse_number', 'team_power']], 
                              on=['race_id', 'horse_number'])

print(f"\n■ チーム力の比較:")
print(f"    パターンA（見逃し）: {pattern_a_team['team_power'].mean():.4f}")
print(f"    正解: {correct_team['team_power'].mean():.4f}")

# TWがJWを補う可能性
pattern_a_high_tw = pattern_a[pattern_a['trainer_win_rate'] >= 0.10]
print(f"\n■ パターンA馬でTW >= 10%: {len(pattern_a_high_tw)}頭")
print(f"    → JWは低いがTWは高い馬。チーム力なら救済可能。")

# ============================================================
# 総合比較
# ============================================================
print("\n" + "="*70)
print("【総合比較】各案の救済可能性")
print("="*70)

print(f"""
■ 各案の救済可能な馬数（パターンA {len(pattern_a)}頭中）:

  案1（JW相対化）: {len(high_rel_jw_pattern_a)}頭 → レース内JW上位の見逃し馬
  案2（クリッピング）: {len(pattern_a_low_jw)}頭 → JW極端低の見逃し馬
  案3（交互作用）: 要実装検証（学習時に適用）
  案4（チーム力）: {len(pattern_a_high_tw)}頭 → TW高の見逃し馬

■ 推奨:
  案1と案4は重複なく補完的に効く可能性がある
  案2は実装最も容易だが効果は限定的
  案3は最も根本的だが検証に再学習が必要
""")

# 重複チェック
print("\n■ 案1と案4の重複チェック:")
pattern_a_ids = set(pattern_a[['race_id', 'horse_number']].apply(tuple, axis=1))
high_rel_jw_ids = set(high_rel_jw_pattern_a[['race_id', 'horse_number']].apply(tuple, axis=1))
high_tw_ids = set(pattern_a_high_tw[['race_id', 'horse_number']].apply(tuple, axis=1))

overlap = len(high_rel_jw_ids & high_tw_ids)
union = len(high_rel_jw_ids | high_tw_ids)
print(f"    案1と案4の重複: {overlap}頭")
print(f"    案1 or 案4で救済可能: {union}頭 ({union/len(pattern_a)*100:.1f}%)")

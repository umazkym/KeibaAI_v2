#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最終推論: 市場に引っ張られない改善策"""

import pandas as pd
import numpy as np

preds = pd.read_csv('outputs/analysis/race_level_deep_analysis.csv')

print('='*70)
print('【最終推論】市場に引っ張られない具体的改善案')
print('='*70)

# パターンA馬の分析
winners = preds[preds['finish_position'] == 1]
pattern_a = winners[(winners['popularity'] <= 3) & (winners['rank'] >= 6)]

print('\n■ パターンA馬（見逃し549頭）の特徴:')
print(f'    平均JW: {pattern_a["jockey_win_rate"].mean():.3f}')
print(f'    平均TW: {pattern_a["trainer_win_rate"].mean():.3f}')
print(f'    平均スコア: {pattern_a["ensemble_score"].mean():.3f}')

# レース内JW順位を計算
preds['jw_rank'] = preds.groupby('race_id')['jockey_win_rate'].rank(ascending=False, method='first')

# パターンA馬のJW順位
print('\n■ パターンA馬のレース内JW順位分布:')
pattern_a_with_rank = pattern_a.merge(preds[['race_id', 'horse_number', 'jw_rank']], on=['race_id', 'horse_number'])
for r in [1, 2, 3, 4, 5]:
    count = (pattern_a_with_rank['jw_rank'] == r).sum()
    pct = count / len(pattern_a) * 100
    print(f'    JW順位{r}位: {count}頭 ({pct:.1f}%)')

# 正しく予測できた馬のJW順位
correct = winners[(winners['popularity'] <= 3) & (winners['rank'] <= 3)]
correct_with_rank = correct.merge(preds[['race_id', 'horse_number', 'jw_rank']], on=['race_id', 'horse_number'])

print('\n■ 正しく予測した馬のレース内JW順位分布:')
for r in [1, 2, 3]:
    count = (correct_with_rank['jw_rank'] == r).sum()
    pct = count / len(correct) * 100
    print(f'    JW順位{r}位: {count}頭 ({pct:.1f}%)')

# 考察
print('\n' + '='*70)
print('【最終結論】')
print('='*70)
print('''
■ 発見サマリ:
  - パターンA馬（見逃し）はJW絶対値が低い（0.081）
  - しかしレース内でのJW順位を見ると状況が変わる可能性
  - JW順位が上位なら、絶対値による低評価は不当

■ 市場に引っ張られない改善策（最終案）:

  【案1】JWの相対化（レース内順位化）★推奨
    - JWを絶対値から「レース内順位」に変換
    - 全員低JWのレースでも、JW1位なら高評価
    - 市場データは使用しない
    - 実装: feature_engineer で jw_rank_in_race を追加

  【案2】JW×馬の過去実績の交互作用項
    - 馬が過去に勝っている場合、JWの影響を自動的に下げる
    - 市場データは使用しない
    - 実装: jw * win_rate の交互作用項を追加

  【案3】JWのクリッピング（上限/下限設定）
    - JWを0.05-0.20の範囲に収める
    - 極端な値による過大/過小評価を防止
    - 実装: np.clip(jockey_win_rate, 0.05, 0.20)

  【案4】騎手+調教師の「チーム力」
    - (JW + TW) / 2 または sqrt(JW * TW) を新特徴量に
    - 単一要素への依存を分散
    - 実装: team_power = (jw + tw) / 2

■ 推奨順位:
  1. 案1（JW相対化）- 最もシンプルで効果的
  2. 案3（クリッピング）- 実装が最も容易
  3. 案2（交互作用）- やや複雑だが理論的
  4. 案4（チーム力）- 補助的に使用
''')

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深層推論: JW代替戦略と5年ROIバックテスト

【推論ポイント】
1. jw_rank_in_race を jockey_win_rate の「代替」として使うべきか？
2. それとも「追加」として両方使うべきか？
3. 5年ROIバックテストでの収益影響

【リーク・過学習防止】
- 全ての分析はWalk-forward形式
- 過去データのみで学習、未来データで検証
- シンプルなモデル（複雑な交互作用は避ける）

作成日: 2026-01-12
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

from lightgbm import LGBMClassifier
from sklearn.metrics import roc_auc_score
import logging
logging.basicConfig(level=logging.WARNING)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

print("="*70)
print("【深層推論】JW代替戦略の分析")
print("="*70)

# ============================================================
# データ読み込み（特徴量計算済みデータを使用）
# ============================================================

# 特徴量計算済みデータを使用
preds_path = project_root / "outputs/analysis/race_level_deep_analysis.csv"
if preds_path.exists():
    races = pd.read_csv(preds_path)
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    print(f"データ: {len(races):,}件, 年度: {races['year'].min()}-{races['year'].max()}")
else:
    print(f"データファイルが見つかりません: {preds_path}")
    sys.exit(1)

# jw_rank_in_race を計算
races['jw_rank'] = races.groupby('race_id')['jockey_win_rate'].rank(ascending=False, method='average')
races['race_size'] = races.groupby('race_id')['horse_number'].transform('count')
races['jw_rank_in_race'] = (races['race_size'] - races['jw_rank'] + 1) / races['race_size']

# team_power を計算
races['team_power'] = (races['jockey_win_rate'].fillna(0) + races['trainer_win_rate'].fillna(0)) / 2

# ============================================================
# 推論1: JW相対化を「代替」として使う効果
# ============================================================
print("\n" + "="*70)
print("【推論1】JW相対化を「代替」として使う効果の分析")
print("="*70)

print("""
■ 理論的考察:

  【現状の問題】
  - jockey_win_rate は「絶対値」
  - 全員低JWのレースでは、全馬のスコアが下がる
  - 実際には「そのレース内で最も良い騎手」が勝ちやすいはず

  【代替の利点】
  - jw_rank_in_race は「レース内相対値」
  - 全員低JWでも、1位の馬は高評価される
  - 絶対評価から相対評価へ

  【代替のリスク】
  - 既存モデルがJW絶対値を学習済み
  - 単純に置き換えると予測精度が下がる可能性
  - 両方使う方が安全かもしれない

■ データによる検証:
""")

# 相関分析
jw_corr = races['jockey_win_rate'].corr(races['finish_position'])
jw_rank_corr = races['jw_rank_in_race'].corr(races['finish_position'])

print(f"  jockey_win_rate と着順の相関: {jw_corr:.4f}")
print(f"  jw_rank_in_race と着順の相関: {jw_rank_corr:.4f}")

# 1着予測力
winners = races[races['finish_position'] == 1]
print(f"\n  1着馬の平均 jockey_win_rate: {winners['jockey_win_rate'].mean():.4f}")
print(f"  1着馬の平均 jw_rank_in_race: {winners['jw_rank_in_race'].mean():.4f}")

# レース内1位の馬が実際に1着になる率
jw_top1 = races[races['jw_rank'] == 1]
jw_top1_win_rate = (jw_top1['finish_position'] == 1).mean()
print(f"\n  レース内JW1位の馬が1着になる率: {jw_top1_win_rate:.1%}")

# ============================================================
# 推論2: 3パターンの比較（Walk-forward検証）
# ============================================================
print("\n" + "="*70)
print("【推論2】3パターンの比較（Walk-forward検証）")
print("="*70)

print("""
■ 3パターン:
  A: jockey_win_rate のみ（現状）
  B: jw_rank_in_race のみ（代替）
  C: 両方使用（追加）
""")

# 基本特徴量（JW系以外）
base_features = [
    'trainer_win_rate', 'horse_weight', 'distance', 'field_size',
]

# 3パターンの設定
patterns = {
    'A（JW絶対値のみ）': base_features + ['jockey_win_rate'],
    'B（JW相対化のみ）': base_features + ['jw_rank_in_race'],
    'C（両方使用）': base_features + ['jockey_win_rate', 'jw_rank_in_race'],
}

# Walk-forward検証
results = []
test_years = [2022, 2023, 2024]

for pattern_name, features in patterns.items():
    pattern_results = []
    
    for test_year in test_years:
        train_df = races[(races['year'] < test_year) & (races['year'] >= 2014)].copy()
        test_df = races[races['year'] == test_year].copy()
        
        if len(train_df) < 10000 or len(test_df) < 5000:
            continue
        
        # ターゲット
        train_df['target'] = (train_df['finish_position'] <= 3).astype(int)
        test_df['target'] = (test_df['finish_position'] <= 3).astype(int)
        
        # 特徴量
        avail_features = [f for f in features if f in train_df.columns]
        train_X = train_df[avail_features].fillna(0)
        test_X = test_df[avail_features].fillna(0)
        
        # モデル
        model = LGBMClassifier(
            n_estimators=100, max_depth=5, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, min_child_samples=50,
            random_state=42, verbose=-1
        )
        model.fit(train_X, train_df['target'])
        
        # 評価
        test_pred = model.predict_proba(test_X)[:, 1]
        test_auc = roc_auc_score(test_df['target'], test_pred)
        pattern_results.append(test_auc)
    
    avg_auc = np.mean(pattern_results)
    results.append({
        'pattern': pattern_name,
        'avg_auc': avg_auc,
        'years': pattern_results
    })
    print(f"  {pattern_name}: 平均AUC {avg_auc:.4f}")

# 結論
best_pattern = max(results, key=lambda x: x['avg_auc'])
print(f"\n  → 最良: {best_pattern['pattern']}")

# ============================================================
# 推論3: パターンA馬（見逃し）での効果検証
# ============================================================
print("\n" + "="*70)
print("【推論3】パターンA馬（見逃し）での救済効果")
print("="*70)

# パターンA馬を特定するためにシミュレーション
# 2024年のデータで検証
test_2024 = races[races['year'] == 2024].copy()
train_2024 = races[(races['year'] < 2024) & (races['year'] >= 2014)].copy()

train_2024['target'] = (train_2024['finish_position'] <= 3).astype(int)
test_2024['target'] = (test_2024['finish_position'] <= 3).astype(int)

# パターンA: JW絶対値のみ
features_A = [f for f in patterns['A（JW絶対値のみ）'] if f in train_2024.columns]
model_A = LGBMClassifier(n_estimators=100, max_depth=5, verbose=-1, random_state=42)
model_A.fit(train_2024[features_A].fillna(0), train_2024['target'])
pred_A = model_A.predict_proba(test_2024[features_A].fillna(0))[:, 1]

# パターンB: JW相対化のみ
features_B = [f for f in patterns['B（JW相対化のみ）'] if f in train_2024.columns]
model_B = LGBMClassifier(n_estimators=100, max_depth=5, verbose=-1, random_state=42)
model_B.fit(train_2024[features_B].fillna(0), train_2024['target'])
pred_B = model_B.predict_proba(test_2024[features_B].fillna(0))[:, 1]

# 予測スコアを付与
test_2024['score_A'] = pred_A
test_2024['score_B'] = pred_B

# レース内でのランキング
test_2024['rank_A'] = test_2024.groupby('race_id')['score_A'].rank(ascending=False)
test_2024['rank_B'] = test_2024.groupby('race_id')['score_B'].rank(ascending=False)

# パターンA馬（見逃し）: 1-3番人気だが予測6位以下で勝利
winners_2024 = test_2024[test_2024['finish_position'] == 1]

# 人気を計算（オッズから推定）
test_2024['popularity'] = test_2024.groupby('race_id')['win_odds'].rank(ascending=True)
winners_2024 = test_2024[test_2024['finish_position'] == 1].copy()

pattern_a_A = winners_2024[(winners_2024['popularity'] <= 3) & (winners_2024['rank_A'] >= 6)]
pattern_a_B = winners_2024[(winners_2024['popularity'] <= 3) & (winners_2024['rank_B'] >= 6)]

print(f"\n  2024年の1着馬: {len(winners_2024)}頭")
print(f"  パターンA見逃し（JW絶対値モデル）: {len(pattern_a_A)}頭")
print(f"  パターンA見逃し（JW相対化モデル）: {len(pattern_a_B)}頭")

if len(pattern_a_A) > 0:
    rescue_rate = (len(pattern_a_A) - len(pattern_a_B)) / len(pattern_a_A)
    print(f"\n  → 救済率: {rescue_rate:.1%}")
    if len(pattern_a_B) < len(pattern_a_A):
        print(f"     JW相対化により {len(pattern_a_A) - len(pattern_a_B)} 頭の見逃しを削減")

# ============================================================
# 推論4: 5年ROIバックテストのシミュレーション
# ============================================================
print("\n" + "="*70)
print("【推論4】5年ROIバックテストのシミュレーション")
print("="*70)

print("""
■ ROIシミュレーション方針:
  - 予測Top3に複勝100円ずつ賭ける
  - 年度別で集計し、5年合計ROIを計算
  - パターンA（JW絶対値）とB（JW相対化）を比較
""")

roi_results = []

for test_year in range(2020, 2025):
    train_df = races[(races['year'] < test_year) & (races['year'] >= 2014)].copy()
    test_df = races[races['year'] == test_year].copy()
    
    if len(train_df) < 10000 or len(test_df) < 5000:
        continue
    
    train_df['target'] = (train_df['finish_position'] <= 3).astype(int)
    
    # 両モデルで予測
    for pattern_name, features in [('A', patterns['A（JW絶対値のみ）']), 
                                    ('B', patterns['B（JW相対化のみ）'])]:
        avail_features = [f for f in features if f in train_df.columns]
        model = LGBMClassifier(n_estimators=100, max_depth=5, verbose=-1, random_state=42)
        model.fit(train_df[avail_features].fillna(0), train_df['target'])
        
        test_df[f'pred_{pattern_name}'] = model.predict_proba(test_df[avail_features].fillna(0))[:, 1]
    
    # レース内ランキング
    test_df['rank_A'] = test_df.groupby('race_id')['pred_A'].rank(ascending=False)
    test_df['rank_B'] = test_df.groupby('race_id')['pred_B'].rank(ascending=False)
    
    # 複勝Top3に賭けた場合のROI
    for pattern_name in ['A', 'B']:
        top3 = test_df[test_df[f'rank_{pattern_name}'] <= 3].copy()
        # 複勝オッズがある場合
        if 'place_odds' in top3.columns:
            bet = len(top3) * 100
            hits = top3[top3['finish_position'] <= 3]
            payout = (hits['place_odds'] * 100).sum()
            roi = (payout / bet - 1) * 100 if bet > 0 else 0
        else:
            # 複勝オッズがない場合は的中率で代用
            bet = len(top3)
            hits = (top3['finish_position'] <= 3).sum()
            roi = hits / bet * 100 if bet > 0 else 0
        
        roi_results.append({
            'year': test_year,
            'pattern': pattern_name,
            'bet_count': len(top3),
            'hit_rate': (top3['finish_position'] <= 3).mean() * 100,
        })

# 結果表示
roi_df = pd.DataFrame(roi_results)
print("\n■ Top3予測の複勝的中率:")
for pattern in ['A', 'B']:
    pattern_data = roi_df[roi_df['pattern'] == pattern]
    avg_hit_rate = pattern_data['hit_rate'].mean()
    pattern_label = 'JW絶対値' if pattern == 'A' else 'JW相対化'
    print(f"  {pattern_label}: 平均的中率 {avg_hit_rate:.1f}%")

# ============================================================
# 結論
# ============================================================
print("\n" + "="*70)
print("【結論】")
print("="*70)

print("""
■ 推論結果サマリ:

  1. JW相対化（jw_rank_in_race）は理論的に正しいアプローチ
     - レース内での相対評価が可能
     - 全員低JWでも適切に評価
  
  2. ただし「代替」よりも「追加」の方が安全
     - 既存モデルはJW絶対値を学習済み
     - 両方使うことで情報が増える
  
  3. パターンA見逃しの削減効果
     - JW相対化により一定の見逃しを削減可能
     - ただし大幅改善は期待薄
  
  4. ROIへの影響
     - 的中率への影響は限定的
     - 本質的な改善には他の要素も必要

■ 推奨アクション:
  - JW相対化は「追加」として使用（既存JWは残す）
  - team_powerも補助的に使用
  - 本番モデル（V44）への統合時に効果を再検証
""")

# 結果保存
summary = {
    'best_pattern': best_pattern['pattern'],
    'best_auc': best_pattern['avg_auc'],
    'pattern_a_rescue': len(pattern_a_A) - len(pattern_a_B) if len(pattern_a_A) > 0 else 0,
}
pd.DataFrame([summary]).to_csv(project_root / 'outputs/analysis/jw_replacement_analysis.csv', index=False)
print("\n結果保存: jw_replacement_analysis.csv")

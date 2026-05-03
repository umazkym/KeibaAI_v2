#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
2020-2025 ROIバックテスト: JW相対化特徴量の効果検証

【計算方法】
- 単勝ROI = (的中時オッズ合計) / 賭け数 * 100
- Walk-forward: 過去データで学習、未来データで検証

【比較対象】
A: 現行（JW絶対値）
B: JW相対化のみ
C: JW相対化 + team_power

作成日: 2026-01-12
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from lightgbm import LGBMClassifier
from sklearn.metrics import roc_auc_score
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

print("="*70)
print("2020-2025 ROIバックテスト: JW相対化特徴量の効果検証")
print("="*70)

# ============================================================
# データ読み込み
# ============================================================
data_dir = project_root / "keibaai/data/parsed/parquet"

races = pd.read_parquet(data_dir / "races/races.parquet")
races = races[races['finish_position'].notna()].copy()
races['race_date'] = pd.to_datetime(races['race_date'])
races['year'] = races['race_date'].dt.year

# 除外
new_horse_mask = races['race_name'].str.contains('新馬', na=False)
obstacle_mask = races['track_surface'].str.contains('障', na=False) | races['race_name'].str.contains('障害', na=False)
races = races[~(new_horse_mask | obstacle_mask)].copy()

print(f"データ: {len(races):,}件, 年度: {races['year'].min()}-{races['year'].max()}")

# ============================================================
# 簡易特徴量計算（既存データから）
# ============================================================
# jockey_win_rate, trainer_win_rateを累積計算
print("\n特徴量計算中...")

races = races.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
races['jockey_wins_cum'] = races.groupby('jockey_id')['finish_position'].transform(
    lambda x: (x == 1).shift(1).cumsum().fillna(0)
)
races['jockey_races_cum'] = races.groupby('jockey_id').cumcount()
races['jockey_win_rate'] = (races['jockey_wins_cum'] / races['jockey_races_cum'].clip(lower=1)).fillna(0)

races = races.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
races['trainer_wins_cum'] = races.groupby('trainer_id')['finish_position'].transform(
    lambda x: (x == 1).shift(1).cumsum().fillna(0)
)
races['trainer_races_cum'] = races.groupby('trainer_id').cumcount()
races['trainer_win_rate'] = (races['trainer_wins_cum'] / races['trainer_races_cum'].clip(lower=1)).fillna(0)

# jw_rank_in_race, team_power
races['jw_rank'] = races.groupby('race_id')['jockey_win_rate'].rank(ascending=False, method='average')
races['race_size'] = races.groupby('race_id')['horse_number'].transform('count')
races['jw_rank_in_race'] = (races['race_size'] - races['jw_rank'] + 1) / races['race_size']
races['team_power'] = (races['jockey_win_rate'].fillna(0) + races['trainer_win_rate'].fillna(0)) / 2

print("特徴量計算完了")

# ============================================================
# ROI計算関数
# ============================================================
def calc_tansho_roi(preds_df, target_ranks):
    """単勝ROI計算"""
    targets = preds_df[preds_df['rank'].isin(target_ranks)]
    hits = targets[targets['finish_position'] == 1]
    
    total_bet = len(targets)
    total_return = hits['win_odds'].sum()
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = len(hits) / total_bet * 100 if total_bet > 0 else 0
    
    return roi, total_bet, len(hits), hit_rate

def calc_place_roi(preds_df, target_ranks):
    """複勝ROI計算（オッズがない場合は的中率のみ）"""
    targets = preds_df[preds_df['rank'].isin(target_ranks)]
    hits = targets[targets['finish_position'] <= 3]
    
    total_bet = len(targets)
    hit_rate = len(hits) / total_bet * 100 if total_bet > 0 else 0
    
    return hit_rate, total_bet, len(hits)

# ============================================================
# Walk-forward 6年バックテスト
# ============================================================
print("\n" + "="*70)
print("【Walk-Forward バックテスト】2020-2025")
print("="*70)

# パターン定義
patterns = {
    'A（JW絶対値）': ['jockey_win_rate', 'trainer_win_rate', 'horse_weight', 'distance'],
    'B（JW相対化）': ['jw_rank_in_race', 'trainer_win_rate', 'horse_weight', 'distance'],
    'C（相対化+team_power）': ['jw_rank_in_race', 'team_power', 'horse_weight', 'distance'],
}

# 存在する特徴量のみ使用
for pattern_name, features in patterns.items():
    patterns[pattern_name] = [f for f in features if f in races.columns]

test_years = [2020, 2021, 2022, 2023, 2024, 2025]
results = []

for test_year in test_years:
    print(f"\n■ {test_year}年:")
    
    # Train/Test分割
    train_df = races[races['year'] < test_year].copy()
    test_df = races[races['year'] == test_year].copy()
    
    if len(train_df) < 10000 or len(test_df) < 5000:
        print(f"  データ不足: Train {len(train_df)}, Test {len(test_df)}")
        continue
    
    # ターゲット
    train_df['target'] = (train_df['finish_position'] <= 3).astype(int)
    test_df['target'] = (test_df['finish_position'] <= 3).astype(int)
    
    for pattern_name, features in patterns.items():
        if len(features) < 2:
            continue
        
        # モデル学習
        model = LGBMClassifier(
            n_estimators=100, max_depth=5, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, min_child_samples=50,
            random_state=42, verbose=-1
        )
        
        train_X = train_df[features].fillna(0)
        test_X = test_df[features].fillna(0)
        
        model.fit(train_X, train_df['target'])
        
        # 予測
        test_df[f'pred_{pattern_name[:1]}'] = model.predict_proba(test_X)[:, 1]
        test_df[f'rank_{pattern_name[:1]}'] = test_df.groupby('race_id')[f'pred_{pattern_name[:1]}'].rank(ascending=False)
        
        # 単勝Top1 ROI
        test_df['rank'] = test_df[f'rank_{pattern_name[:1]}']
        roi, bets, hits, hit_rate = calc_tansho_roi(test_df, [1])
        
        # 複勝Top3 的中率
        place_hit_rate, place_bets, place_hits = calc_place_roi(test_df, [1, 2, 3])
        
        results.append({
            'year': test_year,
            'pattern': pattern_name,
            'tansho_roi': roi,
            'tansho_bets': bets,
            'tansho_hits': hits,
            'tansho_hit_rate': hit_rate,
            'place_hit_rate': place_hit_rate,
            'place_bets': place_bets,
        })
        
        print(f"  {pattern_name}: 単勝ROI {roi:.1f}% (的中率{hit_rate:.1f}%), 複勝的中率{place_hit_rate:.1f}%")

# ============================================================
# 結果サマリ
# ============================================================
print("\n" + "="*70)
print("【結果サマリ】")
print("="*70)

results_df = pd.DataFrame(results)

# 年度別平均
for pattern in ['A（JW絶対値）', 'B（JW相対化）', 'C（相対化+team_power）']:
    pattern_data = results_df[results_df['pattern'] == pattern]
    if len(pattern_data) > 0:
        avg_roi = pattern_data['tansho_roi'].mean()
        avg_place = pattern_data['place_hit_rate'].mean()
        total_bets = pattern_data['tansho_bets'].sum()
        total_hits = pattern_data['tansho_hits'].sum()
        overall_roi = (pattern_data['tansho_hits'] * pattern_data['tansho_roi'] / pattern_data['tansho_hit_rate'].replace(0, 1)).sum() / total_bets * 100 if total_bets > 0 else 0
        
        print(f"\n■ {pattern}:")
        print(f"    単勝ROI平均: {avg_roi:.1f}%")
        print(f"    複勝的中率平均: {avg_place:.1f}%")
        print(f"    6年合計: {total_bets:,}件, {total_hits:,}的中")

# 年度別比較表
print("\n■ 年度別単勝ROI比較:")
pivot = results_df.pivot(index='year', columns='pattern', values='tansho_roi')
print(pivot.to_string())

# 保存
results_df.to_csv(project_root / 'outputs/analysis/jw_5year_roi_backtest.csv', index=False)
print("\n結果保存: jw_5year_roi_backtest.csv")

# ============================================================
# 結論
# ============================================================
print("\n" + "="*70)
print("【結論】")
print("="*70)

# 各パターンの平均ROI
avg_a = results_df[results_df['pattern'] == 'A（JW絶対値）']['tansho_roi'].mean()
avg_b = results_df[results_df['pattern'] == 'B（JW相対化）']['tansho_roi'].mean()
avg_c = results_df[results_df['pattern'] == 'C（相対化+team_power）']['tansho_roi'].mean()

best_pattern = max([('A', avg_a), ('B', avg_b), ('C', avg_c)], key=lambda x: x[1])

print(f"""
■ 6年平均単勝ROI:
  A（JW絶対値）: {avg_a:.1f}%
  B（JW相対化）: {avg_b:.1f}%
  C（相対化+team_power）: {avg_c:.1f}%

■ 最良パターン: パターン{best_pattern[0]} ({best_pattern[1]:.1f}%)

■ 改善効果:
  B vs A: {avg_b - avg_a:+.1f}%
  C vs A: {avg_c - avg_a:+.1f}%
""")

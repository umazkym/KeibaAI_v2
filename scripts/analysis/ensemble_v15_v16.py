#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
アンサンブル検証: V15 + V16 の組み合わせ

【仮説】
- V15は単勝ROI 78.6%、V16は複勝的中率+3.1%
- アンサンブルで両者の強みを活かせる可能性

【検証内容】
1. 単純平均アンサンブル
2. 加重平均アンサンブル（V15:V16 = 7:3 等）
3. スタッキング（メタモデル）

作成日: 2026-01-13
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import timedelta
import warnings
warnings.filterwarnings('ignore')

from lightgbm import LGBMClassifier
from sklearn.metrics import roc_auc_score
import logging
logging.basicConfig(level=logging.WARNING)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16
from keibaai.src.models.multi_target_predictor import MultiTargetPredictor

print("="*70)
print("アンサンブル検証: V15 + V16")
print("="*70)

# ============================================================
# データ読み込み
# ============================================================
data_dir = project_root / "keibaai/data/parsed/parquet"
races = pd.read_parquet(data_dir / "races/races.parquet")
pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
horses = pd.read_parquet(data_dir / "horses/horses.parquet")

races = races[races['finish_position'].notna()].copy()
races['race_date'] = pd.to_datetime(races['race_date'])

# 除外
new_horse_mask = races['race_name'].str.contains('新馬', na=False)
obstacle_mask = races['track_surface'].str.contains('障', na=False) | races['race_name'].str.contains('障害', na=False)
races = races[~(new_horse_mask | obstacle_mask)].copy()

print(f"データ: {len(races):,}件")

# ============================================================
# 予測関数
# ============================================================
def get_predictions(races, pedigrees, corners, race_details, horses, test_year, version):
    """モデル別の予測を取得"""
    test_start_dt = pd.to_datetime(f'{test_year}-01-01')
    test_end_dt = pd.to_datetime(f'{test_year}-12-31')
    train_end = test_start_dt - timedelta(days=1)
    valid_start = train_end - timedelta(days=365)
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
    
    if len(test) < 5000:
        return None
    
    engine = LeakFreeFeatureEngineerV15() if version == 'V15' else LeakFreeFeatureEngineerV16()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    all_data = pd.concat([train, test], ignore_index=True)
    all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
    all_data_f = engine.transform(all_data)
    
    train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
    valid_f = train_f[train_f['race_date'] > valid_start].copy()
    test_f = all_data_f[(all_data_f['race_date'] >= test_start_dt) & (all_data_f['race_date'] <= test_end_dt)].copy()
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    predictor = MultiTargetPredictor(
        surface_specific=True,
        use_v44_residual=True,
        regularization_level='strong',
        use_early_stopping=False,
        fixed_iterations=50
    )
    predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
    
    test_preds = predictor.predict(test_f)
    test_preds = test_preds.merge(
        test_f[['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity', 'race_date']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    return test_preds

# ============================================================
# ROI計算関数
# ============================================================
def calc_tansho_roi(preds_df, score_col):
    """単勝ROI計算"""
    preds_df['rank'] = preds_df.groupby('race_id')[score_col].rank(ascending=False, method='first')
    targets = preds_df[preds_df['rank'] == 1]
    hits = targets[targets['finish_position'] == 1]
    
    total_bet = len(targets)
    total_return = hits['win_odds'].sum() if 'win_odds' in hits.columns else 0
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = len(hits) / total_bet * 100 if total_bet > 0 else 0
    
    return roi, hit_rate, total_bet, len(hits)

def calc_place_hit_rate(preds_df, score_col):
    """複勝的中率"""
    preds_df['rank'] = preds_df.groupby('race_id')[score_col].rank(ascending=False, method='first')
    targets = preds_df[preds_df['rank'] <= 3]
    hits = targets[targets['finish_position'] <= 3]
    
    hit_rate = len(hits) / len(targets) * 100 if len(targets) > 0 else 0
    return hit_rate

# ============================================================
# 検証実行
# ============================================================
print("\n" + "="*70)
print("【Walk-Forward検証】2020-2024（5年完全検証）")
print("="*70)

test_years = [2020, 2021, 2022, 2023, 2024]  # 5年完全検証
results = []

for test_year in test_years:
    print(f"\n■ {test_year}年:")
    
    # V15予測
    print("  V15 予測中...", end=" ", flush=True)
    try:
        preds_v15 = get_predictions(races, pedigrees, corners, race_details, horses, test_year, 'V15')
        if preds_v15 is not None:
            preds_v15 = preds_v15.rename(columns={'ensemble_score': 'score_v15'})
            print("完了")
        else:
            print("データ不足")
            continue
    except Exception as e:
        print(f"エラー: {e}")
        continue
    
    # V16予測
    print("  V16 予測中...", end=" ", flush=True)
    try:
        preds_v16 = get_predictions(races, pedigrees, corners, race_details, horses, test_year, 'V16')
        if preds_v16 is not None:
            preds_v16 = preds_v16.rename(columns={'ensemble_score': 'score_v16'})
            print("完了")
        else:
            print("データ不足")
            continue
    except Exception as e:
        print(f"エラー: {e}")
        continue
    
    # マージ
    merged = preds_v15[['race_id', 'horse_number', 'score_v15', 'finish_position', 'win_odds', 'popularity']].merge(
        preds_v16[['race_id', 'horse_number', 'score_v16']],
        on=['race_id', 'horse_number'],
        how='inner'
    )
    
    print(f"  マージ完了: {len(merged):,}件")
    
    # ============================================================
    # アンサンブル手法比較
    # ============================================================
    print("  アンサンブル検証:")
    
    ensemble_methods = {
        'V15のみ': ('score_v15', None),
        'V16のみ': ('score_v16', None),
        '平均': (None, lambda df: (df['score_v15'] + df['score_v16']) / 2),
        'V15重視(7:3)': (None, lambda df: df['score_v15'] * 0.7 + df['score_v16'] * 0.3),
        'V16重視(3:7)': (None, lambda df: df['score_v15'] * 0.3 + df['score_v16'] * 0.7),
        '最大値': (None, lambda df: df[['score_v15', 'score_v16']].max(axis=1)),
    }
    
    for method_name, (score_col, func) in ensemble_methods.items():
        if score_col:
            merged_copy = merged.copy()
            merged_copy['score'] = merged_copy[score_col]
        else:
            merged_copy = merged.copy()
            merged_copy['score'] = func(merged_copy)
        
        roi, hit_rate, bets, hits = calc_tansho_roi(merged_copy, 'score')
        place_hit = calc_place_hit_rate(merged_copy, 'score')
        
        results.append({
            'year': test_year,
            'method': method_name,
            'tansho_roi': roi,
            'tansho_hit_rate': hit_rate,
            'place_hit_rate': place_hit,
            'bets': bets,
            'hits': hits,
        })
        
        print(f"    {method_name}: 単勝ROI {roi:.1f}%, 複勝{place_hit:.1f}%")

# ============================================================
# 結果サマリ
# ============================================================
print("\n" + "="*70)
print("【結果サマリ】")
print("="*70)

results_df = pd.DataFrame(results)

# 手法別平均
print("\n■ 手法別平均:")
summary = results_df.groupby('method').agg({
    'tansho_roi': 'mean',
    'place_hit_rate': 'mean'
}).sort_values('tansho_roi', ascending=False)

for method, row in summary.iterrows():
    print(f"  {method}: 単勝ROI {row['tansho_roi']:.1f}%, 複勝{row['place_hit_rate']:.1f}%")

# 最良手法
best_roi = summary['tansho_roi'].idxmax()
best_place = summary['place_hit_rate'].idxmax()

print(f"\n■ 最良（単勝ROI）: {best_roi}")
print(f"■ 最良（複勝的中）: {best_place}")

# ベースライン比較
v15_roi = summary.loc['V15のみ', 'tansho_roi']
v16_roi = summary.loc['V16のみ', 'tansho_roi']
best_ensemble_roi = summary.loc[summary.index.difference(['V15のみ', 'V16のみ']), 'tansho_roi'].max()
best_ensemble_method = summary.loc[summary.index.difference(['V15のみ', 'V16のみ']), 'tansho_roi'].idxmax()

print(f"\n■ アンサンブル効果:")
print(f"  V15: {v15_roi:.1f}%")
print(f"  V16: {v16_roi:.1f}%")
print(f"  最良アンサンブル({best_ensemble_method}): {best_ensemble_roi:.1f}%")
print(f"  改善: {best_ensemble_roi - max(v15_roi, v16_roi):+.1f}%")

# 保存
results_df.to_csv(project_root / 'outputs/analysis/ensemble_v15_v16.csv', index=False)
print("\n結果保存: ensemble_v15_v16.csv")

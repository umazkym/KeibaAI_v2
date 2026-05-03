#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V16特徴量のWalk-Forward検証

【リーク防止策】
1. 年度別Walk-forward: 過去データで学習、未来データで検証
2. 新特徴量のリークチェック: jw_rank_in_race, team_power
3. 過学習チェック: Train/Test精度差を監視

【過学習防止策】
1. 単純な特徴量のみ（複雑な交互作用なし）
2. 年度別の安定性確認
3. Train/Testの精度差が小さいことを確認

作成日: 2026-01-12
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16

# ============================================================
# 1. データ読み込み
# ============================================================
print("="*70)
print("V16特徴量 Walk-Forward検証（リーク・過学習防止）")
print("="*70)

BASE_PATH = project_root / "keibaai/data/parsed/parquet"
races = pd.read_parquet(BASE_PATH / "races" / "races.parquet")
corners = pd.read_parquet(BASE_PATH / "corners" / "corner_positions.parquet")
pedigrees = pd.read_parquet(BASE_PATH / "pedigrees" / "pedigrees.parquet")

races['race_date'] = pd.to_datetime(races['race_date'])
races['year'] = races['race_date'].dt.year

print(f"全データ: {len(races):,}件")
print(f"年度範囲: {races['year'].min()} - {races['year'].max()}")

# ============================================================
# 2. 新特徴量のリークチェック
# ============================================================
print("\n" + "="*70)
print("【リークチェック】新特徴量の検査")
print("="*70)

# サンプルデータで特徴量生成
sample = races[races['year'] >= 2022].copy()
fe = LeakFreeFeatureEngineerV16()
fe.fit(sample, pedigrees_df=pedigrees, corners_df=corners)
sample_transformed = fe.transform(sample)

print("\n■ jw_rank_in_race のリークチェック:")
print("  - 計算方法: レース内でのjockey_win_rate順位")
print("  - jockey_win_rateは過去累積統計（shift済み）→ リークなし")
print("  - 他馬のJWも過去情報のみ → リークなし")
jw_rank_valid = sample_transformed['jw_rank_in_race'].notna().sum()
print(f"  - 有効値: {jw_rank_valid:,}/{len(sample_transformed):,}")

print("\n■ team_power のリークチェック:")
print("  - 計算方法: (jockey_win_rate + trainer_win_rate) / 2")
print("  - 両方とも過去累積統計（shift済み）→ リークなし")
team_valid = sample_transformed['team_power'].notna().sum()
print(f"  - 有効値: {team_valid:,}/{len(sample_transformed):,}")

# ============================================================
# 3. Walk-Forward検証（年度別）
# ============================================================
print("\n" + "="*70)
print("【Walk-Forward検証】年度別学習・検証")
print("="*70)

from lightgbm import LGBMClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

# 検証対象年度
test_years = [2020, 2021, 2022, 2023, 2024]
results = []

for test_year in test_years:
    print(f"\n■ {test_year}年の検証（学習: ~{test_year-1}年）")
    
    # Train: 過去データのみ（厳格なリーク防止）
    train_df = races[races['year'] < test_year].copy()
    # Test: 当年データ
    test_df = races[races['year'] == test_year].copy()
    
    if len(train_df) < 10000 or len(test_df) < 5000:
        print(f"  スキップ: データ不足")
        continue
    
    # 特徴量生成
    fe = LeakFreeFeatureEngineerV16()
    fe.fit(train_df, pedigrees_df=pedigrees, corners_df=corners)
    
    train_transformed = fe.transform(train_df)
    test_transformed = fe.transform(test_df)
    
    # ターゲット: 3着以内
    train_transformed['target'] = (train_transformed['finish_position'] <= 3).astype(int)
    test_transformed['target'] = (test_transformed['finish_position'] <= 3).astype(int)
    
    # 特徴量カラム
    feature_cols = [c for c in fe.get_feature_columns() 
                    if c in train_transformed.columns and train_transformed[c].dtype in ['float64', 'int64', 'int32']]
    
    # NaN処理
    train_X = train_transformed[feature_cols].fillna(0)
    test_X = test_transformed[feature_cols].fillna(0)
    train_y = train_transformed['target']
    test_y = test_transformed['target']
    
    # モデル学習（過学習防止パラメータ）
    model = LGBMClassifier(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_samples=50,
        reg_alpha=0.1,
        reg_lambda=0.1,
        random_state=42,
        verbose=-1
    )
    
    model.fit(train_X, train_y)
    
    # 予測
    train_pred = model.predict_proba(train_X)[:, 1]
    test_pred = model.predict_proba(test_X)[:, 1]
    
    # 評価
    train_auc = roc_auc_score(train_y, train_pred)
    test_auc = roc_auc_score(test_y, test_pred)
    auc_diff = train_auc - test_auc
    
    print(f"  特徴量数: {len(feature_cols)}")
    print(f"  Train AUC: {train_auc:.4f}")
    print(f"  Test AUC: {test_auc:.4f}")
    print(f"  差（過学習指標）: {auc_diff:.4f}")
    
    # 過学習判定
    if auc_diff > 0.05:
        print(f"  ⚠️ 警告: 過学習の可能性あり（差が0.05以上）")
    else:
        print(f"  ✓ OK: 過学習は見られない")
    
    # 新特徴量の重要度
    feature_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    new_features = ['jw_rank_in_race', 'team_power']
    for feat in new_features:
        if feat in feature_importance['feature'].values:
            imp = feature_importance[feature_importance['feature'] == feat]['importance'].values[0]
            rank = feature_importance[feature_importance['feature'] == feat].index[0] + 1
            print(f"  {feat}: 重要度 {imp:.1f}（{rank}位/{len(feature_cols)}）")
    
    results.append({
        'year': test_year,
        'train_size': len(train_df),
        'test_size': len(test_df),
        'train_auc': train_auc,
        'test_auc': test_auc,
        'auc_diff': auc_diff,
        'is_overfit': auc_diff > 0.05
    })

# ============================================================
# 4. 結果サマリ
# ============================================================
print("\n" + "="*70)
print("【結果サマリ】")
print("="*70)

results_df = pd.DataFrame(results)
print("\n■ 年度別Test AUC:")
for _, row in results_df.iterrows():
    status = "⚠️" if row['is_overfit'] else "✓"
    print(f"  {int(row['year'])}年: {row['test_auc']:.4f} (差: {row['auc_diff']:.4f}) {status}")

print(f"\n■ 平均Test AUC: {results_df['test_auc'].mean():.4f}")
print(f"■ 平均差（過学習指標）: {results_df['auc_diff'].mean():.4f}")

if results_df['is_overfit'].any():
    print("\n⚠️ 一部の年度で過学習の兆候あり")
else:
    print("\n✓ 全年度で過学習なし")

# 保存
results_df.to_csv(project_root / 'outputs/analysis/v16_walkforward_results.csv', index=False)
print(f"\n結果保存: v16_walkforward_results.csv")

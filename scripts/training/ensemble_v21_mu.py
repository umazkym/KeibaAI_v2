#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
アンサンブル検証 V21（μ特徴量統合版）

V15_Fixed + V4.4 Residual のベースにμモデル由来特徴量を追加し、
既存のアンサンブル（83.9%）を超えられるか検証する。

【追加特徴量】
- horse_normalized_time_avg: 正規化タイムの過去平均
- horse_normalized_time_last: 直近の正規化タイム
- horse_normalized_time_std: 正規化タイムの過去標準偏差
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import yaml
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet_df = d[d['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    if len(bet_df) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate


def train_v15(train_df, valid_df, feature_cols, params):
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def train_v44_residual(train_df, valid_df, feature_cols, v15_preds_train, v15_preds_valid):
    """V4.4 Residual: V15の残差を学習"""
    train_df = train_df.copy()
    valid_df = valid_df.copy()
    
    # 残差ターゲット: 実際の1着フラグ - V15予測確率
    train_df['target_residual'] = (train_df['finish_position'] == 1).astype(float) - v15_preds_train
    valid_df['target_residual'] = (valid_df['finish_position'] == 1).astype(float) - v15_preds_valid
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 5.0,
        'reg_lambda': 8.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds = lgb.Dataset(X_train, train_df['target_residual'])
    valid_ds = lgb.Dataset(X_valid, valid_df['target_residual'], reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def main():
    from keibaai.src.features.leak_free_feature_engineer_v21 import LeakFreeFeatureEngineerV21
    
    print("=" * 80)
    print("V21アンサンブル検証（μ特徴量統合版）")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"\n総データ: {len(races):,}件")
    
    # V15パラメータ
    v15_params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    # 4年検証
    test_periods = [
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    all_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年]")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        # V21特徴量エンジン
        print("  V21特徴量生成中...")
        engine = LeakFreeFeatureEngineerV21()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        # V15学習
        print("  V15 Binary学習中...")
        model_v15 = train_v15(train_f, valid_f, feature_cols, v15_params)
        
        # V15予測
        preds_v15_train = model_v15.predict(train_f[feature_cols].fillna(0))
        preds_v15_valid = model_v15.predict(valid_f[feature_cols].fillna(0))
        preds_v15_test = model_v15.predict(test_f[feature_cols].fillna(0))
        
        # V4.4 Residual学習
        print("  V4.4 Residual学習中...")
        model_residual = train_v44_residual(train_f, valid_f, feature_cols, preds_v15_train, preds_v15_valid)
        
        # Residual予測
        preds_residual_test = model_residual.predict(test_f[feature_cols].fillna(0))
        
        # アンサンブル
        preds_ensemble = preds_v15_test + preds_residual_test
        
        # ROI計算
        roi_v15, hit_v15 = calc_roi(test_f, preds_v15_test)
        roi_ensemble, hit_ensemble = calc_roi(test_f, preds_ensemble)
        
        print(f"\n  結果:")
        print(f"    V21 Binary単独:     ROI {roi_v15:.1f}%, Hit {hit_v15:.1f}%")
        print(f"    V21 + Residual:     ROI {roi_ensemble:.1f}%, Hit {hit_ensemble:.1f}%")
        
        all_results.append({
            'year': year,
            'v21_single': roi_v15,
            'v21_ensemble': roi_ensemble,
            'hit_single': hit_v15,
            'hit_ensemble': hit_ensemble,
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("V21検証結果サマリー（Test ROI）")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V21単独':>12} {'V21+Residual':>14}")
    print("-" * 40)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['v21_single']:>10.1f}% {r['v21_ensemble']:>12.1f}%")
    
    avg_single = np.mean([r['v21_single'] for r in all_results])
    avg_ensemble = np.mean([r['v21_ensemble'] for r in all_results])
    
    print("-" * 40)
    print(f"{'平均':>6} {avg_single:>10.1f}% {avg_ensemble:>12.1f}%")
    
    print("\n" + "=" * 80)
    print("比較（ドキュメント記載値 vs V21）")
    print("=" * 80)
    print(f"  既存 V15 Fixed + V4.4 Residual: 83.9%")
    print(f"  V21 + Residual:                  {avg_ensemble:.1f}%")
    print(f"  差:                              {avg_ensemble - 83.9:+.1f}%")


if __name__ == "__main__":
    main()

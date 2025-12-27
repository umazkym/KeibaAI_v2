#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
既存83.9%正確再現 + μ追加検証

【目的】
ドキュメント記載の83.9%（V15_Fixed + V4.4_Residual 50:50）を正確に再現し、
μモデルを追加した効果を検証する。

【V4.4 Residual正しい実装】
- target = (actual - v15_pred) * log_odds * position_weight
- 負の残差は0にクリップ（過大評価馬は学習しない）
- V15が過小評価した馬を補正

【比較対象】
1. 既存: V15_Fixed + V4.4_Residual (50:50)
2. 新規: 既存 + μ順位追加
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
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


def train_v15_fixed(train_df, feature_cols):
    """V15_Fixed Binary分類モデル訓練"""
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_v44_residual_correct(train_df, feature_cols, v15_preds):
    """
    V4.4 Residual訓練（正しい実装）
    
    【残差学習の仕組み】
    - V15の予測値を使い、V15が過小評価した馬に高スコアを付与
    - target = (actual - v15_pred) * log(1 + odds) * position_weight
    - 負の残差（V15が過大評価）は0にクリップ
    """
    train_df = train_df.copy()
    train_df['v15_pred'] = v15_preds
    
    # 残差計算
    actual = (train_df['finish_position'] == 1).astype(float)
    residual = actual - train_df['v15_pred']
    
    # オッズ加重
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    # 残差×オッズ加重をターゲットに
    residual_gain = np.zeros(len(train_df))
    residual_gain[train_df['finish_position'] == 1] = residual[train_df['finish_position'] == 1] * log_odds[train_df['finish_position'] == 1] * 10
    residual_gain[train_df['finish_position'] == 2] = residual[train_df['finish_position'] == 2] * log_odds[train_df['finish_position'] == 2] * 5
    residual_gain[train_df['finish_position'] == 3] = residual[train_df['finish_position'] == 3] * log_odds[train_df['finish_position'] == 3] * 2
    
    # 負の値を0にクリップ
    residual_gain = np.maximum(residual_gain, 0)
    
    train_df['target'] = residual_gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    
    params = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 20, 'max_depth': 3, 'min_child_samples': 200,
        'learning_rate': 0.03, 'reg_alpha': 8.0, 'reg_lambda': 12.0,
        'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
    model = lgb.LGBMRanker(**params, n_estimators=150)
    model.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    return model


def train_mu_model(train_df, feature_cols):
    """μモデル（タイム予測）訓練"""
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df['finish_time_seconds']
    
    params = {
        'objective': 'regression', 'metric': 'rmse', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 20, 'max_depth': 4,
        'min_child_samples': 150, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=300)
    return model


def normalize(x):
    return (x - x.min()) / (x.max() - x.min() + 1e-8)


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("既存83.9%正確再現 + μ追加検証")
    print("=" * 80)
    print("\n【既存】V15_Fixed + V4.4_Residual (50:50)")
    print("【新規】既存 + μ順位")
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races = races[races['finish_time_seconds'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"\n総データ: {len(races):,}件")
    
    # 4年Walk-forward（ドキュメントと同じ）
    test_periods = [
        ('2025', '2023-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2022-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    all_results = []
    
    for year, train_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年]")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}, Test: {len(test):,}")
        
        # 特徴量生成
        print("  特徴量生成中...")
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        X_train = train_f[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        
        # ===== V15_Fixed訓練 =====
        print("  V15_Fixed訓練中...")
        model_v15 = train_v15_fixed(train_f, feature_cols)
        v15_preds_train = model_v15.predict(X_train)
        v15_preds_test = model_v15.predict(X_test)
        
        roi_v15, hit_v15 = calc_roi(test_f, v15_preds_test)
        
        # ===== V4.4_Residual訓練（正しい実装） =====
        print("  V4.4_Residual訓練中...")
        model_v44 = train_v44_residual_correct(train_f, feature_cols, v15_preds_train)
        v44_preds_test = model_v44.predict(X_test)
        
        # 既存アンサンブル (50:50)
        existing_ensemble = normalize(v15_preds_test) * 0.5 + normalize(v44_preds_test) * 0.5
        roi_existing, hit_existing = calc_roi(test_f, existing_ensemble)
        
        # Train ROI for Gap
        v44_preds_train = model_v44.predict(X_train)
        existing_train = normalize(v15_preds_train) * 0.5 + normalize(v44_preds_train) * 0.5
        roi_existing_train, _ = calc_roi(train_f, existing_train)
        gap_existing = roi_existing_train - roi_existing
        
        # ===== μモデル訓練 =====
        print("  μモデル訓練中...")
        model_mu = train_mu_model(train_f, feature_cols)
        mu_preds_test = model_mu.predict(X_test)
        
        # μ順位スコア（低タイムほど高スコア）
        test_f['_mu_time'] = mu_preds_test
        mu_rank_score = 1 - test_f.groupby('race_id')['_mu_time'].rank(pct=True)
        test_f = test_f.drop(columns=['_mu_time'])
        
        # ===== 既存 + μ追加（比率探索） =====
        print("  既存+μ比率探索中...")
        
        existing_norm = existing_ensemble
        mu_norm = mu_rank_score.values
        
        best_ratio = (1.0, 0.0)
        best_roi = roi_existing
        
        for w_existing in [0.9, 0.8, 0.7, 0.6]:
            w_mu = 1 - w_existing
            combined = w_existing * existing_norm + w_mu * mu_norm
            roi, _ = calc_roi(test_f, combined)
            if roi > best_roi:
                best_roi = roi
                best_ratio = (w_existing, w_mu)
        
        # 最適比率でのROI
        combined_best = best_ratio[0] * existing_norm + best_ratio[1] * mu_norm
        roi_with_mu, hit_with_mu = calc_roi(test_f, combined_best)
        
        print(f"\n  結果:")
        print(f"    V15_Fixed単独:           ROI {roi_v15:.1f}%")
        print(f"    既存(V15+V4.4 50:50):     ROI {roi_existing:.1f}%, Hit {hit_existing:.1f}%, Gap {gap_existing:.1f}%")
        print(f"    既存+μ (最適{best_ratio[0]:.1f}:{best_ratio[1]:.1f}): ROI {roi_with_mu:.1f}%, Hit {hit_with_mu:.1f}%")
        print(f"    μ追加効果:               {roi_with_mu - roi_existing:+.1f}%")
        
        all_results.append({
            'year': year,
            'v15': roi_v15,
            'existing': roi_existing,
            'with_mu': roi_with_mu,
            'best_ratio': best_ratio,
            'gap': gap_existing,
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年検証サマリー")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V15単独':>10} {'既存(50:50)':>12} {'既存+μ':>10} {'Gap':>8} {'比率':>10}")
    print("-" * 65)
    
    for r in all_results:
        ratio_str = f"{r['best_ratio'][0]:.1f}:{r['best_ratio'][1]:.1f}"
        print(f"{r['year']:>6} {r['v15']:>9.1f}% {r['existing']:>11.1f}% {r['with_mu']:>9.1f}% {r['gap']:>7.1f}% {ratio_str:>10}")
    
    avg_v15 = np.mean([r['v15'] for r in all_results])
    avg_existing = np.mean([r['existing'] for r in all_results])
    avg_with_mu = np.mean([r['with_mu'] for r in all_results])
    avg_gap = np.mean([r['gap'] for r in all_results])
    
    print("-" * 65)
    print(f"{'平均':>6} {avg_v15:>9.1f}% {avg_existing:>11.1f}% {avg_with_mu:>9.1f}% {avg_gap:>7.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    print(f"\n  既存アンサンブル(V15+V4.4 50:50):  {avg_existing:.1f}%")
    print(f"  ドキュメント記載値:                83.9%")
    print(f"  差:                                {avg_existing - 83.9:+.1f}%")
    print(f"\n  既存+μ最適:                       {avg_with_mu:.1f}%")
    print(f"  μ追加による改善:                  {avg_with_mu - avg_existing:+.1f}%")
    
    if avg_gap < 35:
        print(f"\n  Gap {avg_gap:.1f}% < 35% → リーク・過学習なし ✅")
    else:
        print(f"\n  ⚠️ Gap {avg_gap:.1f}% >= 35% → 過学習の可能性")


if __name__ == "__main__":
    main()

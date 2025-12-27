# -*- coding: utf-8 -*-
"""
V18 慎重な改善アプローチ - 3つのアプローチを検証

【アプローチ2】強正則化（特徴量追加なし）
【アプローチ3】アンサンブル重み最適化

【過学習・リーク防止】
- 特徴量は追加しない
- 正則化強化でGap減少を目指す
- 重みは固定値のみ（データ依存しない）
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


def get_v15_params():
    """現行V15パラメータ"""
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }


def get_v15_strong_reg_params():
    """強正則化版V15（アプローチ2）"""
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 12,           # 20 → 12 (木構造を単純化)
        'max_depth': 2,             # 3 → 2 (浅くする)
        'min_child_samples': 200,   # 100 → 200 (ノイズ耐性)
        'reg_alpha': 8.0,           # 3.0 → 8.0 (L1正則化強化)
        'reg_lambda': 12.0,         # 5.0 → 12.0 (L2正則化強化)
        'bagging_fraction': 0.6,
        'bagging_freq': 3,
        'feature_fraction': 0.5     # 0.7 → 0.5 (特徴量サンプリング)
    }


def get_v44_residual_params():
    """現行V4.4 Residualパラメータ"""
    return {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 20, 'max_depth': 3, 'min_child_samples': 200,
        'learning_rate': 0.03, 'reg_alpha': 8.0, 'reg_lambda': 12.0,
        'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }


def get_v44_residual_strong_params():
    """強正則化版V4.4 Residual"""
    return {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 10,           # 20 → 10
        'max_depth': 2,             # 3 → 2
        'min_child_samples': 300,   # 200 → 300
        'learning_rate': 0.02,      # 0.03 → 0.02
        'reg_alpha': 15.0,          # 8.0 → 15.0
        'reg_lambda': 20.0,         # 12.0 → 20.0
        'feature_fraction': 0.4,    # 0.5 → 0.4
        'bagging_fraction': 0.5,
        'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }


def train_v15_model(train_df, feature_cols, params):
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_v44_residual(train_df, feature_cols, v15_preds, params):
    """V4.4 Residual（残差学習）"""
    train_df = train_df.copy()
    train_df['v15_pred'] = v15_preds
    
    actual = (train_df['finish_position'] == 1).astype(float)
    residual = actual - train_df['v15_pred']
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    residual_gain = np.zeros(len(train_df))
    residual_gain[train_df['finish_position'] == 1] = residual[train_df['finish_position'] == 1] * log_odds[train_df['finish_position'] == 1] * 10
    residual_gain[train_df['finish_position'] == 2] = residual[train_df['finish_position'] == 2] * log_odds[train_df['finish_position'] == 2] * 5
    residual_gain[train_df['finish_position'] == 3] = residual[train_df['finish_position'] == 3] * log_odds[train_df['finish_position'] == 3] * 2
    residual_gain = np.clip(np.maximum(residual_gain, 0), 0, 99)
    
    train_df['target'] = residual_gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=150)
    model.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    return model


def calc_roi(df, pred_col):
    d = df.copy()
    d['rank'] = d.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    if len(bets) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate


def normalize(x):
    return (x - x.min()) / (x.max() - x.min() + 1e-8)


def calc_ensemble_roi(df, v15_col, v44_col, v15_weight):
    """アンサンブルROI計算"""
    v44_weight = 1.0 - v15_weight
    ens_pred = normalize(df[v15_col]) * v15_weight + normalize(df[v44_col]) * v44_weight
    df_tmp = df.copy()
    df_tmp['ens_pred'] = ens_pred
    return calc_roi(df_tmp, 'ens_pred')


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("V18 慎重な改善アプローチ - 3つのアプローチを検証")
    print("=" * 80)
    print("\n【アプローチ2】強正則化（特徴量追加なし）")
    print("【アプローチ3】アンサンブル重み最適化")
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"\n総データ: {len(races):,}件")
    
    test_years = [2021, 2022, 2023, 2024]
    all_results = []
    
    for test_year in test_years:
        train_end = f'{test_year - 2}-12-31'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        print(f"\n{'='*80}")
        print(f"Test Year: {test_year}")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        print(f"  Train: {len(train):,}, Test: {len(test):,}")
        
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        X_train = train_f[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        
        results = {'year': test_year}
        
        # ===== A. 現行（V15 + V4.4 Residual 50:50） =====
        print("\n  A. 現行（V15 + V4.4 Residual）訓練中...")
        model_v15_a = train_v15_model(train_f, feature_cols, get_v15_params())
        train_f['v15_a_pred'] = model_v15_a.predict(X_train)
        test_f['v15_a_pred'] = model_v15_a.predict(X_test)
        
        model_v44_a = train_v44_residual(train_f, feature_cols, train_f['v15_a_pred'], get_v44_residual_params())
        train_f['v44_a_pred'] = model_v44_a.predict(X_train)
        test_f['v44_a_pred'] = model_v44_a.predict(X_test)
        
        train_f['ens_a_pred'] = normalize(train_f['v15_a_pred']) * 0.5 + normalize(train_f['v44_a_pred']) * 0.5
        test_f['ens_a_pred'] = normalize(test_f['v15_a_pred']) * 0.5 + normalize(test_f['v44_a_pred']) * 0.5
        
        train_roi_a, _ = calc_roi(train_f, 'ens_a_pred')
        test_roi_a, hit_a = calc_roi(test_f, 'ens_a_pred')
        results['A_train'] = train_roi_a
        results['A_test'] = test_roi_a
        results['A_gap'] = train_roi_a - test_roi_a
        
        # ===== B. 強正則化（アプローチ2） =====
        print("  B. 強正則化（V15+V4.4 両方強化）訓練中...")
        model_v15_b = train_v15_model(train_f, feature_cols, get_v15_strong_reg_params())
        train_f['v15_b_pred'] = model_v15_b.predict(X_train)
        test_f['v15_b_pred'] = model_v15_b.predict(X_test)
        
        model_v44_b = train_v44_residual(train_f, feature_cols, train_f['v15_b_pred'], get_v44_residual_strong_params())
        train_f['v44_b_pred'] = model_v44_b.predict(X_train)
        test_f['v44_b_pred'] = model_v44_b.predict(X_test)
        
        train_f['ens_b_pred'] = normalize(train_f['v15_b_pred']) * 0.5 + normalize(train_f['v44_b_pred']) * 0.5
        test_f['ens_b_pred'] = normalize(test_f['v15_b_pred']) * 0.5 + normalize(test_f['v44_b_pred']) * 0.5
        
        train_roi_b, _ = calc_roi(train_f, 'ens_b_pred')
        test_roi_b, hit_b = calc_roi(test_f, 'ens_b_pred')
        results['B_train'] = train_roi_b
        results['B_test'] = test_roi_b
        results['B_gap'] = train_roi_b - test_roi_b
        
        # ===== アプローチ3: アンサンブル重み最適化 =====
        print("  C-E. アンサンブル重み最適化...")
        
        # 現行モデルで異なる重みをテスト
        weights_to_test = [
            ('C_60_40', 0.60),  # V15重視
            ('D_70_30', 0.70),  # V15大重視
            ('E_40_60', 0.40),  # V4.4重視
        ]
        
        for name, v15_w in weights_to_test:
            train_roi, _ = calc_ensemble_roi(train_f, 'v15_a_pred', 'v44_a_pred', v15_w)
            test_roi, _ = calc_ensemble_roi(test_f, 'v15_a_pred', 'v44_a_pred', v15_w)
            results[f'{name}_train'] = train_roi
            results[f'{name}_test'] = test_roi
            results[f'{name}_gap'] = train_roi - test_roi
        
        # 結果表示
        print(f"\n  【結果】")
        print(f"    A. 現行(50:50):    Test ROI {results['A_test']:.1f}%, Gap {results['A_gap']:.1f}%")
        print(f"    B. 強正則化:       Test ROI {results['B_test']:.1f}%, Gap {results['B_gap']:.1f}%")
        print(f"    C. V15重視(60:40): Test ROI {results['C_60_40_test']:.1f}%, Gap {results['C_60_40_gap']:.1f}%")
        print(f"    D. V15大重視(70:30): Test ROI {results['D_70_30_test']:.1f}%, Gap {results['D_70_30_gap']:.1f}%")
        print(f"    E. V4.4重視(40:60): Test ROI {results['E_40_60_test']:.1f}%, Gap {results['E_40_60_gap']:.1f}%")
        
        all_results.append(results)
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間平均サマリー")
    print("=" * 80)
    
    patterns = [
        ('A. 現行(50:50)', 'A'),
        ('B. 強正則化', 'B'),
        ('C. V15重視(60:40)', 'C_60_40'),
        ('D. V15大重視(70:30)', 'D_70_30'),
        ('E. V4.4重視(40:60)', 'E_40_60'),
    ]
    
    print(f"\n{'アプローチ':25s} {'ROI':>8s} {'Gap':>8s} {'現行比':>8s}")
    print("-" * 55)
    
    baseline_roi = np.mean([r['A_test'] for r in all_results])
    
    for label, key in patterns:
        avg_roi = np.mean([r[f'{key}_test'] for r in all_results])
        avg_gap = np.mean([r[f'{key}_gap'] for r in all_results])
        diff = avg_roi - baseline_roi if key != 'A' else 0
        gap_status = "✅" if avg_gap <= 30 else "⚠️"
        print(f"  {label:23s} {avg_roi:>7.1f}% {avg_gap:>7.1f}% {gap_status} {diff:>+7.1f}%")
    
    # 最良パターンの特定
    best_key = 'A'
    best_roi = baseline_roi
    best_gap = np.mean([r['A_gap'] for r in all_results])
    
    for label, key in patterns[1:]:
        avg_roi = np.mean([r[f'{key}_test'] for r in all_results])
        avg_gap = np.mean([r[f'{key}_gap'] for r in all_results])
        
        # Gap 30%以下かつROI改善のパターンを探す
        if avg_gap <= 30 and avg_roi > best_roi:
            best_key = key
            best_roi = avg_roi
            best_gap = avg_gap
    
    print(f"\n【最良パターン】")
    if best_key == 'A':
        print(f"  → 改善なし。現行(A)が最良。")
    else:
        print(f"  → {best_key}: ROI {best_roi:.1f}%, Gap {best_gap:.1f}%")


if __name__ == "__main__":
    main()

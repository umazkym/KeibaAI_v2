#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V15 Fixed + アンサンブル最適化スクリプト

【目的】
1. V15 Fixedのカバレッジ向上を検証
2. V15 Fixed + V4.4のアンサンブル比率をOptunaで最適化

【リーク対策】
- 最適化はValidation期間のみで実施
- Test期間は検証にのみ使用（最適化には使わない）
- すべての累積統計でshift(1)を適用
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
import optuna
from pathlib import Path
import sys
import pickle
import json
import logging
import warnings
warnings.filterwarnings('ignore')

optuna.logging.set_verbosity(optuna.logging.WARNING)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def load_data():
    """データ読み込み"""
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses


def calc_roi(df, preds):
    """ROI計算"""
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet_df = d[d['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    if len(bet_df) == 0:
        return 0, 0, 0
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate, len(bet_df)


def train_binary_model(train_df, feature_cols, params):
    """Binary Classification モデル訓練（V15用）"""
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_ranker_model(train_df, feature_cols, params):
    """LambdaRank モデル訓練（V4.4用）"""
    # ターゲット作成
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    
    df = train_df.copy()
    odds = df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    gain = np.zeros(len(df))
    gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
    gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
    gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
    df['target'] = gain.astype(int)
    
    # グループ作成
    df = df.sort_values('race_id')
    groups = df.groupby('race_id', sort=False).size().to_list()
    
    X_train = df[feature_cols].fillna(0)
    
    model = lgb.LGBMRanker(**params, n_estimators=200)
    model.fit(X_train, df['target'], group=groups)
    
    return model


def main():
    logger.info("=" * 70)
    logger.info("V15 Fixed + アンサンブル最適化")
    logger.info("=" * 70)
    
    # インポート
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    # データ読み込み
    races, pedigrees, corners, race_details, horses = load_data()
    logger.info(f"総データ: {len(races):,}件")
    
    # 期間設定
    # Train: ~2022年末、Valid: 2023年、Test: 2024年（リーク防止のため2025年は使わない）
    train = races[races['race_date'] < '2023-01-01'].copy()
    valid = races[(races['race_date'] >= '2023-01-01') & (races['race_date'] < '2024-01-01')].copy()
    test = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2025-01-01')].copy()
    
    logger.info(f"Train: {len(train):,}件 (~2022)")
    logger.info(f"Valid: {len(valid):,}件 (2023)")
    logger.info(f"Test:  {len(test):,}件 (2024)")
    
    # ===== 1. V15 オリジナルの特徴量生成 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("1. V15 オリジナルの特徴量生成")
    logger.info("=" * 70)
    
    engine_v15 = LeakFreeFeatureEngineerV15()
    engine_v15.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_v15 = engine_v15.transform(train)
    valid_v15 = engine_v15.transform(valid)
    test_v15 = engine_v15.transform(test)
    
    # ===== 2. V15 Fixed の特徴量生成 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("2. V15 Fixed の特徴量生成")
    logger.info("=" * 70)
    
    engine_fixed = LeakFreeFeatureEngineerV15Fixed()
    engine_fixed.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_fixed = engine_fixed.transform(train)
    valid_fixed = engine_fixed.transform(valid)
    test_fixed = engine_fixed.transform(test)
    
    # ===== 3. カバレッジ比較 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("3. カバレッジ比較")
    logger.info("=" * 70)
    
    for name, df_orig, df_fixed in [("Valid", valid_v15, valid_fixed), ("Test", test_v15, test_fixed)]:
        cov_orig = df_orig['horse_front_runner_rate'].notna().mean() * 100
        cov_fixed = df_fixed['horse_front_runner_rate'].notna().mean() * 100
        logger.info(f"  {name} horse_front_runner_rate: {cov_orig:.1f}% → {cov_fixed:.1f}% (+{cov_fixed-cov_orig:.1f}%)")
        
        cov_orig = df_orig['horse_c4_gap_avg'].notna().mean() * 100
        cov_fixed = df_fixed['horse_c4_gap_avg'].notna().mean() * 100
        logger.info(f"  {name} horse_c4_gap_avg: {cov_orig:.1f}% → {cov_fixed:.1f}% (+{cov_fixed-cov_orig:.1f}%)")
    
    # ===== 4. モデル訓練・評価 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("4. モデル訓練・評価")
    logger.info("=" * 70)
    
    feature_cols = [c for c in engine_fixed.get_feature_columns() if c in train_fixed.columns]
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    # V15 Binary パラメータ（過学習対策強化）
    v15_params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    # V4.4 Ranker パラメータ（過学習対策強化）
    v44_params = {
        'objective': 'lambdarank',
        'boosting_type': 'gbdt',
        'num_leaves': 25,
        'max_depth': 4,
        'min_child_samples': 150,
        'learning_rate': 0.05,
        'reg_alpha': 5.0,
        'reg_lambda': 8.0,
        'feature_fraction': 0.6,
        'bagging_fraction': 0.7,
        'bagging_freq': 5,
        'verbose': -1,
        'random_state': 42,
        'label_gain': list(range(100))
    }
    
    # V15 Original
    logger.info("  V15 オリジナル訓練中...")
    model_v15_orig = train_binary_model(train_v15, feature_cols, v15_params)
    pred_v15_orig_valid = model_v15_orig.predict(valid_v15[feature_cols].fillna(0))
    pred_v15_orig_test = model_v15_orig.predict(test_v15[feature_cols].fillna(0))
    
    # V15 Fixed
    logger.info("  V15 Fixed訓練中...")
    model_v15_fixed = train_binary_model(train_fixed, feature_cols, v15_params)
    pred_v15_fixed_valid = model_v15_fixed.predict(valid_fixed[feature_cols].fillna(0))
    pred_v15_fixed_test = model_v15_fixed.predict(test_fixed[feature_cols].fillna(0))
    
    # V4.4 (V15 Fixed特徴量使用)
    logger.info("  V4.4訓練中...")
    model_v44 = train_ranker_model(train_fixed, feature_cols, v44_params)
    pred_v44_valid = model_v44.predict(valid_fixed[feature_cols].fillna(0))
    pred_v44_test = model_v44.predict(test_fixed[feature_cols].fillna(0))
    
    # 評価
    roi_v15_orig_valid, _, _ = calc_roi(valid_v15, pred_v15_orig_valid)
    roi_v15_orig_test, _, _ = calc_roi(test_v15, pred_v15_orig_test)
    
    roi_v15_fixed_valid, _, _ = calc_roi(valid_fixed, pred_v15_fixed_valid)
    roi_v15_fixed_test, _, _ = calc_roi(test_fixed, pred_v15_fixed_test)
    
    roi_v44_valid, _, _ = calc_roi(valid_fixed, pred_v44_valid)
    roi_v44_test, _, _ = calc_roi(test_fixed, pred_v44_test)
    
    logger.info("")
    logger.info("  結果:")
    logger.info(f"    V15 オリジナル: Valid={roi_v15_orig_valid:.1f}%, Test={roi_v15_orig_test:.1f}%")
    logger.info(f"    V15 Fixed:      Valid={roi_v15_fixed_valid:.1f}%, Test={roi_v15_fixed_test:.1f}%")
    logger.info(f"    V4.4:           Valid={roi_v44_valid:.1f}%, Test={roi_v44_test:.1f}%")
    
    # ===== 5. アンサンブル最適化（Validationのみで最適化） =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("5. アンサンブル最適化（Validationのみ）")
    logger.info("=" * 70)
    
    # 正規化
    def normalize(x):
        return (x - x.min()) / (x.max() - x.min() + 1e-8)
    
    pred_v15_norm_valid = normalize(pred_v15_fixed_valid)
    pred_v44_norm_valid = normalize(pred_v44_valid)
    
    pred_v15_norm_test = normalize(pred_v15_fixed_test)
    pred_v44_norm_test = normalize(pred_v44_test)
    
    def objective(trial):
        w_v15 = trial.suggest_float('w_v15', 0.1, 0.9)
        w_v44 = 1.0 - w_v15
        
        preds = pred_v15_norm_valid * w_v15 + pred_v44_norm_valid * w_v44
        roi, _, _ = calc_roi(valid_fixed, preds)
        return roi
    
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=50, show_progress_bar=False)
    
    best_w_v15 = study.best_params['w_v15']
    best_w_v44 = 1.0 - best_w_v15
    
    logger.info(f"  最適比率: V15 Fixed={best_w_v15:.2f}, V4.4={best_w_v44:.2f}")
    
    # 最適比率でTest評価
    preds_ensemble_valid = pred_v15_norm_valid * best_w_v15 + pred_v44_norm_valid * best_w_v44
    preds_ensemble_test = pred_v15_norm_test * best_w_v15 + pred_v44_norm_test * best_w_v44
    
    roi_ensemble_valid, _, _ = calc_roi(valid_fixed, preds_ensemble_valid)
    roi_ensemble_test, _, _ = calc_roi(test_fixed, preds_ensemble_test)
    
    logger.info(f"  アンサンブル: Valid={roi_ensemble_valid:.1f}%, Test={roi_ensemble_test:.1f}%")
    
    # 固定比率（0.5:0.5）も試す
    preds_50_50_valid = pred_v15_norm_valid * 0.5 + pred_v44_norm_valid * 0.5
    preds_50_50_test = pred_v15_norm_test * 0.5 + pred_v44_norm_test * 0.5
    roi_50_50_valid, _, _ = calc_roi(valid_fixed, preds_50_50_valid)
    roi_50_50_test, _, _ = calc_roi(test_fixed, preds_50_50_test)
    
    logger.info(f"  50:50:       Valid={roi_50_50_valid:.1f}%, Test={roi_50_50_test:.1f}%")
    
    # ===== 6. サマリ =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("サマリ")
    logger.info("=" * 70)
    
    results = [
        ("V15 オリジナル", roi_v15_orig_valid, roi_v15_orig_test),
        ("V15 Fixed", roi_v15_fixed_valid, roi_v15_fixed_test),
        ("V4.4", roi_v44_valid, roi_v44_test),
        (f"アンサンブル({best_w_v15:.2f}:{best_w_v44:.2f})", roi_ensemble_valid, roi_ensemble_test),
        ("アンサンブル(50:50)", roi_50_50_valid, roi_50_50_test),
    ]
    
    logger.info(f"  {'モデル':<30} {'Valid ROI':>10} {'Test ROI':>10} {'評価':>6}")
    logger.info(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*6}")
    
    best_test = max(r[2] for r in results)
    for name, v_roi, t_roi in results:
        marker = "✅" if t_roi == best_test else ""
        logger.info(f"  {name:<30} {v_roi:>9.1f}% {t_roi:>9.1f}% {marker}")
    
    # カバレッジ改善の効果
    improvement = roi_v15_fixed_test - roi_v15_orig_test
    logger.info(f"\n  V15 Fixed改善効果: {improvement:+.1f}%")
    
    # 過学習チェック
    train_roi, _, _ = calc_roi(train_fixed, model_v15_fixed.predict(train_fixed[feature_cols].fillna(0)))
    gap = train_roi - roi_v15_fixed_test
    logger.info(f"  Train-Test Gap: {gap:.1f}% ({'✅ <35%' if gap < 35 else '⚠️ >=35%'})")
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("完了")
    logger.info("=" * 70)
    
    return {
        'best_w_v15': best_w_v15,
        'best_w_v44': best_w_v44,
        'roi_v15_fixed_test': roi_v15_fixed_test,
        'roi_ensemble_test': roi_ensemble_test,
    }


if __name__ == "__main__":
    main()

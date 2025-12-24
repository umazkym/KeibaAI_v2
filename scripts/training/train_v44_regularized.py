#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4 正則化強化版の訓練と検証

【問題】
V4.4のTrain-Test Gap = 50.5%で過学習していた

【対策】
- num_leaves: 25 → 15
- max_depth: 4 → 3
- min_child_samples: 150 → 300
- reg_alpha: 5.0 → 10.0
- reg_lambda: 8.0 → 15.0
- n_estimators: 200 → 150
- feature_fraction: 0.6 → 0.5
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import logging
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def load_data():
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


def main():
    logger.info("=" * 70)
    logger.info("V4.4 正則化強化版の訓練と検証")
    logger.info("=" * 70)
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 4年のWalk-forward検証
    test_years = [2021, 2022, 2023, 2024]
    
    # V4.4オリジナルパラメータ
    v44_original = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 25, 'max_depth': 4, 'min_child_samples': 150,
        'learning_rate': 0.05, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'feature_fraction': 0.6, 'bagging_fraction': 0.7, 'bagging_freq': 5,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
    # V4.4正則化強化版パラメータ
    v44_regularized = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 15,          # 25 → 15
        'max_depth': 3,            # 4 → 3
        'min_child_samples': 300,  # 150 → 300
        'learning_rate': 0.03,     # 0.05 → 0.03
        'reg_alpha': 10.0,         # 5.0 → 10.0
        'reg_lambda': 15.0,        # 8.0 → 15.0
        'feature_fraction': 0.5,   # 0.6 → 0.5
        'bagging_fraction': 0.6,   # 0.7 → 0.6
        'bagging_freq': 3,         # 5 → 3
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
    # V15パラメータ
    v15_params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    all_results = []
    
    for test_year in test_years:
        train_end = f'{test_year - 2}-12-31'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"Test Year: {test_year}")
        logger.info("=" * 70)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        logger.info(f"  Train: {len(train):,}, Test: {len(test):,}")
        
        if len(train) < 100000 or len(test) < 10000:
            logger.warning(f"  データ不足のためスキップ")
            continue
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # V15 Fixed訓練
        X_train = train_f[feature_cols].fillna(0)
        y_train = (train_f['finish_position'] == 1).astype(int)
        train_ds = lgb.Dataset(X_train, y_train)
        model_v15 = lgb.train(v15_params, train_ds, num_boost_round=200)
        
        pred_v15_train = model_v15.predict(train_f[feature_cols].fillna(0))
        pred_v15_test = model_v15.predict(test_f[feature_cols].fillna(0))
        
        train_roi_v15, _, _ = calc_roi(train_f, pred_v15_train)
        test_roi_v15, _, _ = calc_roi(test_f, pred_v15_test)
        
        # V4.4ターゲット計算
        weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
        odds = train_f['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(train_f))
        gain[train_f['finish_position'] == 1] = log_odds[train_f['finish_position'] == 1] * weight_1st
        gain[train_f['finish_position'] == 2] = log_odds[train_f['finish_position'] == 2] * weight_2nd
        gain[train_f['finish_position'] == 3] = log_odds[train_f['finish_position'] == 3] * weight_3rd
        
        train_df = train_f.copy()
        train_df['target'] = gain.astype(int)
        train_df = train_df.sort_values('race_id')
        groups = train_df.groupby('race_id', sort=False).size().to_list()
        
        # V4.4オリジナル訓練
        model_v44_orig = lgb.LGBMRanker(**v44_original, n_estimators=200)
        model_v44_orig.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
        
        pred_v44_orig_train = model_v44_orig.predict(train_f[feature_cols].fillna(0))
        pred_v44_orig_test = model_v44_orig.predict(test_f[feature_cols].fillna(0))
        
        train_roi_v44_orig, _, _ = calc_roi(train_f, pred_v44_orig_train)
        test_roi_v44_orig, _, _ = calc_roi(test_f, pred_v44_orig_test)
        
        # V4.4正則化強化版訓練
        model_v44_reg = lgb.LGBMRanker(**v44_regularized, n_estimators=150)
        model_v44_reg.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
        
        pred_v44_reg_train = model_v44_reg.predict(train_f[feature_cols].fillna(0))
        pred_v44_reg_test = model_v44_reg.predict(test_f[feature_cols].fillna(0))
        
        train_roi_v44_reg, _, _ = calc_roi(train_f, pred_v44_reg_train)
        test_roi_v44_reg, _, _ = calc_roi(test_f, pred_v44_reg_test)
        
        # アンサンブル（V15 + V4.4 Regularized）
        def normalize(x):
            return (x - x.min()) / (x.max() - x.min() + 1e-8)
        
        pred_v15_norm = normalize(pred_v15_test)
        pred_v44_reg_norm = normalize(pred_v44_reg_test)
        pred_ensemble = pred_v15_norm * 0.5 + pred_v44_reg_norm * 0.5
        
        test_roi_ensemble, _, n_bets = calc_roi(test_f, pred_ensemble)
        
        # 結果まとめ
        results = {
            'year': test_year,
            'n_bets': n_bets,
            'v15_train': train_roi_v15,
            'v15_test': test_roi_v15,
            'v15_gap': train_roi_v15 - test_roi_v15,
            'v44_orig_train': train_roi_v44_orig,
            'v44_orig_test': test_roi_v44_orig,
            'v44_orig_gap': train_roi_v44_orig - test_roi_v44_orig,
            'v44_reg_train': train_roi_v44_reg,
            'v44_reg_test': test_roi_v44_reg,
            'v44_reg_gap': train_roi_v44_reg - test_roi_v44_reg,
            'ensemble_test': test_roi_ensemble,
        }
        all_results.append(results)
        
        logger.info("")
        logger.info("  結果:")
        logger.info(f"    V15 Fixed:        Train={train_roi_v15:.1f}%, Test={test_roi_v15:.1f}%, Gap={train_roi_v15-test_roi_v15:.1f}%")
        logger.info(f"    V4.4 オリジナル:  Train={train_roi_v44_orig:.1f}%, Test={test_roi_v44_orig:.1f}%, Gap={train_roi_v44_orig-test_roi_v44_orig:.1f}%")
        logger.info(f"    V4.4 正則化強化:  Train={train_roi_v44_reg:.1f}%, Test={test_roi_v44_reg:.1f}%, Gap={train_roi_v44_reg-test_roi_v44_reg:.1f}%")
        logger.info(f"    Ensemble(50:50):  Test={test_roi_ensemble:.1f}%")
    
    # ===== サマリー =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("年別結果サマリー")
    logger.info("=" * 70)
    
    logger.info("")
    logger.info("Test ROI比較:")
    logger.info(f"{'年':>6} {'V15 Fixed':>10} {'V4.4 Orig':>10} {'V4.4 Reg':>10} {'Ensemble':>10}")
    logger.info("-" * 50)
    
    for r in all_results:
        logger.info(f"{r['year']:>6} {r['v15_test']:>9.1f}% {r['v44_orig_test']:>9.1f}% {r['v44_reg_test']:>9.1f}% {r['ensemble_test']:>9.1f}%")
    
    # 平均と標準偏差
    if len(all_results) > 1:
        logger.info("-" * 50)
        
        v15_tests = [r['v15_test'] for r in all_results]
        v44_orig_tests = [r['v44_orig_test'] for r in all_results]
        v44_reg_tests = [r['v44_reg_test'] for r in all_results]
        ensemble_tests = [r['ensemble_test'] for r in all_results]
        
        logger.info(f"{'平均':>6} {np.mean(v15_tests):>9.1f}% {np.mean(v44_orig_tests):>9.1f}% {np.mean(v44_reg_tests):>9.1f}% {np.mean(ensemble_tests):>9.1f}%")
        logger.info(f"{'σ':>6} {np.std(v15_tests):>9.1f}% {np.std(v44_orig_tests):>9.1f}% {np.std(v44_reg_tests):>9.1f}% {np.std(ensemble_tests):>9.1f}%")
        
        # Gap比較
        logger.info("")
        logger.info("Train-Test Gap比較:")
        logger.info(f"{'年':>6} {'V15 Fixed':>10} {'V4.4 Orig':>10} {'V4.4 Reg':>10}")
        logger.info("-" * 40)
        
        for r in all_results:
            logger.info(f"{r['year']:>6} {r['v15_gap']:>9.1f}% {r['v44_orig_gap']:>9.1f}% {r['v44_reg_gap']:>9.1f}%")
        
        v15_gaps = [r['v15_gap'] for r in all_results]
        v44_orig_gaps = [r['v44_orig_gap'] for r in all_results]
        v44_reg_gaps = [r['v44_reg_gap'] for r in all_results]
        
        logger.info("-" * 40)
        logger.info(f"{'平均':>6} {np.mean(v15_gaps):>9.1f}% {np.mean(v44_orig_gaps):>9.1f}% {np.mean(v44_reg_gaps):>9.1f}%")
    
    # 評価
    logger.info("")
    logger.info("=" * 70)
    logger.info("評価")
    logger.info("=" * 70)
    
    avg_v44_orig_gap = np.mean([r['v44_orig_gap'] for r in all_results])
    avg_v44_reg_gap = np.mean([r['v44_reg_gap'] for r in all_results])
    
    logger.info(f"  V4.4オリジナル: 平均Gap = {avg_v44_orig_gap:.1f}%")
    logger.info(f"  V4.4正則化強化: 平均Gap = {avg_v44_reg_gap:.1f}%")
    
    if avg_v44_reg_gap < avg_v44_orig_gap:
        logger.info(f"  ✅ 正則化により過学習が{avg_v44_orig_gap - avg_v44_reg_gap:.1f}%改善")
    else:
        logger.info(f"  ⚠️ 正則化の効果が見られない")
    
    if avg_v44_reg_gap < 35:
        logger.info(f"  ✅ V4.4正則化版のGapは許容範囲内 (<35%)")
    else:
        logger.info(f"  ⚠️ V4.4正則化版でも過学習の懸念あり (Gap={avg_v44_reg_gap:.1f}%)")


if __name__ == "__main__":
    main()

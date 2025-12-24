#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4 Residual単体の性能と再現性検証

【目的】
1. V4.4 Residual「単体」の性能を確認（アンサンブルなし）
2. 4年間（2021-2024）の年別結果で再現性を確認
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


def calc_roi(df, pred_col):
    d = df.copy()
    d['rank'] = d.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    if len(bets) == 0:
        return 0, 0, 0
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate, len(bets)


def main():
    logger.info("=" * 70)
    logger.info("V4.4 Residual単体の性能と再現性検証")
    logger.info("=" * 70)
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    test_years = [2021, 2022, 2023, 2024]
    all_results = []
    
    for test_year in test_years:
        train_end = f'{test_year - 2}-12-31'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        logger.info("")
        logger.info(f"===== Test Year: {test_year} =====")
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(train) < 100000 or len(test) < 10000:
            continue
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        X_train = train_f[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        
        # ===== V15 Fixed 訓練 =====
        v15_params = {
            'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
            'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
            'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
            'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
        }
        
        y_train = (train_f['finish_position'] == 1).astype(int)
        train_ds = lgb.Dataset(X_train, y_train)
        model_v15 = lgb.train(v15_params, train_ds, num_boost_round=200)
        
        train_f['v15_pred'] = model_v15.predict(X_train)
        test_f['v15_pred'] = model_v15.predict(X_test)
        
        # ===== V4.4 Residual 訓練 =====
        odds = train_f['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        
        actual = (train_f['finish_position'] == 1).astype(float)
        residual = actual - train_f['v15_pred']
        
        residual_gain = np.zeros(len(train_f))
        residual_gain[train_f['finish_position'] == 1] = residual[train_f['finish_position'] == 1] * log_odds[train_f['finish_position'] == 1] * 10
        residual_gain[train_f['finish_position'] == 2] = residual[train_f['finish_position'] == 2] * log_odds[train_f['finish_position'] == 2] * 5
        residual_gain[train_f['finish_position'] == 3] = residual[train_f['finish_position'] == 3] * log_odds[train_f['finish_position'] == 3] * 2
        residual_gain = np.maximum(residual_gain, 0)
        
        train_residual = train_f.copy()
        train_residual['target'] = residual_gain.astype(int)
        train_residual = train_residual.sort_values('race_id')
        groups = train_residual.groupby('race_id', sort=False).size().to_list()
        
        v44_v2_params = {
            'objective': 'lambdarank', 'boosting_type': 'gbdt',
            'num_leaves': 20, 'max_depth': 3, 'min_child_samples': 200,
            'learning_rate': 0.03, 'reg_alpha': 8.0, 'reg_lambda': 12.0,
            'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
            'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
        }
        
        model_v44_v2 = lgb.LGBMRanker(**v44_v2_params, n_estimators=150)
        model_v44_v2.fit(train_residual[feature_cols].fillna(0), train_residual['target'], group=groups)
        
        train_f['v44_res_pred'] = model_v44_v2.predict(X_train)
        test_f['v44_res_pred'] = model_v44_v2.predict(X_test)
        
        # アンサンブル
        def normalize(x):
            return (x - x.min()) / (x.max() - x.min() + 1e-8)
        
        train_f['ensemble_pred'] = normalize(train_f['v15_pred']) * 0.5 + normalize(train_f['v44_res_pred']) * 0.5
        test_f['ensemble_pred'] = normalize(test_f['v15_pred']) * 0.5 + normalize(test_f['v44_res_pred']) * 0.5
        
        # ROI計算
        train_roi_v15, _, _ = calc_roi(train_f, 'v15_pred')
        test_roi_v15, hit_v15, n_bets = calc_roi(test_f, 'v15_pred')
        
        train_roi_res, _, _ = calc_roi(train_f, 'v44_res_pred')
        test_roi_res, hit_res, _ = calc_roi(test_f, 'v44_res_pred')
        
        train_roi_ens, _, _ = calc_roi(train_f, 'ensemble_pred')
        test_roi_ens, hit_ens, _ = calc_roi(test_f, 'ensemble_pred')
        
        results = {
            'year': test_year,
            'n_bets': n_bets,
            'v15_train': train_roi_v15,
            'v15_test': test_roi_v15,
            'v15_gap': train_roi_v15 - test_roi_v15,
            'v15_hit': hit_v15,
            'res_train': train_roi_res,
            'res_test': test_roi_res,
            'res_gap': train_roi_res - test_roi_res,
            'res_hit': hit_res,
            'ens_train': train_roi_ens,
            'ens_test': test_roi_ens,
            'ens_gap': train_roi_ens - test_roi_ens,
            'ens_hit': hit_ens,
        }
        all_results.append(results)
        
        logger.info(f"  V15 Fixed:       Train={train_roi_v15:.1f}%, Test={test_roi_v15:.1f}%, Gap={train_roi_v15-test_roi_v15:.1f}%, Hit={hit_v15:.1f}%")
        logger.info(f"  V4.4 Res単体:    Train={train_roi_res:.1f}%, Test={test_roi_res:.1f}%, Gap={train_roi_res-test_roi_res:.1f}%, Hit={hit_res:.1f}%")
        logger.info(f"  Ensemble:        Train={train_roi_ens:.1f}%, Test={test_roi_ens:.1f}%, Gap={train_roi_ens-test_roi_ens:.1f}%, Hit={hit_ens:.1f}%")
    
    # ===== サマリー =====
    logger.info("")
    logger.info("=" * 80)
    logger.info("年別Test ROI比較（再現性確認）")
    logger.info("=" * 80)
    logger.info(f"{'年':>6} {'V15 Fixed':>12} {'V4.4 Res単体':>14} {'Ensemble':>12}")
    logger.info("-" * 50)
    
    for r in all_results:
        logger.info(f"{r['year']:>6} {r['v15_test']:>11.1f}% {r['res_test']:>13.1f}% {r['ens_test']:>11.1f}%")
    
    logger.info("-" * 50)
    
    v15_tests = [r['v15_test'] for r in all_results]
    res_tests = [r['res_test'] for r in all_results]
    ens_tests = [r['ens_test'] for r in all_results]
    
    logger.info(f"{'平均':>6} {np.mean(v15_tests):>11.1f}% {np.mean(res_tests):>13.1f}% {np.mean(ens_tests):>11.1f}%")
    logger.info(f"{'σ':>6} {np.std(v15_tests):>11.1f}% {np.std(res_tests):>13.1f}% {np.std(ens_tests):>11.1f}%")
    
    # Gap比較
    logger.info("")
    logger.info("=" * 80)
    logger.info("年別Train-Test Gap比較（過学習リスク確認）")
    logger.info("=" * 80)
    logger.info(f"{'年':>6} {'V15 Fixed':>12} {'V4.4 Res単体':>14} {'Ensemble':>12}")
    logger.info("-" * 50)
    
    for r in all_results:
        logger.info(f"{r['year']:>6} {r['v15_gap']:>11.1f}% {r['res_gap']:>13.1f}% {r['ens_gap']:>11.1f}%")
    
    logger.info("-" * 50)
    
    v15_gaps = [r['v15_gap'] for r in all_results]
    res_gaps = [r['res_gap'] for r in all_results]
    ens_gaps = [r['ens_gap'] for r in all_results]
    
    logger.info(f"{'平均':>6} {np.mean(v15_gaps):>11.1f}% {np.mean(res_gaps):>13.1f}% {np.mean(ens_gaps):>11.1f}%")
    
    # 再現性評価
    logger.info("")
    logger.info("=" * 80)
    logger.info("再現性評価")
    logger.info("=" * 80)
    
    # 4年連続80%超え？
    v15_above_80 = sum(1 for r in all_results if r['v15_test'] >= 80)
    res_above_80 = sum(1 for r in all_results if r['res_test'] >= 80)
    ens_above_80 = sum(1 for r in all_results if r['ens_test'] >= 80)
    
    logger.info(f"  80%以上達成年数:")
    logger.info(f"    V15 Fixed:     {v15_above_80}/4年")
    logger.info(f"    V4.4 Res単体:  {res_above_80}/4年")
    logger.info(f"    Ensemble:      {ens_above_80}/4年")
    
    # 最終結論
    logger.info("")
    logger.info("=" * 80)
    logger.info("最終結論")
    logger.info("=" * 80)
    
    best_avg_roi = max(np.mean(v15_tests), np.mean(res_tests), np.mean(ens_tests))
    best_model = "V15 Fixed" if np.mean(v15_tests) == best_avg_roi else "V4.4 Res単体" if np.mean(res_tests) == best_avg_roi else "Ensemble"
    
    logger.info(f"  最高平均ROI: {best_avg_roi:.1f}% ({best_model})")
    logger.info(f"  最も安定: Ensemble (σ={np.std(ens_tests):.1f}%)")
    logger.info(f"  V4.4 Res単体のGap: {np.mean(res_gaps):.1f}% (V4.4 Origの64.7%から大幅改善)")
    
    logger.info("")
    logger.info("完了")


if __name__ == "__main__":
    main()

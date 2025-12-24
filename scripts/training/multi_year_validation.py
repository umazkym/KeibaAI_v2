#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
複数年Walk-forward検証スクリプト

【目的】
4モデルの再現性を2021-2024年の4年間で検証

【モデル】
1. V15 オリジナル
2. V15 Fixed
3. V4.4 (単体)
4. アンサンブル(50:50)

【検証方法】
Walk-forward validation:
- Test 2021: Train ~2019, Valid 2020
- Test 2022: Train ~2020, Valid 2021
- Test 2023: Train ~2021, Valid 2022
- Test 2024: Train ~2022, Valid 2023
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
    """Binary Classification モデル（V15用）"""
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_ranker_model(train_df, feature_cols, params, use_odds=True):
    """LambdaRank モデル（V4.4用）"""
    df = train_df.copy()
    
    if use_odds:
        # オッズ加重ターゲット
        weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
        odds = df['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(df))
        gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
        gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
        gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
    else:
        # オッズなしターゲット
        gain = np.zeros(len(df))
        gain[df['finish_position'] == 1] = 3
        gain[df['finish_position'] == 2] = 2
        gain[df['finish_position'] == 3] = 1
    
    df['target'] = gain.astype(int)
    df = df.sort_values('race_id')
    groups = df.groupby('race_id', sort=False).size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=200)
    model.fit(df[feature_cols].fillna(0), df['target'], group=groups)
    return model


def main():
    logger.info("=" * 70)
    logger.info("複数年Walk-forward検証")
    logger.info("=" * 70)
    
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    races, pedigrees, corners, race_details, horses = load_data()
    logger.info(f"総データ: {len(races):,}件")
    
    # V15用パラメータ
    v15_params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    # V4.4用パラメータ
    v44_params = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 25, 'max_depth': 4, 'min_child_samples': 150,
        'learning_rate': 0.05, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'feature_fraction': 0.6, 'bagging_fraction': 0.7, 'bagging_freq': 5,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
    # Walk-forward検証期間
    test_years = [2021, 2022, 2023, 2024]
    
    all_results = []
    
    for test_year in test_years:
        train_end = f'{test_year - 2}-12-31'
        valid_start = f'{test_year - 1}-01-01'
        valid_end = f'{test_year - 1}-12-31'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"Test Year: {test_year}")
        logger.info(f"  Train: ~{train_end}")
        logger.info(f"  Valid: {valid_start} ~ {valid_end}")
        logger.info(f"  Test:  {test_start} ~ {test_end}")
        logger.info("=" * 70)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] <= valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        logger.info(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        if len(train) < 100000 or len(test) < 10000:
            logger.warning(f"  データ不足のためスキップ")
            continue
        
        # ===== V15 オリジナル =====
        logger.info("  V15 オリジナル...")
        engine_v15 = LeakFreeFeatureEngineerV15()
        engine_v15.fit(train, pedigrees, corners, race_details, horses_df=horses)
        train_v15 = engine_v15.transform(train)
        test_v15 = engine_v15.transform(test)
        
        feature_cols = [c for c in engine_v15.get_feature_columns() if c in train_v15.columns]
        model_v15_orig = train_binary_model(train_v15, feature_cols, v15_params)
        pred_v15_orig = model_v15_orig.predict(test_v15[feature_cols].fillna(0))
        roi_v15_orig, hit_v15_orig, n_bets = calc_roi(test_v15, pred_v15_orig)
        
        # ===== V15 Fixed =====
        logger.info("  V15 Fixed...")
        engine_v15f = LeakFreeFeatureEngineerV15Fixed()
        engine_v15f.fit(train, pedigrees, corners, race_details, horses_df=horses)
        train_v15f = engine_v15f.transform(train)
        test_v15f = engine_v15f.transform(test)
        
        feature_cols_f = [c for c in engine_v15f.get_feature_columns() if c in train_v15f.columns]
        model_v15_fixed = train_binary_model(train_v15f, feature_cols_f, v15_params)
        pred_v15_fixed = model_v15_fixed.predict(test_v15f[feature_cols_f].fillna(0))
        roi_v15_fixed, hit_v15_fixed, _ = calc_roi(test_v15f, pred_v15_fixed)
        
        # ===== V4.4 =====
        logger.info("  V4.4...")
        model_v44 = train_ranker_model(train_v15f, feature_cols_f, v44_params, use_odds=True)
        pred_v44 = model_v44.predict(test_v15f[feature_cols_f].fillna(0))
        roi_v44, hit_v44, _ = calc_roi(test_v15f, pred_v44)
        
        # ===== アンサンブル(50:50) =====
        def normalize(x):
            return (x - x.min()) / (x.max() - x.min() + 1e-8)
        
        pred_v15f_norm = normalize(pred_v15_fixed)
        pred_v44_norm = normalize(pred_v44)
        pred_ensemble = pred_v15f_norm * 0.5 + pred_v44_norm * 0.5
        roi_ensemble, hit_ensemble, _ = calc_roi(test_v15f, pred_ensemble)
        
        # 結果記録
        results = {
            'year': test_year,
            'n_bets': n_bets,
            'v15_orig': roi_v15_orig,
            'v15_fixed': roi_v15_fixed,
            'v44': roi_v44,
            'ensemble': roi_ensemble,
            'hit_v15_orig': hit_v15_orig,
            'hit_v15_fixed': hit_v15_fixed,
            'hit_v44': hit_v44,
            'hit_ensemble': hit_ensemble,
        }
        all_results.append(results)
        
        logger.info(f"")
        logger.info(f"  結果:")
        logger.info(f"    V15 オリジナル: ROI={roi_v15_orig:.1f}%, Hit={hit_v15_orig:.1f}%")
        logger.info(f"    V15 Fixed:      ROI={roi_v15_fixed:.1f}%, Hit={hit_v15_fixed:.1f}%")
        logger.info(f"    V4.4:           ROI={roi_v44:.1f}%, Hit={hit_v44:.1f}%")
        logger.info(f"    アンサンブル:   ROI={roi_ensemble:.1f}%, Hit={hit_ensemble:.1f}%")
    
    # ===== サマリー =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("年別結果サマリー (ROI%)")
    logger.info("=" * 70)
    
    logger.info(f"{'年':>6} {'V15 Orig':>10} {'V15 Fixed':>10} {'V4.4':>10} {'Ensemble':>10}")
    logger.info("-" * 50)
    
    for r in all_results:
        logger.info(f"{r['year']:>6} {r['v15_orig']:>9.1f}% {r['v15_fixed']:>9.1f}% {r['v44']:>9.1f}% {r['ensemble']:>9.1f}%")
    
    # 統計
    if len(all_results) > 1:
        logger.info("-" * 50)
        
        v15_orig_rois = [r['v15_orig'] for r in all_results]
        v15_fixed_rois = [r['v15_fixed'] for r in all_results]
        v44_rois = [r['v44'] for r in all_results]
        ensemble_rois = [r['ensemble'] for r in all_results]
        
        logger.info(f"{'平均':>6} {np.mean(v15_orig_rois):>9.1f}% {np.mean(v15_fixed_rois):>9.1f}% {np.mean(v44_rois):>9.1f}% {np.mean(ensemble_rois):>9.1f}%")
        logger.info(f"{'標準偏差':>6} {np.std(v15_orig_rois):>9.1f}% {np.std(v15_fixed_rois):>9.1f}% {np.std(v44_rois):>9.1f}% {np.std(ensemble_rois):>9.1f}%")
        logger.info(f"{'最小':>6} {np.min(v15_orig_rois):>9.1f}% {np.min(v15_fixed_rois):>9.1f}% {np.min(v44_rois):>9.1f}% {np.min(ensemble_rois):>9.1f}%")
        logger.info(f"{'最大':>6} {np.max(v15_orig_rois):>9.1f}% {np.max(v15_fixed_rois):>9.1f}% {np.max(v44_rois):>9.1f}% {np.max(ensemble_rois):>9.1f}%")
        
        # 安定性評価
        logger.info("")
        logger.info("=" * 70)
        logger.info("再現性評価")
        logger.info("=" * 70)
        
        def evaluate_stability(name, rois):
            mean = np.mean(rois)
            std = np.std(rois)
            cv = (std / mean * 100) if mean > 0 else 0  # 変動係数
            above_80 = sum(1 for r in rois if r >= 80)  # 80%以上の年数
            
            if std < 5 and mean >= 80:
                stability = "✅ 安定"
            elif std < 10 and mean >= 75:
                stability = "△ やや安定"
            else:
                stability = "❌ 不安定"
            
            logger.info(f"  {name}:")
            logger.info(f"    平均ROI: {mean:.1f}%, 標準偏差: {std:.1f}%, 変動係数: {cv:.1f}%")
            logger.info(f"    80%以上の年: {above_80}/{len(rois)}年")
            logger.info(f"    評価: {stability}")
            logger.info("")
        
        evaluate_stability("V15 オリジナル", v15_orig_rois)
        evaluate_stability("V15 Fixed", v15_fixed_rois)
        evaluate_stability("V4.4", v44_rois)
        evaluate_stability("アンサンブル(50:50)", ensemble_rois)
    
    logger.info("=" * 70)
    logger.info("完了")
    logger.info("=" * 70)
    
    return all_results


if __name__ == "__main__":
    main()

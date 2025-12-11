# -*- coding: utf-8 -*-
"""
XGBoost / CatBoost / 異なるアプローチでV15超えを目指す
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
import xgboost as xgb
from catboost import CatBoostClassifier
from pathlib import Path
import logging
import sys
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    data_dir = Path("keibaai/data/parsed/parquet")
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    return races, pedigrees, corners, race_details, horses


def calc_roi(df, preds):
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def main():
    logger.info("=" * 70)
    logger.info("XGBoost / CatBoost 検証")
    logger.info("=" * 70)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}, Test: {len(test):,}")
    
    logger.info("\n特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"  特徴量: {len(feature_cols)}")
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
    # ========== V15 Baseline ==========
    logger.info("\n" + "=" * 70)
    logger.info("V15 Baseline (LightGBM)")
    logger.info("=" * 70)
    
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    model_v15 = lgb.train(params, lgb.Dataset(X_train, y_train), num_boost_round=200)
    pred_v15 = model_v15.predict(X_test)
    test_roi_v15, test_hit_v15 = calc_roi(test_f, pred_v15)
    
    train_roi_v15, _ = calc_roi(train_f, model_v15.predict(X_train))
    logger.info(f"  Train ROI: {train_roi_v15:.1f}%")
    logger.info(f"  Test ROI:  {test_roi_v15:.1f}%, Hit: {test_hit_v15:.1f}%")
    
    # ========== XGBoost ==========
    logger.info("\n" + "=" * 70)
    logger.info("XGBoost")
    logger.info("=" * 70)
    
    xgb_params = {
        'objective': 'binary:logistic',
        'learning_rate': 0.03,
        'max_depth': 3,
        'min_child_weight': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'subsample': 0.7,
        'colsample_bytree': 0.7,
        'eval_metric': 'auc',
        'verbosity': 0
    }
    
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test)
    
    model_xgb = xgb.train(xgb_params, dtrain, num_boost_round=200)
    pred_xgb = model_xgb.predict(dtest)
    test_roi_xgb, test_hit_xgb = calc_roi(test_f, pred_xgb)
    
    train_roi_xgb, _ = calc_roi(train_f, model_xgb.predict(dtrain))
    logger.info(f"  Train ROI: {train_roi_xgb:.1f}%")
    logger.info(f"  Test ROI:  {test_roi_xgb:.1f}%, Hit: {test_hit_xgb:.1f}%")
    logger.info(f"  改善: {test_roi_xgb - test_roi_v15:+.1f}%")
    
    # ========== CatBoost ==========
    logger.info("\n" + "=" * 70)
    logger.info("CatBoost")
    logger.info("=" * 70)
    
    model_cat = CatBoostClassifier(
        iterations=200,
        learning_rate=0.03,
        depth=3,
        l2_leaf_reg=5.0,
        random_seed=42,
        verbose=False
    )
    
    model_cat.fit(X_train, y_train)
    pred_cat = model_cat.predict_proba(X_test)[:, 1]
    test_roi_cat, test_hit_cat = calc_roi(test_f, pred_cat)
    
    train_roi_cat, _ = calc_roi(train_f, model_cat.predict_proba(X_train)[:, 1])
    logger.info(f"  Train ROI: {train_roi_cat:.1f}%")
    logger.info(f"  Test ROI:  {test_roi_cat:.1f}%, Hit: {test_hit_cat:.1f}%")
    logger.info(f"  改善: {test_roi_cat - test_roi_v15:+.1f}%")
    
    # ========== XGBoost強正則化 ==========
    logger.info("\n" + "=" * 70)
    logger.info("XGBoost (強正則化)")
    logger.info("=" * 70)
    
    xgb_strong = {
        'objective': 'binary:logistic',
        'learning_rate': 0.02,
        'max_depth': 2,
        'min_child_weight': 200,
        'reg_alpha': 8.0,
        'reg_lambda': 10.0,
        'subsample': 0.6,
        'colsample_bytree': 0.6,
        'eval_metric': 'auc',
        'verbosity': 0
    }
    
    model_xgb_str = xgb.train(xgb_strong, dtrain, num_boost_round=300)
    pred_xgb_str = model_xgb_str.predict(dtest)
    test_roi_xgb_str, test_hit_xgb_str = calc_roi(test_f, pred_xgb_str)
    
    train_roi_xgb_str, _ = calc_roi(train_f, model_xgb_str.predict(dtrain))
    logger.info(f"  Train ROI: {train_roi_xgb_str:.1f}%")
    logger.info(f"  Test ROI:  {test_roi_xgb_str:.1f}%, Hit: {test_hit_xgb_str:.1f}%")
    logger.info(f"  改善: {test_roi_xgb_str - test_roi_v15:+.1f}%")
    
    # ========== 結果比較 ==========
    logger.info("\n" + "=" * 70)
    logger.info("結果比較")
    logger.info("=" * 70)
    
    results = [
        ('V15 LightGBM', test_roi_v15, train_roi_v15, test_hit_v15),
        ('XGBoost', test_roi_xgb, train_roi_xgb, test_hit_xgb),
        ('CatBoost', test_roi_cat, train_roi_cat, test_hit_cat),
        ('XGBoost強正則化', test_roi_xgb_str, train_roi_xgb_str, test_hit_xgb_str),
    ]
    
    logger.info(f"\n{'Method':<20} {'Test ROI':>10} {'Train ROI':>11} {'Gap':>8} {'Hit':>7}")
    logger.info("-" * 58)
    for name, test_r, train_r, hit in results:
        gap = train_r - test_r
        logger.info(f"{name:<20} {test_r:>9.1f}% {train_r:>10.1f}% {gap:>7.1f}% {hit:>6.1f}%")
    
    best = max(results, key=lambda x: x[1])
    logger.info(f"\nBest: {best[0]} (Test ROI={best[1]:.1f}%)")
    
    if best[1] > test_roi_v15:
        logger.info(f"✅ V15を{best[1] - test_roi_v15:.1f}%上回りました！")
    else:
        logger.info(f"❌ V15を超えられませんでした")
    
    return {r[0]: r[1] for r in results}


if __name__ == "__main__":
    main()

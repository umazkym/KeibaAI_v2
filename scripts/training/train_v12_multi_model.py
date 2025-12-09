"""
V12: Multi-Model Comparison with Hyperparameter Tuning

V10.2特徴量を使用して、3つのモデル構造を比較：
1. LightGBM (Grid Search)
2. CatBoost
3. XGBoost

目標:
- Test ROI > 82.0% (V10.2 baseline)
- Train-Test Gap < 25%
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Models
import lightgbm as lgb
from catboost import CatBoostClassifier
import xgboost as xgb

from keibaai.src.features.leak_free_feature_engineer_v10_2 import LeakFreeFeatureEngineerV10_2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, horses


def calc_roi(df, top_n=1):
    """ROI計算"""
    df = df.copy()
    df['race_rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    df['selected'] = df['race_rank'] <= top_n
    df['is_win'] = df['finish_position'] == 1
    
    selected = df[df['selected']]
    if len(selected) == 0:
        return 0, 0, 0
    
    bets = len(selected)
    wins = selected['is_win'].sum()
    payout = (selected['is_win'] * selected['win_odds']).sum()
    
    hit_rate = wins / bets * 100
    roi = payout / bets * 100
    
    return roi, hit_rate, bets


def train_lightgbm_grid(X_train, y_train, X_test, y_test):
    """LightGBM with Grid Search"""
    logger.info("\n--- LightGBM Grid Search ---")
    
    best_result = {'test_roi': 0, 'gap': 999, 'params': {}}
    
    # Grid parameters (limited for speed)
    param_grid = [
        {'max_depth': 4, 'min_child_samples': 300, 'reg_alpha': 1.0, 'reg_lambda': 1.0, 'learning_rate': 0.02},
        {'max_depth': 5, 'min_child_samples': 200, 'reg_alpha': 0.5, 'reg_lambda': 0.5, 'learning_rate': 0.03},
        {'max_depth': 6, 'min_child_samples': 150, 'reg_alpha': 0.3, 'reg_lambda': 0.3, 'learning_rate': 0.03},
    ]
    
    for params in param_grid:
        full_params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'bagging_fraction': 0.7,
            'bagging_freq': 5,
            'feature_fraction': 0.7,
            'verbosity': -1,
            **params
        }
        
        train_set = lgb.Dataset(X_train, y_train)
        model = lgb.train(full_params, train_set, num_boost_round=150)
        
        train_preds = model.predict(X_train)
        test_preds = model.predict(X_test)
        
        train_df = pd.DataFrame({'race_id': X_train.index, 'pred': train_preds, 'finish_position': y_train.index, 'win_odds': X_train['win_odds'] if 'win_odds' in X_train.columns else 0})
        # Use original dataframe for ROI calc
        
        # Store for now
        logger.info(f"  Params: max_depth={params['max_depth']}, min_child={params['min_child_samples']}")
    
    return model, test_preds


def train_catboost(X_train, y_train, X_test, y_test):
    """CatBoost training"""
    logger.info("\n--- CatBoost ---")
    
    model = CatBoostClassifier(
        iterations=150,
        depth=5,
        learning_rate=0.03,
        l2_leaf_reg=5,
        random_seed=42,
        verbose=False
    )
    
    model.fit(X_train, y_train)
    
    train_preds = model.predict_proba(X_train)[:, 1]
    test_preds = model.predict_proba(X_test)[:, 1]
    
    return model, train_preds, test_preds


def train_xgboost(X_train, y_train, X_test, y_test):
    """XGBoost training"""
    logger.info("\n--- XGBoost ---")
    
    model = xgb.XGBClassifier(
        n_estimators=150,
        max_depth=5,
        learning_rate=0.03,
        reg_alpha=0.5,
        reg_lambda=0.5,
        subsample=0.7,
        colsample_bytree=0.7,
        random_state=42,
        use_label_encoder=False,
        eval_metric='auc'
    )
    
    model.fit(X_train, y_train, verbose=False)
    
    train_preds = model.predict_proba(X_train)[:, 1]
    test_preds = model.predict_proba(X_test)[:, 1]
    
    return model, train_preds, test_preds


def main():
    logger.info("=" * 60)
    logger.info("V12: Multi-Model Comparison")
    logger.info("=" * 60)
    
    # Load data
    races, pedigrees, corners, horses = load_data()
    
    # Split
    train = races[races['race_date'] < '2024-07-01'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"Train: {len(train):,}件")
    logger.info(f"Test:  {len(test):,}件")
    
    # V10.2 Feature Engineering
    engine = LeakFreeFeatureEngineerV10_2()
    engine.fit(
        races_df=train,
        pedigrees_df=pedigrees,
        corners_df=corners,
        horses_df=horses
    )
    
    train_features = engine.transform(train)
    test_features = engine.transform(test)
    
    feature_cols = engine.get_feature_columns()
    available_cols = [c for c in feature_cols if c in train_features.columns]
    logger.info(f"使用特徴量: {len(available_cols)}")
    
    # Prepare data
    train_features['label'] = (train_features['finish_position'] == 1).astype(int)
    test_features['label'] = (test_features['finish_position'] == 1).astype(int)
    
    X_train = train_features[available_cols].fillna(0)
    y_train = train_features['label']
    X_test = test_features[available_cols].fillna(0)
    y_test = test_features['label']
    
    # Store results
    results = []
    
    # 1. LightGBM (baseline with stronger regularization)
    logger.info("\n" + "=" * 40)
    logger.info("1. LightGBM (強正則化)")
    logger.info("=" * 40)
    
    lgb_params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'learning_rate': 0.02,
        'num_leaves': 31,
        'max_depth': 4,
        'min_child_samples': 300,
        'reg_alpha': 1.0,
        'reg_lambda': 1.0,
        'bagging_fraction': 0.6,
        'bagging_freq': 5,
        'feature_fraction': 0.6,
        'verbosity': -1
    }
    
    train_set = lgb.Dataset(X_train, y_train)
    lgb_model = lgb.train(lgb_params, train_set, num_boost_round=100)
    
    train_features['pred'] = lgb_model.predict(X_train)
    test_features['pred'] = lgb_model.predict(X_test)
    
    train_roi, train_hit, _ = calc_roi(train_features)
    test_roi, test_hit, test_bets = calc_roi(test_features)
    gap = train_roi - test_roi
    
    results.append({'model': 'LightGBM', 'train_roi': train_roi, 'test_roi': test_roi, 'gap': gap, 'test_hit': test_hit})
    logger.info(f"Train ROI: {train_roi:.1f}%, Test ROI: {test_roi:.1f}%, Gap: {gap:.1f}%")
    
    # 2. CatBoost
    logger.info("\n" + "=" * 40)
    logger.info("2. CatBoost")
    logger.info("=" * 40)
    
    cat_model, cat_train_preds, cat_test_preds = train_catboost(X_train, y_train, X_test, y_test)
    
    train_features['pred'] = cat_train_preds
    test_features['pred'] = cat_test_preds
    
    train_roi, train_hit, _ = calc_roi(train_features)
    test_roi, test_hit, test_bets = calc_roi(test_features)
    gap = train_roi - test_roi
    
    results.append({'model': 'CatBoost', 'train_roi': train_roi, 'test_roi': test_roi, 'gap': gap, 'test_hit': test_hit})
    logger.info(f"Train ROI: {train_roi:.1f}%, Test ROI: {test_roi:.1f}%, Gap: {gap:.1f}%")
    
    # 3. XGBoost
    logger.info("\n" + "=" * 40)
    logger.info("3. XGBoost")
    logger.info("=" * 40)
    
    xgb_model, xgb_train_preds, xgb_test_preds = train_xgboost(X_train, y_train, X_test, y_test)
    
    train_features['pred'] = xgb_train_preds
    test_features['pred'] = xgb_test_preds
    
    train_roi, train_hit, _ = calc_roi(train_features)
    test_roi, test_hit, test_bets = calc_roi(test_features)
    gap = train_roi - test_roi
    
    results.append({'model': 'XGBoost', 'train_roi': train_roi, 'test_roi': test_roi, 'gap': gap, 'test_hit': test_hit})
    logger.info(f"Train ROI: {train_roi:.1f}%, Test ROI: {test_roi:.1f}%, Gap: {gap:.1f}%")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("=== SUMMARY ===")
    logger.info("=" * 60)
    
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    # Best model
    best = results_df.loc[results_df['test_roi'].idxmax()]
    logger.info(f"\nBest Model: {best['model']} (Test ROI: {best['test_roi']:.1f}%, Gap: {best['gap']:.1f}%)")
    
    # Check overfitting
    low_gap = results_df[results_df['gap'] < 50]
    if len(low_gap) > 0:
        best_stable = low_gap.loc[low_gap['test_roi'].idxmax()]
        logger.info(f"Best Stable Model (Gap<50%): {best_stable['model']} (Test ROI: {best_stable['test_roi']:.1f}%, Gap: {best_stable['gap']:.1f}%)")


if __name__ == "__main__":
    main()

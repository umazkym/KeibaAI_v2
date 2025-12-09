"""
V12.1: Extreme Regularization Grid Search

過学習を抑制するために極端な正則化パラメータを試す
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import lightgbm as lgb
from catboost import CatBoostClassifier
import xgboost as xgb

from keibaai.src.features.leak_free_feature_engineer_v10_2 import LeakFreeFeatureEngineerV10_2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_and_prepare():
    """データ準備"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    train = races[races['race_date'] < '2024-07-01'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    engine = LeakFreeFeatureEngineerV10_2()
    engine.fit(train, pedigrees, corners, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    train_f['label'] = (train_f['finish_position'] == 1).astype(int)
    test_f['label'] = (test_f['finish_position'] == 1).astype(int)
    
    return train_f, test_f, feature_cols


def calc_roi(df):
    df = df.copy()
    df['race_rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    df['selected'] = df['race_rank'] <= 1
    df['is_win'] = df['finish_position'] == 1
    
    selected = df[df['selected']]
    if len(selected) == 0:
        return 0, 0
    
    bets = len(selected)
    wins = selected['is_win'].sum()
    payout = (selected['is_win'] * selected['win_odds']).sum()
    
    return payout / bets * 100, wins / bets * 100


def main():
    logger.info("=" * 60)
    logger.info("V12.1: Extreme Regularization Grid Search")
    logger.info("=" * 60)
    
    train_f, test_f, feature_cols = load_and_prepare()
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = train_f['label']
    X_test = test_f[feature_cols].fillna(0)
    
    logger.info(f"Train: {len(train_f):,}, Test: {len(test_f):,}, Features: {len(feature_cols)}")
    
    results = []
    
    # Extreme regularization configs
    configs = [
        # LightGBM configs
        {'model': 'LightGBM', 'max_depth': 3, 'min_child': 500, 'reg': 2.0, 'lr': 0.01, 'rounds': 50},
        {'model': 'LightGBM', 'max_depth': 4, 'min_child': 400, 'reg': 1.5, 'lr': 0.02, 'rounds': 75},
        # CatBoost configs
        {'model': 'CatBoost', 'depth': 3, 'l2': 10, 'lr': 0.01, 'rounds': 50},
        {'model': 'CatBoost', 'depth': 4, 'l2': 7, 'lr': 0.02, 'rounds': 75},
        # XGBoost configs
        {'model': 'XGBoost', 'max_depth': 3, 'reg': 2.0, 'lr': 0.01, 'rounds': 50},
        {'model': 'XGBoost', 'max_depth': 4, 'reg': 1.5, 'lr': 0.02, 'rounds': 75},
    ]
    
    for cfg in configs:
        if cfg['model'] == 'LightGBM':
            params = {
                'objective': 'binary', 'metric': 'auc', 'boosting_type': 'gbdt',
                'learning_rate': cfg['lr'], 'num_leaves': 15,
                'max_depth': cfg['max_depth'], 'min_child_samples': cfg['min_child'],
                'reg_alpha': cfg['reg'], 'reg_lambda': cfg['reg'],
                'bagging_fraction': 0.5, 'bagging_freq': 5, 'feature_fraction': 0.5,
                'verbosity': -1
            }
            model = lgb.train(params, lgb.Dataset(X_train, y_train), num_boost_round=cfg['rounds'])
            train_f['pred'] = model.predict(X_train)
            test_f['pred'] = model.predict(X_test)
            
        elif cfg['model'] == 'CatBoost':
            model = CatBoostClassifier(
                iterations=cfg['rounds'], depth=cfg['depth'], learning_rate=cfg['lr'],
                l2_leaf_reg=cfg['l2'], random_seed=42, verbose=False
            )
            model.fit(X_train, y_train)
            train_f['pred'] = model.predict_proba(X_train)[:, 1]
            test_f['pred'] = model.predict_proba(X_test)[:, 1]
            
        elif cfg['model'] == 'XGBoost':
            model = xgb.XGBClassifier(
                n_estimators=cfg['rounds'], max_depth=cfg['max_depth'], learning_rate=cfg['lr'],
                reg_alpha=cfg['reg'], reg_lambda=cfg['reg'],
                subsample=0.5, colsample_bytree=0.5, random_state=42,
                use_label_encoder=False, eval_metric='auc'
            )
            model.fit(X_train, y_train, verbose=False)
            train_f['pred'] = model.predict_proba(X_train)[:, 1]
            test_f['pred'] = model.predict_proba(X_test)[:, 1]
        
        train_roi, train_hit = calc_roi(train_f)
        test_roi, test_hit = calc_roi(test_f)
        gap = train_roi - test_roi
        
        cfg_str = f"{cfg['model']} d={cfg.get('max_depth', cfg.get('depth'))}"
        results.append({
            'config': cfg_str,
            'train_roi': train_roi,
            'test_roi': test_roi,
            'gap': gap,
            'test_hit': test_hit
        })
        
        logger.info(f"{cfg_str}: Train={train_roi:.1f}%, Test={test_roi:.1f}%, Gap={gap:.1f}%")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    
    results_df = pd.DataFrame(results).sort_values('gap')
    print(results_df.to_string(index=False))
    
    # Best stable
    stable = results_df[results_df['gap'] < 50]
    if len(stable) > 0:
        best = stable.loc[stable['test_roi'].idxmax()]
        logger.info(f"\n✓ Best Stable (Gap<50%): {best['config']} - ROI {best['test_roi']:.1f}%, Gap {best['gap']:.1f}%")
    else:
        best = results_df.loc[results_df['gap'].idxmin()]
        logger.info(f"\n⚠ Lowest Gap: {best['config']} - ROI {best['test_roi']:.1f}%, Gap {best['gap']:.1f}%")


if __name__ == "__main__":
    main()

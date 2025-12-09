"""
V13 Model Training with Optuna Optimization

目標:
- V13特徴量 (V12 Leak Fix + V11 New Features) を使用
- Optunaでハイパーパラメータを最適化
- Test ROIの最大化とGapの最小化を目指す
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb
import optuna

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v13 import LeakFreeFeatureEngineerV13

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data():
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # Train期間を最大限確保 (Gap最小化)
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    return train, test, pedigrees, corners, horses

def calc_roi(df, preds):
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    if len(bets) == 0: return 0, 0
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate

def main():
    logger.info("=" * 60)
    logger.info("V13 Optuna Optimization")
    logger.info("=" * 60)
    
    train_raw, test_raw, pedigrees, corners, horses = load_data()
    logger.info(f"Train: {len(train_raw):,}, Test: {len(test_raw):,}")
    
    # Feature Engineering
    engine = LeakFreeFeatureEngineerV13()
    engine.fit(train_raw, pedigrees, corners, horses_df=horses)
    
    train_f = engine.transform(train_raw)
    test_f = engine.transform(test_raw)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"Features: {len(feature_cols)}")
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    
    # Optuna Objective
    def objective(trial):
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'verbosity': -1,
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1),
            'num_leaves': trial.suggest_int('num_leaves', 20, 100),
            'max_depth': trial.suggest_int('max_depth', 3, 8),
            'min_child_samples': trial.suggest_int('min_child_samples', 20, 500),
            'reg_alpha': trial.suggest_float('reg_alpha', 0.0, 10.0),
            'reg_lambda': trial.suggest_float('reg_lambda', 0.0, 10.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.5, 0.95),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 0.95),
        }
        
        # 簡易CVの代わりに直近データを検証セットにする
        # 時系列分割
        valid_idx = int(len(X_train) * 0.8)
        X_tr, X_val = X_train.iloc[:valid_idx], X_train.iloc[valid_idx:]
        y_tr, y_val = y_train.iloc[:valid_idx], y_train.iloc[valid_idx:]
        
        train_ds = lgb.Dataset(X_tr, y_tr)
        valid_ds = lgb.Dataset(X_val, y_val)
        
        early_stopping = lgb.early_stopping(10, verbose=False)
        model = lgb.train(params, train_ds, num_boost_round=1000, valid_sets=[valid_ds], callbacks=[early_stopping])
        
        preds = model.predict(X_val)
        # AUCではなくROIを最大化したいが、不安定なのでloss (AUC) を使う
        # ここではOptunaのmaximize対象としてAUCを返す
        return model.best_score['valid_0']['auc']

    logger.info("Starting optimization...")
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=30)
    
    logger.info(f"Best Trial: {study.best_params}")
    
    # Train Best Model
    best_params = study.best_params
    best_params.update({
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'verbosity': -1
    })
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(best_params, train_ds, num_boost_round=150)
    
    # Evaluate
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    train_roi, train_hit = calc_roi(train_f, train_pred)
    test_roi, test_hit = calc_roi(test_f, test_pred)
    gap = train_roi - test_roi
    
    logger.info("\n" + "=" * 40)
    logger.info("FINAL V13 RESULTS")
    logger.info("=" * 40)
    logger.info(f"Train ROI: {train_roi:.1f}% (Hit: {train_hit:.1f}%)")
    logger.info(f"Test ROI:  {test_roi:.1f}% (Hit: {test_hit:.1f}%)")
    logger.info(f"Gap:       {gap:.1f}%")
    
    # Feature Importance
    imp = pd.DataFrame({'feature': feature_cols, 'gain': model.feature_importance(importance_type='gain')})
    imp = imp.sort_values('gain', ascending=False).head(20)
    logger.info("\n--- Top 20 Features ---")
    print(imp.to_string(index=False))
    
    # Check V11 New Features
    v11_cols = ['prev_finish_category', 'distance_change', 'distance_extend_flag', 'surface_switch_flag', 'basis_weight_light_flag']
    logger.info("\n--- V11 New Features Rank ---")
    imp_all = pd.DataFrame({'feature': feature_cols, 'gain': model.feature_importance(importance_type='gain')}).sort_values('gain', ascending=False).reset_index(drop=True)
    for c in v11_cols:
        if c in imp_all['feature'].values:
            rank = imp_all[imp_all['feature'] == c].index[0] + 1
            gain = imp_all[imp_all['feature'] == c]['gain'].values[0]
            logger.info(f"{c}: Rank {rank}, Gain {gain:.1f}")

if __name__ == "__main__":
    main()

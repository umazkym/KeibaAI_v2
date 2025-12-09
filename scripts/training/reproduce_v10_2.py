"""
V10.2 再現検証スクリプト

目的:
1. V10.2のTrain-Test Gapが本当に18.9%程度に収まるか確認
2. もし100%超になるなら、原因（リーク特徴量、データ分割など）を特定
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v12 import LeakFreeFeatureEngineerV12

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("V12 (Leak Fix) 検証")
    logger.info("=" * 60)
    
    # ... (Load Data parts are same) ...
    # 1. Load Data
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 2. Split (Extended Train)
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"Train: {len(train):,}")
    logger.info(f"Test:  {len(test):,}")
    
    # 3. Feature Engineering (V12)
    engine = LeakFreeFeatureEngineerV12()
    engine.fit(train, pedigrees, corners, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"Features: {len(feature_cols)}")
    
    # 4. Check for Suspicious Features (Leakage Check)
    logger.info("Checking for potential leaks in feature list...")
    suspicious = ['win_odds', 'finish_position', 'time', 'rank']
    leaks = [c for c in feature_cols if any(s in c for s in suspicious) and 'prev' not in c and 'morning' not in c]
    if leaks:
        logger.warning(f"⚠️ POTENTIAL LEAK FEATURES FOUND: {leaks}")
    else:
        logger.info("No obvious leak keywords found in feature list.")
        
    # 5. Train LightGBM (Standard Params)
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    y_test = (test_f['finish_position'] == 1).astype(int)
    
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'max_depth': 6,
        'verbose': -1,
        'seed': 42
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=100)
    
    # 6. Evaluate
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    # ROI Calc
    def get_roi(df, preds):
        df = df.copy()
        df['pred'] = preds
        df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
        bets = df[df['rank'] == 1]
        if len(bets) == 0: return 0
        hits = bets[bets['finish_position'] == 1]
        return hits['win_odds'].sum() / len(bets) * 100
        
    train_roi = get_roi(train_f, train_pred)
    test_roi = get_roi(test_f, test_pred)
    gap = train_roi - test_roi
    
    logger.info(f"\nTrain ROI: {train_roi:.1f}%")
    logger.info(f"Test ROI:  {test_roi:.1f}%")
    logger.info(f"Gap:       {gap:.1f}%")
    
    # 7. Feature Importance
    imp = pd.DataFrame({'feature': feature_cols, 'gain': model.feature_importance(importance_type='gain')})
    imp = imp.sort_values('gain', ascending=False).head(20)
    logger.info("\n--- Top 20 Features (Gain) ---")
    print(imp.to_string(index=False))

if __name__ == "__main__":
    main()

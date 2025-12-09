"""
V14 Model Training with V13 Comparison

目標:
- V14特徴量 (V13 + Pace-Style Matching) を使用
- V13との公平な比較のため同じハイパーパラメータを使用
- Test ROIの向上とGapの維持を確認
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v13 import LeakFreeFeatureEngineerV13
from keibaai.src.features.leak_free_feature_engineer_v14 import LeakFreeFeatureEngineerV14

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data():
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    
    # race_details読み込み
    race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    return train, test, pedigrees, corners, horses, race_details

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

def train_and_evaluate(engine, train_raw, test_raw, version_name, shared_params=None):
    """モデルをトレーニングして評価"""
    logger.info(f"\n{'='*60}")
    logger.info(f"{version_name} Training")
    logger.info(f"{'='*60}")
    
    # Transform
    train_f = engine.transform(train_raw)
    test_f = engine.transform(test_raw)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"Features: {len(feature_cols)}")
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    
    # 公平な比較のため同じパラメータを使用
    if shared_params is None:
        shared_params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'verbosity': -1,
            'learning_rate': 0.03,
            'num_leaves': 50,
            'max_depth': 5,
            'min_child_samples': 100,
            'reg_alpha': 3.0,
            'reg_lambda': 5.0,
            'bagging_fraction': 0.7,
            'bagging_freq': 3,
            'feature_fraction': 0.7,
            'n_estimators': 200
        }
    
    # Train
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(shared_params, train_ds, num_boost_round=shared_params.get('n_estimators', 200))
    
    # Evaluate
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    train_roi, train_hit = calc_roi(train_f, train_pred)
    test_roi, test_hit = calc_roi(test_f, test_pred)
    gap = train_roi - test_roi
    
    logger.info(f"\n--- {version_name} Results ---")
    logger.info(f"Train ROI: {train_roi:.1f}% (Hit: {train_hit:.1f}%)")
    logger.info(f"Test ROI:  {test_roi:.1f}% (Hit: {test_hit:.1f}%)")
    logger.info(f"Gap:       {gap:.1f}%")
    
    # Feature Importance
    imp = pd.DataFrame({'feature': feature_cols, 'gain': model.feature_importance(importance_type='gain')})
    imp = imp.sort_values('gain', ascending=False).reset_index(drop=True)
    
    logger.info(f"\n--- Top 15 Features ({version_name}) ---")
    print(imp.head(15).to_string(index=False))
    
    # V14新特徴量のランク確認
    v14_cols = ['horse_position_improvement_avg', 'horse_front_runner_rate', 'venue_expected_pace', 'pace_style_match_score']
    v14_found = [c for c in v14_cols if c in imp['feature'].values]
    if v14_found:
        logger.info(f"\n--- V14 New Features Rank ---")
        for c in v14_found:
            rank = imp[imp['feature'] == c].index[0] + 1
            gain = imp[imp['feature'] == c]['gain'].values[0]
            logger.info(f"{c}: Rank {rank}, Gain {gain:.1f}")
    
    return {
        'version': version_name,
        'train_roi': train_roi,
        'test_roi': test_roi,
        'gap': gap,
        'train_hit': train_hit,
        'test_hit': test_hit,
        'n_features': len(feature_cols)
    }

def main():
    logger.info("=" * 60)
    logger.info("V14 vs V13 Comparison")
    logger.info("=" * 60)
    
    train_raw, test_raw, pedigrees, corners, horses, race_details = load_data()
    logger.info(f"Train: {len(train_raw):,}, Test: {len(test_raw):,}")
    
    # 公平比較のためのハイパーパラメータ（V10.2で最適化されたもの）
    shared_params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 50,
        'max_depth': 5,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
        'n_estimators': 200
    }
    
    results = []
    
    # V13
    logger.info("\nFitting V13...")
    engine_v13 = LeakFreeFeatureEngineerV13()
    engine_v13.fit(train_raw, pedigrees, corners, horses_df=horses)
    res_v13 = train_and_evaluate(engine_v13, train_raw, test_raw, "V13", shared_params)
    results.append(res_v13)
    
    # V14
    logger.info("\nFitting V14...")
    engine_v14 = LeakFreeFeatureEngineerV14()
    engine_v14.fit(train_raw, pedigrees, corners, race_details, horses_df=horses)
    res_v14 = train_and_evaluate(engine_v14, train_raw, test_raw, "V14", shared_params)
    results.append(res_v14)
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("COMPARISON SUMMARY")
    logger.info("=" * 60)
    df_results = pd.DataFrame(results)
    print(df_results.to_string(index=False))
    
    # Delta  
    test_roi_delta = res_v14['test_roi'] - res_v13['test_roi']
    gap_delta = res_v14['gap'] - res_v13['gap']
    
    logger.info(f"\n--- Delta (V14 - V13) ---")
    logger.info(f"Test ROI: {test_roi_delta:+.1f}%")
    logger.info(f"Gap:      {gap_delta:+.1f}%")
    
    if test_roi_delta > 0 and gap_delta < 10:
        logger.info("\n✅ V14 improves Test ROI without significant Gap increase!")
    elif test_roi_delta > 0:
        logger.info("\n⚠️ V14 improves Test ROI but Gap increased - overfitting risk!")
    else:
        logger.info("\n❌ V14 did not improve Test ROI")

if __name__ == "__main__":
    main()

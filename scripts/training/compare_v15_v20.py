"""
V15 vs V20 比較検証
ドキュメントと同一条件で再テスト

データ期間:
- Train: <= 2024-12-31
- Test: 2025-01-01 ~ 2025-11-01
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.features.leak_free_feature_engineer_v20 import LeakFreeFeatureEngineerV20
import lightgbm as lgb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data():
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    return races, pedigrees, corners, horses, race_details


def train_and_evaluate(engine, train_raw, test_raw, version_name, params):
    """モデルをトレーニングして評価"""
    train_f = engine.transform(train_raw)
    test_f = engine.transform(test_raw)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"{version_name}: {len(feature_cols)}特徴量")
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    # 評価
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    def calc_roi(df, preds):
        df = df.copy()
        df['pred'] = preds
        df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
        bets = df[df['rank'] == 1]
        hits = bets[bets['finish_position'] == 1]
        roi = hits['win_odds'].sum() / len(bets) * 100
        hit_rate = len(hits) / len(bets) * 100
        return roi, hit_rate
    
    train_roi, train_hit = calc_roi(train_f, train_pred)
    test_roi, test_hit = calc_roi(test_f, test_pred)
    gap = train_roi - test_roi
    
    # 特徴量重要度Top10
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance()
    }).sort_values('importance', ascending=False)
    
    print(f"\n{version_name} Top 10 Features:")
    for i, row in importance.head(10).iterrows():
        print(f"  {row['feature']}: {row['importance']}")
    
    # V20新特徴量の重要度
    new_features = ['damsire_win_rate', 'damsire_races', 'breeder_win_rate', 'breeder_races']
    new_imp = importance[importance['feature'].isin(new_features)]
    if len(new_imp) > 0:
        print(f"\n{version_name} 新特徴量重要度:")
        for _, row in new_imp.iterrows():
            print(f"  {row['feature']}: {row['importance']}")
    
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
    logger.info("V15 vs V20 比較検証（ドキュメント同一条件）")
    logger.info("=" * 60)
    
    races, pedigrees, corners, horses, race_details = load_data()
    
    # ドキュメントと同一条件
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"Train: {len(train):,}, Test: {len(test):,}")
    
    # ドキュメントと同一パラメータ
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7
    }
    
    results = []
    
    # V15
    logger.info("\n--- V15 (Baseline) ---")
    engine_v15 = LeakFreeFeatureEngineerV15()
    engine_v15.fit(train, pedigrees, corners, race_details, horses_df=horses)
    result_v15 = train_and_evaluate(engine_v15, train, test, "V15", params)
    results.append(result_v15)
    logger.info(f"V15: Train ROI={result_v15['train_roi']:.1f}%, Test ROI={result_v15['test_roi']:.1f}%, Gap={result_v15['gap']:.1f}%")
    
    # V20
    logger.info("\n--- V20 (Damsire + Breeder) ---")
    engine_v20 = LeakFreeFeatureEngineerV20()
    engine_v20.fit(train, pedigrees, corners, race_details, horses_df=horses)
    result_v20 = train_and_evaluate(engine_v20, train, test, "V20", params)
    results.append(result_v20)
    logger.info(f"V20: Train ROI={result_v20['train_roi']:.1f}%, Test ROI={result_v20['test_roi']:.1f}%, Gap={result_v20['gap']:.1f}%")
    
    # 比較
    logger.info("\n" + "=" * 60)
    logger.info("COMPARISON SUMMARY")
    logger.info("=" * 60)
    
    df_results = pd.DataFrame(results)
    print(df_results.to_string(index=False))
    
    # V20 vs V15
    logger.info(f"\n--- V20 vs V15 ---")
    logger.info(f"Test ROI: {result_v20['test_roi'] - result_v15['test_roi']:+.1f}%")
    logger.info(f"Gap:      {result_v20['gap'] - result_v15['gap']:+.1f}%")
    logger.info(f"Features: {result_v20['n_features'] - result_v15['n_features']:+d}")
    
    if result_v20['test_roi'] > result_v15['test_roi']:
        logger.info("\n✓ V20は改善!")
    else:
        logger.info("\n✗ V20は改善せず")


if __name__ == "__main__":
    main()

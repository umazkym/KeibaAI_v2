# -*- coding: utf-8 -*-
"""
正則化強度チューニング

【概要】
- V15パラメータをベースに正則化パラメータのみを調整
- 過学習を抑制しつつROI向上を狙う
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
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
    """ROI計算（Top1予測）"""
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def main():
    logger.info("=" * 60)
    logger.info("正則化強度チューニング")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # V15公式期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2024-12-31)")
    logger.info(f"  Test:  {len(test):,}件 (2025-01-01~)")
    
    # 特徴量
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
    # パラメータ候補
    configs = [
        # V15ベースライン
        {
            'name': '1. V15 Baseline',
            'params': {
                'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
                'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
                'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
                'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
            }
        },
        # 強正則化
        {
            'name': '2. Strong Regularization',
            'params': {
                'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
                'learning_rate': 0.02, 'num_leaves': 15, 'max_depth': 2,
                'min_child_samples': 200, 'reg_alpha': 5.0, 'reg_lambda': 10.0,
                'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
            }
        },
        # 極端正則化
        {
            'name': '3. Extreme Regularization',
            'params': {
                'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
                'learning_rate': 0.01, 'num_leaves': 8, 'max_depth': 2,
                'min_child_samples': 300, 'reg_alpha': 10.0, 'reg_lambda': 15.0,
                'bagging_fraction': 0.5, 'bagging_freq': 5, 'feature_fraction': 0.5,
            }
        },
        # 少し弱めた正則化（V15より軽い）
        {
            'name': '4. Light Regularization',
            'params': {
                'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
                'learning_rate': 0.05, 'num_leaves': 31, 'max_depth': 4,
                'min_child_samples': 50, 'reg_alpha': 1.0, 'reg_lambda': 2.0,
                'bagging_fraction': 0.8, 'bagging_freq': 3, 'feature_fraction': 0.8,
            }
        },
        # 木を少なく深く
        {
            'name': '5. Fewer Deep Trees',
            'params': {
                'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
                'learning_rate': 0.01, 'num_leaves': 10, 'max_depth': 4,
                'min_child_samples': 150, 'reg_alpha': 5.0, 'reg_lambda': 5.0,
                'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
            }
        },
    ]
    
    results = []
    for config in configs:
        logger.info("")
        logger.info("=" * 60)
        logger.info(config['name'])
        logger.info("=" * 60)
        
        train_ds = lgb.Dataset(X_train, y_train)
        model = lgb.train(config['params'], train_ds, num_boost_round=200)
        
        pred_train = model.predict(X_train)
        pred_test = model.predict(X_test)
        
        train_roi, train_hit = calc_roi(train_f, pred_train)
        test_roi, test_hit = calc_roi(test_f, pred_test)
        
        gap = train_roi - test_roi
        
        logger.info(f"  Train: 的中率={train_hit:.1f}%, ROI={train_roi:.1f}%")
        logger.info(f"  Test:  的中率={test_hit:.1f}%, ROI={test_roi:.1f}%")
        logger.info(f"  Gap:   {gap:.1f}%")
        
        results.append({
            'name': config['name'],
            'train_roi': train_roi,
            'test_roi': test_roi,
            'gap': gap,
            'train_hit': train_hit,
            'test_hit': test_hit,
        })
    
    # 結果比較
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較")
    logger.info("=" * 60)
    
    logger.info("")
    logger.info(f"{'Config':<30} {'Test ROI':>10} {'Train ROI':>10} {'Gap':>10} {'Hit%':>8}")
    logger.info("-" * 70)
    
    for r in results:
        logger.info(f"{r['name']:<30} {r['test_roi']:>9.1f}% {r['train_roi']:>9.1f}% {r['gap']:>9.1f}% {r['test_hit']:>7.1f}%")
    
    # 最良結果
    best = max(results, key=lambda x: x['test_roi'])
    logger.info("")
    logger.info(f"Best: {best['name']} (Test ROI={best['test_roi']:.1f}%, Gap={best['gap']:.1f}%)")
    
    return results


if __name__ == "__main__":
    main()

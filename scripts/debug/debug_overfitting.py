"""
V7 vs V8 過学習比較と正則化強化テスト

【目的】
1. V7とV8の過学習傾向を比較
2. より強力な正則化でTrain-Test差を縮小できるか検証
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import logging
import lightgbm as lgb

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def calculate_roi(features_df, returns_df):
    """ROI計算"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho['race_id'] = tansho['race_id'].astype(str)
    tansho['horse_number'] = pd.to_numeric(tansho['horse_number'], errors='coerce')
    
    top1 = features_df[features_df['rank'] == 1].copy()
    top1['race_id'] = top1['race_id'].astype(str)
    top1['horse_number'] = pd.to_numeric(top1['horse_number'], errors='coerce')
    
    merged = top1.merge(tansho, on=['race_id', 'horse_number'], how='left')
    
    total_bet = len(top1) * 100
    total_payout = merged['payout'].fillna(0).sum()
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    
    return roi


def train_and_eval(train_features, valid_features, test_features, feature_cols, params, returns_df):
    """モデル学習と評価"""
    train_sorted = train_features.sort_values('race_id')
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups = train_sorted.groupby('race_id').size().values
    
    valid_sorted = valid_features.sort_values('race_id')
    X_valid = valid_sorted[feature_cols].fillna(0)
    y_valid = valid_sorted['finish_position']
    groups_valid = valid_sorted.groupby('race_id').size().values
    
    y_rel = np.zeros(len(y_train))
    y_rel[y_train.values == 1] = 5
    y_rel[y_train.values == 2] = 4
    y_rel[y_train.values == 3] = 3
    
    y_rel_valid = np.zeros(len(y_valid))
    y_rel_valid[y_valid.values == 1] = 5
    y_rel_valid[y_valid.values == 2] = 4
    y_rel_valid[y_valid.values == 3] = 3
    
    model = lgb.LGBMRanker(
        objective='lambdarank',
        random_state=42,
        verbose=-1,
        **params
    )
    
    model.fit(
        X_train, y_rel, group=groups,
        eval_set=[(X_valid, y_rel_valid)],
        eval_group=[groups_valid],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    
    # 予測
    train_features = train_features.copy()
    test_features = test_features.copy()
    
    train_features['score'] = model.predict(train_features[feature_cols].fillna(0))
    train_features['rank'] = train_features.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    test_features['score'] = model.predict(test_features[feature_cols].fillna(0))
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    train_roi = calculate_roi(train_features, returns_df)
    test_roi = calculate_roi(test_features, returns_df)
    
    train_hit = (train_features[train_features['rank'] == 1]['finish_position'] == 1).mean() * 100
    test_hit = (test_features[test_features['rank'] == 1]['finish_position'] == 1).mean() * 100
    
    return {
        'train_roi': train_roi,
        'test_roi': test_roi,
        'train_hit': train_hit,
        'test_hit': test_hit,
        'hit_diff': train_hit - test_hit,
        'best_iter': model.best_iteration_
    }


def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    logger.info("="*70)
    logger.info("V7 vs V8 過学習比較と正則化強化テスト")
    logger.info("="*70)
    
    # データ読み込み
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & 
                        (races_df['race_date'] < '2025-01-01')].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    logger.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
    
    from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7
    from keibaai.src.features.leak_free_feature_engineer_v8 import LeakFreeFeatureEngineerV8
    
    # 正則化設定
    configs = [
        {
            'name': '現行（強化2）',
            'params': {
                'n_estimators': 1000,
                'learning_rate': 0.005,
                'max_depth': 2,
                'num_leaves': 4,
                'min_child_samples': 300,
                'reg_alpha': 5.0,
                'reg_lambda': 10.0,
                'subsample': 0.5,
                'colsample_bytree': 0.5,
            }
        },
        {
            'name': '超強正則化',
            'params': {
                'n_estimators': 2000,
                'learning_rate': 0.002,
                'max_depth': 1,  # 極浅
                'num_leaves': 2,  # 極小
                'min_child_samples': 1000,  # 極大
                'reg_alpha': 20.0,
                'reg_lambda': 50.0,
                'subsample': 0.3,
                'colsample_bytree': 0.3,
            }
        },
        {
            'name': '最小限モデル',
            'params': {
                'n_estimators': 100,
                'learning_rate': 0.01,
                'max_depth': 1,
                'num_leaves': 2,
                'min_child_samples': 2000,
                'reg_alpha': 50.0,
                'reg_lambda': 100.0,
                'subsample': 0.2,
                'colsample_bytree': 0.2,
            }
        },
    ]
    
    # V7でテスト
    logger.info("\n" + "="*70)
    logger.info("【V7】過学習チェック")
    logger.info("="*70)
    
    fe_v7 = LeakFreeFeatureEngineerV7()
    fe_v7.fit(races_df=train_df, pedigrees_df=pedigrees_df, corners_df=corners_df,
              race_details_df=race_details_df, returns_df=returns_df)
    
    train_v7 = fe_v7.transform(train_df)
    valid_v7 = fe_v7.transform(valid_df)
    test_v7 = fe_v7.transform(test_df)
    
    features_v7 = [c for c in fe_v7.get_feature_columns() if c in train_v7.columns]
    
    logger.info(f"\nV7特徴量数: {len(features_v7)}")
    
    for config in configs:
        result = train_and_eval(train_v7, valid_v7, test_v7, features_v7, config['params'], returns_df)
        logger.info(f"\n【{config['name']}】")
        logger.info(f"  Train ROI: {result['train_roi']:.1f}%, Test ROI: {result['test_roi']:.1f}%")
        logger.info(f"  Train Hit: {result['train_hit']:.1f}%, Test Hit: {result['test_hit']:.1f}%")
        logger.info(f"  Hit差分: {result['hit_diff']:.1f}%")
        logger.info(f"  Best Iter: {result['best_iter']}")
    
    # V8でテスト
    logger.info("\n" + "="*70)
    logger.info("【V8】過学習チェック")
    logger.info("="*70)
    
    fe_v8 = LeakFreeFeatureEngineerV8()
    fe_v8.fit(races_df=train_df, pedigrees_df=pedigrees_df, corners_df=corners_df,
              race_details_df=race_details_df, returns_df=returns_df)
    
    train_v8 = fe_v8.transform(train_df)
    valid_v8 = fe_v8.transform(valid_df)
    test_v8 = fe_v8.transform(test_df)
    
    features_v8 = [c for c in fe_v8.get_feature_columns() if c in train_v8.columns]
    
    logger.info(f"\nV8特徴量数: {len(features_v8)}")
    
    for config in configs:
        result = train_and_eval(train_v8, valid_v8, test_v8, features_v8, config['params'], returns_df)
        logger.info(f"\n【{config['name']}】")
        logger.info(f"  Train ROI: {result['train_roi']:.1f}%, Test ROI: {result['test_roi']:.1f}%")
        logger.info(f"  Train Hit: {result['train_hit']:.1f}%, Test Hit: {result['test_hit']:.1f}%")
        logger.info(f"  Hit差分: {result['hit_diff']:.1f}%")
        logger.info(f"  Best Iter: {result['best_iter']}")
    
    # 人気1位ベースライン
    pop1 = test_df[test_df['popularity'] == 1].copy()
    pop1['race_id'] = pop1['race_id'].astype(str)
    pop1['horse_number'] = pd.to_numeric(pop1['horse_number'], errors='coerce')
    
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho['race_id'] = tansho['race_id'].astype(str)
    tansho['horse_number'] = pd.to_numeric(tansho['horse_number'], errors='coerce')
    
    merged_pop1 = pop1.merge(tansho, on=['race_id', 'horse_number'], how='left')
    baseline_roi = merged_pop1['payout'].fillna(0).sum() / (len(pop1) * 100) * 100
    
    logger.info("\n" + "="*70)
    logger.info("【ベースライン】")
    logger.info("="*70)
    logger.info(f"人気1位 ROI: {baseline_roi:.1f}%")


if __name__ == '__main__':
    main()

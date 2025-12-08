"""
V8正則化強化テスト

【目的】
V8のTrain ROI (449.8%) が過学習の兆候。
正則化パラメータを強化してTrain-Test差を縮小する。
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
    
    hits = (merged['payout'] > 0).sum()
    hit_rate = hits / len(top1) * 100 if len(top1) > 0 else 0
    
    return roi, hit_rate, len(top1)


def test_regularization(fe_class, fe_name, train_df, valid_df, test_df, 
                        pedigrees_df, corners_df, race_details_df, returns_df,
                        reg_config, config_name):
    """正則化設定でのテスト"""
    
    # fit & transform
    fe = fe_class()
    fe.fit(races_df=train_df, pedigrees_df=pedigrees_df, corners_df=corners_df,
           race_details_df=race_details_df, returns_df=returns_df)
    
    train_features = fe.transform(train_df)
    valid_features = fe.transform(valid_df)
    test_features = fe.transform(test_df)
    
    feature_cols = [c for c in fe.get_feature_columns() if c in train_features.columns]
    
    # モデル学習
    train_sorted = train_features.sort_values('race_id')
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups = train_sorted.groupby('race_id').size().values
    
    valid_sorted = valid_features.sort_values('race_id')
    X_valid = valid_sorted[feature_cols].fillna(0)
    y_valid = valid_sorted['finish_position']
    groups_valid = valid_sorted.groupby('race_id').size().values
    
    # Relevanceスコア
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
        **reg_config
    )
    
    model.fit(
        X_train, y_rel, group=groups,
        eval_set=[(X_valid, y_rel_valid)],
        eval_group=[groups_valid],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    
    # 予測
    train_features['score'] = model.predict(train_features[feature_cols].fillna(0))
    train_features['rank'] = train_features.groupby('race_id')['score'].rank(ascending=False, method='first')
    test_features['score'] = model.predict(test_features[feature_cols].fillna(0))
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # ROI計算
    train_roi, train_hr, _ = calculate_roi(train_features, returns_df)
    test_roi, test_hr, _ = calculate_roi(test_features, returns_df)
    
    return {
        'config_name': config_name,
        'train_roi': train_roi,
        'test_roi': test_roi,
        'train_hit': train_hr,
        'test_hit': test_hr,
        'train_test_diff': train_roi - test_roi,
        'best_iteration': model.best_iteration_
    }


def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    logger.info("="*70)
    logger.info("V8 正則化強化テスト")
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
    
    # 時系列分割
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & 
                        (races_df['race_date'] < '2025-01-01')].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    logger.info(f"Train: {len(train_df):,}行")
    logger.info(f"Valid: {len(valid_df):,}行")
    logger.info(f"Test: {len(test_df):,}行")
    
    from keibaai.src.features.leak_free_feature_engineer_v8 import LeakFreeFeatureEngineerV8
    
    # 正則化設定の比較
    configs = [
        {
            'name': '現行設定',
            'params': {
                'n_estimators': 1000,
                'learning_rate': 0.01,
                'max_depth': 3,
                'num_leaves': 8,
                'min_child_samples': 100,
                'reg_alpha': 2.0,
                'reg_lambda': 3.0,
                'subsample': 0.6,
                'colsample_bytree': 0.6,
            }
        },
        {
            'name': '強化1: 浅い木',
            'params': {
                'n_estimators': 1000,
                'learning_rate': 0.01,
                'max_depth': 2,
                'num_leaves': 4,
                'min_child_samples': 200,
                'reg_alpha': 2.0,
                'reg_lambda': 3.0,
                'subsample': 0.6,
                'colsample_bytree': 0.6,
            }
        },
        {
            'name': '強化2: 高正則化',
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
            'name': '強化3: 極端正則化',
            'params': {
                'n_estimators': 2000,
                'learning_rate': 0.003,
                'max_depth': 2,
                'num_leaves': 4,
                'min_child_samples': 500,
                'reg_alpha': 10.0,
                'reg_lambda': 20.0,
                'subsample': 0.4,
                'colsample_bytree': 0.4,
            }
        },
    ]
    
    results = []
    
    for config in configs:
        logger.info(f"\n{'='*70}")
        logger.info(f"テスト: {config['name']}")
        logger.info(f"{'='*70}")
        
        result = test_regularization(
            LeakFreeFeatureEngineerV8, "V8",
            train_df, valid_df, test_df,
            pedigrees_df, corners_df, race_details_df, returns_df,
            config['params'], config['name']
        )
        results.append(result)
        
        logger.info(f"  Train ROI: {result['train_roi']:.1f}%")
        logger.info(f"  Test ROI: {result['test_roi']:.1f}%")
        logger.info(f"  差分: {result['train_test_diff']:.1f}%")
        logger.info(f"  Iterations: {result['best_iteration']}")
    
    # サマリー
    logger.info("\n" + "="*70)
    logger.info("結果サマリー")
    logger.info("="*70)
    
    logger.info(f"\n{'設定名':<20} {'Train ROI':<12} {'Test ROI':<12} {'差分':<12}")
    logger.info("-"*60)
    for r in results:
        logger.info(f"{r['config_name']:<20} {r['train_roi']:.1f}%{'':<7} {r['test_roi']:.1f}%{'':<7} {r['train_test_diff']:.1f}%")
    
    # 最適設定の推奨
    # Test ROIが最大かつTrain-Test差が小さいものを選択
    best = max(results, key=lambda x: x['test_roi'] if x['train_test_diff'] < 300 else 0)
    logger.info(f"\n推奨設定: {best['config_name']} (Test ROI: {best['test_roi']:.1f}%)")


if __name__ == '__main__':
    main()

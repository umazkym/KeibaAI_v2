"""
V8.1 (V7 + ClassChange) 最終検証スクリプト
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import logging
import lightgbm as lgb
import matplotlib.pyplot as plt

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_roi(features_df, returns_df):
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

def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    logger.info("V8.1 Verification Start")

    # Load Data
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[races_df['race_date'] >= '2020-01-01'].dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    # Split
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & (races_df['race_date'] < '2025-01-01')].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    from keibaai.src.features.leak_free_feature_engineer_v8 import LeakFreeFeatureEngineerV8
    
    # Generate Features (V8.1)
    fe = LeakFreeFeatureEngineerV8()
    fe.fit(train_df, pedigrees_df, corners_df, race_details_df, returns_df)
    
    train_feat = fe.transform(train_df)
    valid_feat = fe.transform(valid_df)
    test_feat = fe.transform(test_df)
    
    feature_cols = [c for c in fe.get_feature_columns() if c in train_feat.columns]
    logger.info(f"Feature Count: {len(feature_cols)}")
    
    # Train
    train_sorted = train_feat.sort_values('race_id')
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups_train = train_sorted.groupby('race_id').size().values
    
    valid_sorted = valid_feat.sort_values('race_id')
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
        n_estimators=1000,
        learning_rate=0.005,
        max_depth=2,
        num_leaves=4,
        min_child_samples=300,
        reg_alpha=5.0,
        reg_lambda=10.0,
        subsample=0.5,
        colsample_bytree=0.5,
        random_state=42,
        verbose=-1
    )
    
    model.fit(
        X_train, y_rel, group=groups_train,
        eval_set=[(X_valid, y_rel_valid)],
        eval_group=[groups_valid],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    
    # Evaluate
    test_feat['score'] = model.predict(test_feat[feature_cols].fillna(0))
    test_feat['rank'] = test_feat.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    roi, hr, bets = calculate_roi(test_feat, returns_df)
    
    train_feat['score'] = model.predict(train_feat[feature_cols].fillna(0))
    train_feat['rank'] = train_feat.groupby('race_id')['score'].rank(ascending=False, method='first')
    tr_roi, tr_hr, _ = calculate_roi(train_feat, returns_df)
    
    logger.info("="*30)
    logger.info("V8.1 Final Result")
    logger.info("="*30)
    logger.info(f"Test ROI: {roi:.1f}% ({bets} races)")
    logger.info(f"Test Hit Rate: {hr:.1f}%")
    logger.info(f"Train Hit Rate: {tr_hr:.1f}%")
    logger.info(f"Hit Gap: {tr_hr - hr:.1f}%")
    
    # Feature Importance
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    logger.info("\nFeature Importance Top 10:")
    for _, row in importance.head(10).iterrows():
        logger.info(f"{row['feature']}: {row['importance']}")
        
    # Check new features
    new_features = ['race_class', 'prev_race_class', 'class_change', 'is_class_up', 'is_class_down']
    logger.info("\nNew Features Importance:")
    for f in new_features:
        if f in importance['feature'].values:
            imp = importance[importance['feature'] == f]['importance'].values[0]
            logger.info(f"{f}: {imp}")

if __name__ == '__main__':
    main()

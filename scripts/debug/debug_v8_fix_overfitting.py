"""
V8 過学習対策: V7ベースのForward Selection
V7に1つずつ特徴量グループを追加して効果を検証
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
    
    return roi, hit_rate

def train_and_eval(train_df, valid_df, test_df, feature_cols, returns_df, label):
    # Train
    train_sorted = train_df.sort_values('race_id')
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups_train = train_sorted.groupby('race_id').size().values
    
    valid_sorted = valid_df.sort_values('race_id')
    X_valid = valid_sorted[feature_cols].fillna(0)
    y_valid = valid_sorted['finish_position']
    groups_valid = valid_sorted.groupby('race_id').size().values
    
    # Relevance Score
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
    
    # Evaluate on Train
    train_pred = train_df.copy()
    train_pred['score'] = model.predict(train_pred[feature_cols].fillna(0))
    train_pred['rank'] = train_pred.groupby('race_id')['score'].rank(ascending=False, method='first')
    tr_roi, tr_hr = calculate_roi(train_pred, returns_df)
    
    # Evaluate on Test
    test_pred = test_df.copy()
    test_pred['score'] = model.predict(test_pred[feature_cols].fillna(0))
    test_pred['rank'] = test_pred.groupby('race_id')['score'].rank(ascending=False, method='first')
    te_roi, te_hr = calculate_roi(test_pred, returns_df)
    
    gap = tr_hr - te_hr
    logger.info(f"[{label}] Train Hit: {tr_hr:.1f}% | Test Hit: {te_hr:.1f}% | Gap: {gap:.1f}% | Test ROI: {te_roi:.1f}%")

def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    logger.info("V8 Forward Selection (From V7) Start")

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
    from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7
    
    # Generate Features
    fe = LeakFreeFeatureEngineerV8()
    fe.fit(train_df, pedigrees_df, corners_df, race_details_df, returns_df)
    
    logger.info("Generating features...")
    train_feat = fe.transform(train_df)
    valid_feat = fe.transform(valid_df)
    test_feat = fe.transform(test_df)
    
    v7_features = LeakFreeFeatureEngineerV7.FEATURE_COLS
    
    # Feature Groups
    groups = {
        '1_Add_Damsire': ['damsire_win_rate', 'damsire_races'],
        '2_Add_CourseStats': ['horse_win_rate_sprint', 'horse_win_rate_mile', 'horse_win_rate_middle', 'horse_win_rate_long', 'horse_win_rate_turf', 'horse_win_rate_dirt'],
        '3_Add_ClassChange': ['race_class', 'prev_race_class', 'class_change', 'is_class_up', 'is_class_down'],
        '4_Add_Weight': ['weight_change_from_last', 'weight_change_trend_3'],
        '5_Add_JockeyStats': ['jockey_venue_win_rate', 'jockey_distance_win_rate'],
        '6_Add_Last3F': ['horse_last3f_rank_avg'],
    }
    
    # Baseline V7
    train_and_eval(train_feat, valid_feat, test_feat, v7_features, returns_df, '0_Baseline(V7)')
    
    # Run Forward Selection
    for name, added_cols in groups.items():
        cols = v7_features + added_cols
        train_and_eval(train_feat, valid_feat, test_feat, cols, returns_df, name)

if __name__ == '__main__':
    main()

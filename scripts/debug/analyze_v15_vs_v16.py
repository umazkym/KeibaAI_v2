#!/usr/bin/env python3
"""
V15 vs V16 詳細比較分析

- 予測スコアの相関
- 勝ち負けが分かれたレースの分析
- 新特徴量の単変量解析
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
import matplotlib.pyplot as plt
import seaborn as sns

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16

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

def compare_models():
    races, pedigrees, corners, horses, race_details = load_data()
    
    # 2024年データで比較（Train: ~2023, Test: 2024）
    train_df = races[races['race_date'] < '2024-01-01'].copy()
    test_df = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2025-01-01')].copy()
    
    logger.info(f"Train: {len(train_df):,}, Test: {len(test_df):,}")
    
    # --- V15 ---
    logger.info("Evaluating V15...")
    v15 = LeakFreeFeatureEngineerV15()
    v15.fit(train_df, pedigrees, corners, race_details, horses_df=horses)
    train_15 = v15.transform(train_df)
    test_15 = v15.transform(test_df)
    
    feats_15 = v15.get_feature_columns()
    X_tr_15 = train_15[feats_15].fillna(0)
    y_tr = (train_15['finish_position'] == 1).astype(int)
    
    params = {
        'objective': 'binary', 'metric': 'auc', 'learning_rate': 0.03, 'max_depth': 3,
        'num_leaves': 20, 'min_child_samples': 100, 'bagging_fraction': 0.7, 'feature_fraction': 0.7,
        'verbose': -1
    }
    
    model_15 = lgb.train(params, lgb.Dataset(X_tr_15, y_tr), num_boost_round=200)
    test_15['pred_v15'] = model_15.predict(test_15[feats_15].fillna(0))
    test_15['rank_v15'] = test_15.groupby('race_id')['pred_v15'].rank(ascending=False)
    
    # --- V16 ---
    logger.info("Evaluating V16...")
    v16 = LeakFreeFeatureEngineerV16()
    v16.fit(train_df, pedigrees, corners, race_details, horses_df=horses)
    # V16 transform
    # 注意: V16はV15を継承しているので同じインスタンスでも良いが、念のため作り直す
    # ただし今回は同じデータで比較したいので、特徴量だけ追加計算する形でも良いが
    # 公平な比較のため再計算推奨
    train_16 = v16.transform(train_df)
    test_16 = v16.transform(test_df)
    
    feats_16 = v16.get_feature_columns()
    X_tr_16 = train_16[feats_16].fillna(0)
    
    model_16 = lgb.train(params, lgb.Dataset(X_tr_16, y_tr), num_boost_round=200)
    test_16['pred_v16'] = model_16.predict(test_16[feats_16].fillna(0))
    test_16['rank_v16'] = test_16.groupby('race_id')['pred_v16'].rank(ascending=False)
    
    # --- 比較 ---
    # 結果をマージ
    suffixes = ('_15', '_16')
    merged = test_15[['race_id', 'horse_number', 'finish_position', 'win_odds', 'pred_v15', 'rank_v15']].merge(
        test_16[['race_id', 'horse_number', 'pred_v16', 'rank_v16', 
                 'jockey_venue_win_rate', 'jockey_venue_races', 
                 'race_interval_days', 'relative_post_venue_advantage']],
        on=['race_id', 'horse_number']
    )
    
    # Top1の一致率
    top1_match = merged[(merged['rank_v15'] == 1) & (merged['rank_v16'] == 1)]
    match_rate = len(top1_match) / merged[merged['rank_v15'] == 1].shape[0]
    logger.info(f"Top1 Agreement Rate: {match_rate:.1%}")
    
    # V15勝ち / V16負け のサンプル (V15 Top1的中, V16 Top1外れ)
    v15_wins = merged[
        (merged['rank_v15'] == 1) & (merged['finish_position'] == 1) & 
        (merged['rank_v16'] > 1)
    ]
    logger.info(f"V15 Only Wins (Top1): {len(v15_wins)} cases")
    
    # V16勝ち / V15負け のサンプル
    v16_wins = merged[
        (merged['rank_v16'] == 1) & (merged['finish_position'] == 1) & 
        (merged['rank_v15'] > 1)
    ]
    logger.info(f"V16 Only Wins (Top1): {len(v16_wins)} cases")
    
    # --- 新特徴量の分析 ---
    logger.info("\nChecking new features correlation with target...")
    target = (train_16['finish_position'] == 1).astype(int)
    new_feats = ['jockey_venue_win_rate', 'race_interval_days', 'relative_post_venue_advantage']
    
    for f in new_feats:
        corr = train_16[f].corr(target)
        logger.info(f"  {f}: Correlation = {corr:.4f}")
        
    # 区間ごとの勝率
    print("\n--- Jockey Venue Win Rate Analysis ---")
    train_16['jockey_rate_bin'] = pd.cut(train_16['jockey_venue_win_rate'], 10)
    print(train_16.groupby('jockey_rate_bin', observed=True)['finish_position'].apply(lambda x: (x==1).mean()).to_string())
    
    print("\n--- Race Interval Days Analysis ---")
    # 異常値除外してビン化
    valid_interval = train_16[(train_16['race_interval_days'] >= 0) & (train_16['race_interval_days'] < 365)]
    valid_interval['interval_bin'] = pd.cut(valid_interval['race_interval_days'], [0, 7, 14, 28, 60, 180, 365])
    print(valid_interval.groupby('interval_bin', observed=True)['finish_position'].apply(lambda x: (x==1).mean()).to_string())

    return merged, v15_wins, v16_wins

if __name__ == "__main__":
    compare_models()

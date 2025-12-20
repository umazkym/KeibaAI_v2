#!/usr/bin/env python3
"""
V16 Walk-Forward Validation

V16特徴量エンジニアの効果を検証するWalk-Forward Validation
V17と同条件で比較するために実行
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_all_data():
    """全データ読み込み"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races = races[(races['finish_position'].notna()) & (races['finish_position'] > 0)].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    try:
        pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    except:
        pedigrees = None
    
    try:
        race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    except:
        race_details = None
    
    # コーナーデータを構築
    corners = []
    for corner in [1, 2, 3, 4]:
        col = f'passing_order_{corner}'
        if col in races.columns:
            temp = races[['race_id', 'horse_number', col]].copy()
            temp = temp[temp[col].notna()]
            temp['corner'] = corner
            temp['position'] = temp[col]
            temp['gap_from_leader'] = 0
            corners.append(temp[['race_id', 'horse_number', 'corner', 'position', 'gap_from_leader']])
    
    corners_df = pd.concat(corners, ignore_index=True) if corners else None
    
    return races, pedigrees, corners_df, race_details


def calc_roi(pred_df, rank_col='pred_rank'):
    """ROI計算（Top1のみ）"""
    top1 = pred_df[pred_df[rank_col] == 1].copy()
    total_bet = len(top1) * 100  # 各レース100円賭け
    # 的中した馬の払い戻し = オッズ × 100円
    wins = top1[top1['finish_position'] == 1]
    total_return = (wins['win_odds'] * 100).sum() if len(wins) > 0 else 0
    return total_return / total_bet * 100 if total_bet > 0 else 0


def run_walkforward_v16():
    """Walk-Forward Validation実行"""
    from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16
    
    races, pedigrees, corners_df, race_details = load_all_data()
    
    logger.info(f"Total races: {len(races):,}")
    
    years = [2022, 2023, 2024, 2025]
    results = []
    
    for year in years:
        logger.info(f"\n{'='*40}")
        logger.info(f"Year {year}")
        logger.info(f"{'='*40}")
        
        # Train: 直前8年分, Test: 当該年
        train_start = f"{year - 8}-01-01"
        train_end = f"{year - 1}-12-31"
        test_start = f"{year}-01-01"
        test_end = f"{year}-12-31"
        
        train_df = races[(races['race_date'] >= train_start) & (races['race_date'] <= train_end)].copy()
        test_df = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(test_df) == 0:
            logger.warning(f"  No test data for {year}")
            continue
        
        logger.info(f"  Train: {len(train_df):,}, Test: {len(test_df):,}")
        
        # V16 Feature Engineering
        v16 = LeakFreeFeatureEngineerV16()
        v16.fit(train_df, pedigrees, corners_df, race_details)
        
        train_feat = v16.transform(train_df)
        test_feat = v16.transform(test_df)
        
        # 特徴量カラム
        feature_cols = v16.get_feature_columns()
        available_cols = [c for c in feature_cols if c in train_feat.columns and c in test_feat.columns]
        
        # 学習
        X_train = train_feat[available_cols].copy()
        y_train = train_feat['finish_position'].copy()
        X_test = test_feat[available_cols].copy()
        
        # NaN処理
        X_train = X_train.fillna(-999)
        X_test = X_test.fillna(-999)
        
        # LightGBM
        params = {
            'objective': 'regression',
            'metric': 'rmse',
            'boosting_type': 'gbdt',
            'learning_rate': 0.05,
            'num_leaves': 63,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
            'seed': 42
        }
        
        train_data = lgb.Dataset(X_train, label=y_train)
        model = lgb.train(params, train_data, num_boost_round=500)
        
        # 予測
        test_feat['pred'] = model.predict(X_test)
        
        # レース内順位
        test_feat['pred_rank'] = test_feat.groupby('race_id')['pred'].rank()
        
        # ROI計算
        train_feat['pred'] = model.predict(X_train.fillna(-999))
        train_feat['pred_rank'] = train_feat.groupby('race_id')['pred'].rank()
        
        train_roi = calc_roi(train_feat)
        test_roi = calc_roi(test_feat, 'pred_rank')
        
        gap = train_roi - test_roi
        
        logger.info(f"  Train ROI: {train_roi:.1f}%")
        logger.info(f"  Test ROI: {test_roi:.1f}%")
        logger.info(f"  Gap: {gap:.1f}%")
        
        results.append({
            'year': year,
            'train_roi': train_roi,
            'test_roi': test_roi,
            'gap': gap
        })
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("V16 SUMMARY")
    logger.info(f"{'='*60}")
    
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    logger.info(f"\n--- Statistics ---")
    logger.info(f"Average Test ROI: {results_df['test_roi'].mean():.1f}%")
    logger.info(f"Average Gap: {results_df['gap'].mean():.1f}%")


if __name__ == "__main__":
    run_walkforward_v16()

#!/usr/bin/env python3
"""
V17 Walk-Forward Validation

V17特徴量エンジニアの効果を検証するWalk-Forward Validation
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.preprocessing import LabelEncoder
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


def run_walkforward_v17():
    """Walk-Forward Validation実行"""
    from keibaai.src.features.leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17
    
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
        
        # V17 Feature Engineering
        v17 = LeakFreeFeatureEngineerV17()
        v17.fit(train_df, pedigrees, corners_df, race_details)
        
        train_feat = v17.transform(train_df)
        test_feat = v17.transform(test_df)
        
        # 特徴量カラム
        feature_cols = v17.get_feature_columns()
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
        test_feat['pred_rank2'] = test_feat.groupby('race_id')['pred'].rank().apply(lambda x: 1 if x <= 2 else 0)
        test_feat.loc[test_feat['pred_rank2'] == 0, 'pred_rank2'] = test_feat.loc[test_feat['pred_rank2'] == 0, 'pred_rank']
        
        # ROI計算
        train_feat['pred'] = model.predict(X_train.fillna(-999))
        train_feat['pred_rank'] = train_feat.groupby('race_id')['pred'].rank()
        
        train_roi = calc_roi(train_feat)
        test_roi_r1 = calc_roi(test_feat, 'pred_rank')
        
        # Top2 ROI
        test_top2 = test_feat[test_feat['pred_rank'] <= 2].copy()
        total_bet = len(test_top2) * 100
        wins_top2 = test_top2[test_top2['finish_position'] == 1]
        total_return = (wins_top2['win_odds'] * 100).sum() if len(wins_top2) > 0 else 0
        test_roi_r2 = total_return / total_bet * 100 if total_bet > 0 else 0
        
        gap = train_roi - test_roi_r1
        
        logger.info(f"  Train ROI: {train_roi:.1f}%")
        logger.info(f"  Test ROI (R1): {test_roi_r1:.1f}%")
        logger.info(f"  Test ROI (R2): {test_roi_r2:.1f}%")
        logger.info(f"  Gap: {gap:.1f}%")
        
        results.append({
            'year': year,
            'train_roi': train_roi,
            'test_roi_r1': test_roi_r1,
            'test_roi_r2': test_roi_r2,
            'gap': gap
        })
        
        # Feature Importance (最後の年のみ)
        if year == years[-1]:
            importance = pd.DataFrame({
                'feature': available_cols,
                'importance': model.feature_importance(importance_type='gain')
            }).sort_values('importance', ascending=False)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")
    
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    logger.info(f"\n--- Statistics ---")
    logger.info(f"Average Test ROI (R1): {results_df['test_roi_r1'].mean():.1f}%")
    logger.info(f"Average Test ROI (R2): {results_df['test_roi_r2'].mean():.1f}%")
    logger.info(f"Average Gap: {results_df['gap'].mean():.1f}%")
    
    # Feature Importance Top 20
    logger.info(f"\n--- Feature Importance (Top 20) ---")
    print(importance.head(20).to_string())
    
    # V17新特徴量の重要度
    logger.info(f"\n--- V17 New Features Importance ---")
    v17_features = ['close_loss_rate', 'finish_ewm', 'last3f_rank_avg', 'last3f_consistency',
                    'position_improvement_avg', 'position_change_avg', 'weight_change_trend',
                    'optimal_weight_diff', 'pace_adaptability']
    v17_imp = importance[importance['feature'].isin(v17_features)]
    print(v17_imp.to_string())


if __name__ == "__main__":
    run_walkforward_v17()

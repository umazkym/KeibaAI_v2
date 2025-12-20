#!/usr/bin/env python3
"""
V17 Feature Engineer 動作確認テスト
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data():
    """データ読み込み"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races = races[races['finish_position'].notna()].copy()
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
            temp['gap_from_leader'] = 0  # 簡易版
            corners.append(temp[['race_id', 'horse_number', 'corner', 'position', 'gap_from_leader']])
    
    if corners:
        corners_df = pd.concat(corners, ignore_index=True)
    else:
        corners_df = None
    
    return races, pedigrees, corners_df, race_details

def test_v17():
    """V17のテスト"""
    from keibaai.src.features.leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17
    
    races, pedigrees, corners_df, race_details = load_data()
    
    # 直近2年分のみ使用（高速化）
    sample = races[races['race_date'] >= '2022-01-01'].copy()
    train = sample[sample['race_date'] < '2024-01-01'].copy()
    test = sample[(sample['race_date'] >= '2024-01-01') & (sample['race_date'] < '2024-03-01')].copy()
    
    logger.info(f"Train: {len(train):,}, Test: {len(test):,}")
    
    # V17インスタンス作成
    v17 = LeakFreeFeatureEngineerV17()
    
    # Fit
    logger.info("Fitting V17...")
    v17.fit(train, pedigrees, corners_df, race_details)
    
    # Transform
    logger.info("Transforming test data...")
    feat_df = v17.transform(test)
    
    # 結果確認
    print("\n=== V17 Feature Summary ===")
    feature_cols = v17.get_feature_columns()
    print(f"Total features: {len(feature_cols)}")
    
    # 新規追加特徴量の統計
    new_features = [
        'close_loss_rate', 'finish_ewm', 'last3f_rank_avg', 'last3f_consistency',
        'position_improvement_avg', 'position_change_avg',
        'weight_change_trend', 'optimal_weight_diff',
        'pace_adaptability'
    ]
    
    print("\n=== New Feature Statistics ===")
    for col in new_features:
        if col in feat_df.columns:
            valid = feat_df[col].notna().sum()
            pct = valid / len(feat_df) * 100
            print(f"{col}: valid={valid:,}/{len(feat_df):,} ({pct:.1f}%), mean={feat_df[col].mean():.4f}")
        else:
            print(f"{col}: NOT FOUND")
    
    # リークチェック：Testデータで統計が計算されているか確認
    print("\n=== Leak Check ===")
    # finish_ewmはshift(1)なので、初出走馬はNaNのはず
    first_race_horses = test.groupby('horse_id').first().reset_index()['horse_id']
    # 実際の値を確認
    sample_horse = test[test['horse_id'].isin(first_race_horses[:5])][['horse_id', 'race_date', 'finish_position']]
    print("First race horses in test (should have NaN for cumulative features):")
    print(sample_horse.head(10))

if __name__ == "__main__":
    test_v17()

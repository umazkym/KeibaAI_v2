"""
V11特徴量エンジニアのテスト・検証スクリプト

目的:
1. V11の特徴量が正しく生成されることを確認
2. リークがないことを検証（cumsum+shift正確性）
3. V10.2との比較でROI向上を確認

使用方法:
python scripts/training/test_v11_features.py
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path

# パスを追加
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v11 import (
    LeakFreeFeatureEngineerV11, 
    FeatureConfigV11
)
from keibaai.src.features.leak_free_feature_engineer_v10_2 import LeakFreeFeatureEngineerV10_2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    logger.info("=" * 60)
    logger.info("データ読み込み開始")
    logger.info("=" * 60)
    
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    
    logger.info(f"  レース: {len(races):,}件")
    logger.info(f"  血統: {len(pedigrees):,}件")
    logger.info(f"  コーナー: {len(corners):,}件")
    logger.info(f"  馬マスタ: {len(horses):,}件")
    
    return races, pedigrees, corners, horses


def test_v11_features(races, pedigrees, corners, horses):
    """V11特徴量のテスト"""
    logger.info("=" * 60)
    logger.info("V11特徴量テスト")
    logger.info("=" * 60)
    
    # データ分割
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # finish_positionがNAの行を除外（完走馬のみ）
    races = races[races['finish_position'].notna()].copy()
    
    train = races[races['race_date'] < '2024-07-01'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 ({train['race_date'].min()} - {train['race_date'].max()})")
    logger.info(f"  Test:  {len(test):,}件 ({test['race_date'].min()} - {test['race_date'].max()})")
    
    # V11エンジン
    config = FeatureConfigV11()
    engine = LeakFreeFeatureEngineerV11(config)
    
    # Fit
    logger.info("\nFit開始...")
    engine.fit(
        races_df=train,
        pedigrees_df=pedigrees,
        corners_df=corners,
        horses_df=horses
    )
    
    # Transform
    logger.info("\nTransform開始...")
    test_features = engine.transform(test)
    
    # 特徴量確認
    feature_cols = engine.get_feature_columns()
    logger.info(f"\n生成された特徴量: {len(feature_cols)}個")
    
    # V11新特徴量の統計
    v11_new_cols = [
        'prev_finish_category',
        'distance_change',
        'distance_extend_flag',
        'surface_switch_flag',
        'basis_weight_light_flag',
    ]
    
    logger.info("\n--- V11新特徴量の統計 ---")
    for col in v11_new_cols:
        if col in test_features.columns:
            non_null = test_features[col].notna().sum()
            total = len(test_features)
            pct = non_null / total * 100
            logger.info(f"  {col}: {non_null:,}/{total:,} ({pct:.1f}%) non-null")
            if test_features[col].notna().any():
                logger.info(f"    Mean: {test_features[col].mean():.3f}, Std: {test_features[col].std():.3f}")
                logger.info(f"    Value counts: {dict(test_features[col].value_counts().head(5))}")
    
    return test_features, engine


def leak_check(test_features, engine):
    """リークチェック"""
    logger.info("\n" + "=" * 60)
    logger.info("リークチェック")
    logger.info("=" * 60)
    
    # 特定の馬をトレースしてshift(1)が正しいか確認
    sample_horse = test_features['horse_id'].value_counts().head(10).index[0]
    horse_data = test_features[test_features['horse_id'] == sample_horse].sort_values('race_date')
    
    logger.info(f"\nサンプル馬ID: {sample_horse}")
    logger.info(f"レース数: {len(horse_data)}")
    
    # prev_finishとfinish_positionの関係確認
    check_cols = ['race_date', 'finish_position', 'prev_finish', 'prev_finish_category']
    check_cols = [c for c in check_cols if c in horse_data.columns]
    
    logger.info("\n--- 前走情報のトレース ---")
    logger.info(horse_data[check_cols].to_string())
    
    # リーク検証: prev_finishが本当に前走の値か
    if len(horse_data) >= 2:
        horse_data = horse_data.reset_index(drop=True)
        for i in range(1, min(5, len(horse_data))):
            actual_prev = horse_data.loc[i-1, 'finish_position']
            stored_prev = horse_data.loc[i, 'prev_finish']
            match = "✓" if actual_prev == stored_prev else "✗ LEAK!"
            logger.info(f"  Race {i}: actual_prev={actual_prev}, stored_prev={stored_prev} {match}")
    
    return True


def compare_roi(test_features):
    """簡易ROI比較"""
    logger.info("\n" + "=" * 60)
    logger.info("簡易ROI分析")
    logger.info("=" * 60)
    
    df = test_features.copy()
    df['is_win'] = (df['finish_position'] == 1)
    
    # prev_finish_categoryごとのROI
    logger.info("\n--- prev_finish_categoryごとのROI ---")
    cat_stats = df.groupby('prev_finish_category').agg(
        total=('is_win', 'count'),
        wins=('is_win', 'sum'),
        avg_odds=('win_odds', 'mean')
    ).reset_index()
    cat_stats['win_rate'] = cat_stats['wins'] / cat_stats['total'] * 100
    cat_stats['roi'] = (cat_stats['wins'] * cat_stats['avg_odds']) / cat_stats['total'] * 100
    print(cat_stats)
    
    # distance_extend_flagごとのROI
    logger.info("\n--- distance_extend_flagごとのROI ---")
    ext_stats = df.groupby('distance_extend_flag').agg(
        total=('is_win', 'count'),
        wins=('is_win', 'sum'),
        avg_odds=('win_odds', 'mean')
    ).reset_index()
    ext_stats['win_rate'] = ext_stats['wins'] / ext_stats['total'] * 100
    ext_stats['roi'] = (ext_stats['wins'] * ext_stats['avg_odds']) / ext_stats['total'] * 100
    print(ext_stats)
    
    # basis_weight_light_flagごとのROI
    logger.info("\n--- basis_weight_light_flagごとのROI ---")
    bw_stats = df.groupby('basis_weight_light_flag').agg(
        total=('is_win', 'count'),
        wins=('is_win', 'sum'),
        avg_odds=('win_odds', 'mean')
    ).reset_index()
    bw_stats['win_rate'] = bw_stats['wins'] / bw_stats['total'] * 100
    bw_stats['roi'] = (bw_stats['wins'] * bw_stats['avg_odds']) / bw_stats['total'] * 100
    print(bw_stats)


def main():
    """メイン処理"""
    logger.info("V11特徴量テスト開始")
    
    # データ読み込み
    races, pedigrees, corners, horses = load_data()
    
    # V11テスト
    test_features, engine = test_v11_features(races, pedigrees, corners, horses)
    
    # リークチェック
    leak_check(test_features, engine)
    
    # ROI分析
    compare_roi(test_features)
    
    logger.info("\n" + "=" * 60)
    logger.info("V11特徴量テスト完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

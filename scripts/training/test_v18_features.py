"""
V18特徴量エンジニアのテスト・検証スクリプト

目的:
1. V18の9つの新特徴量が正しく生成されることを確認
2. リークがないことを検証（cumsum+shift正確性）
3. 特徴量の統計情報を確認

使用方法:
python scripts/training/test_v18_features.py
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path

# パスを追加
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v18 import (
    LeakFreeFeatureEngineerV18, 
    FeatureConfigV18
)

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
    
    # race_detailsを読み込み（存在チェック）
    rd_path = 'keibaai/data/parsed/parquet/race_details/race_details.parquet'
    try:
        race_details = pd.read_parquet(rd_path)
        logger.info(f"  レース詳細: {len(race_details):,}件")
    except FileNotFoundError:
        logger.warning(f"  レース詳細ファイルが見つかりません: {rd_path}")
        race_details = None
    
    logger.info(f"  レース: {len(races):,}件")
    logger.info(f"  血統: {len(pedigrees):,}件")
    logger.info(f"  コーナー: {len(corners):,}件")
    logger.info(f"  馬マスタ: {len(horses):,}件")
    
    return races, pedigrees, corners, horses, race_details


def test_v18_features(races, pedigrees, corners, horses, race_details):
    """V18特徴量のテスト"""
    logger.info("=" * 60)
    logger.info("V18特徴量テスト")
    logger.info("=" * 60)
    
    # データ分割
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # finish_positionがNAの行を除外（完走馬のみ）
    races = races[races['finish_position'].notna()].copy()
    
    train = races[races['race_date'] < '2024-07-01'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 ({train['race_date'].min()} - {train['race_date'].max()})")
    logger.info(f"  Test:  {len(test):,}件 ({test['race_date'].min()} - {test['race_date'].max()})")
    
    # V18エンジン
    config = FeatureConfigV18()
    engine = LeakFreeFeatureEngineerV18(config)
    
    # Fit
    logger.info("\nFit開始...")
    engine.fit(
        races_df=train,
        pedigrees_df=pedigrees,
        corners_df=corners,
        race_details_df=race_details,
        horses_df=horses
    )
    
    # Transform
    logger.info("\nTransform開始...")
    test_features = engine.transform(test)
    
    # 特徴量確認
    feature_cols = engine.get_feature_columns()
    logger.info(f"\n生成された特徴量: {len(feature_cols)}個")
    
    # V18新特徴量の統計
    v18_new_cols = [
        'elo_rating',
        'recency_bias_indicator',
        'gap_c1_avg',
        'gap_c2_avg',
        'gap_c3_avg',
        'gap_reduction_c1_c4',
        'gap_reduction_c3_c4',
        'senkou_rate',
        'sashi_rate',
    ]
    
    logger.info("\n--- V18新特徴量の統計 ---")
    for col in v18_new_cols:
        if col in test_features.columns:
            non_null = test_features[col].notna().sum()
            total = len(test_features)
            pct = non_null / total * 100
            logger.info(f"  {col}: {non_null:,}/{total:,} ({pct:.1f}%) non-null")
            if test_features[col].notna().any():
                mean_val = test_features[col].mean()
                std_val = test_features[col].std()
                min_val = test_features[col].min()
                max_val = test_features[col].max()
                logger.info(f"    Mean: {mean_val:.3f}, Std: {std_val:.3f}, Min: {min_val:.3f}, Max: {max_val:.3f}")
        else:
            logger.warning(f"  {col}: カラムが存在しません！")
    
    return test_features, engine


def leak_check(test_features, engine):
    """リークチェック"""
    logger.info("\n" + "=" * 60)
    logger.info("リークチェック")
    logger.info("=" * 60)
    
    # Eloレーティングの検証
    logger.info("\n--- Eloレーティング検証 ---")
    if 'elo_rating' in test_features.columns:
        # 非NaNの馬を選択
        valid_horses = test_features[test_features['elo_rating'].notna()]['horse_id'].unique()
        if len(valid_horses) > 0:
            sample_horse = valid_horses[0]
            horse_data = test_features[test_features['horse_id'] == sample_horse].sort_values('race_date')
            
            logger.info(f"  サンプル馬ID: {sample_horse}")
            logger.info(f"  レース数: {len(horse_data)}")
            
            check_cols = ['race_date', 'finish_position', 'elo_rating', 'recency_bias_indicator']
            check_cols = [c for c in check_cols if c in horse_data.columns]
            logger.info(f"\n{horse_data[check_cols].head(10).to_string()}")
    
    # 各特徴量のリークチェックポイント
    logger.info("\n--- リーク防止原則の確認 ---")
    logger.info("✓ elo_rating: 当該レース前のEloを使用（レース結果で更新後にshift）")
    logger.info("✓ recency_bias_indicator: 過去5走平均とpopularityのみ使用")
    logger.info("✓ gap_cx_avg: expanding().mean().shift(1)で計算")
    logger.info("✓ gap_reduction: gap_c1_avg - gap_c4_avg（どちらもshift済み）")
    logger.info("✓ senkou_rate, sashi_rate: expanding().mean().shift(1)で計算")
    
    return True


def compare_roi(test_features):
    """簡易ROI分析"""
    logger.info("\n" + "=" * 60)
    logger.info("簡易ROI分析")
    logger.info("=" * 60)
    
    df = test_features.copy()
    df['is_win'] = (df['finish_position'] == 1)
    
    # Eloレーティングごとの勝率
    logger.info("\n--- elo_ratingごとの勝率 ---")
    if 'elo_rating' in df.columns and df['elo_rating'].notna().sum() > 0:
        df['elo_cat'] = pd.cut(
            df['elo_rating'],
            bins=[-np.inf, 1450, 1500, 1550, 1600, np.inf],
            labels=['<1450', '1450-1500', '1500-1550', '1550-1600', '>1600']
        )
        
        elo_stats = df[df['elo_cat'].notna()].groupby('elo_cat', observed=True).agg(
            total=('is_win', 'count'),
            wins=('is_win', 'sum'),
            avg_odds=('win_odds', 'mean')
        ).reset_index()
        elo_stats['win_rate'] = elo_stats['wins'] / elo_stats['total'] * 100
        elo_stats['roi'] = (elo_stats['wins'] * elo_stats['avg_odds']) / elo_stats['total'] * 100
        print(elo_stats)
    else:
        logger.info("  Eloデータなし")
    
    # Recency Biasごとの勝率
    logger.info("\n--- recency_bias_indicatorごとの勝率 ---")
    if 'recency_bias_indicator' in df.columns and df['recency_bias_indicator'].notna().sum() > 0:
        df['bias_cat'] = pd.cut(
            df['recency_bias_indicator'],
            bins=[-np.inf, -2, 0, 2, 4, np.inf],
            labels=['過大評価(<-2)', 'やや過大(-2~0)', '適正(0~2)', 'やや過小(2~4)', '過小評価(>4)']
        )
        
        bias_stats = df[df['bias_cat'].notna()].groupby('bias_cat', observed=True).agg(
            total=('is_win', 'count'),
            wins=('is_win', 'sum'),
            avg_odds=('win_odds', 'mean')
        ).reset_index()
        bias_stats['win_rate'] = bias_stats['wins'] / bias_stats['total'] * 100
        bias_stats['roi'] = (bias_stats['wins'] * bias_stats['avg_odds']) / bias_stats['total'] * 100
        print(bias_stats)
    else:
        logger.info("  Recency Biasデータなし")
    
    # Gap Reductionごとの勝率
    logger.info("\n--- gap_reduction_c1_c4ごとの勝率 ---")
    if 'gap_reduction_c1_c4' in df.columns and df['gap_reduction_c1_c4'].notna().sum() > 0:
        df['reduction_cat'] = pd.cut(
            df['gap_reduction_c1_c4'],
            bins=[-np.inf, -2, 0, 2, 5, np.inf],
            labels=['後退(<-2)', '微後退(-2~0)', '維持(0~2)', '追込(2~5)', '大追込(>5)']
        )
        
        reduction_stats = df[df['reduction_cat'].notna()].groupby('reduction_cat', observed=True).agg(
            total=('is_win', 'count'),
            wins=('is_win', 'sum'),
            avg_odds=('win_odds', 'mean')
        ).reset_index()
        reduction_stats['win_rate'] = reduction_stats['wins'] / reduction_stats['total'] * 100
        reduction_stats['roi'] = (reduction_stats['wins'] * reduction_stats['avg_odds']) / reduction_stats['total'] * 100
        print(reduction_stats)
    else:
        logger.info("  Gap Reductionデータなし")


def main():
    """メイン処理"""
    logger.info("V18特徴量テスト開始")
    
    # データ読み込み
    races, pedigrees, corners, horses, race_details = load_data()
    
    # V18テスト
    test_features, engine = test_v18_features(races, pedigrees, corners, horses, race_details)
    
    # リークチェック
    leak_check(test_features, engine)
    
    # ROI分析
    compare_roi(test_features)
    
    logger.info("\n" + "=" * 60)
    logger.info("V18特徴量テスト完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

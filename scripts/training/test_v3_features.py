"""
V3特徴量エンジニアのテストと評価
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import logging

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    data_dir = Path("keibaai/data")
    
    # ==========================================================
    # データ読み込み
    # ==========================================================
    logger.info("データ読み込み中...")
    
    races_df = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01') & (races_df['race_date'] <= '2025-12-31')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "parsed/parquet/corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "parsed/parquet/race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "parsed/parquet/returns/returns.parquet")
    
    logger.info(f"Races: {len(races_df):,}")
    logger.info(f"Corners: {len(corners_df):,}")
    logger.info(f"Race Details: {len(race_details_df):,}")
    logger.info(f"Returns: {len(returns_df):,}")
    
    # ==========================================================
    # 時系列分割
    # ==========================================================
    max_date = races_df['race_date'].max()
    test_start = max_date - pd.DateOffset(months=6)
    train_end = test_start - pd.DateOffset(months=1)
    
    train_df = races_df[races_df['race_date'] < train_end].copy()
    test_df = races_df[races_df['race_date'] >= test_start].copy()
    
    logger.info(f"\nTrain: {len(train_df):,} ({train_df['race_date'].min()} - {train_df['race_date'].max()})")
    logger.info(f"Test:  {len(test_df):,} ({test_df['race_date'].min()} - {test_df['race_date'].max()})")
    
    # ==========================================================
    # V3特徴量エンジニア
    # ==========================================================
    logger.info("\nV3特徴量エンジニア fit...")
    
    from keibaai.src.features.leak_free_feature_engineer_v3 import LeakFreeFeatureEngineerV3
    
    fe = LeakFreeFeatureEngineerV3()
    fe.fit(
        races_df=train_df,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df,
        race_details_df=race_details_df,
        returns_df=returns_df
    )
    
    logger.info("\nV3特徴量エンジニア transform...")
    train_features = fe.transform(train_df)
    test_features = fe.transform(test_df)
    
    feature_cols = fe.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_features.columns]
    logger.info(f"使用特徴量数: {len(feature_cols)}")
    
    # ==========================================================
    # 特徴量の統計確認
    # ==========================================================
    logger.info("\n" + "=" * 60)
    logger.info("特徴量統計")
    logger.info("=" * 60)
    
    for col in feature_cols:
        if col in test_features.columns:
            non_null = test_features[col].notna().sum()
            pct = non_null / len(test_features) * 100
            mean_val = test_features[col].mean() if test_features[col].dtype in ['float64', 'int64'] else 'N/A'
            logger.info(f"  {col}: {pct:.1f}% non-null, mean={mean_val:.4f}" if isinstance(mean_val, float) else f"  {col}: {pct:.1f}% non-null")
    
    # ==========================================================
    # モデル学習
    # ==========================================================
    logger.info("\nモデル学習...")
    import lightgbm as lgb
    
    train_features_sorted = train_features.sort_values('race_id')
    X_train = train_features_sorted[feature_cols].fillna(0)
    y_train = train_features_sorted['finish_position']
    groups_sorted = train_features_sorted['race_id']
    group_sizes = groups_sorted.value_counts().sort_index().values
    
    y_relevance = np.zeros(len(y_train))
    y_relevance[y_train.values == 1] = 5
    y_relevance[y_train.values == 2] = 4
    y_relevance[y_train.values == 3] = 3
    y_relevance[(y_train.values >= 4) & (y_train.values <= 5)] = 1
    
    model = lgb.LGBMRanker(
        objective='lambdarank',
        metric='ndcg',
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        random_state=42,
        n_jobs=-1,
        verbose=-1
    )
    
    model.fit(X_train, y_relevance, group=group_sizes)
    logger.info("学習完了")
    
    # ==========================================================
    # 予測とROI計算
    # ==========================================================
    X_test = test_features[feature_cols].fillna(0)
    test_features = test_features.copy()
    test_features['score'] = model.predict(X_test)
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False)
    
    # 払戻データをマージ
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho = tansho.rename(columns={'payout': 'tansho_payout'})
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    fukusho = fukusho.rename(columns={'payout': 'fukusho_payout'})
    
    test_features = test_features.merge(tansho, on=['race_id', 'horse_number'], how='left')
    test_features = test_features.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    
    # Top1戦略
    top1_df = test_features[test_features['rank'] == 1].copy()
    
    logger.info("\n" + "=" * 60)
    logger.info("ROI計算結果（実払戻ベース）")
    logger.info("=" * 60)
    
    # 全体ROI
    total_bet = len(top1_df) * 100
    total_tansho_payout = top1_df['tansho_payout'].fillna(0).sum()
    total_fukusho_payout = top1_df['fukusho_payout'].fillna(0).sum()
    
    logger.info(f"\n全体（{len(top1_df)}レース）:")
    logger.info(f"  単勝ROI: {total_tansho_payout / total_bet * 100:.1f}%")
    logger.info(f"  複勝ROI: {total_fukusho_payout / total_bet * 100:.1f}%")
    
    # オッズ帯別ROI
    odds_ranges = [(1, 3), (3, 5), (5, 10), (10, 20), (20, 50), (50, 80), (80, 500)]
    
    logger.info("\nオッズ帯別:")
    logger.info(f"{'オッズ帯':>12} {'件数':>6} {'単勝ROI':>10} {'複勝ROI':>10}")
    logger.info("-" * 45)
    
    for odds_min, odds_max in odds_ranges:
        subset = top1_df[(top1_df['win_odds'] >= odds_min) & (top1_df['win_odds'] < odds_max)]
        if len(subset) == 0:
            continue
        bet = len(subset) * 100
        tansho_roi = subset['tansho_payout'].fillna(0).sum() / bet * 100
        fukusho_roi = subset['fukusho_payout'].fillna(0).sum() / bet * 100
        status = "✓" if tansho_roi >= 100 or fukusho_roi >= 100 else ""
        logger.info(f"{odds_min:>4}-{odds_max:<4}倍: {len(subset):>5} {tansho_roi:>9.1f}% {fukusho_roi:>9.1f}% {status}")
    
    # ==========================================================
    # 特徴量重要度
    # ==========================================================
    logger.info("\n特徴量重要度 Top 15:")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    for i, row in importance.head(15).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")


if __name__ == '__main__':
    main()

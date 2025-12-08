"""
V5特徴量エンジニアのテスト
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
    
    logger.info("=" * 70)
    logger.info("V5特徴量エンジニアテスト（時点完全考慮版）")
    logger.info("=" * 70)
    
    # データ読み込み
    races_df = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01') & (races_df['race_date'] <= '2025-12-31')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "parsed/parquet/corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "parsed/parquet/race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "parsed/parquet/returns/returns.parquet")
    
    # 時系列分割
    max_date = races_df['race_date'].max()
    test_start = max_date - pd.DateOffset(months=6)
    train_end = test_start - pd.DateOffset(months=1)
    
    train_df = races_df[races_df['race_date'] < train_end].copy()
    test_df = races_df[races_df['race_date'] >= test_start].copy()
    
    logger.info(f"\nTrain: {len(train_df):,} ({train_df['race_date'].min()} - {train_df['race_date'].max()})")
    logger.info(f"Test:  {len(test_df):,} ({test_df['race_date'].min()} - {test_df['race_date'].max()})")
    
    # V5特徴量エンジニア
    from keibaai.src.features.leak_free_feature_engineer_v5 import LeakFreeFeatureEngineerV5
    
    logger.info("\nV5 fit...")
    fe = LeakFreeFeatureEngineerV5()
    fe.fit(
        races_df=train_df,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df,
        race_details_df=race_details_df,
        returns_df=returns_df
    )
    
    logger.info("\nV5 transform (train)...")
    train_features = fe.transform(train_df)
    
    logger.info("\nV5 transform (test)...")
    test_features = fe.transform(test_df)
    
    feature_cols = fe.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_features.columns]
    logger.info(f"使用特徴量数: {len(feature_cols)}")
    
    # モデル学習
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
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        random_state=42,
        verbose=-1
    )
    model.fit(X_train, y_relevance, group=group_sizes)
    logger.info("学習完了")
    
    # 予測
    train_features['score'] = model.predict(train_features[feature_cols].fillna(0))
    train_features['rank'] = train_features.groupby('race_id')['score'].rank(ascending=False)
    train_top1 = train_features[train_features['rank'] == 1]
    
    test_features['score'] = model.predict(test_features[feature_cols].fillna(0))
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False)
    test_top1 = test_features[test_features['rank'] == 1]
    
    # Train vs Test的中率
    train_hits = (train_top1['finish_position'] == 1).sum()
    test_hits = (test_top1['finish_position'] == 1).sum()
    train_rate = train_hits / len(train_top1) * 100
    test_rate = test_hits / len(test_top1) * 100
    
    logger.info("\n" + "=" * 70)
    logger.info("リークチェック: Train vs Test的中率")
    logger.info("=" * 70)
    logger.info(f"Train的中率: {train_rate:.1f}% ({train_hits}/{len(train_top1)})")
    logger.info(f"Test的中率:  {test_rate:.1f}% ({test_hits}/{len(test_top1)})")
    logger.info(f"差分: {train_rate - test_rate:.1f}%")
    
    if train_rate - test_rate < 10:
        logger.info("✓ リーク修正成功！差分 < 10%")
    elif train_rate - test_rate < 20:
        logger.info("△ 差分 10-20%、軽度の過学習")
    else:
        logger.warning("✗ まだ差分が大きい")
    
    # ROI計算
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho = tansho.rename(columns={'payout': 'tansho_payout'})
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    fukusho = fukusho.rename(columns={'payout': 'fukusho_payout'})
    
    test_top1 = test_top1.merge(tansho, on=['race_id', 'horse_number'], how='left')
    test_top1 = test_top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    
    logger.info("\n" + "=" * 70)
    logger.info("ROI結果（実払戻ベース）")
    logger.info("=" * 70)
    
    total_bet = len(test_top1) * 100
    tansho_roi = test_top1['tansho_payout'].fillna(0).sum() / total_bet * 100
    fukusho_roi = test_top1['fukusho_payout'].fillna(0).sum() / total_bet * 100
    
    logger.info(f"全体（{len(test_top1)}レース）:")
    logger.info(f"  単勝ROI: {tansho_roi:.1f}%")
    logger.info(f"  複勝ROI: {fukusho_roi:.1f}%")
    
    # オッズ帯別
    logger.info("\nオッズ帯別:")
    logger.info(f"{'オッズ帯':>12} {'件数':>6} {'単勝ROI':>10} {'複勝ROI':>10}")
    logger.info("-" * 45)
    
    for odds_min, odds_max in [(1, 3), (3, 5), (5, 10), (10, 20), (20, 50), (50, 80), (80, 500)]:
        subset = test_top1[(test_top1['win_odds'] >= odds_min) & (test_top1['win_odds'] < odds_max)]
        if len(subset) == 0:
            continue
        bet = len(subset) * 100
        t_roi = subset['tansho_payout'].fillna(0).sum() / bet * 100
        f_roi = subset['fukusho_payout'].fillna(0).sum() / bet * 100
        status = "✓" if t_roi >= 100 or f_roi >= 100 else ""
        logger.info(f"{odds_min:>4}-{odds_max:<4}倍: {len(subset):>5} {t_roi:>9.1f}% {f_roi:>9.1f}% {status}")


if __name__ == '__main__':
    main()

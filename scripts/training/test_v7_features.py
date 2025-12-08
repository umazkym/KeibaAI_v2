"""
V7特徴量エンジニアのテスト（真の時点考慮版）
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
    logger.info("V7特徴量エンジニアテスト（真の時点考慮版）")
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
    
    # 時系列分割（明確な期間指定）
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & (races_df['race_date'] < '2025-01-01')].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    logger.info(f"\nTrain: {len(train_df):,} ({train_df['race_date'].min()} - {train_df['race_date'].max()})")
    logger.info(f"Valid: {len(valid_df):,} ({valid_df['race_date'].min()} - {valid_df['race_date'].max()})")
    logger.info(f"Test:  {len(test_df):,} ({test_df['race_date'].min()} - {test_df['race_date'].max()})")
    
    # V7
    from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7
    
    logger.info("\nV7 fit...")
    fe = LeakFreeFeatureEngineerV7()
    fe.fit(
        races_df=train_df,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df,
        race_details_df=race_details_df,
        returns_df=returns_df
    )
    
    logger.info("\nV7 transform...")
    train_features = fe.transform(train_df)
    valid_features = fe.transform(valid_df)
    test_features = fe.transform(test_df)
    
    feature_cols = fe.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_features.columns]
    logger.info(f"使用特徴量数: {len(feature_cols)}")
    
    # モデル学習
    import lightgbm as lgb
    
    train_features_sorted = train_features.sort_values('race_id')
    X_train = train_features_sorted[feature_cols].fillna(0)
    y_train = train_features_sorted['finish_position']
    groups_train = train_features_sorted.groupby('race_id').size().values
    
    valid_features_sorted = valid_features.sort_values('race_id')
    X_valid = valid_features_sorted[feature_cols].fillna(0)
    y_valid = valid_features_sorted['finish_position']
    groups_valid = valid_features_sorted.groupby('race_id').size().values
    
    y_relevance_train = np.zeros(len(y_train))
    y_relevance_train[y_train.values == 1] = 5
    y_relevance_train[y_train.values == 2] = 4
    y_relevance_train[y_train.values == 3] = 3
    y_relevance_train[(y_train.values >= 4) & (y_train.values <= 5)] = 1
    
    y_relevance_valid = np.zeros(len(y_valid))
    y_relevance_valid[y_valid.values == 1] = 5
    y_relevance_valid[y_valid.values == 2] = 4
    y_relevance_valid[y_valid.values == 3] = 3
    y_relevance_valid[(y_valid.values >= 4) & (y_valid.values <= 5)] = 1
    
    model = lgb.LGBMRanker(
        objective='lambdarank',
        n_estimators=3000,
        learning_rate=0.005,      # 極めて低く
        max_depth=2,              # 極浅
        num_leaves=4,             # 極少
        min_child_samples=200,    # 極大
        reg_alpha=3.0,            # 極強L1
        reg_lambda=5.0,           # 極強L2
        subsample=0.5,            # 半分
        colsample_bytree=0.5,
        max_bin=63,               # ビン数極限
        min_split_gain=0.1,       # 分割閾値
        random_state=42,
        verbose=-1
    )
    
    model.fit(
        X_train, y_relevance_train, 
        group=groups_train,
        eval_set=[(X_valid, y_relevance_valid)],
        eval_group=[groups_valid],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    logger.info(f"学習完了 (best: {model.best_iteration_})")
    
    # 予測
    train_features['score'] = model.predict(train_features[feature_cols].fillna(0))
    train_features['rank'] = train_features.groupby('race_id')['score'].rank(ascending=False)
    train_top1 = train_features[train_features['rank'] == 1]
    
    test_features['score'] = model.predict(test_features[feature_cols].fillna(0))
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False)
    test_top1 = test_features[test_features['rank'] == 1]
    
    # 的中率（NAを除外して計算）
    train_hit = train_top1['finish_position'].dropna() == 1
    test_hit = test_top1['finish_position'].dropna() == 1
    
    train_rate = train_hit.mean() * 100 if len(train_hit) > 0 else 0
    test_rate = test_hit.mean() * 100 if len(test_hit) > 0 else 0
    
    logger.info("\n" + "=" * 70)
    logger.info("リークチェック: Train vs Test 的中率")
    logger.info("=" * 70)
    logger.info(f"Train的中率: {train_rate:.1f}% (n={len(train_hit)})")
    logger.info(f"Test的中率:  {test_rate:.1f}% (n={len(test_hit)})")
    logger.info(f"差分: {train_rate - test_rate:.1f}%")
    
    if abs(train_rate - test_rate) < 5:
        logger.info("✓ リークなし！差分 < 5%")
    elif abs(train_rate - test_rate) < 10:
        logger.info("✓ 軽微、差分 < 10%")
    elif abs(train_rate - test_rate) < 20:
        logger.info("△ 許容範囲、差分 < 20%")
    else:
        logger.warning("✗ 過学習傾向")
    
    # ROI
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho = tansho.rename(columns={'payout': 'tansho_payout'})
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    fukusho = fukusho.rename(columns={'payout': 'fukusho_payout'})
    
    test_top1 = test_top1.merge(tansho, on=['race_id', 'horse_number'], how='left')
    test_top1 = test_top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    
    logger.info("\n" + "=" * 70)
    logger.info("ROI結果")
    logger.info("=" * 70)
    
    total_bet = len(test_top1) * 100
    tansho_roi = test_top1['tansho_payout'].fillna(0).sum() / total_bet * 100
    fukusho_roi = test_top1['fukusho_payout'].fillna(0).sum() / total_bet * 100
    
    logger.info(f"全体（{len(test_top1)}レース）:")
    logger.info(f"  単勝ROI: {tansho_roi:.1f}%")
    logger.info(f"  複勝ROI: {fukusho_roi:.1f}%")
    
    logger.info("\nオッズ帯別:")
    for odds_min, odds_max in [(1, 3), (3, 5), (5, 10), (10, 20), (20, 50), (50, 80), (80, 500)]:
        subset = test_top1[(test_top1['win_odds'] >= odds_min) & (test_top1['win_odds'] < odds_max)]
        if len(subset) == 0:
            continue
        bet = len(subset) * 100
        t_roi = subset['tansho_payout'].fillna(0).sum() / bet * 100
        f_roi = subset['fukusho_payout'].fillna(0).sum() / bet * 100
        status = "✓" if t_roi >= 100 or f_roi >= 100 else ""
        logger.info(f"  {odds_min:>3}-{odds_max:<3}倍: n={len(subset):>4} 単勝{t_roi:>6.1f}% 複勝{f_roi:>6.1f}% {status}")
    
    # 特徴量重要度
    logger.info("\n特徴量重要度 Top 10:")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    for i, row in importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.0f}")


if __name__ == '__main__':
    main()

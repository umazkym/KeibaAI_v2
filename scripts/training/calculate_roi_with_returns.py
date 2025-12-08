"""
実際の払戻データ（returns.parquet）を使用したROI計算

returns.parquetから単勝・複勝の実際の払戻金額を取得し、
正確なROIを計算する。
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import logging

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer import LeakFreeFeatureEngineer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_returns(data_dir: Path) -> pd.DataFrame:
    """払戻データを読み込み"""
    returns_path = data_dir / "parsed/parquet/returns/returns.parquet"
    returns_df = pd.read_parquet(returns_path)
    
    # 単勝払戻
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho = tansho.rename(columns={'payout': 'tansho_payout'})
    
    # 複勝払戻
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    fukusho = fukusho.rename(columns={'payout': 'fukusho_payout'})
    
    return tansho, fukusho


def main():
    data_dir = Path("keibaai/data")
    
    # =========================================================================
    # データ読み込み
    # =========================================================================
    logger.info("データ読み込み中...")
    
    races_df = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01') & (races_df['race_date'] <= '2024-12-31')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    # 払戻データ
    tansho_df, fukusho_df = load_returns(data_dir)
    logger.info(f"単勝払戻: {len(tansho_df):,}件")
    logger.info(f"複勝払戻: {len(fukusho_df):,}件")
    
    # =========================================================================
    # 時系列分割
    # =========================================================================
    max_date = races_df['race_date'].max()
    test_start = max_date - pd.DateOffset(months=12)
    train_end = test_start - pd.DateOffset(months=1)
    
    train_df = races_df[races_df['race_date'] < train_end].copy()
    test_df = races_df[races_df['race_date'] >= test_start].copy()
    
    logger.info(f"\nTrain: {len(train_df):,} records ({train_df['race_date'].min()} - {train_df['race_date'].max()})")
    logger.info(f"Test:  {len(test_df):,} records ({test_df['race_date'].min()} - {test_df['race_date'].max()})")
    
    # =========================================================================
    # 特徴量生成
    # =========================================================================
    logger.info("\n特徴量生成中...")
    feature_engineer = LeakFreeFeatureEngineer()
    feature_engineer.fit(train_df, pedigrees_df)
    
    train_features = feature_engineer.transform(train_df)
    test_features = feature_engineer.transform(test_df)
    
    feature_cols = feature_engineer.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_features.columns]
    
    # =========================================================================
    # モデル学習
    # =========================================================================
    logger.info("\nモデル学習中...")
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
        max_depth=5,
        num_leaves=31,
        random_state=42,
        n_jobs=-1,
        verbose=-1
    )
    
    model.fit(X_train, y_relevance, group=group_sizes)
    logger.info("学習完了")
    
    # =========================================================================
    # テスト予測
    # =========================================================================
    X_test = test_features[feature_cols].fillna(0)
    test_features = test_features.copy()
    test_features['score'] = model.predict(X_test)
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False)
    
    # 人気計算
    if 'popularity' not in test_features.columns:
        test_features['popularity'] = test_features.groupby('race_id')['win_odds'].rank()
    
    # 払戻データをマージ
    test_features = test_features.merge(
        tansho_df, 
        on=['race_id', 'horse_number'], 
        how='left'
    )
    test_features = test_features.merge(
        fukusho_df, 
        on=['race_id', 'horse_number'], 
        how='left'
    )
    
    logger.info(f"\n払戻データとのマッチ: 単勝 {test_features['tansho_payout'].notna().sum():,}件, 複勝 {test_features['fukusho_payout'].notna().sum():,}件")
    
    # =========================================================================
    # Top1戦略のROI計算（実払戻ベース）
    # =========================================================================
    logger.info("\n" + "=" * 70)
    logger.info("実払戻データによるROI計算")
    logger.info("=" * 70)
    
    top1_df = test_features[test_features['rank'] == 1].copy()
    
    # オッズ帯別の集計
    odds_ranges = [
        (1, 3), (3, 5), (5, 10), (10, 20), (20, 30), (30, 50), (50, 80), (80, 150), (150, 500)
    ]
    
    logger.info("\n[1] 単勝戦略（実払戻ベース）:")
    logger.info("-" * 70)
    logger.info(f"{'オッズ帯':>12} {'件数':>6} {'的中':>4} {'的中率':>8} {'投資':>12} {'払戻':>12} {'ROI':>10}")
    logger.info("-" * 70)
    
    for odds_min, odds_max in odds_ranges:
        subset = top1_df[(top1_df['win_odds'] >= odds_min) & (top1_df['win_odds'] < odds_max)]
        if len(subset) == 0:
            continue
        
        bet_amount = len(subset) * 100
        # 実払戻金額の合計（的中した馬の払戻のみが有効）
        payout = subset['tansho_payout'].fillna(0).sum()
        hits = (subset['tansho_payout'].notna() & (subset['tansho_payout'] > 0)).sum()
        hit_rate = hits / len(subset) * 100
        roi = payout / bet_amount * 100
        
        status = "✓" if roi >= 100 else ""
        logger.info(f"{odds_min:>4}-{odds_max:<4}倍: {len(subset):>5} {hits:>4} {hit_rate:>7.1f}% ¥{bet_amount:>10,} ¥{int(payout):>10,} {roi:>8.1f}% {status}")
    
    # 全体
    total_bet = len(top1_df) * 100
    total_payout = top1_df['tansho_payout'].fillna(0).sum()
    total_hits = (top1_df['tansho_payout'].notna() & (top1_df['tansho_payout'] > 0)).sum()
    total_roi = total_payout / total_bet * 100
    logger.info("-" * 70)
    logger.info(f"{'全体':>12}: {len(top1_df):>5} {total_hits:>4} {total_hits/len(top1_df)*100:>7.1f}% ¥{total_bet:>10,} ¥{int(total_payout):>10,} {total_roi:>8.1f}%")
    
    # =========================================================================
    # 複勝戦略（実払戻ベース）
    # =========================================================================
    logger.info("\n\n[2] 複勝戦略（実払戻ベース）:")
    logger.info("-" * 70)
    logger.info(f"{'オッズ帯':>12} {'件数':>6} {'的中':>4} {'的中率':>8} {'投資':>12} {'払戻':>12} {'ROI':>10}")
    logger.info("-" * 70)
    
    for odds_min, odds_max in odds_ranges:
        subset = top1_df[(top1_df['win_odds'] >= odds_min) & (top1_df['win_odds'] < odds_max)]
        if len(subset) == 0:
            continue
        
        bet_amount = len(subset) * 100
        payout = subset['fukusho_payout'].fillna(0).sum()
        hits = (subset['fukusho_payout'].notna() & (subset['fukusho_payout'] > 0)).sum()
        hit_rate = hits / len(subset) * 100
        roi = payout / bet_amount * 100
        
        status = "✓" if roi >= 100 else ""
        logger.info(f"{odds_min:>4}-{odds_max:<4}倍: {len(subset):>5} {hits:>4} {hit_rate:>7.1f}% ¥{bet_amount:>10,} ¥{int(payout):>10,} {roi:>8.1f}% {status}")
    
    # 全体
    total_payout_f = top1_df['fukusho_payout'].fillna(0).sum()
    total_hits_f = (top1_df['fukusho_payout'].notna() & (top1_df['fukusho_payout'] > 0)).sum()
    total_roi_f = total_payout_f / total_bet * 100
    logger.info("-" * 70)
    logger.info(f"{'全体':>12}: {len(top1_df):>5} {total_hits_f:>4} {total_hits_f/len(top1_df)*100:>7.1f}% ¥{total_bet:>10,} ¥{int(total_payout_f):>10,} {total_roi_f:>8.1f}%")
    
    # =========================================================================
    # 月別推移（50倍以上、複勝）
    # =========================================================================
    logger.info("\n\n[3] 50倍以上 複勝戦略 - 月別推移:")
    logger.info("-" * 50)
    
    test_features['month'] = test_features['race_date'].dt.to_period('M')
    high_odds_df = top1_df[top1_df['win_odds'] >= 50].copy()
    high_odds_df['month'] = high_odds_df['race_date'].dt.to_period('M') if 'race_date' in high_odds_df.columns else test_features.loc[high_odds_df.index, 'month']
    
    for month in sorted(high_odds_df['month'].unique()):
        month_data = high_odds_df[high_odds_df['month'] == month]
        bet = len(month_data) * 100
        payout = month_data['fukusho_payout'].fillna(0).sum()
        hits = (month_data['fukusho_payout'].notna() & (month_data['fukusho_payout'] > 0)).sum()
        roi = payout / bet * 100 if bet > 0 else 0
        status = "✓" if roi >= 100 else ""
        logger.info(f"  {month}: {len(month_data):>3}件, 的中{hits:>2}, ROI {roi:>6.1f}% {status}")
    
    # 全体集計
    total_bet_h = len(high_odds_df) * 100
    total_payout_h = high_odds_df['fukusho_payout'].fillna(0).sum()
    total_hits_h = (high_odds_df['fukusho_payout'].notna() & (high_odds_df['fukusho_payout'] > 0)).sum()
    total_roi_h = total_payout_h / total_bet_h * 100 if total_bet_h > 0 else 0
    
    logger.info("-" * 50)
    logger.info(f"合計: {len(high_odds_df)}件, 的中{total_hits_h}, ROI {total_roi_h:.1f}%")
    
    # =========================================================================
    # サマリー
    # =========================================================================
    logger.info("\n\n" + "=" * 70)
    logger.info("サマリー（実払戻ベース）")
    logger.info("=" * 70)
    logger.info(f"Top1単勝全体ROI: {total_roi:.1f}%")
    logger.info(f"Top1複勝全体ROI: {total_roi_f:.1f}%")
    logger.info(f"50倍以上複勝ROI: {total_roi_h:.1f}%")


if __name__ == '__main__':
    main()

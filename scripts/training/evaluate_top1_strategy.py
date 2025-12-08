"""
Hybrid AI Top1戦略評価スクリプト

モデルが予測した各レースの1位馬（能力スコア最大）に単勝ベットする単純戦略。
EV計算なしで純粋なモデル精度を評価。
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


def main():
    data_dir = Path("keibaai/data")
    
    # データ読み込み
    logger.info("データ読み込み中...")
    races_df = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01') & (races_df['race_date'] <= '2024-12-31')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    logger.info(f"総レコード数: {len(races_df):,}")
    
    # 時系列分割
    max_date = races_df['race_date'].max()
    test_start = max_date - pd.DateOffset(months=6)
    train_end = test_start - pd.DateOffset(months=2)
    
    train_df = races_df[races_df['race_date'] < train_end].copy()
    test_df = races_df[races_df['race_date'] >= test_start].copy()
    
    logger.info(f"Train: {len(train_df):,} records ({train_df['race_date'].min()} - {train_df['race_date'].max()})")
    logger.info(f"Test: {len(test_df):,} records ({test_df['race_date'].min()} - {test_df['race_date'].max()})")
    
    # 特徴量生成
    logger.info("\n特徴量生成中...")
    feature_engineer = LeakFreeFeatureEngineer()
    feature_engineer.fit(train_df, pedigrees_df)
    
    train_features = feature_engineer.transform(train_df)
    test_features = feature_engineer.transform(test_df)
    
    feature_cols = feature_engineer.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_features.columns]
    logger.info(f"使用特徴量数: {len(feature_cols)}")
    
    # モデル学習
    logger.info("\nモデル学習中...")
    import lightgbm as lgb
    
    X_train = train_features[feature_cols].fillna(0)
    y_train = train_features['finish_position']
    groups_train = train_features['race_id']
    
    # グループサイズ計算
    train_features_sorted = train_features.sort_values('race_id')
    X_train_sorted = train_features_sorted[feature_cols].fillna(0)
    y_train_sorted = train_features_sorted['finish_position']
    groups_sorted = train_features_sorted['race_id']
    group_sizes = groups_sorted.value_counts().sort_index().values
    
    # 着順ラベル（小さいほど良い = 5, 4, 3, 2, 1...）
    y_relevance = np.zeros(len(y_train_sorted))
    y_relevance[y_train_sorted.values == 1] = 5
    y_relevance[y_train_sorted.values == 2] = 4
    y_relevance[y_train_sorted.values == 3] = 3
    y_relevance[(y_train_sorted.values >= 4) & (y_train_sorted.values <= 5)] = 1
    
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
    
    model.fit(X_train_sorted, y_relevance, group=group_sizes)
    logger.info("学習完了")
    
    # テストデータで予測
    logger.info("\n評価中...")
    X_test = test_features[feature_cols].fillna(0)
    test_features = test_features.copy()
    test_features['score'] = model.predict(X_test)
    
    # 各レースでTop1を選択
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False)
    top1_df = test_features[test_features['rank'] == 1].copy()
    
    # 評価
    total_bet = len(top1_df) * 100  # 各レース100円
    total_payout = 0
    n_hits = 0
    favorite_hits = 0
    
    # 人気計算
    if 'popularity' not in top1_df.columns:
        test_features['popularity'] = test_features.groupby('race_id')['win_odds'].rank()
        top1_df = test_features[test_features['rank'] == 1].copy()
    
    for _, row in top1_df.iterrows():
        if row['finish_position'] == 1:
            total_payout += int(100 * row['win_odds'])
            n_hits += 1
        if row['popularity'] == 1:
            favorite_hits += 1
    
    roi = (total_payout / total_bet * 100) if total_bet > 0 else 0
    hit_rate = (n_hits / len(top1_df) * 100) if len(top1_df) > 0 else 0
    fav_rate = (favorite_hits / len(top1_df) * 100) if len(top1_df) > 0 else 0
    
    logger.info("\n" + "=" * 60)
    logger.info("Top1 Strategy Results (Simple Evaluation)")
    logger.info("=" * 60)
    logger.info(f"レース数: {len(top1_df):,}")
    logger.info(f"総投資: ¥{total_bet:,}")
    logger.info(f"総払戻: ¥{total_payout:,}")
    logger.info(f"ROI: {roi:.1f}%")
    logger.info(f"的中率: {hit_rate:.1f}%")
    logger.info(f"1番人気選択率: {fav_rate:.1f}%")
    
    # 人気別分析
    logger.info("\n人気別の予測精度:")
    for pop in range(1, 6):
        pop_df = top1_df[top1_df['popularity'] == pop]
        if len(pop_df) > 0:
            pop_hits = (pop_df['finish_position'] == 1).sum()
            pop_rate = pop_hits / len(pop_df) * 100
            logger.info(f"  {pop}番人気選択時: {len(pop_df)}件, 的中率 {pop_rate:.1f}%")
    
    # オッズ帯別分析
    logger.info("\nオッズ帯別の的中率:")
    for odds_min, odds_max in [(1, 3), (3, 5), (5, 10), (10, 20), (20, 50), (50, 100)]:
        odds_df = top1_df[(top1_df['win_odds'] >= odds_min) & (top1_df['win_odds'] < odds_max)]
        if len(odds_df) > 0:
            odds_hits = (odds_df['finish_position'] == 1).sum()
            odds_rate = odds_hits / len(odds_df) * 100
            avg_odds = odds_df['win_odds'].mean()
            odds_roi = (odds_df[odds_df['finish_position'] == 1]['win_odds'].sum() * 100) / (len(odds_df) * 100) * 100 if len(odds_df) > 0 else 0
            logger.info(f"  {odds_min}-{odds_max}倍: {len(odds_df)}件, 的中率 {odds_rate:.1f}%, ROI {odds_roi:.1f}%")


if __name__ == '__main__':
    main()

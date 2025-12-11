# -*- coding: utf-8 -*-
"""
Phase 2: 馬連・馬単向けLambdaRankモデル

18_improvement_roadmap.md より:
- 関連度ラベル: [1着=5, 2着=4, 3着=3, 4-5着=1, 他=0]
- 極端な正則化: max_depth=2, num_leaves=4
- 馬連・馬単ROI検証

【馬連ROI計算】
- 各レースでTop2を選択
- (1着馬, 2着馬)の組み合わせが当たればhit
- returns.parquetから払い戻し金額を取得
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import sys
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns


def calc_single_roi(df):
    """単勝ROI計算"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def calc_umaren_roi(df, returns_df):
    """
    馬連ROI計算
    
    各レースでTop2を選びそれ馬連として購入
    的中時の払い戻しはreturns_dfから取得
    """
    df = df.copy()
    
    # スコアでランク
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    # Top2を取得
    top2 = df[df['rank'] <= 2].copy()
    top2 = top2.sort_values(['race_id', 'rank'])
    
    # レースごとにTop2の馬番を取得
    bets = top2.groupby('race_id').agg({
        'horse_number': list,
        'finish_position': list
    }).reset_index()
    
    # 馬番セットを作成（順不同）
    bets['bet_set'] = bets['horse_number'].apply(lambda x: tuple(sorted(x)) if len(x) == 2 else None)
    bets = bets[bets['bet_set'].notna()]
    
    # 馬連払い戻しを取得
    umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    
    # 馬連のhorse_1, horse_2で馬番の組み合わせを作成
    umaren = umaren.dropna(subset=['horse_1', 'horse_2'])
    umaren['winner_set'] = umaren.apply(
        lambda x: tuple(sorted([int(x['horse_1']), int(x['horse_2'])])),
        axis=1
    )
    
    # race_idとwinner_setでインデックス化
    umaren_dict = dict(zip(
        zip(umaren['race_id'], umaren['winner_set']),
        umaren['payout']
    ))
    
    # ベットの払い戻しを計算
    bets['payout'] = bets.apply(
        lambda x: umaren_dict.get((x['race_id'], x['bet_set']), 0),
        axis=1
    )
    
    # ROI計算
    total_bet = len(bets) * 100  # 各レース100円
    total_payout = bets['payout'].sum()
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_rate = (bets['payout'] > 0).sum() / len(bets) * 100 if len(bets) > 0 else 0
    
    return roi, hit_rate, len(bets)


def main():
    logger.info("=" * 60)
    logger.info("Phase 2: 馬連・馬単向けLambdaRankモデル")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 馬連払戻データ確認
    umaren = returns[returns['bet_type'] == 'umaren']
    logger.info(f"  馬連払戻データ: {len(umaren):,}件")
    
    if len(umaren) > 0:
        logger.info(f"  馬連払戻範囲: {umaren['payout'].min():.0f} ~ {umaren['payout'].max():.0f}円")
        logger.info(f"  馬連払戻平均: {umaren['payout'].mean():.0f}円")
    
    # 期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # 特徴量生成
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    
    # ========== モデル1: Binary Classification (V15) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("モデル1: Binary Classification (V15)")
    logger.info("=" * 60)
    
    y_train_binary = (train_f['finish_position'] == 1).astype(int)
    
    params_binary = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds_binary = lgb.Dataset(X_train, y_train_binary)
    model_binary = lgb.train(params_binary, train_ds_binary, num_boost_round=200)
    
    test_f['pred'] = model_binary.predict(X_test)
    
    single_roi, single_hit = calc_single_roi(test_f)
    umaren_roi, umaren_hit, umaren_n = calc_umaren_roi(test_f, returns)
    
    logger.info(f"  単勝ROI: {single_roi:.1f}%, 的中率: {single_hit:.1f}%")
    logger.info(f"  馬連ROI: {umaren_roi:.1f}%, 的中率: {umaren_hit:.1f}%, ベット数: {umaren_n:,}")
    
    # ========== モデル2: LambdaRank ロードマップ設定 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("モデル2: LambdaRank (関連度ラベル)")
    logger.info("=" * 60)
    
    # 関連度ラベル: [1着=5, 2着=4, 3着=3, 4-5着=1, 他=0]
    def relevance_label(pos):
        if pos == 1:
            return 5
        elif pos == 2:
            return 4
        elif pos == 3:
            return 3
        elif pos <= 5:
            return 1
        else:
            return 0
    
    y_train_rank = train_f['finish_position'].apply(relevance_label)
    
    # グループ（レースごと）
    train_groups = train_f.groupby('race_id').size().values
    test_groups = test_f.groupby('race_id').size().values
    
    params_rank = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'ndcg_eval_at': [1, 2, 3],
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 4,  # 極端な正則化
        'max_depth': 2,   # 極端な正則化
        'min_child_samples': 100,
        'reg_alpha': 5.0,
        'reg_lambda': 10.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds_rank = lgb.Dataset(X_train, y_train_rank, group=train_groups)
    model_rank = lgb.train(params_rank, train_ds_rank, num_boost_round=200)
    
    test_f['pred'] = model_rank.predict(X_test)
    
    single_roi_rank, single_hit_rank = calc_single_roi(test_f)
    umaren_roi_rank, umaren_hit_rank, umaren_n_rank = calc_umaren_roi(test_f, returns)
    
    logger.info(f"  単勝ROI: {single_roi_rank:.1f}%, 的中率: {single_hit_rank:.1f}%")
    logger.info(f"  馬連ROI: {umaren_roi_rank:.1f}%, 的中率: {umaren_hit_rank:.1f}%, ベット数: {umaren_n_rank:,}")
    
    # ========== 比較 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較")
    logger.info("=" * 60)
    
    logger.info(f"                   Binary      LambdaRank   改善")
    logger.info(f"  単勝ROI:        {single_roi:7.1f}%    {single_roi_rank:7.1f}%   {single_roi_rank - single_roi:+.1f}%")
    logger.info(f"  馬連ROI:        {umaren_roi:7.1f}%    {umaren_roi_rank:7.1f}%   {umaren_roi_rank - umaren_roi:+.1f}%")
    logger.info(f"  馬連的中率:      {umaren_hit:7.1f}%    {umaren_hit_rank:7.1f}%   {umaren_hit_rank - umaren_hit:+.1f}%")
    
    return model_binary, model_rank


if __name__ == "__main__":
    main()

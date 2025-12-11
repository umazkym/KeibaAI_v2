# -*- coding: utf-8 -*-
"""
LambdaRankモデルでの単勝オッズ加重学習

【目的】
- V15と同じLambdaRankで加重学習を試す
- Binary ClassificationではなくRankingで評価

【期待するベースライン】
- V15 LambdaRank: Test ROI 91.8%
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
    
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[races_df['race_date'] >= '2020-01-01'].dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    return races_df, pedigrees_df, corners_df, race_details_df, returns_df


def create_relevance_label(finish_pos):
    """関連度ラベル作成"""
    if finish_pos == 1: return 5
    elif finish_pos == 2: return 4
    elif finish_pos == 3: return 3
    elif finish_pos <= 5: return 1
    else: return 0


def calculate_roi(df, pred_col='score'):
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1]
    
    hits = top1[top1['finish_position'] == 1]
    
    total_bets = len(top1) * 100
    total_payout = hits['win_odds'].sum() * 100
    
    hit_rate = len(hits) / len(top1) * 100 if len(top1) > 0 else 0
    roi = total_payout / total_bets * 100 if total_bets > 0 else 0
    
    return hit_rate, roi


def main():
    logger.info("=" * 60)
    logger.info("LambdaRank + オッズ加重学習")
    logger.info("=" * 60)
    
    races_df, pedigrees_df, corners_df, race_details_df, returns_df = load_data()
    
    train_end = '2023-01-01'
    valid_end = '2024-01-01'
    
    train_mask = races_df['race_date'] < train_end
    valid_mask = (races_df['race_date'] >= train_end) & (races_df['race_date'] < valid_end)
    test_mask = races_df['race_date'] >= valid_end
    
    train_df = races_df[train_mask].copy()
    valid_df = races_df[valid_mask].copy()
    test_df = races_df[test_mask].copy()
    
    logger.info(f"  Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
    
    # 特徴量生成
    logger.info("特徴量生成中...")
    fe = LeakFreeFeatureEngineerV15()
    fe.fit(train_df, pedigrees_df, corners_df, race_details_df, returns_df)
    
    train_features = fe.transform(train_df)
    valid_features = fe.transform(valid_df)
    test_features = fe.transform(test_df)
    
    feature_cols = fe.get_feature_columns()
    
    # 関連度ラベル
    train_features['relevance'] = train_features['finish_position'].apply(create_relevance_label)
    valid_features['relevance'] = valid_features['finish_position'].apply(create_relevance_label)
    test_features['relevance'] = test_features['finish_position'].apply(create_relevance_label)
    
    # グループ
    train_sorted = train_features.sort_values('race_id')
    valid_sorted = valid_features.sort_values('race_id')
    test_sorted = test_features.sort_values('race_id')
    
    groups_train = train_sorted.groupby('race_id').size().values
    groups_valid = valid_sorted.groupby('race_id').size().values
    groups_test = test_sorted.groupby('race_id').size().values
    
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['relevance']
    X_valid = valid_sorted[feature_cols].fillna(0)
    y_valid = valid_sorted['relevance']
    X_test = test_sorted[feature_cols].fillna(0)
    
    # sample_weight
    train_sorted['sample_weight'] = np.log1p(train_sorted['win_odds'])
    
    # ベースライン LambdaRank
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. ベースライン LambdaRank（V15相当）")
    logger.info("=" * 60)
    
    base_params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'max_depth': 3,
        'num_leaves': 15,
        'min_child_samples': 150,
        'learning_rate': 0.02,
        'reg_alpha': 2.0,
        'reg_lambda': 3.0,
        'subsample': 0.6,
        'colsample_bytree': 0.6,
        'random_state': 42,
        'n_jobs': -1,
        'verbose': -1
    }
    
    train_ds_base = lgb.Dataset(X_train, y_train, group=groups_train)
    valid_ds_base = lgb.Dataset(X_valid, y_valid, group=groups_valid, reference=train_ds_base)
    
    base_model = lgb.train(
        base_params,
        train_ds_base,
        num_boost_round=2000,
        valid_sets=[valid_ds_base],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(500)]
    )
    
    logger.info(f"  Best iteration: {base_model.best_iteration}")
    
    train_sorted['score_base'] = base_model.predict(X_train)
    valid_sorted['score_base'] = base_model.predict(X_valid)
    test_sorted['score_base'] = base_model.predict(X_test)
    
    base_train_hit, base_train_roi = calculate_roi(train_sorted, 'score_base')
    base_valid_hit, base_valid_roi = calculate_roi(valid_sorted, 'score_base')
    base_test_hit, base_test_roi = calculate_roi(test_sorted, 'score_base')
    
    logger.info(f"  Train: 的中率={base_train_hit:.1f}%, ROI={base_train_roi:.1f}%")
    logger.info(f"  Valid: 的中率={base_valid_hit:.1f}%, ROI={base_valid_roi:.1f}%")
    logger.info(f"  Test:  的中率={base_test_hit:.1f}%, ROI={base_test_roi:.1f}%")
    
    # オッズ加重 LambdaRank
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. オッズ加重 LambdaRank")
    logger.info("=" * 60)
    
    train_ds_weighted = lgb.Dataset(X_train, y_train, group=groups_train, weight=train_sorted['sample_weight'])
    valid_ds_weighted = lgb.Dataset(X_valid, y_valid, group=groups_valid, reference=train_ds_weighted)
    
    weighted_model = lgb.train(
        base_params,
        train_ds_weighted,
        num_boost_round=2000,
        valid_sets=[valid_ds_weighted],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(500)]
    )
    
    logger.info(f"  Best iteration: {weighted_model.best_iteration}")
    
    train_sorted['score_weighted'] = weighted_model.predict(X_train)
    valid_sorted['score_weighted'] = weighted_model.predict(X_valid)
    test_sorted['score_weighted'] = weighted_model.predict(X_test)
    
    weighted_train_hit, weighted_train_roi = calculate_roi(train_sorted, 'score_weighted')
    weighted_valid_hit, weighted_valid_roi = calculate_roi(valid_sorted, 'score_weighted')
    weighted_test_hit, weighted_test_roi = calculate_roi(test_sorted, 'score_weighted')
    
    logger.info(f"  Train: 的中率={weighted_train_hit:.1f}%, ROI={weighted_train_roi:.1f}%")
    logger.info(f"  Valid: 的中率={weighted_valid_hit:.1f}%, ROI={weighted_valid_roi:.1f}%")
    logger.info(f"  Test:  的中率={weighted_test_hit:.1f}%, ROI={weighted_test_roi:.1f}%")
    
    # 比較
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較")
    logger.info("=" * 60)
    
    logger.info(f"                       ベースライン    オッズ加重    改善幅")
    logger.info(f"  Test ROI:            {base_test_roi:7.1f}%       {weighted_test_roi:7.1f}%     {weighted_test_roi - base_test_roi:+.1f}%")
    logger.info(f"  Test 的中率:         {base_test_hit:7.1f}%       {weighted_test_hit:7.1f}%     {weighted_test_hit - base_test_hit:+.1f}%")
    logger.info(f"  Train-Test Gap:      {base_train_roi - base_test_roi:7.1f}%       {weighted_train_roi - weighted_test_roi:7.1f}%")
    
    return base_model, weighted_model


if __name__ == "__main__":
    main()

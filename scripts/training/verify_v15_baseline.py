# -*- coding: utf-8 -*-
"""
V15相当の正しい期間設定でのベースライン検証

【期間設定】
- Train: 2020-01-01 ~ 2024-01-01（学習・特徴量fit）
- Test: 2024-01-01 ~ （評価のみ）

これがV15公式値91.8%の再現を目指す
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
    """関連度ラベル"""
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


def calculate_place_roi(df, pred_col, returns_df):
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1][['race_id', 'horse_number', 'finish_position']].copy()
    top1 = top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    
    total_bets = len(top1) * 100
    total_payout = top1['payout'].fillna(0).sum()
    
    hit_rate = top1['payout'].notna().sum() / len(top1) * 100 if len(top1) > 0 else 0
    roi = total_payout / total_bets * 100 if total_bets > 0 else 0
    
    return hit_rate, roi


def main():
    logger.info("=" * 60)
    logger.info("V15ベースライン再現テスト（正しい期間設定）")
    logger.info("=" * 60)
    
    races_df, pedigrees_df, corners_df, race_details_df, returns_df = load_data()
    
    # V15相当の期間設定: Train全体でfit, Testで評価
    train_end = '2024-01-01'
    
    train_mask = races_df['race_date'] < train_end
    test_mask = races_df['race_date'] >= train_end
    
    train_df = races_df[train_mask].copy()
    test_df = races_df[test_mask].copy()
    
    logger.info(f"  Train: {len(train_df):,}件 (~{train_end})")
    logger.info(f"  Test:  {len(test_df):,}件 ({train_end}~)")
    logger.info(f"  Trainレース数: {train_df['race_id'].nunique():,}")
    logger.info(f"  Testレース数: {test_df['race_id'].nunique():,}")
    
    # 特徴量生成（Train全体でfit）
    logger.info("")
    logger.info("特徴量生成中...")
    fe = LeakFreeFeatureEngineerV15()
    fe.fit(train_df, pedigrees_df, corners_df, race_details_df, returns_df)
    
    train_features = fe.transform(train_df)
    test_features = fe.transform(test_df)
    
    feature_cols = fe.get_feature_columns()
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    # 関連度ラベル
    train_features['relevance'] = train_features['finish_position'].apply(create_relevance_label)
    test_features['relevance'] = test_features['finish_position'].apply(create_relevance_label)
    
    # is_place も追加
    train_features['is_place'] = (train_features['finish_position'] <= 3).astype(int)
    test_features['is_place'] = (test_features['finish_position'] <= 3).astype(int)
    
    # グループ
    train_sorted = train_features.sort_values('race_id')
    test_sorted = test_features.sort_values('race_id')
    
    groups_train = train_sorted.groupby('race_id').size().values
    groups_test = test_sorted.groupby('race_id').size().values
    
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['relevance']
    X_test = test_sorted[feature_cols].fillna(0)
    
    # LambdaRank（V15相当パラメータ）
    logger.info("")
    logger.info("=" * 60)
    logger.info("LambdaRank単勝モデル（V15相当）")
    logger.info("=" * 60)
    
    # V15相当のパラメータを推定
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'max_depth': 3,
        'num_leaves': 15,
        'min_child_samples': 200,
        'learning_rate': 0.02,
        'reg_alpha': 10.0,
        'reg_lambda': 10.0,
        'subsample': 0.6,
        'colsample_bytree': 0.5,
        'random_state': 42,
        'n_jobs': -1,
        'verbose': -1
    }
    
    train_ds = lgb.Dataset(X_train, y_train, group=groups_train)
    
    model = lgb.train(
        params,
        train_ds,
        num_boost_round=500,
        callbacks=[lgb.log_evaluation(100)]
    )
    
    train_sorted['score'] = model.predict(X_train)
    test_sorted['score'] = model.predict(X_test)
    
    train_hit, train_roi = calculate_roi(train_sorted, 'score')
    test_hit, test_roi = calculate_roi(test_sorted, 'score')
    
    logger.info(f"  Train: 的中率={train_hit:.1f}%, ROI={train_roi:.1f}%")
    logger.info(f"  Test:  的中率={test_hit:.1f}%, ROI={test_roi:.1f}%")
    logger.info(f"  Train-Test Gap: {train_roi - test_roi:.1f}%")
    
    # 複勝ROIも計算
    logger.info("")
    logger.info("=" * 60)
    logger.info("複勝ROI（同じモデルで評価）")
    logger.info("=" * 60)
    
    train_place_hit, train_place_roi = calculate_place_roi(train_sorted, 'score', returns_df)
    test_place_hit, test_place_roi = calculate_place_roi(test_sorted, 'score', returns_df)
    
    logger.info(f"  Train: 的中率={train_place_hit:.1f}%, ROI={train_place_roi:.1f}%")
    logger.info(f"  Test:  的中率={test_place_hit:.1f}%, ROI={test_place_roi:.1f}%")
    
    # 結果サマリー
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリー")
    logger.info("=" * 60)
    logger.info(f"  【単勝】Test ROI: {test_roi:.1f}% (目標: 91.8%)")
    logger.info(f"  【複勝】Test ROI: {test_place_roi:.1f}%")
    
    target = 91.8
    if test_roi >= target:
        logger.info(f"  ✅ V15ベースライン再現成功")
    else:
        logger.info(f"  ❌ 差分: {target - test_roi:.1f}%")
    
    # 特徴量重要度Top10
    logger.info("")
    logger.info("Top 10 特徴量:")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for i, row in importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")
    
    return model, fe, test_roi


if __name__ == "__main__":
    main()

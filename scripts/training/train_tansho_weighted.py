# -*- coding: utf-8 -*-
"""
単勝オッズ加重学習スクリプト

18_improvement_roadmap.md Phase 1 実装

【改善施策】
- sample_weight = log1p(win_odds) で高配当馬を重視
- 特徴量としてはwin_oddsを使用しない（リーク防止）

【目標】
- 単勝Test ROI: 91.8% → 95%+
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
    logger.info("データ読み込み中...")
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[races_df['race_date'] >= '2020-01-01'].dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    logger.info(f"  レコード数: {len(races_df):,}")
    
    return races_df, pedigrees_df, corners_df, race_details_df, returns_df


def calculate_roi(df, pred_col='score'):
    """
    単勝ROIを計算
    予測1位の馬に100円ベット、1着なら確定オッズ*100円返却
    """
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1]
    
    # 的中（1着）の場合の払戻
    hits = top1[top1['finish_position'] == 1].copy()
    
    total_bets = len(top1) * 100
    total_payout = hits['win_odds'].sum() * 100
    
    hit_rate = len(hits) / len(top1) * 100 if len(top1) > 0 else 0
    roi = total_payout / total_bets * 100 if total_bets > 0 else 0
    
    return hit_rate, roi


def main():
    logger.info("=" * 60)
    logger.info("単勝オッズ加重学習 (Phase 1)")
    logger.info("=" * 60)
    
    races_df, pedigrees_df, corners_df, race_details_df, returns_df = load_data()
    
    # 時系列分割
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
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    # win_oddsが特徴量に含まれていないことを確認
    assert 'win_odds' not in feature_cols, "リーク: win_oddsが特徴量に含まれています！"
    logger.info("  ✅ win_oddsは特徴量に含まれていません（リーク防止OK）")
    
    # ラベル（単勝: 1着か否か）
    train_features['is_win'] = (train_features['finish_position'] == 1).astype(int)
    valid_features['is_win'] = (valid_features['finish_position'] == 1).astype(int)
    test_features['is_win'] = (test_features['finish_position'] == 1).astype(int)
    
    # sample_weight（高配当馬を重視）
    # ★これはリークではない：学習時の重み付けのみに使用し、予測時には使用しない
    train_features['sample_weight'] = np.log1p(train_features['win_odds'])
    
    logger.info(f"  sample_weight分布: min={train_features['sample_weight'].min():.2f}, max={train_features['sample_weight'].max():.2f}")
    
    # データ準備
    X_train = train_features[feature_cols].fillna(0)
    y_train = train_features['is_win']
    w_train = train_features['sample_weight']
    X_valid = valid_features[feature_cols].fillna(0)
    y_valid = valid_features['is_win']
    X_test = test_features[feature_cols].fillna(0)
    
    # まず重みなしのベースラインを確認
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. ベースラインモデル（重みなし）")
    logger.info("=" * 60)
    
    base_params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'max_depth': 4,
        'num_leaves': 20,
        'min_child_samples': 100,
        'learning_rate': 0.03,
        'reg_alpha': 2.0,
        'reg_lambda': 3.0,
        'subsample': 0.7,
        'colsample_bytree': 0.7,
        'random_state': 42,
        'n_jobs': -1,
        'verbose': -1
    }
    
    train_ds_base = lgb.Dataset(X_train, y_train)
    valid_ds_base = lgb.Dataset(X_valid, y_valid, reference=train_ds_base)
    
    base_model = lgb.train(
        base_params,
        train_ds_base,
        num_boost_round=2000,
        valid_sets=[valid_ds_base],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    
    train_features['score_base'] = base_model.predict(X_train)
    valid_features['score_base'] = base_model.predict(X_valid)
    test_features['score_base'] = base_model.predict(X_test)
    
    base_train_hit, base_train_roi = calculate_roi(train_features, 'score_base')
    base_valid_hit, base_valid_roi = calculate_roi(valid_features, 'score_base')
    base_test_hit, base_test_roi = calculate_roi(test_features, 'score_base')
    
    logger.info(f"  Best iteration: {base_model.best_iteration}")
    logger.info(f"  Train: 的中率={base_train_hit:.1f}%, ROI={base_train_roi:.1f}%")
    logger.info(f"  Valid: 的中率={base_valid_hit:.1f}%, ROI={base_valid_roi:.1f}%")
    logger.info(f"  Test:  的中率={base_test_hit:.1f}%, ROI={base_test_roi:.1f}%")
    
    # オッズ加重モデル
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. オッズ加重モデル（sample_weight=log1p(odds)）")
    logger.info("=" * 60)
    
    train_ds_weighted = lgb.Dataset(X_train, y_train, weight=w_train)
    valid_ds_weighted = lgb.Dataset(X_valid, y_valid, reference=train_ds_weighted)
    
    weighted_model = lgb.train(
        base_params,
        train_ds_weighted,
        num_boost_round=2000,
        valid_sets=[valid_ds_weighted],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    
    train_features['score_weighted'] = weighted_model.predict(X_train)
    valid_features['score_weighted'] = weighted_model.predict(X_valid)
    test_features['score_weighted'] = weighted_model.predict(X_test)
    
    weighted_train_hit, weighted_train_roi = calculate_roi(train_features, 'score_weighted')
    weighted_valid_hit, weighted_valid_roi = calculate_roi(valid_features, 'score_weighted')
    weighted_test_hit, weighted_test_roi = calculate_roi(test_features, 'score_weighted')
    
    logger.info(f"  Best iteration: {weighted_model.best_iteration}")
    logger.info(f"  Train: 的中率={weighted_train_hit:.1f}%, ROI={weighted_train_roi:.1f}%")
    logger.info(f"  Valid: 的中率={weighted_valid_hit:.1f}%, ROI={weighted_valid_roi:.1f}%")
    logger.info(f"  Test:  的中率={weighted_test_hit:.1f}%, ROI={weighted_test_roi:.1f}%")
    
    # 比較サマリー
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較")
    logger.info("=" * 60)
    
    base_gap = base_train_roi - base_test_roi
    weighted_gap = weighted_train_roi - weighted_test_roi
    
    logger.info(f"                       ベースライン    オッズ加重    改善幅")
    logger.info(f"  Test ROI:            {base_test_roi:7.1f}%       {weighted_test_roi:7.1f}%     {weighted_test_roi - base_test_roi:+.1f}%")
    logger.info(f"  Test 的中率:         {base_test_hit:7.1f}%       {weighted_test_hit:7.1f}%     {weighted_test_hit - base_test_hit:+.1f}%")
    logger.info(f"  Train-Test Gap:      {base_gap:7.1f}%       {weighted_gap:7.1f}%")
    
    target_roi = 95.0
    if weighted_test_roi >= target_roi:
        logger.info(f"\n  ✅ 目標達成! Test ROI {weighted_test_roi:.1f}% >= {target_roi}%")
    else:
        logger.info(f"\n  ❌ 目標未達 Test ROI {weighted_test_roi:.1f}% < {target_roi}%")
        logger.info(f"     あと {target_roi - weighted_test_roi:.1f}% 必要")
    
    # 特徴量重要度
    logger.info("")
    logger.info("Top 10 特徴量 (オッズ加重モデル):")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': weighted_model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for i, row in importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")
    
    return base_model, weighted_model, fe


if __name__ == "__main__":
    main()

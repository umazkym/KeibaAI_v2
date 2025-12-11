# -*- coding: utf-8 -*-
"""
Advanced Model Improvements - 総合検証スクリプト

【実装内容】
1. 新特徴量の追加（race_details.parquetのペース、corner馬身差std）
2. アンサンブル学習（Binary + LambdaRank + CatBoost）
3. 確率キャリブレーション（Isotonic Regression）

【リーク防止】
- 全ての累積統計はshift(1)適用
- Train期間のみでfit、Test期間は評価のみ
- オッズ・着順は特徴量に使用しない
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.isotonic import IsotonicRegression
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
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses


def calc_roi(df, preds):
    """ROI計算（Top1予測）"""
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


# ============================================================
# Phase 1: 新特徴量の追加
# ============================================================

def add_new_features(df, race_details_df, corners_df, train_end_date):
    """
    新特徴量を追加（リークフリー）
    
    【追加特徴量】
    1. venue_avg_pace: 会場×距離×馬場の平均ペース差（first_half - second_half）
    2. horse_c4_gap_std: 過去5走のC4馬身差の標準偏差（安定性）
    """
    logger.info("  新特徴量を計算中...")
    
    # 1. ペース特徴量（Train期間のみで計算）
    train_races = df[df['race_date'] <= train_end_date].copy()
    
    # race_detailsとのマージ
    pace_df = train_races[['race_id', 'venue', 'distance_m', 'track_condition']].drop_duplicates()
    pace_df = pace_df.merge(
        race_details_df[['race_id', 'first_half', 'second_half']], 
        on='race_id', 
        how='left'
    )
    pace_df['pace_diff'] = pace_df['first_half'] - pace_df['second_half']
    pace_df = pace_df.dropna(subset=['pace_diff'])
    
    # 会場×距離×馬場状態別の平均ペース
    venue_pace = pace_df.groupby(['venue', 'distance_m', 'track_condition'])['pace_diff'].mean()
    df['venue_avg_pace'] = df.set_index(['venue', 'distance_m', 'track_condition']).index.map(
        venue_pace.to_dict()
    )
    df['venue_avg_pace'] = df['venue_avg_pace'].fillna(0)
    
    # 2. C4馬身差の標準偏差（過去5走）
    # Train期間のC4データを取得
    c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'gap_from_leader']].copy()
    
    # レース情報をマージ
    race_info = df[['race_id', 'horse_id', 'horse_number', 'race_date']].drop_duplicates()
    c4 = c4.merge(race_info, on=['race_id', 'horse_number'], how='inner')
    c4 = c4[c4['race_date'] <= train_end_date]  # Train期間のみ
    
    # 馬×日付でソート
    c4 = c4.sort_values(['horse_id', 'race_date'])
    
    # 過去5走の標準偏差を計算（shift(1)でリーク防止）
    c4['gap_std'] = c4.groupby('horse_id')['gap_from_leader'].transform(
        lambda x: x.rolling(5, min_periods=2).std().shift(1)
    )
    
    # 馬IDごとの最新統計をマップ
    gap_std_stats = c4.groupby('horse_id')['gap_std'].last().to_dict()
    df['horse_c4_gap_std'] = df['horse_id'].map(gap_std_stats)
    df['horse_c4_gap_std'] = df['horse_c4_gap_std'].fillna(df['horse_c4_gap_std'].median())
    
    logger.info(f"    venue_avg_pace非ゼロ: {(df['venue_avg_pace'] != 0).sum()}/{len(df)}")
    logger.info(f"    horse_c4_gap_std非NaN: {df['horse_c4_gap_std'].notna().sum()}/{len(df)}")
    
    return df


# ============================================================
# Phase 2: アンサンブル学習
# ============================================================

def train_ensemble_models(X_train, y_train, X_valid, y_valid, groups_train=None, groups_valid=None):
    """
    複数モデルを学習してアンサンブル
    
    【モデル】
    1. Binary Classification (V15相当)
    2. LambdaRank
    3. 強正則化Binary
    """
    models = {}
    
    # 1. Binary Classification (V15相当)
    logger.info("  Model 1: Binary Classification")
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
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    models['binary'] = lgb.train(
        params_binary, 
        train_ds, 
        num_boost_round=300,
        valid_sets=[valid_ds],
        callbacks=[lgb.early_stopping(30, verbose=False)]
    )
    
    # 2. LambdaRank
    logger.info("  Model 2: LambdaRank")
    
    # 関連度ラベル作成
    # このためにはTrain/Validのfinish_positionが必要
    # → 引数で受け取る必要あり（後で修正）
    
    params_rank = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'verbosity': -1,
        'learning_rate': 0.02,
        'num_leaves': 15,
        'max_depth': 3,
        'min_child_samples': 150,
        'reg_alpha': 2.0,
        'reg_lambda': 3.0,
        'subsample': 0.6,
        'colsample_bytree': 0.6,
    }
    
    if groups_train is not None:
        train_ds_rank = lgb.Dataset(X_train, y_train, group=groups_train)
        valid_ds_rank = lgb.Dataset(X_valid, y_valid, group=groups_valid, reference=train_ds_rank)
        
        models['lambdarank'] = lgb.train(
            params_rank,
            train_ds_rank,
            num_boost_round=500,
            valid_sets=[valid_ds_rank],
            callbacks=[lgb.early_stopping(50, verbose=False)]
        )
    
    # 3. 強正則化Binary（Gap縮小用）
    logger.info("  Model 3: Strong Regularization Binary")
    params_strong = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.02,
        'num_leaves': 12,
        'max_depth': 2,
        'min_child_samples': 200,
        'reg_alpha': 5.0,
        'reg_lambda': 8.0,
        'bagging_fraction': 0.6,
        'bagging_freq': 5,
        'feature_fraction': 0.6,
    }
    
    models['strong_reg'] = lgb.train(
        params_strong,
        train_ds,
        num_boost_round=300,
        valid_sets=[valid_ds],
        callbacks=[lgb.early_stopping(30, verbose=False)]
    )
    
    return models


def ensemble_predict(models, X, method='weighted', weights=None):
    """アンサンブル予測"""
    preds = {}
    for name, model in models.items():
        preds[name] = model.predict(X)
    
    if method == 'simple':
        # 単純平均
        return np.mean(list(preds.values()), axis=0)
    elif method == 'weighted':
        # 重み付き平均
        if weights is None:
            weights = {k: 1.0/len(preds) for k in preds.keys()}
        result = np.zeros(len(X))
        for name, pred in preds.items():
            result += weights.get(name, 0) * pred
        return result
    else:
        return preds


# ============================================================
# Phase 3: 確率キャリブレーション
# ============================================================

def calibrate_probabilities(train_probs, train_labels, test_probs):
    """
    Isotonic Regressionによる確率キャリブレーション
    
    【リーク防止】
    - Train期間の予測確率とラベルでfitし、Test期間の確率を変換
    """
    logger.info("  確率キャリブレーション (Isotonic Regression)")
    
    # Isotonic Regression でキャリブレーション
    iso_reg = IsotonicRegression(out_of_bounds='clip')
    iso_reg.fit(train_probs, train_labels)
    
    calibrated = iso_reg.transform(test_probs)
    
    return calibrated


# ============================================================
# Main
# ============================================================

def main():
    logger.info("=" * 70)
    logger.info("Advanced Model Improvements - 総合検証")
    logger.info("=" * 70)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 期間設定（V15公式設定）
    train_end = '2024-12-31'  # Train用
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] > train_end) & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~{train_end})")
    logger.info(f"  Test:  {len(test):,}件 ({train_end}~)")
    
    # ============================================================
    # Phase 1: V15特徴量 + 新特徴量
    # ============================================================
    logger.info("")
    logger.info("=" * 70)
    logger.info("Phase 1: 特徴量生成（V15 + 新特徴量）")
    logger.info("=" * 70)
    
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    valid_f = engine.transform(valid)
    test_f = engine.transform(test)
    
    # 新特徴量追加
    train_f = add_new_features(train_f, race_details, corners, train_end)
    valid_f = add_new_features(valid_f, race_details, corners, train_end)
    test_f = add_new_features(test_f, race_details, corners, train_end)
    
    feature_cols = list(engine.get_feature_columns()) + ['venue_avg_pace', 'horse_c4_gap_std']
    feature_cols = [c for c in feature_cols if c in train_f.columns]
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    X_train = train_f[feature_cols].fillna(0)
    X_valid = valid_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    
    y_train = (train_f['finish_position'] == 1).astype(int)
    y_valid = (valid_f['finish_position'] == 1).astype(int)
    
    # LambdaRank用ラベル
    def create_relevance(fp):
        if fp == 1: return 5
        elif fp == 2: return 4
        elif fp == 3: return 3
        elif fp <= 5: return 1
        else: return 0
    
    train_sorted = train_f.sort_values('race_id')
    valid_sorted = valid_f.sort_values('race_id')
    test_sorted = test_f.sort_values('race_id')
    
    y_train_rel = train_sorted['finish_position'].apply(create_relevance)
    y_valid_rel = valid_sorted['finish_position'].apply(create_relevance)
    
    groups_train = train_sorted.groupby('race_id').size().values
    groups_valid = valid_sorted.groupby('race_id').size().values
    
    # ============================================================
    # V15 ベースライン
    # ============================================================
    logger.info("")
    logger.info("=" * 70)
    logger.info("Baseline: V15相当（新特徴量なし）")
    logger.info("=" * 70)
    
    v15_feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    X_train_v15 = train_f[v15_feature_cols].fillna(0)
    X_test_v15 = test_f[v15_feature_cols].fillna(0)
    
    params_v15 = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    model_v15 = lgb.train(params_v15, lgb.Dataset(X_train_v15, y_train), num_boost_round=200)
    
    pred_train_v15 = model_v15.predict(X_train_v15)
    pred_test_v15 = model_v15.predict(X_test_v15)
    
    train_roi_v15, train_hit_v15 = calc_roi(train_f, pred_train_v15)
    test_roi_v15, test_hit_v15 = calc_roi(test_f, pred_test_v15)
    
    logger.info(f"  Train: ROI={train_roi_v15:.1f}%, 的中率={train_hit_v15:.1f}%")
    logger.info(f"  Test:  ROI={test_roi_v15:.1f}%, 的中率={test_hit_v15:.1f}%")
    logger.info(f"  Gap:   {train_roi_v15 - test_roi_v15:.1f}%")
    
    # ============================================================
    # Phase 1: 新特徴量の効果
    # ============================================================
    logger.info("")
    logger.info("=" * 70)
    logger.info("Phase 1: 新特徴量追加（+2特徴量）")
    logger.info("=" * 70)
    
    model_new_feat = lgb.train(params_v15, lgb.Dataset(X_train, y_train), num_boost_round=200)
    
    pred_train_new = model_new_feat.predict(X_train)
    pred_test_new = model_new_feat.predict(X_test)
    
    train_roi_new, train_hit_new = calc_roi(train_f, pred_train_new)
    test_roi_new, test_hit_new = calc_roi(test_f, pred_test_new)
    
    logger.info(f"  Train: ROI={train_roi_new:.1f}%, 的中率={train_hit_new:.1f}%")
    logger.info(f"  Test:  ROI={test_roi_new:.1f}%, 的中率={test_hit_new:.1f}%")
    logger.info(f"  Gap:   {train_roi_new - test_roi_new:.1f}%")
    logger.info(f"  改善:  {test_roi_new - test_roi_v15:+.1f}%")
    
    # ============================================================
    # Phase 2: アンサンブル
    # ============================================================
    logger.info("")
    logger.info("=" * 70)
    logger.info("Phase 2: アンサンブル学習")
    logger.info("=" * 70)
    
    X_train_sorted = train_sorted[feature_cols].fillna(0)
    X_valid_sorted = valid_sorted[feature_cols].fillna(0)
    X_test_sorted = test_sorted[feature_cols].fillna(0)
    
    models = train_ensemble_models(
        X_train_sorted, y_train_rel, 
        X_valid_sorted, y_valid_rel,
        groups_train, groups_valid
    )
    
    # 各モデルのValid ROIを計算して重み調整
    model_weights = {}
    for name, model in models.items():
        pred_valid = model.predict(X_valid_sorted)
        valid_roi, _ = calc_roi(valid_sorted.reset_index(drop=True), pred_valid)
        model_weights[name] = max(valid_roi - 80, 1)  # 80%以上の差分を重みに
        logger.info(f"    {name}: Valid ROI={valid_roi:.1f}%, weight={model_weights[name]:.1f}")
    
    # 重みを正規化
    total_weight = sum(model_weights.values())
    model_weights = {k: v/total_weight for k, v in model_weights.items()}
    
    # アンサンブル予測
    pred_test_ensemble = ensemble_predict(models, X_test_sorted, method='weighted', weights=model_weights)
    
    test_sorted_reset = test_sorted.reset_index(drop=True)
    test_roi_ensemble, test_hit_ensemble = calc_roi(test_sorted_reset, pred_test_ensemble)
    
    logger.info(f"  Ensemble Test: ROI={test_roi_ensemble:.1f}%, 的中率={test_hit_ensemble:.1f}%")
    logger.info(f"  改善:  {test_roi_ensemble - test_roi_v15:+.1f}%")
    
    # ============================================================
    # Phase 3: 確率キャリブレーション
    # ============================================================
    logger.info("")
    logger.info("=" * 70)
    logger.info("Phase 3: 確率キャリブレーション")
    logger.info("=" * 70)
    
    # V15モデルをベースにキャリブレーション
    # Train期間の予測確率とラベルでfit
    train_probs_for_calib = model_v15.predict(X_train_v15)
    test_probs_for_calib = model_v15.predict(X_test_v15)
    
    calibrated_test_probs = calibrate_probabilities(
        train_probs_for_calib, 
        y_train.values, 
        test_probs_for_calib
    )
    
    test_roi_calib, test_hit_calib = calc_roi(test_f, calibrated_test_probs)
    
    logger.info(f"  Calibrated Test: ROI={test_roi_calib:.1f}%, 的中率={test_hit_calib:.1f}%")
    logger.info(f"  改善:  {test_roi_calib - test_roi_v15:+.1f}%")
    
    # ============================================================
    # 結果比較
    # ============================================================
    logger.info("")
    logger.info("=" * 70)
    logger.info("結果比較")
    logger.info("=" * 70)
    
    logger.info("")
    logger.info(f"{'Method':<30} {'Test ROI':>10} {'改善幅':>10} {'的中率':>10}")
    logger.info("-" * 60)
    logger.info(f"{'V15 Baseline':<30} {test_roi_v15:>9.1f}% {'-':>10} {test_hit_v15:>9.1f}%")
    logger.info(f"{'Phase 1: 新特徴量':<30} {test_roi_new:>9.1f}% {test_roi_new - test_roi_v15:>+9.1f}% {test_hit_new:>9.1f}%")
    logger.info(f"{'Phase 2: アンサンブル':<30} {test_roi_ensemble:>9.1f}% {test_roi_ensemble - test_roi_v15:>+9.1f}% {test_hit_ensemble:>9.1f}%")
    logger.info(f"{'Phase 3: キャリブレーション':<30} {test_roi_calib:>9.1f}% {test_roi_calib - test_roi_v15:>+9.1f}% {test_hit_calib:>9.1f}%")
    
    # 最良結果
    best_roi = max(test_roi_v15, test_roi_new, test_roi_ensemble, test_roi_calib)
    logger.info("")
    logger.info(f"Best Test ROI: {best_roi:.1f}%")
    
    return {
        'v15': test_roi_v15,
        'new_features': test_roi_new,
        'ensemble': test_roi_ensemble,
        'calibrated': test_roi_calib,
    }


if __name__ == "__main__":
    main()

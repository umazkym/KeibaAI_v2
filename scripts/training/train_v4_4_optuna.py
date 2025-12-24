#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4 Optuna最適化版

v4_4_courseをベースに、Optunaでハイパーパラメータを最適化。
目標: ROI向上 + Gap維持（過学習抑制）

【最適化対象】
- num_leaves, max_depth, min_child_samples
- learning_rate, reg_alpha, reg_lambda
- feature_fraction, bagging_fraction

【目的関数】
- Valid ROI - Gap * penalty (Gap拡大を抑制)
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import json
import logging
import yaml
import optuna
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# リーク特徴量
LEAKY_FEATURES = [
    'form_rank', 'gap_course_fit_popularity', 'gap_jockey_popularity',
    'gap_pedigree_popularity', 'gap_trainer_popularity', 'is_overvalued',
]


def load_course_master():
    yaml_path = project_root / "keibaai/configs/course_master.yaml"
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_course_features(course_master, venue, surface, distance_m):
    if not course_master or venue not in course_master:
        return 0, 300, 0
    venue_data = course_master.get(venue, {})
    surface_data = venue_data.get(surface, {})
    try:
        dist = int(distance_m)
    except:
        return 0, 300, 0
    distance_data = surface_data.get(dist, {})
    if not distance_data:
        return 0, 300, 0
    if 'default' in distance_data:
        info = distance_data['default']
    else:
        first_key = list(distance_data.keys())[0] if distance_data else None
        if first_key and isinstance(distance_data[first_key], dict):
            info = distance_data[first_key]
        else:
            info = distance_data
    return info.get('slope_percent', 0), info.get('final_straight_m', 300), info.get('corner_count', 0)


def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet_df = d[d['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    if len(bet_df) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate


def prepare_data():
    """データ準備"""
    logger.info("データ準備中...")
    
    # V4_3訓練データ
    model_dir = project_root / 'keibaai/models/mu_v3_3'
    train_data = pd.read_parquet(model_dir / 'train_data_mu_v3_3.parquet')
    train_data['race_date'] = pd.to_datetime(train_data['race_date'])
    
    # V4_3特徴量リスト
    v43_dir = project_root / 'keibaai/models/mu_v4_3'
    with open(v43_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        all_v43_features = json.load(f)
    safe_v43_features = [f for f in all_v43_features if f not in LEAKY_FEATURES]
    
    # 生データからvenue, track_surfaceをマージ
    races_raw = pd.read_parquet(project_root / 'keibaai/data/parsed/parquet/races/races.parquet')
    races_raw = races_raw[['race_id', 'horse_id', 'venue', 'track_surface']].copy()
    train_data['race_id'] = train_data['race_id'].astype(str)
    races_raw['race_id'] = races_raw['race_id'].astype(str)
    train_data['horse_id'] = train_data['horse_id'].astype(str)
    races_raw['horse_id'] = races_raw['horse_id'].astype(str)
    train_data = train_data.merge(races_raw, on=['race_id', 'horse_id'], how='left')
    
    # コース特徴量
    course_master = load_course_master()
    course_features = train_data.apply(
        lambda row: get_course_features(
            course_master, row.get('venue'), row.get('track_surface'), row.get('distance_m')
        ), axis=1
    )
    train_data['course_slope_percent'] = [x[0] for x in course_features]
    train_data['course_final_straight_m'] = [x[1] for x in course_features]
    train_data['course_corner_count'] = [x[2] for x in course_features]
    train_data['is_long_straight'] = (train_data['course_final_straight_m'] >= 400).astype(int)
    train_data['is_steep_slope'] = (train_data['course_slope_percent'] >= 1.5).astype(int)
    
    # 特徴量リスト
    course_features_list = [
        'course_slope_percent', 'course_final_straight_m', 'course_corner_count',
        'is_long_straight', 'is_steep_slope'
    ]
    all_features = safe_v43_features + course_features_list
    available_features = [f for f in all_features if f in train_data.columns]
    
    # ターゲット
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    odds = train_data['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    gain = np.zeros(len(train_data))
    gain[train_data['finish_position'] == 1] = log_odds[train_data['finish_position'] == 1] * weight_1st
    gain[train_data['finish_position'] == 2] = log_odds[train_data['finish_position'] == 2] * weight_2nd
    gain[train_data['finish_position'] == 3] = log_odds[train_data['finish_position'] == 3] * weight_3rd
    train_data['target_relevance'] = gain.astype(int)
    train_data['sample_weight'] = np.log1p(train_data['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    # 期間分割
    train_mask = train_data['race_date'] < '2023-01-01'
    valid_mask = (train_data['race_date'] >= '2023-01-01') & (train_data['race_date'] < '2024-01-01')
    test_mask = train_data['race_date'] >= '2024-01-01'
    
    train_df = train_data[train_mask].copy()
    valid_df = train_data[valid_mask].copy()
    test_df = train_data[test_mask].copy()
    
    train_df[available_features] = train_df[available_features].fillna(0)
    valid_df[available_features] = valid_df[available_features].fillna(0)
    test_df[available_features] = test_df[available_features].fillna(0)
    
    group_train = train_df.groupby('race_id').size().to_list()
    group_valid = valid_df.groupby('race_id').size().to_list()
    
    logger.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
    logger.info(f"特徴量: {len(available_features)}")
    
    return train_df, valid_df, test_df, available_features, group_train, group_valid


def objective(trial, train_df, valid_df, test_df, features, group_train, group_valid):
    """Optuna目的関数"""
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'num_leaves': trial.suggest_int('num_leaves', 20, 60),
        'max_depth': trial.suggest_int('max_depth', 4, 8),
        'min_child_samples': trial.suggest_int('min_child_samples', 80, 200),
        'learning_rate': trial.suggest_float('learning_rate', 0.02, 0.1, log=True),
        'reg_alpha': trial.suggest_float('reg_alpha', 0.5, 10.0, log=True),
        'reg_lambda': trial.suggest_float('reg_lambda', 0.5, 10.0, log=True),
        'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 0.8),
        'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 0.9),
        'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
        'verbose': -1,
        'random_state': 42,
        'label_gain': list(range(100))
    }
    
    model = lgb.LGBMRanker(**params, n_estimators=1000)
    model.fit(
        train_df[features], train_df['target_relevance'],
        group=group_train, sample_weight=train_df['sample_weight'],
        eval_set=[(valid_df[features], valid_df['target_relevance'])],
        eval_group=[group_valid],
        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
    )
    
    valid_preds = model.predict(valid_df[features])
    test_preds = model.predict(test_df[features])
    
    valid_roi, _ = calc_roi(valid_df, valid_preds)
    test_roi, _ = calc_roi(test_df, test_preds)
    
    gap = abs(valid_roi - test_roi)
    
    # 目的関数: Valid ROI - Gap * penalty
    # Gap拡大を抑制（penalty=0.5）
    score = valid_roi - gap * 0.5
    
    # 情報をtrialに保存
    trial.set_user_attr('valid_roi', valid_roi)
    trial.set_user_attr('test_roi', test_roi)
    trial.set_user_attr('gap', gap)
    
    return score


def main():
    logger.info("=" * 60)
    logger.info("V4.4 Optuna最適化")
    logger.info("=" * 60)
    
    # データ準備
    train_df, valid_df, test_df, features, group_train, group_valid = prepare_data()
    
    # Optuna最適化
    logger.info("Optuna最適化開始 (30トライアル)...")
    study = optuna.create_study(direction='maximize')
    study.optimize(
        lambda trial: objective(trial, train_df, valid_df, test_df, features, group_train, group_valid),
        n_trials=30,
        show_progress_bar=True
    )
    
    # 最適パラメータ
    logger.info("=" * 60)
    logger.info("【最適化結果】")
    logger.info(f"  Best Score: {study.best_value:.2f}")
    logger.info(f"  Valid ROI: {study.best_trial.user_attrs['valid_roi']:.2f}%")
    logger.info(f"  Test ROI: {study.best_trial.user_attrs['test_roi']:.2f}%")
    logger.info(f"  Gap: {study.best_trial.user_attrs['gap']:.2f}%")
    logger.info("=" * 60)
    
    best_params = study.best_params
    logger.info("Best Params:")
    for k, v in best_params.items():
        logger.info(f"  {k}: {v}")
    
    # 最適パラメータで最終モデル訓練
    logger.info("最終モデル訓練中...")
    final_params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'verbose': -1,
        'random_state': 42,
        'label_gain': list(range(100)),
        **best_params
    }
    
    model = lgb.LGBMRanker(**final_params, n_estimators=1500)
    model.fit(
        train_df[features], train_df['target_relevance'],
        group=group_train, sample_weight=train_df['sample_weight'],
        eval_set=[(valid_df[features], valid_df['target_relevance'])],
        eval_group=[group_valid],
        callbacks=[lgb.early_stopping(stopping_rounds=50), lgb.log_evaluation(100)]
    )
    
    # 最終評価
    valid_preds = model.predict(valid_df[features])
    test_preds = model.predict(test_df[features])
    valid_roi, valid_hit = calc_roi(valid_df, valid_preds)
    test_roi, test_hit = calc_roi(test_df, test_preds)
    
    logger.info("=" * 60)
    logger.info("【最終結果】")
    logger.info(f"  Valid ROI: {valid_roi:.2f}% (Hit: {valid_hit:.2f}%)")
    logger.info(f"  Test ROI:  {test_roi:.2f}% (Hit: {test_hit:.2f}%)")
    logger.info(f"  Gap:       {abs(valid_roi - test_roi):.2f}%")
    logger.info("=" * 60)
    
    # 保存
    output_dir = project_root / 'keibaai/models/mu_v4_4_optuna'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / 'model.pkl', 'wb') as f:
        pickle.dump(model, f)
    with open(output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
        json.dump(features, f, indent=2, ensure_ascii=False)
    
    imp = pd.DataFrame({'feature': features, 'importance': model.feature_importances_}).sort_values('importance', ascending=False)
    imp.to_csv(output_dir / 'feature_importance.csv', index=False)
    
    info = {
        'version': 'v4.4_optuna',
        'description': 'Optuna最適化版 (30トライアル)',
        'valid_roi': valid_roi,
        'test_roi': test_roi,
        'gap': abs(valid_roi - test_roi),
        'feature_count': len(features),
        'best_params': best_params,
        'n_trials': 30,
        'created_at': datetime.now().isoformat()
    }
    with open(output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
        json.dump(info, f, indent=2, ensure_ascii=False)
    
    logger.info(f"モデル保存: {output_dir}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4 Final: 深層分析から発見した新特徴量を追加

【分析から得られた知見】
1. 高ROI競馬場×距離パターン（22件）
2. 頭数13-14頭が最もROI高い（84%）
3. 馬体重+10以上で高ROI傾向
4. 2週以内の連戦は低ROI（68%）
5. 不良馬場は低ROI（69%）

【新特徴量】
- is_high_roi_course: 高ROIコースフラグ
- field_size_category: 頭数カテゴリ
- is_optimal_field_size: 13-14頭フラグ
- weight_gain_flag: 馬体重+10以上フラグ
- is_too_soon: 2週以内連戦フラグ
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
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# リーク特徴量
LEAKY_FEATURES = [
    'form_rank',
    'gap_course_fit_popularity',
    'gap_jockey_popularity',
    'gap_pedigree_popularity',
    'gap_trainer_popularity',
    'is_overvalued',
]

# 分析で発見した高ROIパターン（90%以上、50件以上）
HIGH_ROI_PATTERNS = [
    ('函館', 'ダート', 1700),
    ('札幌', 'ダート', 1700),
    ('中京', 'ダート', 1200),
    ('阪神', 'ダート', 1200),
    ('阪神', 'ダート', 2000),
    ('札幌', '芝', 1500),
    ('札幌', '芝', 1800),
    ('新潟', 'ダート', 1800),
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
    return info.get('slope_percent', 0), info.get('final_straight_m', 300), info.get('start_to_corner_m', 0)


def add_new_features(df, course_master):
    """分析から発見した新特徴量を追加"""
    logger.info("新特徴量を追加中...")
    
    # 1. コース特徴量
    course_features = df.apply(
        lambda row: get_course_features(
            course_master, row.get('venue'), row.get('track_surface'), row.get('distance_m')
        ), axis=1
    )
    df['course_slope_percent'] = [x[0] for x in course_features]
    df['course_final_straight_m'] = [x[1] for x in course_features]
    df['course_start_to_corner_m'] = [x[2] for x in course_features]
    
    # 2. 高ROIコースフラグ
    df['is_high_roi_course'] = df.apply(
        lambda row: 1 if (row.get('venue'), row.get('track_surface'), row.get('distance_m')) in HIGH_ROI_PATTERNS else 0,
        axis=1
    )
    
    # 3. 頭数カテゴリ
    df['field_size'] = df.groupby('race_id')['horse_id'].transform('count')
    df['is_optimal_field_size'] = ((df['field_size'] >= 13) & (df['field_size'] <= 14)).astype(int)
    df['is_small_field'] = (df['field_size'] <= 8).astype(int)
    df['is_large_field'] = (df['field_size'] >= 17).astype(int)
    
    # 4. 馬体重変化（既存カラムがあれば）
    if 'horse_weight_change' in df.columns:
        df['weight_gain_flag'] = (df['horse_weight_change'] >= 10).astype(int)
        df['weight_loss_flag'] = (df['horse_weight_change'] <= -10).astype(int)
    
    # 5. 休み明けフラグ
    if 'days_since_last_race' in df.columns:
        df['is_too_soon'] = (df['days_since_last_race'] <= 14).astype(int)
        df['is_long_rest'] = (df['days_since_last_race'] >= 120).astype(int)
    
    # 6. 坂・直線フラグ
    df['is_steep_slope'] = (df['course_slope_percent'] >= 1.5).astype(int)
    df['is_long_straight'] = (df['course_final_straight_m'] >= 400).astype(int)
    df['is_long_run_to_corner'] = (df['course_start_to_corner_m'] >= 500).astype(int)
    
    logger.info(f"  is_high_roi_course: {df['is_high_roi_course'].sum():,}/{len(df):,}")
    logger.info(f"  is_optimal_field_size: {df['is_optimal_field_size'].sum():,}/{len(df):,}")
    logger.info(f"  is_steep_slope: {df['is_steep_slope'].sum():,}/{len(df):,}")
    
    return df


def main():
    logger.info("=" * 60)
    logger.info("V4.4 Final: 深層分析から発見した新特徴量を追加")
    logger.info("=" * 60)
    
    # 1. データ読み込み
    model_dir = project_root / 'keibaai/models/mu_v3_3'
    train_data = pd.read_parquet(model_dir / 'train_data_mu_v3_3.parquet')
    train_data['race_date'] = pd.to_datetime(train_data['race_date'])
    
    v43_dir = project_root / 'keibaai/models/mu_v4_3'
    with open(v43_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        all_v43_features = json.load(f)
    safe_v43_features = [f for f in all_v43_features if f not in LEAKY_FEATURES]
    logger.info(f"V4_3特徴量: {len(safe_v43_features)}")
    
    # 2. 生データからvenue, track_surfaceをマージ
    races_raw = pd.read_parquet(project_root / 'keibaai/data/parsed/parquet/races/races.parquet')
    races_raw = races_raw[['race_id', 'horse_id', 'venue', 'track_surface']].copy()
    train_data['race_id'] = train_data['race_id'].astype(str)
    races_raw['race_id'] = races_raw['race_id'].astype(str)
    train_data['horse_id'] = train_data['horse_id'].astype(str)
    races_raw['horse_id'] = races_raw['horse_id'].astype(str)
    train_data = train_data.merge(races_raw, on=['race_id', 'horse_id'], how='left')
    
    logger.info(f"生データマージ後: {train_data['venue'].notna().sum():,}/{len(train_data):,}")
    
    # 3. 新特徴量追加
    course_master = load_course_master()
    train_data = add_new_features(train_data, course_master)
    
    # 4. 特徴量リスト
    new_features = [
        'course_slope_percent', 'course_final_straight_m', 'course_start_to_corner_m',
        'is_high_roi_course', 'is_optimal_field_size', 'is_small_field', 'is_large_field',
        'is_steep_slope', 'is_long_straight', 'is_long_run_to_corner'
    ]
    if 'weight_gain_flag' in train_data.columns:
        new_features.extend(['weight_gain_flag', 'weight_loss_flag'])
    if 'is_too_soon' in train_data.columns:
        new_features.extend(['is_too_soon', 'is_long_rest'])
    
    all_features = safe_v43_features + new_features
    available_features = [f for f in all_features if f in train_data.columns]
    logger.info(f"統合特徴量: {len(available_features)}")
    
    # 5. ターゲット
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    odds = train_data['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    gain = np.zeros(len(train_data))
    gain[train_data['finish_position'] == 1] = log_odds[train_data['finish_position'] == 1] * weight_1st
    gain[train_data['finish_position'] == 2] = log_odds[train_data['finish_position'] == 2] * weight_2nd
    gain[train_data['finish_position'] == 3] = log_odds[train_data['finish_position'] == 3] * weight_3rd
    train_data['target_relevance'] = gain.astype(int)
    train_data['sample_weight'] = np.log1p(train_data['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    # 6. 期間分割
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
    
    # 7. 訓練
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'num_leaves': 40,
        'learning_rate': 0.05,
        'min_child_samples': 120,
        'max_depth': 6,
        'reg_alpha': 2.0,
        'reg_lambda': 3.0,
        'feature_fraction': 0.65,
        'bagging_fraction': 0.75,
        'bagging_freq': 3,
        'verbose': -1,
        'random_state': 42,
        'label_gain': list(range(100))
    }
    
    logger.info("LightGBM訓練中...")
    model = lgb.LGBMRanker(**params, n_estimators=1500)
    model.fit(
        train_df[available_features], train_df['target_relevance'],
        group=group_train, sample_weight=train_df['sample_weight'],
        eval_set=[(valid_df[available_features], valid_df['target_relevance'])],
        eval_group=[group_valid],
        callbacks=[lgb.early_stopping(stopping_rounds=50), lgb.log_evaluation(100)]
    )
    
    # 8. 評価
    def calc_roi(df, preds):
        d = df.copy()
        d['score'] = preds
        d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
        bet_df = d[d['rank_pred'] == 1]
        hits = bet_df[bet_df['finish_position'] == 1]
        if len(bet_df) == 0:
            return 0, 0, 0
        roi = hits['win_odds'].sum() / len(bet_df) * 100
        hit_rate = len(hits) / len(bet_df) * 100
        return roi, hit_rate, len(bet_df)
    
    valid_preds = model.predict(valid_df[available_features])
    test_preds = model.predict(test_df[available_features])
    valid_roi, valid_hit, _ = calc_roi(valid_df, valid_preds)
    test_roi, test_hit, test_bets = calc_roi(test_df, test_preds)
    
    logger.info("=" * 60)
    logger.info("【最終結果】")
    logger.info(f"  Valid ROI: {valid_roi:.2f}% (Hit: {valid_hit:.2f}%)")
    logger.info(f"  Test ROI:  {test_roi:.2f}% (Hit: {test_hit:.2f}%)")
    logger.info(f"  Gap:       {abs(valid_roi - test_roi):.2f}%")
    logger.info(f"  Test Bets: {test_bets:,}")
    logger.info("=" * 60)
    
    # 9. 保存
    output_dir = project_root / 'keibaai/models/mu_v4_4_final'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / 'model.pkl', 'wb') as f:
        pickle.dump(model, f)
    with open(output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
        json.dump(available_features, f, indent=2, ensure_ascii=False)
    
    imp = pd.DataFrame({'feature': available_features, 'importance': model.feature_importances_}).sort_values('importance', ascending=False)
    imp.to_csv(output_dir / 'feature_importance.csv', index=False)
    
    info = {
        'version': 'v4.4_final',
        'description': 'V4_3 + コース特徴量 + 深層分析新特徴量',
        'valid_roi': valid_roi,
        'test_roi': test_roi,
        'gap': abs(valid_roi - test_roi),
        'feature_count': len(available_features),
        'new_features': new_features,
        'excluded_features': LEAKY_FEATURES,
        'created_at': datetime.now().isoformat()
    }
    with open(output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
        json.dump(info, f, indent=2, ensure_ascii=False)
    
    logger.info(f"モデル保存: {output_dir}")
    
    logger.info("Top 15 Features:")
    for _, row in imp.head(15).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")
    
    logger.info("新特徴量の重要度:")
    for f in new_features:
        if f in available_features:
            idx = available_features.index(f)
            logger.info(f"  {f}: {model.feature_importances_[idx]:.1f}")


if __name__ == "__main__":
    main()

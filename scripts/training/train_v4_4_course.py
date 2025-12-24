#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4 Phase 5: コース特徴量を正しく統合したハイブリッドモデル

【問題解決】
train_data_mu_v3_3にはvenueとtrack_surfaceカラムがないため、
生データ(races.parquet)からマージしてコース特徴量を計算する。

【構成】
- V4_3の150特徴量（リーク除外）
- course_master.yamlから計算したコース特徴量（正しく統合）
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


def load_course_master():
    """course_master.yamlを読み込む"""
    yaml_path = project_root / "keibaai/configs/course_master.yaml"
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_course_features(course_master, venue, surface, distance_m):
    """コース情報から特徴量を取得"""
    if not course_master or venue not in course_master:
        return 0, 300, 'none', 0
    
    venue_data = course_master.get(venue, {})
    surface_data = venue_data.get(surface, {})
    
    # 距離を整数に変換
    try:
        dist = int(distance_m)
    except:
        return 0, 300, 'none', 0
    
    distance_data = surface_data.get(dist, {})
    
    if not distance_data:
        return 0, 300, 'none', 0
    
    # defaultがある場合
    if 'default' in distance_data:
        info = distance_data['default']
    else:
        # 直接値がある場合（コース別にinner/outerがある場合など）
        # 最初のキーを取る
        first_key = list(distance_data.keys())[0] if distance_data else None
        if first_key and isinstance(distance_data[first_key], dict):
            info = distance_data[first_key]
        else:
            info = distance_data
    
    slope = info.get('slope_percent', 0)
    straight = info.get('final_straight_m', 300)
    course_type = info.get('course_type', 'none')
    corners = info.get('corner_count', 0)
    
    return slope, straight, course_type, corners


def main():
    logger.info("=" * 60)
    logger.info("V4.4 Phase 5: コース特徴量を正しく統合")
    logger.info("=" * 60)
    
    # 1. V4_3の訓練データを読み込む
    model_dir = project_root / 'keibaai/models/mu_v3_3'
    train_data = pd.read_parquet(model_dir / 'train_data_mu_v3_3.parquet')
    train_data['race_date'] = pd.to_datetime(train_data['race_date'])
    
    # V4_3の特徴量リスト
    v43_dir = project_root / 'keibaai/models/mu_v4_3'
    with open(v43_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        all_v43_features = json.load(f)
    
    safe_v43_features = [f for f in all_v43_features if f not in LEAKY_FEATURES]
    logger.info(f"V4_3特徴量: {len(all_v43_features)} → {len(safe_v43_features)} (リーク除外)")
    
    # 2. 生データからvenue, track_surface, distance_mを取得してマージ
    races_raw = pd.read_parquet(project_root / 'keibaai/data/parsed/parquet/races/races.parquet')
    races_raw = races_raw[['race_id', 'horse_id', 'venue', 'track_surface', 'distance_m']].copy()
    
    # race_idを文字列に統一
    train_data['race_id'] = train_data['race_id'].astype(str)
    races_raw['race_id'] = races_raw['race_id'].astype(str)
    train_data['horse_id'] = train_data['horse_id'].astype(str)
    races_raw['horse_id'] = races_raw['horse_id'].astype(str)
    
    # マージ
    train_data = train_data.merge(
        races_raw[['race_id', 'horse_id', 'venue', 'track_surface']],
        on=['race_id', 'horse_id'],
        how='left'
    )
    
    logger.info(f"生データマージ後: venue非NaN {train_data['venue'].notna().sum():,}/{len(train_data):,}")
    
    # 3. course_master.yamlからコース特徴量を計算
    course_master = load_course_master()
    
    logger.info("コース特徴量を計算中...")
    course_features = train_data.apply(
        lambda row: get_course_features(
            course_master, 
            row.get('venue'), 
            row.get('track_surface'), 
            row.get('distance_m')
        ),
        axis=1
    )
    
    train_data['course_slope_percent'] = [x[0] for x in course_features]
    train_data['course_final_straight_m'] = [x[1] for x in course_features]
    train_data['course_type'] = [x[2] for x in course_features]
    train_data['course_corner_count'] = [x[3] for x in course_features]
    
    # 派生特徴量
    train_data['is_long_straight'] = (train_data['course_final_straight_m'] >= 400).astype(int)
    train_data['is_steep_slope'] = (train_data['course_slope_percent'] >= 1.5).astype(int)
    train_data['is_outer_course'] = (train_data['course_type'] == 'outer').astype(int)
    
    logger.info(f"  course_slope_percent非ゼロ: {(train_data['course_slope_percent'] > 0).sum():,}/{len(train_data):,}")
    logger.info(f"  is_long_straight: {train_data['is_long_straight'].sum():,}/{len(train_data):,}")
    logger.info(f"  is_steep_slope: {train_data['is_steep_slope'].sum():,}/{len(train_data):,}")
    
    # 4. 特徴量リスト統合
    course_features_list = [
        'course_slope_percent',
        'course_final_straight_m',
        'course_corner_count',
        'is_long_straight',
        'is_steep_slope',
        'is_outer_course'
    ]
    
    all_features = safe_v43_features + course_features_list
    available_features = [f for f in all_features if f in train_data.columns]
    logger.info(f"統合特徴量: {len(available_features)}")
    
    # 5. ターゲット作成
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
    
    # 欠損値処理
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
        callbacks=[
            lgb.early_stopping(stopping_rounds=50),
            lgb.log_evaluation(100)
        ]
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
    logger.info("【結果】")
    logger.info(f"  Valid ROI: {valid_roi:.2f}% (Hit: {valid_hit:.2f}%)")
    logger.info(f"  Test ROI:  {test_roi:.2f}% (Hit: {test_hit:.2f}%)")
    logger.info(f"  Gap:       {abs(valid_roi - test_roi):.2f}%")
    logger.info(f"  Test Bets: {test_bets:,}")
    logger.info("=" * 60)
    
    # 9. 保存
    output_dir = project_root / 'keibaai/models/mu_v4_4_course'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / 'model.pkl', 'wb') as f:
        pickle.dump(model, f)
    
    with open(output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
        json.dump(available_features, f, indent=2, ensure_ascii=False)
    
    imp = pd.DataFrame({
        'feature': available_features,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    imp.to_csv(output_dir / 'feature_importance.csv', index=False)
    
    info = {
        'version': 'v4.4_course',
        'description': 'V4_3特徴量 + course_master.yamlコース特徴量（正しく統合）',
        'valid_roi': valid_roi,
        'test_roi': test_roi,
        'gap': abs(valid_roi - test_roi),
        'feature_count': len(available_features),
        'course_features': course_features_list,
        'excluded_features': LEAKY_FEATURES,
        'created_at': datetime.now().isoformat()
    }
    with open(output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
        json.dump(info, f, indent=2, ensure_ascii=False)
    
    logger.info(f"モデル保存: {output_dir}")
    
    # Top特徴量
    logger.info("Top 15 Features:")
    for _, row in imp.head(15).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")
    
    # コース特徴量の重要度
    logger.info("コース特徴量の重要度:")
    for f in course_features_list:
        if f in available_features:
            idx = available_features.index(f)
            logger.info(f"  {f}: {model.feature_importances_[idx]:.1f}")


if __name__ == "__main__":
    main()

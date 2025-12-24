#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4 Extended: 追加特徴量版

【追加特徴量】
1. 時期のsin/cos表現（季節性の周期的エンコーディング）
   - month_sin, month_cos (12ヶ月周期)
   - week_sin, week_cos (52週周期)
   
2. 過去1-5レースの個別値（平均ではなく生の値）
   - prev_1_finish, prev_2_finish, ..., prev_5_finish
   - prev_1_odds, prev_2_odds, ..., prev_5_odds
   - prev_1_c4_position, prev_2_c4_position, ...
   
3. 右回り/左回り情報
   - is_left_turn (東京、新潟、中京)
   - horse_left_turn_perf (左回りでの成績)
   - horse_right_turn_perf (右回りでの成績)
   - turn_preference (得意な回り)
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
    'form_rank', 'gap_course_fit_popularity', 'gap_jockey_popularity',
    'gap_pedigree_popularity', 'gap_trainer_popularity', 'is_overvalued',
]

# 左回り競馬場
LEFT_TURN_VENUES = ['東京', '新潟', '中京']


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


def add_seasonal_features(df):
    """時期のsin/cos表現"""
    logger.info("  季節特徴量（sin/cos）を追加中...")
    
    df['race_date'] = pd.to_datetime(df['race_date'])
    
    # 月（12ヶ月周期）
    df['month'] = df['race_date'].dt.month
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    # 週（52週周期）
    df['week'] = df['race_date'].dt.isocalendar().week
    df['week_sin'] = np.sin(2 * np.pi * df['week'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week'] / 52)
    
    # 日付（365日周期）
    df['day_of_year'] = df['race_date'].dt.dayofyear
    df['day_sin'] = np.sin(2 * np.pi * df['day_of_year'] / 365)
    df['day_cos'] = np.cos(2 * np.pi * df['day_of_year'] / 365)
    
    return df


def add_individual_past_race_features(df, races_history):
    """過去1-5レースの個別値"""
    logger.info("  過去レース個別特徴量を追加中...")
    
    # レース履歴をソート
    races_history = races_history.sort_values(['horse_id', 'race_date'])
    
    # 馬ごとに過去レースを取得
    for i in range(1, 6):  # prev_1 ~ prev_5
        # shift(i)で i レース前のデータを取得
        races_history[f'prev_{i}_finish'] = races_history.groupby('horse_id')['finish_position'].shift(i)
        races_history[f'prev_{i}_odds'] = races_history.groupby('horse_id')['win_odds'].shift(i)
        
        # コーナー通過順位（c4_positionがあれば）
        if 'c4_position' in races_history.columns:
            races_history[f'prev_{i}_c4'] = races_history.groupby('horse_id')['c4_position'].shift(i)
    
    # 元のdfにマージ
    merge_cols = ['race_id', 'horse_id'] + [f'prev_{i}_finish' for i in range(1, 6)] + [f'prev_{i}_odds' for i in range(1, 6)]
    if 'c4_position' in races_history.columns:
        merge_cols += [f'prev_{i}_c4' for i in range(1, 6)]
    
    df['race_id'] = df['race_id'].astype(str)
    df['horse_id'] = df['horse_id'].astype(str)
    races_history['race_id'] = races_history['race_id'].astype(str)
    races_history['horse_id'] = races_history['horse_id'].astype(str)
    
    available_cols = [c for c in merge_cols if c in races_history.columns]
    df = df.merge(races_history[available_cols].drop_duplicates(['race_id', 'horse_id']), 
                  on=['race_id', 'horse_id'], how='left', suffixes=('', '_new'))
    
    return df


def add_turn_direction_features(df, races_history):
    """右回り/左回り特徴量"""
    logger.info("  回り方向特徴量を追加中...")
    
    # 今回レースの回り方向
    df['is_left_turn'] = df['venue'].isin(LEFT_TURN_VENUES).astype(int)
    
    # レース履歴に回り方向を追加
    races_history = races_history.copy()
    races_history['is_left'] = races_history['venue'].isin(LEFT_TURN_VENUES)
    
    # 馬ごとの左回り/右回り成績を計算
    # 正規化着順 (1が最下位、頭数が1着)
    races_history['field_size'] = races_history.groupby('race_id')['horse_id'].transform('count')
    races_history['norm_finish'] = 1 - (races_history['finish_position'] - 1) / races_history['field_size'].clip(lower=2)
    
    # ソート
    races_history = races_history.sort_values(['horse_id', 'race_date'])
    
    # 左回りでの累積成績（shift(1)でリーク防止）
    left_stats = races_history[races_history['is_left']].groupby('horse_id').apply(
        lambda g: g['norm_finish'].expanding().mean().shift(1)
    ).reset_index(level=0, drop=True)
    races_history.loc[races_history['is_left'], 'cum_left_perf'] = left_stats
    
    # 右回りでの累積成績
    right_stats = races_history[~races_history['is_left']].groupby('horse_id').apply(
        lambda g: g['norm_finish'].expanding().mean().shift(1)
    ).reset_index(level=0, drop=True)
    races_history.loc[~races_history['is_left'], 'cum_right_perf'] = right_stats
    
    # 最新値を取得してマージ
    latest_left = races_history[races_history['is_left']].groupby('horse_id')['cum_left_perf'].last()
    latest_right = races_history[~races_history['is_left']].groupby('horse_id')['cum_right_perf'].last()
    
    df['horse_left_turn_perf'] = df['horse_id'].map(latest_left.to_dict())
    df['horse_right_turn_perf'] = df['horse_id'].map(latest_right.to_dict())
    
    # 回り方向の得意度
    df['turn_preference'] = df['horse_left_turn_perf'].fillna(0.5) - df['horse_right_turn_perf'].fillna(0.5)
    
    # 今回のコースに適した馬かどうか
    df['turn_match'] = np.where(
        df['is_left_turn'] == 1,
        df['horse_left_turn_perf'].fillna(0.5),
        df['horse_right_turn_perf'].fillna(0.5)
    )
    
    return df


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


def main():
    logger.info("=" * 60)
    logger.info("V4.4 Extended: 追加特徴量版")
    logger.info("=" * 60)
    
    # 1. データ読み込み
    model_dir = project_root / 'keibaai/models/mu_v3_3'
    train_data = pd.read_parquet(model_dir / 'train_data_mu_v3_3.parquet')
    train_data['race_date'] = pd.to_datetime(train_data['race_date'])
    
    v43_dir = project_root / 'keibaai/models/mu_v4_3'
    with open(v43_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        all_v43_features = json.load(f)
    safe_v43_features = [f for f in all_v43_features if f not in LEAKY_FEATURES]
    
    # 生データ読み込み
    races_raw = pd.read_parquet(project_root / 'keibaai/data/parsed/parquet/races/races.parquet')
    races_raw['race_date'] = pd.to_datetime(races_raw['race_date'])
    
    # venue, track_surfaceをマージ
    train_data['race_id'] = train_data['race_id'].astype(str)
    train_data['horse_id'] = train_data['horse_id'].astype(str)
    races_raw['race_id'] = races_raw['race_id'].astype(str)
    races_raw['horse_id'] = races_raw['horse_id'].astype(str)
    
    train_data = train_data.merge(
        races_raw[['race_id', 'horse_id', 'venue', 'track_surface']].drop_duplicates(['race_id', 'horse_id']),
        on=['race_id', 'horse_id'], how='left'
    )
    
    logger.info(f"データ: {len(train_data):,}件")
    
    # 2. コース特徴量
    logger.info("特徴量を追加中...")
    course_master = load_course_master()
    course_features = train_data.apply(
        lambda row: get_course_features(
            course_master, row.get('venue'), row.get('track_surface'), row.get('distance_m')
        ), axis=1
    )
    train_data['course_slope_percent'] = [x[0] for x in course_features]
    train_data['course_final_straight_m'] = [x[1] for x in course_features]
    train_data['course_corner_count'] = [x[2] for x in course_features]
    
    # 3. 季節特徴量（sin/cos）
    train_data = add_seasonal_features(train_data)
    
    # 4. 過去レース個別特徴量
    train_data = add_individual_past_race_features(train_data, races_raw)
    
    # 5. 回り方向特徴量
    train_data = add_turn_direction_features(train_data, races_raw)
    
    # 6. 特徴量リスト
    new_features = [
        # 季節
        'month_sin', 'month_cos', 'week_sin', 'week_cos', 'day_sin', 'day_cos',
        # コース
        'course_slope_percent', 'course_final_straight_m', 'course_corner_count',
        # 回り方向
        'is_left_turn', 'horse_left_turn_perf', 'horse_right_turn_perf', 'turn_preference', 'turn_match',
        # 過去レース個別
    ] + [f'prev_{i}_finish' for i in range(1, 6)] + [f'prev_{i}_odds' for i in range(1, 6)]
    
    all_features = safe_v43_features + new_features
    available_features = [f for f in all_features if f in train_data.columns]
    logger.info(f"統合特徴量: {len(available_features)}")
    
    # 7. ターゲット
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    odds = train_data['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    gain = np.zeros(len(train_data))
    gain[train_data['finish_position'] == 1] = log_odds[train_data['finish_position'] == 1] * weight_1st
    gain[train_data['finish_position'] == 2] = log_odds[train_data['finish_position'] == 2] * weight_2nd
    gain[train_data['finish_position'] == 3] = log_odds[train_data['finish_position'] == 3] * weight_3rd
    train_data['target_relevance'] = gain.astype(int)
    train_data['sample_weight'] = np.log1p(train_data['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    # 8. 期間分割
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
    
    # 9. 訓練（Optunaで最適化したパラメータをベースに）
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'num_leaves': 39,
        'max_depth': 6,
        'min_child_samples': 141,
        'learning_rate': 0.097,
        'reg_alpha': 1.7,
        'reg_lambda': 3.6,
        'feature_fraction': 0.61,
        'bagging_fraction': 0.60,
        'bagging_freq': 6,
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
    
    # 10. 評価
    valid_preds = model.predict(valid_df[available_features])
    test_preds = model.predict(test_df[available_features])
    valid_roi, valid_hit = calc_roi(valid_df, valid_preds)
    test_roi, test_hit = calc_roi(test_df, test_preds)
    
    logger.info("=" * 60)
    logger.info("【最終結果】")
    logger.info(f"  Valid ROI: {valid_roi:.2f}% (Hit: {valid_hit:.2f}%)")
    logger.info(f"  Test ROI:  {test_roi:.2f}% (Hit: {test_hit:.2f}%)")
    logger.info(f"  Gap:       {abs(valid_roi - test_roi):.2f}%")
    logger.info("=" * 60)
    
    # 11. 保存
    output_dir = project_root / 'keibaai/models/mu_v4_4_extended'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / 'model.pkl', 'wb') as f:
        pickle.dump(model, f)
    with open(output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
        json.dump(available_features, f, indent=2, ensure_ascii=False)
    
    imp = pd.DataFrame({'feature': available_features, 'importance': model.feature_importances_}).sort_values('importance', ascending=False)
    imp.to_csv(output_dir / 'feature_importance.csv', index=False)
    
    info = {
        'version': 'v4.4_extended',
        'description': '追加特徴量版（季節sin/cos、過去レース個別、回り方向）',
        'valid_roi': valid_roi,
        'test_roi': test_roi,
        'gap': abs(valid_roi - test_roi),
        'feature_count': len(available_features),
        'new_features': new_features,
        'created_at': datetime.now().isoformat()
    }
    with open(output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
        json.dump(info, f, indent=2, ensure_ascii=False)
    
    logger.info(f"モデル保存: {output_dir}")
    
    logger.info("Top 20 Features:")
    for _, row in imp.head(20).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")
    
    logger.info("新特徴量の重要度:")
    for f in new_features:
        if f in available_features:
            idx = available_features.index(f)
            logger.info(f"  {f}: {model.feature_importances_[idx]:.1f}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4 Phase 4: ハイブリッドモデル
- V4_3の150特徴量（リーク6個除外）
- V16のコース特徴量（3個追加）
= 153特徴量

【構成】
1. V4_3学習済みデータ（train_data_mu_v3_3.parquet）を使用
2. V16のコース特徴量を追加計算
3. リーク特徴量を除外
4. LambdaRankで訓練

【リーク対策】
- form_rank, gap_*, is_overvaluedを除外
- V16のコース特徴量はshift(1)で計算済み
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

# リークが疑われる特徴量
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


def get_course_info(course_master, venue, surface, distance_m):
    """コース情報を取得"""
    if not course_master or venue not in course_master:
        return {}
    venue_data = course_master.get(venue, {})
    surface_data = venue_data.get(surface, {})
    distance_data = surface_data.get(distance_m, {})
    if not distance_data:
        return {}
    if 'default' in distance_data:
        return distance_data['default']
    return distance_data


def add_v16_course_features(df, course_master):
    """V16のコース特徴量を追加（レース条件から直接計算、リークなし）"""
    logger.info("V16コース特徴量を追加中...")
    
    # コース情報を取得
    slopes = []
    straights = []
    for _, row in df.iterrows():
        info = get_course_info(course_master, row.get('venue'), row.get('track_surface'), row.get('distance_m'))
        slopes.append(info.get('slope_percent', 0))
        straights.append(info.get('final_straight_m', 300))
    
    df['course_slope_percent'] = slopes
    df['course_final_straight_m'] = straights
    df['is_long_straight'] = (df['course_final_straight_m'] >= 400).astype(int)
    
    logger.info(f"  course_slope_percent非ゼロ: {(df['course_slope_percent'] > 0).sum():,}/{len(df):,}")
    logger.info(f"  is_long_straight: {df['is_long_straight'].sum():,}/{len(df):,}")
    
    return df


def load_v43_data():
    """V4_3の訓練データを読み込む"""
    model_dir = project_root / 'keibaai/models/mu_v3_3'
    train_data = pd.read_parquet(model_dir / 'train_data_mu_v3_3.parquet')
    train_data['race_date'] = pd.to_datetime(train_data['race_date'])
    
    # V4_3の特徴量リスト
    v43_dir = project_root / 'keibaai/models/mu_v4_3'
    with open(v43_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        all_features = json.load(f)
    
    # リーク特徴量を除外
    safe_features = [f for f in all_features if f not in LEAKY_FEATURES]
    
    logger.info(f"V4_3特徴量: {len(all_features)} → {len(safe_features)} (リーク{len(LEAKY_FEATURES)}個除外)")
    
    return train_data, safe_features


def prepare_target(df):
    """LambdaRank用ターゲット"""
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    odds = df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    gain = np.zeros(len(df))
    gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
    gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
    gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
    
    df['target_relevance'] = gain.astype(int)
    df['sample_weight'] = np.log1p(df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    return df


def calc_roi(df, preds):
    """ROI計算"""
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


def main():
    logger.info("=" * 60)
    logger.info("V4.4 Phase 4: ハイブリッドモデル (V4_3 + V16コース)")
    logger.info("=" * 60)
    
    # 1. データ読み込み
    train_data, v43_features = load_v43_data()
    course_master = load_course_master()
    
    logger.info(f"データ期間: {train_data['race_date'].min().date()} ~ {train_data['race_date'].max().date()}")
    logger.info(f"データ件数: {len(train_data):,}")
    
    # 2. V16コース特徴量を追加
    train_data = add_v16_course_features(train_data, course_master)
    
    # 3. 特徴量リスト統合
    v16_features = ['course_slope_percent', 'course_final_straight_m', 'is_long_straight']
    all_features = v43_features + v16_features
    
    # 利用可能な特徴量に絞る
    available_features = [f for f in all_features if f in train_data.columns]
    logger.info(f"統合特徴量: {len(available_features)} (V4_3: {len(v43_features)}, V16: {len(v16_features)})")
    
    # 4. ターゲット作成
    train_data = prepare_target(train_data)
    
    # 5. 期間分割 (V4_3と同じ)
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
    
    # 6. 訓練 (V4_3より正則化を少し強化)
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'num_leaves': 40,
        'learning_rate': 0.05,
        'min_child_samples': 120,  # V4_3より少し強化
        'max_depth': 6,
        'reg_alpha': 2.0,          # V4_3より少し強化
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
    
    # 7. 評価
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
    
    # 8. 保存
    output_dir = project_root / 'keibaai/models/mu_v4_4_hybrid'
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
        'version': 'v4.4_hybrid',
        'description': 'V4_3特徴量(リーク除外) + V16コース特徴量',
        'valid_roi': valid_roi,
        'test_roi': test_roi,
        'gap': abs(valid_roi - test_roi),
        'feature_count': len(available_features),
        'v43_features': len(v43_features),
        'v16_features': len(v16_features),
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
    
    # V16特徴量の重要度
    logger.info("V16コース特徴量の重要度:")
    for f in v16_features:
        if f in available_features:
            idx = available_features.index(f)
            logger.info(f"  {f}: {model.feature_importances_[idx]:.1f}")


if __name__ == "__main__":
    main()

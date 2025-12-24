#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4 Phase 2: リークフリー版 (v4_3の特徴量からリーク特徴量を除外)

【リーク対策】
- gap_*_popularity (4つ) を除外 (確定オッズ由来のpopularityに依存)
- form_rank を除外 (当日レース内ランク)
- is_overvalued を除外 (popularityベース)

【過学習対策】
- 正則化パラメータをv4_3より強化
- 2022年以前のデータでの過学習を監視

Train: ~2024-12-31
Test: 2025-01-01~
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
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# リークが疑われる特徴量 (除外対象)
LEAKY_FEATURES = [
    'form_rank',                    # 当日ランク
    'gap_course_fit_popularity',    # popularity依存
    'gap_jockey_popularity',        # popularity依存
    'gap_pedigree_popularity',      # popularity依存
    'gap_trainer_popularity',       # popularity依存
    'is_overvalued',                # popularity依存
]


def load_v43_data_and_features():
    """v4_3の訓練データと特徴量を読み込む"""
    model_dir = project_root / 'keibaai/models/mu_v3_3'
    
    # v4_3はmu_v3_3のデータを使用
    train_data = pd.read_parquet(model_dir / 'train_data_mu_v3_3.parquet')
    train_data['race_date'] = pd.to_datetime(train_data['race_date'])
    
    # v4_3の特徴量リストを読み込み
    v43_dir = project_root / 'keibaai/models/mu_v4_3'
    with open(v43_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        all_features = json.load(f)
    
    # リーク特徴量を除外
    safe_features = [f for f in all_features if f not in LEAKY_FEATURES]
    
    logger.info(f"全特徴量: {len(all_features)}")
    logger.info(f"除外特徴量: {LEAKY_FEATURES}")
    logger.info(f"使用特徴量: {len(safe_features)}")
    
    return train_data, safe_features


def prepare_target(df):
    """LambdaRank用ターゲット (v4_3と同じロジック)"""
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
        return 0, 0
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate


def train_model(df, feature_cols):
    """モデル訓練"""
    logger.info("=" * 60)
    logger.info("V4.4 Phase 2 (Leak-Free v4_3) 訓練開始")
    logger.info("=" * 60)
    
    # 期間分割 (v4_3と同じ)
    train_mask = df['race_date'] < '2023-01-01'
    valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
    test_mask = df['race_date'] >= '2024-01-01'
    
    train_df = df[train_mask].copy()
    valid_df = df[valid_mask].copy()
    test_df = df[test_mask].copy()
    
    # 使用可能な特徴量のみに絞る
    available_features = [f for f in feature_cols if f in df.columns]
    logger.info(f"利用可能特徴量: {len(available_features)}/{len(feature_cols)}")
    
    # 欠損値処理
    train_df[available_features] = train_df[available_features].fillna(0)
    valid_df[available_features] = valid_df[available_features].fillna(0)
    test_df[available_features] = test_df[available_features].fillna(0)
    
    group_train = train_df.groupby('race_id').size().to_list()
    group_valid = valid_df.groupby('race_id').size().to_list()
    
    logger.info(f"Train: {len(train_df):,} (Groups: {len(group_train):,})")
    logger.info(f"Valid: {len(valid_df):,} (Groups: {len(group_valid):,})")
    logger.info(f"Test:  {len(test_df):,}")
    
    # LambdaRankモデル (正則化をv4_3より強化)
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'num_leaves': 40,          # v4_3: 最適化で決定 → 保守的に設定
        'learning_rate': 0.05,
        'min_child_samples': 150,  # v4_3より増加 (過学習対策)
        'max_depth': 6,
        'reg_alpha': 3.0,          # v4_3より強化
        'reg_lambda': 5.0,         # v4_3より強化
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
    
    # 評価
    valid_preds = model.predict(valid_df[available_features])
    test_preds = model.predict(test_df[available_features])
    
    valid_roi, valid_hit = calc_roi(valid_df, valid_preds)
    test_roi, test_hit = calc_roi(test_df, test_preds)
    
    logger.info("=" * 60)
    logger.info("【結果】")
    logger.info(f"  Valid ROI: {valid_roi:.2f}% (Hit: {valid_hit:.2f}%)")
    logger.info(f"  Test ROI:  {test_roi:.2f}% (Hit: {test_hit:.2f}%)")
    logger.info(f"  Gap:       {abs(valid_roi - test_roi):.2f}%")
    logger.info("=" * 60)
    
    # 特徴量重要度
    imp = pd.DataFrame({
        'feature': available_features,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    logger.info("Top 15 Features:")
    for i, row in imp.head(15).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']}")
    
    return model, available_features, valid_roi, test_roi, imp


def save_model(model, features, valid_roi, test_roi, imp):
    """モデル保存"""
    output_dir = project_root / 'keibaai/models/mu_v4_4_leakfree'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # モデル
    with open(output_dir / 'model.pkl', 'wb') as f:
        pickle.dump(model, f)
    
    # 特徴量
    with open(output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
        json.dump(features, f, indent=2, ensure_ascii=False)
    
    # 重要度
    imp.to_csv(output_dir / 'feature_importance.csv', index=False)
    
    # メタ情報
    info = {
        'version': 'v4.4_leakfree',
        'description': 'v4_3からリーク特徴量(6個)を除外したリークフリー版',
        'valid_roi': valid_roi,
        'test_roi': test_roi,
        'gap': abs(valid_roi - test_roi),
        'feature_count': len(features),
        'excluded_features': LEAKY_FEATURES,
        'created_at': datetime.now().isoformat()
    }
    with open(output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
        json.dump(info, f, indent=2, ensure_ascii=False)
    
    logger.info(f"モデル保存: {output_dir}")


def main():
    # 1. データ読み込み
    train_data, safe_features = load_v43_data_and_features()
    
    # 2. ターゲット作成
    train_data = prepare_target(train_data)
    
    # 3. 訓練
    model, features, valid_roi, test_roi, imp = train_model(train_data, safe_features)
    
    # 4. 保存
    save_model(model, features, valid_roi, test_roi, imp)
    
    logger.info("完了！")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V16特徴量検証スクリプト

V16特徴量エンジニアで生成した新特徴量を使用し、
既存のV4.4モデルと比較してROI改善効果を検証する。

【検証項目】
1. V16新特徴量のみでモデル学習
2. 既存V4.3特徴量 + V16新特徴量でモデル学習
3. 年度別の安定性確認
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16


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


def main():
    logger.info("=" * 80)
    logger.info("V16特徴量検証")
    logger.info("=" * 80)
    
    # データ準備
    logger.info("\nデータ読み込み中...")
    data_path = project_root / 'keibaai/data/parsed/parquet'
    
    races = pd.read_parquet(data_path / 'races/races.parquet')
    corners = pd.read_parquet(data_path / 'corners/corner_positions.parquet')
    pedigrees = pd.read_parquet(data_path / 'pedigrees/pedigrees.parquet')
    
    logger.info(f"レースデータ: {len(races):,}件")
    
    # V16特徴量エンジニアリング
    logger.info("\nV16特徴量エンジニアリング実行中...")
    fe = LeakFreeFeatureEngineerV16()
    fe.fit(races, pedigrees_df=pedigrees, corners_df=corners)
    df = fe.transform(races)
    
    # 日付処理
    df['race_date'] = pd.to_datetime(df['race_date'])
    df['year'] = df['race_date'].dt.year
    df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # 期間分割
    train_mask = df['race_date'] < '2023-01-01'
    valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
    test_mask = df['race_date'] >= '2024-01-01'
    
    train_df = df[train_mask].copy()
    valid_df = df[valid_mask].copy()
    test_df = df[test_mask].copy()
    
    logger.info(f"\nTrain: {len(train_df):,}件 (~2022)")
    logger.info(f"Valid: {len(valid_df):,}件 (2023)")
    logger.info(f"Test:  {len(test_df):,}件 (2024~)")
    
    # V16新特徴量
    v16_features = [
        'prev_3f_avg',
        'horse_prev_winrate',
        'jockey_prev_winrate',
        'trainer_prev_winrate',
        'is_late_race',
        'prev_finish',
    ]
    
    # 既存のV15_Fixed特徴量
    v15_features = [f for f in fe.get_feature_columns() if f not in v16_features and f != 'prev_finish_cat']
    
    # 数値型特徴量のみ使用
    all_features = v15_features + v16_features
    available_features = [f for f in all_features if f in df.columns and df[f].dtype in ['float64', 'int64', 'int32', 'float32']]
    
    logger.info(f"\n利用可能な特徴量: {len(available_features)}個")
    logger.info(f"  V15特徴量: {len([f for f in v15_features if f in available_features])}個")
    logger.info(f"  V16新特徴量: {len([f for f in v16_features if f in available_features])}個")
    
    # 欠損値処理
    train_df[available_features] = train_df[available_features].fillna(0)
    valid_df[available_features] = valid_df[available_features].fillna(0)
    test_df[available_features] = test_df[available_features].fillna(0)
    
    # ターゲット作成（EV-weighted relevance）
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    
    for data in [train_df, valid_df, test_df]:
        odds = data['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(data))
        gain[data['finish_position'] == 1] = log_odds[data['finish_position'] == 1] * weight_1st
        gain[data['finish_position'] == 2] = log_odds[data['finish_position'] == 2] * weight_2nd
        gain[data['finish_position'] == 3] = log_odds[data['finish_position'] == 3] * weight_3rd
        data['target_relevance'] = gain.astype(int)
        data['sample_weight'] = np.log1p(data['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    # グループ
    train_df = train_df.sort_values('race_id').reset_index(drop=True)
    valid_df = valid_df.sort_values('race_id').reset_index(drop=True)
    test_df = test_df.sort_values('race_id').reset_index(drop=True)
    
    group_train = train_df.groupby('race_id', sort=False).size().to_list()
    group_valid = valid_df.groupby('race_id', sort=False).size().to_list()
    
    # ========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("【検証1: V16新特徴量のみ】")
    logger.info("=" * 80)
    # ========================================================================
    
    v16_only_features = [f for f in v16_features if f in available_features]
    logger.info(f"使用特徴量: {len(v16_only_features)}個")
    
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'num_leaves': 31,
        'max_depth': 6,
        'min_child_samples': 100,
        'learning_rate': 0.05,
        'reg_alpha': 2.0,
        'reg_lambda': 2.0,
        'feature_fraction': 0.7,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': -1,
        'random_state': 42,
        'label_gain': list(range(100)),
        'n_estimators': 500
    }
    
    model_v16_only = lgb.LGBMRanker(**params)
    model_v16_only.fit(
        train_df[v16_only_features], train_df['target_relevance'],
        group=group_train, sample_weight=train_df['sample_weight'],
        eval_set=[(valid_df[v16_only_features], valid_df['target_relevance'])],
        eval_group=[group_valid],
        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
    )
    
    valid_preds = model_v16_only.predict(valid_df[v16_only_features])
    test_preds = model_v16_only.predict(test_df[v16_only_features])
    
    valid_roi, valid_hit = calc_roi(valid_df, valid_preds)
    test_roi, test_hit = calc_roi(test_df, test_preds)
    
    logger.info(f"\n結果:")
    logger.info(f"  Valid ROI: {valid_roi:.2f}% (Hit: {valid_hit:.2f}%)")
    logger.info(f"  Test ROI:  {test_roi:.2f}% (Hit: {test_hit:.2f}%)")
    logger.info(f"  Gap:       {abs(valid_roi - test_roi):.2f}%")
    
    # ========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("【検証2: V15 + V16全特徴量】")
    logger.info("=" * 80)
    # ========================================================================
    
    logger.info(f"使用特徴量: {len(available_features)}個")
    
    model_all = lgb.LGBMRanker(**params)
    model_all.fit(
        train_df[available_features], train_df['target_relevance'],
        group=group_train, sample_weight=train_df['sample_weight'],
        eval_set=[(valid_df[available_features], valid_df['target_relevance'])],
        eval_group=[group_valid],
        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
    )
    
    valid_preds_all = model_all.predict(valid_df[available_features])
    test_preds_all = model_all.predict(test_df[available_features])
    
    valid_roi_all, valid_hit_all = calc_roi(valid_df, valid_preds_all)
    test_roi_all, test_hit_all = calc_roi(test_df, test_preds_all)
    
    logger.info(f"\n結果:")
    logger.info(f"  Valid ROI: {valid_roi_all:.2f}% (Hit: {valid_hit_all:.2f}%)")
    logger.info(f"  Test ROI:  {test_roi_all:.2f}% (Hit: {test_hit_all:.2f}%)")
    logger.info(f"  Gap:       {abs(valid_roi_all - test_roi_all):.2f}%")
    
    # 特徴量重要度
    logger.info("\nV16新特徴量の重要度:")
    imp = pd.DataFrame({
        'feature': available_features,
        'importance': model_all.feature_importances_
    }).sort_values('importance', ascending=False)
    
    v16_imp = imp[imp['feature'].isin(v16_features)]
    for _, row in v16_imp.iterrows():
        pct = row['importance'] / imp['importance'].sum() * 100
        logger.info(f"  {row['feature']:25s}: {row['importance']:5d} ({pct:.2f}%)")
    
    # ========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("【総合評価】")
    logger.info("=" * 80)
    # ========================================================================
    
    logger.info(f"""
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        V16特徴量検証結果                                          │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│ 【V16新特徴量のみ】                                                              │
│   Valid ROI: {valid_roi:.2f}%                                                  │
│   Test ROI:  {test_roi:.2f}%                                                   │
│   Gap:       {abs(valid_roi - test_roi):.2f}%                                                    │
│                                                                                   │
│ 【V15 + V16全特徴量】                                                            │
│   Valid ROI: {valid_roi_all:.2f}%                                                  │
│   Test ROI:  {test_roi_all:.2f}%                                                   │
│   Gap:       {abs(valid_roi_all - test_roi_all):.2f}%                                                    │
│                                                                                   │
└──────────────────────────────────────────────────────────────────────────────────┘
""")
    
    logger.info("検証完了")


if __name__ == "__main__":
    main()

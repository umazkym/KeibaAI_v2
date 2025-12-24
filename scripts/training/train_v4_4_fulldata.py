#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4 Phase 3: 全期間データ (2014-2025) を使用したリークフリーモデル

【改善点】
- 2014-2019年の約32万件のデータを追加
- LeakFreeFeatureEngineerV16を使用（コース特徴量含む）
- リーク特徴量は使用しない

【データ期間】
- Train: 2014-01-01 ~ 2023-12-31
- Valid: 2024-01-01 ~ 2024-06-30  
- Test:  2024-07-01 ~

【リーク対策】
- popularity依存の特徴量は生成しない
- 全統計はshift(1)で計算

【過学習対策】
- 正則化強化
- 長期間データで汎化性向上を期待
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

# レークフリー特徴量エンジン
from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16


def load_all_data():
    """全期間データを読み込む"""
    logger.info("全期間データを読み込み中...")
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    logger.info(f"  races: {len(races):,}件 ({races['race_date'].min().date()} ~ {races['race_date'].max().date()})")
    
    return races, pedigrees, corners, race_details, horses


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
    logger.info("V4.4 Phase 3: 全期間データ (2014-2025) リークフリーモデル")
    logger.info("=" * 60)
    
    # 1. データ読み込み
    races, pedigrees, corners, race_details, horses = load_all_data()
    
    # 2. 期間分割
    train_raw = races[races['race_date'] < '2024-01-01'].copy()
    valid_raw = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2024-07-01')].copy()
    test_raw = races[races['race_date'] >= '2024-07-01'].copy()
    
    logger.info(f"Train: {len(train_raw):,}件 (~2023-12-31)")
    logger.info(f"Valid: {len(valid_raw):,}件 (2024-01-01~2024-06-30)")
    logger.info(f"Test:  {len(test_raw):,}件 (2024-07-01~)")
    
    # 3. 特徴量生成 (LeakFreeV16)
    logger.info("特徴量生成中 (LeakFreeFeatureEngineerV16)...")
    engine = LeakFreeFeatureEngineerV16()
    engine.fit(train_raw, pedigrees, corners, race_details, horses_df=horses)
    
    train_df = engine.transform(train_raw)
    valid_df = engine.transform(valid_raw)
    test_df = engine.transform(test_raw)
    
    # 4. ターゲット作成
    train_df = prepare_target(train_df)
    valid_df = prepare_target(valid_df)
    test_df = prepare_target(test_df)
    
    # 使用特徴量
    feature_cols = [c for c in engine.get_feature_columns() if c in train_df.columns]
    logger.info(f"特徴量数: {len(feature_cols)}")
    
    # 欠損値処理
    train_df[feature_cols] = train_df[feature_cols].fillna(0)
    valid_df[feature_cols] = valid_df[feature_cols].fillna(0)
    test_df[feature_cols] = test_df[feature_cols].fillna(0)
    
    # ソート (LambdaRank用)
    train_df = train_df.sort_values('race_id')
    valid_df = valid_df.sort_values('race_id')
    test_df = test_df.sort_values('race_id')
    
    group_train = train_df.groupby('race_id', sort=False).size().to_list()
    group_valid = valid_df.groupby('race_id', sort=False).size().to_list()
    
    # 5. 訓練
    logger.info("LightGBM訓練中...")
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3],
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'min_child_samples': 100,
        'max_depth': 6,
        'reg_alpha': 2.0,
        'reg_lambda': 3.0,
        'colsample_bytree': 0.7,
        'subsample': 0.8,
        'random_state': 42,
        'verbose': -1,
        'label_gain': list(range(100))
    }
    
    model = lgb.LGBMRanker(**params, n_estimators=1000)
    model.fit(
        train_df[feature_cols], train_df['target_relevance'],
        group=group_train,
        eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
        eval_group=[group_valid],
        callbacks=[
            lgb.early_stopping(stopping_rounds=50),
            lgb.log_evaluation(100)
        ]
    )
    
    # 6. 評価
    train_pred = model.predict(train_df[feature_cols])
    valid_pred = model.predict(valid_df[feature_cols])
    test_pred = model.predict(test_df[feature_cols])
    
    train_roi, train_hit, _ = calc_roi(train_df, train_pred)
    valid_roi, valid_hit, _ = calc_roi(valid_df, valid_pred)
    test_roi, test_hit, test_bets = calc_roi(test_df, test_pred)
    
    logger.info("=" * 60)
    logger.info("【結果】")
    logger.info(f"  Train ROI: {train_roi:.2f}% (Hit: {train_hit:.2f}%)")
    logger.info(f"  Valid ROI: {valid_roi:.2f}% (Hit: {valid_hit:.2f}%)")
    logger.info(f"  Test ROI:  {test_roi:.2f}% (Hit: {test_hit:.2f}%)")
    logger.info(f"  Train-Valid Gap: {abs(train_roi - valid_roi):.2f}%")
    logger.info(f"  Valid-Test Gap:  {abs(valid_roi - test_roi):.2f}%")
    logger.info(f"  Test Bets: {test_bets:,}")
    logger.info("=" * 60)
    
    # 7. 保存
    output_dir = project_root / 'keibaai/models/mu_v4_4_fulldata'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / 'model.pkl', 'wb') as f:
        pickle.dump(model, f)
    
    with open(output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
        json.dump(feature_cols, f, indent=2, ensure_ascii=False)
    
    imp = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    imp.to_csv(output_dir / 'feature_importance.csv', index=False)
    
    info = {
        'version': 'v4.4_fulldata',
        'description': '全期間データ(2014-2025)使用、LeakFreeV16特徴量',
        'train_period': '2014-01-01 ~ 2023-12-31',
        'valid_period': '2024-01-01 ~ 2024-06-30',
        'test_period': '2024-07-01 ~',
        'train_roi': train_roi,
        'valid_roi': valid_roi,
        'test_roi': test_roi,
        'train_valid_gap': abs(train_roi - valid_roi),
        'valid_test_gap': abs(valid_roi - test_roi),
        'feature_count': len(feature_cols),
        'train_rows': len(train_df),
        'created_at': datetime.now().isoformat()
    }
    with open(output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
        json.dump(info, f, indent=2, ensure_ascii=False)
    
    logger.info(f"モデル保存: {output_dir}")
    
    # Top特徴量
    logger.info("Top 10 Features:")
    for _, row in imp.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")


if __name__ == "__main__":
    main()

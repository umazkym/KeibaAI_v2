#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4モデル訓練＆保存スクリプト (LambdaRank版)

【構成】
- 特徴量: LeakFreeFeatureEngineerV16 (V15 + コース特徴量 + リークフリー)
- モデル: LightGBM (LambdaRank)
- 目的: フィルタなしで高ROIを目指す
- Train: ~2024-12-31
- Test: 2025-01-01 ~
"""

import sys
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import pickle
import json
from datetime import datetime
import warnings

# プロジェクトルートの設定
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 警告抑制
warnings.filterwarnings('ignore')

def load_data():
    """データ読み込み"""
    logger.info("データを読み込み中...")
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    # 基本的なフィルタリング
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns

def prepare_target(df):
    """LambdaRank用のターゲット作成"""
    # ターゲット: 1着=12.74, 2着=6.73, 3着=3.69 (オッズに応じた重み付け相当)
    # v4_3の設定を踏襲
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    
    gain = np.zeros(len(df))
    # オッズによる重み付け（高配当ほど重視）
    odds = df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    # 順位によるベーススコア * オッズ重み
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
    
    # Top1のみに賭けた場合
    bet_df = d[d['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    
    if len(bet_df) == 0:
        return 0, 0, 0
    
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate, len(bet_df)

def main():
    logger.info("=" * 60)
    logger.info("V4.4モデル訓練開始 (LeakFree V16 + LambdaRank)")
    logger.info("=" * 60)
    
    # 1. データ読み込み
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 2. 期間分割
    train_end = '2024-12-31'
    test_start = '2025-01-01'
    
    train_raw = races[races['race_date'] <= train_end].copy()
    test_raw = races[races['race_date'] >= test_start].copy()
    
    logger.info(f"Train期間: ~{train_end} ({len(train_raw):,}件)")
    logger.info(f"Test期間 : {test_start}~ ({len(test_raw):,}件)")
    
    # 3. 特徴量生成 (V16)
    logger.info("特徴量を生成中 (LeakFreeFeatureEngineerV16)...")
    engine = LeakFreeFeatureEngineerV16()
    engine.fit(train_raw, pedigrees, corners, race_details, horses_df=horses)
    
    train_df = engine.transform(train_raw)
    test_df = engine.transform(test_raw)
    
    # 4. ターゲット作成
    train_df = prepare_target(train_df)
    test_df = prepare_target(test_df)
    
    # 欠損値埋め
    feature_cols = [c for c in engine.get_feature_columns() if c in train_df.columns]
    logger.info(f"特徴量数: {len(feature_cols)}")
    
    train_df[feature_cols] = train_df[feature_cols].fillna(0)
    test_df[feature_cols] = test_df[feature_cols].fillna(0)
    
    # LambdaRank用データセット作成
    # group (クエリ=レース) ごとのデータ数が必要
    query_train = train_df.groupby('race_id').size().to_list()
    query_test = test_df.groupby('race_id').size().to_list()
    
    # ソート (LightGBMのgroup学習にはrace_idでソートされている必要がある...が、groupby().size()の順序とデータの順序が一致している必要がある)
    # 明示的にソートする
    train_df = train_df.sort_values('race_id')
    test_df = test_df.sort_values('race_id')
    
    # 再度group計算 (ソート順)
    # race_idが変わるタイミングを見るのではなく、race_idごとにまとまっている前提で件数を出す
    query_train = train_df.groupby('race_id', sort=False).size().to_list()
    query_test = test_df.groupby('race_id', sort=False).size().to_list()
    
    logger.info("データセット作成完了")
    
    # 5. モデル訓練
    # v4_3のベストパラメータに近い設定（少し保守的に）
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3],
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'min_child_samples': 100,
        'max_depth': 6,
        'reg_alpha': 1.0,
        'reg_lambda': 2.0,
        'colsample_bytree': 0.8,
        'subsample': 0.8,
        'random_state': 42,
        'verbose': -1,
        'label_gain': list(range(100)) # ターゲット値の最大値までカバー
    }
    
    logger.info("LightGBM訓練開始...")
    model = lgb.LGBMRanker(**params, n_estimators=1000)
    
    model.fit(
        train_df[feature_cols],
        train_df['target_relevance'],
        group=query_train,
        eval_set=[(test_df[feature_cols], test_df['target_relevance'])],
        eval_group=[query_test],
        callbacks=[
            lgb.early_stopping(stopping_rounds=50),
            lgb.log_evaluation(100)
        ]
    )
    
    # 6. 評価
    train_pred = model.predict(train_df[feature_cols])
    test_pred = model.predict(test_df[feature_cols])
    
    logger.info("ROI計算中...")
    train_roi, train_hit, _ = calc_roi(train_df, train_pred)
    test_roi, test_hit, n_bets = calc_roi(test_df, test_pred)
    
    logger.info("=" * 60)
    logger.info("【最終結果 V4.4】")
    logger.info(f"Train ROI: {train_roi:.1f}% (Hit: {train_hit:.1f}%)")
    logger.info(f"Test  ROI: {test_roi:.1f}% (Hit: {test_hit:.1f}%)")
    logger.info(f"Test Bets: {n_bets:,}件")
    logger.info("=" * 60)
    
    # 7. 保存
    output_dir = project_root / "keibaai/models/mu_v4_4"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # モデル
    with open(output_dir / "model.pkl", "wb") as f:
        pickle.dump(model, f)
    
    # 特徴量リスト
    with open(output_dir / "feature_names.json", "w") as f:
        json.dump(feature_cols, f, indent=4)
        
    # 重要度
    imp_df = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    imp_df.to_csv(output_dir / "feature_importance.csv", index=False)
    
    logger.info(f"モデル保存完了: {output_dir}")
    
    # トップ特徴量表示
    logger.info("Top 10 Features:")
    for i, row in imp_df.head(10).iterrows():
        logger.info(f"{row['feature']}: {row['importance']:.1f}")

if __name__ == "__main__":
    main()

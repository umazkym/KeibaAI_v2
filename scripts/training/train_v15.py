#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V15モデル訓練＆保存スクリプト

【V15公式設定】
- Train: ~2024-12-31
- Test: 2025-01-01 ~ 2025-10-31
- モデル: Binary Classification (objective='binary')
- パラメータ: learning_rate=0.03, num_leaves=20, max_depth=3 etc.
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import sys
import pickle
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races_df = races_df[races_df['finish_position'].notna()].copy()
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    return races_df, pedigrees, corners, race_details, horses, returns_df


def calc_roi(df, preds):
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    if len(bets) == 0:
        return 0, 0, 0
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate, len(bets)


def main():
    logger.info("=" * 60)
    logger.info("V15モデル訓練＆保存")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns_df = load_data()
    
    # V15公式の期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2024-12-31)")
    logger.info(f"  Test:  {len(test):,}件 (2025-01-01~2025-10-31)")
    
    # 特徴量生成
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    
    # V15公式パラメータ
    params = {
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
    
    logger.info("")
    logger.info("モデル訓練中...")
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    # 評価
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    train_roi, train_hit, _ = calc_roi(train_f, train_pred)
    test_roi, test_hit, n_bets = calc_roi(test_f, test_pred)
    
    logger.info(f"  Train: ROI={train_roi:.1f}%, 的中率={train_hit:.1f}%")
    logger.info(f"  Test:  ROI={test_roi:.1f}%, 的中率={test_hit:.1f}%")
    logger.info(f"  Gap:   {train_roi - test_roi:.1f}%")
    
    # モデル保存
    model_dir = project_root / "keibaai/models/v15"
    model_dir.mkdir(parents=True, exist_ok=True)
    
    # モデル保存 (.pkl)
    model_path = model_dir / "v15_model.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    logger.info(f"モデル保存: {model_path}")
    
    # テキスト形式も保存
    txt_path = model_dir / "v15_model.txt"
    model.save_model(str(txt_path))
    logger.info(f"モデル保存: {txt_path}")
    
    # 特徴量リスト保存
    features_path = model_dir / "feature_names.json"
    with open(features_path, 'w', encoding='utf-8') as f:
        json.dump(feature_cols, f, indent=2, ensure_ascii=False)
    logger.info(f"特徴量保存: {features_path}")
    
    # モデル情報保存
    info = {
        'version': 'v15',
        'description': 'V15 Binary Classification (競合リスク、馬番適合度)',
        'train_roi': train_roi,
        'test_roi': test_roi,
        'train_hit_rate': train_hit,
        'test_hit_rate': test_hit,
        'gap': train_roi - test_roi,
        'feature_count': len(feature_cols),
        'train_period': '~2024-12-31',
        'test_period': '2025-01-01~2025-10-31',
        'params': params,
        'created_at': datetime.now().isoformat()
    }
    info_path = model_dir / "model_info.json"
    with open(info_path, 'w', encoding='utf-8') as f:
        json.dump(info, f, indent=2, ensure_ascii=False)
    logger.info(f"情報保存: {info_path}")
    
    # 特徴量重要度保存
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    importance.to_csv(model_dir / "feature_importance.csv", index=False)
    
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"完了！ Test ROI: {test_roi:.1f}%")
    logger.info("=" * 60)
    
    return model, engine, test_roi


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V16モデル訓練＆保存スクリプト

V15 + ペース特徴量（horse_pace_preference, horse_avg_pace_lf, pace_fit_score）

【V16の追加特徴量】
- horse_pace_preference: 馬の得意ペース（好走時）
- horse_avg_pace_lf: 馬の全レース平均ペース
- pace_fit_score: ペース適合度スコア
- venue_surface_pace_trend: 会場×馬場のペース傾向

【過学習対策】
- V15と同じ正則化パラメータを維持
- min_pace_races=5で少数データ対策
- 特徴量を4個のみ追加（最小限）
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
from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16


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
    
    # 障害除外
    if 'track_surface' in races_df.columns:
        races_df = races_df[races_df['track_surface'] != '障害'].copy()
    
    return races_df, pedigrees, corners, race_details, horses, returns_df


def calc_roi(df, preds):
    """ROIを計算"""
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


def calc_roi_by_year(df, preds):
    """年ごとのROIを計算"""
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    df['year'] = df['race_date'].dt.year
    
    results = []
    for year in sorted(df['year'].unique()):
        year_df = df[df['year'] == year]
        roi, hit_rate, n_bets = calc_roi(year_df, year_df['pred'])
        results.append({
            'year': year,
            'roi': roi,
            'hit_rate': hit_rate,
            'n_bets': n_bets
        })
    return pd.DataFrame(results)


def main():
    logger.info("=" * 60)
    logger.info("V16モデル訓練＆保存（ペース特徴量統合）")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns_df = load_data()
    
    logger.info(f"  総レース数: {len(races):,}件")
    logger.info(f"  race_details: {len(race_details):,}件")
    
    # 訓練・テスト期間設定（V15と同じ）
    # Train: ~2023年末
    # Validation: 2024年
    # Test: 2025年
    train = races[races['race_date'] < '2024-01-01'].copy()
    valid = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2025-01-01')].copy()
    test = races[races['race_date'] >= '2025-01-01'].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2023-12-31)")
    logger.info(f"  Valid: {len(valid):,}件 (2024年)")
    logger.info(f"  Test:  {len(test):,}件 (2025年~)")
    
    # 特徴量生成
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV16()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    valid_f = engine.transform(valid)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    # V16新特徴量のカバレッジ確認
    new_features = ['horse_pace_preference', 'horse_avg_pace_lf', 'pace_fit_score', 'venue_surface_pace_trend']
    for feat in new_features:
        if feat in train_f.columns:
            coverage = train_f[feat].notna().mean() * 100
            logger.info(f"    {feat}: カバレッジ {coverage:.1f}%")
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_valid = valid_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    
    # V15公式パラメータを継続使用（過学習対策に実績あり）
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
    valid_pred = model.predict(X_valid)
    test_pred = model.predict(X_test)
    
    train_roi, train_hit, _ = calc_roi(train_f, train_pred)
    valid_roi, valid_hit, _ = calc_roi(valid_f, valid_pred)
    test_roi, test_hit, n_bets = calc_roi(test_f, test_pred)
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリ")
    logger.info("=" * 60)
    logger.info(f"  Train: ROI={train_roi:.1f}%, 的中率={train_hit:.1f}%")
    logger.info(f"  Valid: ROI={valid_roi:.1f}%, 的中率={valid_hit:.1f}%")
    logger.info(f"  Test:  ROI={test_roi:.1f}%, 的中率={test_hit:.1f}%")
    logger.info(f"  Train-Valid Gap: {train_roi - valid_roi:.1f}%")
    logger.info(f"  Train-Test Gap:  {train_roi - test_roi:.1f}%")
    
    # 年別ROI（バックテスト）
    logger.info("")
    logger.info("年別ROI:")
    all_data_f = pd.concat([train_f, valid_f, test_f])
    all_data_f['pred'] = np.concatenate([train_pred, valid_pred, test_pred])
    yearly_roi = calc_roi_by_year(all_data_f, all_data_f['pred'])
    for _, row in yearly_roi.iterrows():
        marker = "📊" if row['year'] >= 2024 else "📈"
        logger.info(f"  {marker} {int(row['year'])}: ROI={row['roi']:.1f}%, Hit={row['hit_rate']:.1f}%, Bets={int(row['n_bets'])}")
    
    # 特徴量重要度（上位15）
    logger.info("")
    logger.info("特徴量重要度 (Top 15):")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for i, (_, row) in enumerate(importance.head(15).iterrows()):
        new_marker = "⭐" if row['feature'] in new_features else ""
        logger.info(f"  {i+1:2d}. {row['feature']}: {row['importance']:.0f} {new_marker}")
    
    # モデル保存
    model_dir = project_root / "keibaai/models/v16"
    model_dir.mkdir(parents=True, exist_ok=True)
    
    # モデル保存 (.pkl)
    model_path = model_dir / "v16_model.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    logger.info(f"")
    logger.info(f"モデル保存: {model_path}")
    
    # テキスト形式も保存
    txt_path = model_dir / "v16_model.txt"
    model.save_model(str(txt_path))
    
    # 特徴量リスト保存
    features_path = model_dir / "feature_names.json"
    with open(features_path, 'w', encoding='utf-8') as f:
        json.dump(feature_cols, f, indent=2, ensure_ascii=False)
    
    # モデル情報保存
    info = {
        'version': 'v16',
        'description': 'V16 Binary Classification (V15 + ペース特徴量)',
        'base_model': 'V15',
        'new_features': new_features,
        'train_roi': train_roi,
        'valid_roi': valid_roi,
        'test_roi': test_roi,
        'train_hit_rate': train_hit,
        'valid_hit_rate': valid_hit,
        'test_hit_rate': test_hit,
        'train_valid_gap': train_roi - valid_roi,
        'train_test_gap': train_roi - test_roi,
        'feature_count': len(feature_cols),
        'train_period': '~2023-12-31',
        'valid_period': '2024-01-01~2024-12-31',
        'test_period': '2025-01-01~',
        'params': params,
        'created_at': datetime.now().isoformat()
    }
    info_path = model_dir / "model_info.json"
    with open(info_path, 'w', encoding='utf-8') as f:
        json.dump(info, f, indent=2, ensure_ascii=False)
    
    # 特徴量重要度保存
    importance.to_csv(model_dir / "feature_importance.csv", index=False)
    
    # 年別ROI保存
    yearly_roi.to_csv(model_dir / "yearly_roi.csv", index=False)
    
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"完了！ Valid ROI: {valid_roi:.1f}%, Test ROI: {test_roi:.1f}%")
    logger.info("=" * 60)
    
    return model, engine, valid_roi, test_roi


if __name__ == "__main__":
    main()

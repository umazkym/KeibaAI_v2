#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V16b: 最小限のペース特徴量追加版

V16の失敗から学び、以下を変更：
1. 個馬のペース適性（horse_pace_preference, horse_avg_pace_lf）を除外
2. コース統計のみ追加（venue_surface_pace_trend）
3. V15と同じ訓練期間（~2024-12-31）で公平に比較

【仮説】
- 個馬のペース適性は過学習の原因
- コース統計は固定値なので過学習しにくい
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


def add_venue_pace_features(df, race_details_df, train_cutoff='2024-01-01'):
    """
    会場×馬場のペース傾向のみを追加（個馬統計は使わない）
    
    【リーク対策】
    - train_cutoff以前のデータのみで統計を計算
    - 全期間に同じ固定統計を適用
    """
    logger.info("  会場×馬場ペース傾向を計算中...")
    
    # race_detailsのrace_idを文字列に統一
    rd = race_details_df.copy()
    rd['race_id'] = rd['race_id'].astype(str)
    
    # df用の型変換
    df = df.copy()
    df['race_id'] = df['race_id'].astype(str)
    
    # ペース差を計算
    rd['first_half'] = pd.to_numeric(rd['first_half'], errors='coerce')
    rd['second_half'] = pd.to_numeric(rd['second_half'], errors='coerce')
    rd['pace_diff'] = rd['first_half'] - rd['second_half']
    
    # レース情報を抽出
    races_for_merge = df.drop_duplicates('race_id')[['race_id', 'race_date', 'venue', 'track_surface']]
    
    # マージ
    rd_with_info = rd.merge(races_for_merge, on='race_id', how='inner')
    
    # Train期間のみで統計計算
    cutoff = pd.Timestamp(train_cutoff)
    train_rd = rd_with_info[rd_with_info['race_date'] < cutoff]
    
    if len(train_rd) == 0:
        logger.warning("  Train期間のrace_detailsがありません")
        df['venue_pace_tendency'] = 0.0
        return df
    
    # 会場×馬場別の平均ペース差
    pace_stats = train_rd.groupby(['venue', 'track_surface'])['pace_diff'].agg(['mean', 'count']).reset_index()
    
    # 最低サンプル数（30件）を満たすもののみ
    pace_stats = pace_stats[pace_stats['count'] >= 30]
    pace_stats = pace_stats.rename(columns={'mean': 'venue_pace_tendency'})
    
    # dfにマージ
    df = df.merge(
        pace_stats[['venue', 'track_surface', 'venue_pace_tendency']],
        on=['venue', 'track_surface'],
        how='left'
    )
    
    # 欠損値はグローバル平均
    global_mean = train_rd['pace_diff'].mean()
    df['venue_pace_tendency'] = df['venue_pace_tendency'].fillna(global_mean)
    
    non_null = df['venue_pace_tendency'].notna().sum()
    logger.info(f"    venue_pace_tendency: {len(pace_stats)}パターン, 非NaN: {non_null:,}/{len(df):,}")
    
    return df


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
    logger.info("V16b: 最小限ペース特徴量版（コース統計のみ）")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns_df = load_data()
    
    logger.info(f"  総レース数: {len(races):,}件")
    
    # V15と同じ期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2024-12-31)")
    logger.info(f"  Test:  {len(test):,}件 (2025-01-01~)")
    
    # V15特徴量生成
    logger.info("")
    logger.info("V15特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    # V16b追加: 会場×馬場のペース傾向のみ追加
    logger.info("")
    logger.info("V16b追加特徴量(コース統計のみ)...")
    train_f = add_venue_pace_features(train_f, race_details, train_cutoff='2024-01-01')
    test_f = add_venue_pace_features(test_f, race_details, train_cutoff='2024-01-01')
    
    # 特徴量リスト
    base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
    new_features = ['venue_pace_tendency']
    feature_cols = base_features + new_features
    
    logger.info(f"  特徴量数: {len(feature_cols)} (V15: {len(base_features)} + 新規: {len(new_features)})")
    
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
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリ")
    logger.info("=" * 60)
    logger.info(f"  Train: ROI={train_roi:.1f}%, 的中率={train_hit:.1f}%")
    logger.info(f"  Test:  ROI={test_roi:.1f}%, 的中率={test_hit:.1f}%")
    logger.info(f"  Gap:   {train_roi - test_roi:.1f}%")
    
    # 年別ROI
    logger.info("")
    logger.info("年別ROI:")
    all_data = pd.concat([train_f, test_f])
    all_data['pred'] = np.concatenate([train_pred, test_pred])
    yearly_roi = calc_roi_by_year(all_data, all_data['pred'])
    
    for _, row in yearly_roi.iterrows():
        if row['year'] >= 2021:  # 直近5年のみ表示
            marker = "📊" if row['year'] >= 2025 else "📈"
            logger.info(f"  {marker} {int(row['year'])}: ROI={row['roi']:.1f}%, Hit={row['hit_rate']:.1f}%")
    
    # 4年平均ROI（2021-2024）
    recent = yearly_roi[(yearly_roi['year'] >= 2021) & (yearly_roi['year'] <= 2024)]
    avg_roi = recent['roi'].mean()
    logger.info(f"  📊 4年平均 (2021-2024): ROI={avg_roi:.1f}%")
    
    # 特徴量重要度
    logger.info("")
    logger.info("特徴量重要度 (Top 10):")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for i, (_, row) in enumerate(importance.head(10).iterrows()):
        new_marker = "⭐" if row['feature'] in new_features else ""
        logger.info(f"  {i+1:2d}. {row['feature']}: {row['importance']:.0f} {new_marker}")
    
    # V15との比較
    logger.info("")
    logger.info("=" * 60)
    logger.info("V15との比較")
    logger.info("=" * 60)
    logger.info(f"  V15 Test ROI: 79.2%")
    logger.info(f"  V16b Test ROI: {test_roi:.1f}%")
    logger.info(f"  差分: {test_roi - 79.2:+.1f}%")
    
    return model, test_roi


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
V2.6 Restored Model: 複数期間再現性テスト

複数のテスト期間でROIの安定性を検証する。
結果は keibaai/models/v26_restored/multi_period_results.csv に保存される。
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import numpy as np
import logging
from datetime import datetime

import lightgbm as lgb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data():
    """データを読み込む"""
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    # 2014-01-01 〜 2025-10-31 のデータを使用
    races_df = races_df[
        (races_df['race_date'] >= '2014-01-01') & 
        (races_df['race_date'] <= '2025-10-31')
    ].dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    
    return races_df, pedigrees_df, corners_df


def calculate_roi(df, score_col='score'):
    """ROIを計算する"""
    if df.empty:
        return 0.0, 0.0
    
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')[score_col].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1]
    
    if top1.empty:
        return 0.0, 0.0
    
    total_bets = len(top1)
    
    hit_mask = top1['finish_position'] == 1
    total_payout = (top1.loc[hit_mask, 'win_odds'] * 100).sum()
    
    roi = total_payout / (total_bets * 100) * 100
    hit_rate = hit_mask.sum() / total_bets * 100
    
    return hit_rate, roi


def run_single_period_test(
    races_df, 
    pedigrees_df, 
    corners_df,
    train_end: str,
    test_start: str,
    test_end: str = None,
    period_name: str = "Unknown"
):
    """単一期間のテストを実行する"""
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    logger.info(f"\n{'='*60}")
    logger.info(f"期間テスト: {period_name}")
    logger.info(f"  Train: ~{train_end}")
    logger.info(f"  Test: {test_start} ~ {test_end or '最新'}")
    logger.info(f"{'='*60}")
    
    # データ分割
    train_df = races_df[races_df['race_date'] <= train_end].copy()
    test_df = races_df[races_df['race_date'] >= test_start].copy()
    if test_end:
        test_df = test_df[test_df['race_date'] <= test_end]
    
    logger.info(f"  Train: {len(train_df):,}件")
    logger.info(f"  Test: {len(test_df):,}件")
    
    if len(train_df) < 10000 or len(test_df) < 1000:
        logger.warning(f"  データ不足のためスキップ")
        return None
    
    # 特徴量生成 (V15)
    fe = LeakFreeFeatureEngineerV15()
    fe.fit(train_df, pedigrees_df, corners_df)
    
    train_features = fe.transform(train_df)
    test_features = fe.transform(test_df)
    
    # 定義された特徴量と実際のDataFrameカラムの共通部分を使用
    defined_cols = fe.get_feature_columns()
    available_cols = [c for c in defined_cols if c in train_features.columns and c in test_features.columns]
    
    logger.info(f"  特徴量数: {len(available_cols)} / {len(defined_cols)} (実際に存在)")
    
    # Binary Classification ターゲット
    train_features['is_win'] = (train_features['finish_position'] == 1).astype(int)
    test_features['is_win'] = (test_features['finish_position'] == 1).astype(int)
    
    train_sorted = train_features.sort_values('race_id')
    test_sorted = test_features.sort_values('race_id')
    
    X_train = train_sorted[available_cols].fillna(0)
    y_train = train_sorted['is_win']
    X_test = test_sorted[available_cols].fillna(0)
    y_test = test_sorted['is_win']
    
    # LightGBM学習
    lgbm_params = {
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
        'random_state': 42
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    test_ds = lgb.Dataset(X_test, y_test)
    
    model = lgb.train(
        lgbm_params,
        train_ds,
        num_boost_round=200,
        valid_sets=[test_ds],
        callbacks=[lgb.log_evaluation(period=100)]
    )
    
    # 予測
    train_sorted['score'] = model.predict(X_train)
    test_sorted['score'] = model.predict(X_test)
    
    # ROI計算
    train_hit, train_roi = calculate_roi(train_sorted, 'score')
    test_hit, test_roi = calculate_roi(test_sorted, 'score')
    
    logger.info(f"  結果: Train ROI={train_roi:.1f}%, Test ROI={test_roi:.1f}%, Hit={test_hit:.1f}%")
    
    return {
        'period': period_name,
        'train_end': train_end,
        'test_start': test_start,
        'test_end': test_end,
        'train_count': len(train_df),
        'test_count': len(test_df),
        'train_roi': train_roi,
        'test_roi': test_roi,
        'test_hit_rate': test_hit,
        'gap': train_roi - test_roi
    }


def main():
    logger.info("=" * 60)
    logger.info("V2.6 Restored: 複数期間再現性テスト")
    logger.info("=" * 60)
    
    # データ読み込み
    races_df, pedigrees_df, corners_df = load_data()
    logger.info(f"Total races: {len(races_df):,}")
    
    # テスト期間定義
    periods = [
        {"name": "2022", "train_end": "2021-12-31", "test_start": "2022-01-01", "test_end": "2022-12-31"},
        {"name": "2023", "train_end": "2022-12-31", "test_start": "2023-01-01", "test_end": "2023-12-31"},
        {"name": "2024", "train_end": "2023-12-31", "test_start": "2024-01-01", "test_end": "2024-12-31"},
        {"name": "2025", "train_end": "2024-12-31", "test_start": "2025-01-01", "test_end": None},
    ]
    
    results = []
    for period in periods:
        result = run_single_period_test(
            races_df, pedigrees_df, corners_df,
            train_end=period["train_end"],
            test_start=period["test_start"],
            test_end=period["test_end"],
            period_name=period["name"]
        )
        if result:
            results.append(result)
    
    # 結果をDataFrameに変換
    results_df = pd.DataFrame(results)
    
    # 安定性分析
    logger.info("\n" + "=" * 60)
    logger.info("安定性分析")
    logger.info("=" * 60)
    
    mean_roi = results_df['test_roi'].mean()
    std_roi = results_df['test_roi'].std()
    min_roi = results_df['test_roi'].min()
    max_roi = results_df['test_roi'].max()
    
    logger.info(f"  平均ROI: {mean_roi:.1f}%")
    logger.info(f"  標準偏差: {std_roi:.1f}%")
    logger.info(f"  範囲: {min_roi:.1f}% ~ {max_roi:.1f}%")
    
    # 安定性判定
    is_stable = (min_roi >= 75.0) and (max_roi - min_roi <= 30.0)
    if is_stable:
        logger.info("  ✓ 安定性基準をクリア (全期間 ROI >= 75%, 変動 <= 30%)")
    else:
        logger.info("  ✗ 安定性基準を満たさず")
    
    # 結果を保存
    output_path = Path("keibaai/models/v26_restored/multi_period_results.csv")
    results_df.to_csv(output_path, index=False)
    logger.info(f"\n結果を保存しました: {output_path}")
    
    # サマリー出力
    print("\n" + "=" * 60)
    print("Multi-Period ROI Summary")
    print("=" * 60)
    print(results_df[['period', 'test_roi', 'test_hit_rate', 'gap']].to_string(index=False))


if __name__ == "__main__":
    main()

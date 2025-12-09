"""
V11.1モデル学習・評価スクリプト（保守的バージョン）

V10.2との比較でROI向上と過学習抑制を両立できるか検証
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v11_1 import LeakFreeFeatureEngineerV11_1

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    
    # finish_positionがNAの行を除外
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, horses


def train_and_evaluate(engine, train_data, test_data, feature_cols):
    """モデル学習と評価"""
    # ラベル作成
    train_data = train_data.copy()
    test_data = test_data.copy()
    train_data['label'] = (train_data['finish_position'] == 1).astype(int)
    test_data['label'] = (test_data['finish_position'] == 1).astype(int)
    
    # 実際に存在する特徴量に絞り込み
    available_cols = [c for c in feature_cols if c in train_data.columns]
    logger.info(f"使用可能特徴量: {len(available_cols)}/{len(feature_cols)}")
    
    # 特徴量抽出
    X_train = train_data[available_cols].fillna(0)
    y_train = train_data['label']
    X_test = test_data[available_cols].fillna(0)
    y_test = test_data['label']
    
    # LightGBM学習（正則化強化）
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'learning_rate': 0.03,  # 下げる
        'num_leaves': 31,
        'max_depth': 5,  # 下げる
        'min_child_samples': 200,  # 上げる（過学習抑制）
        'reg_alpha': 0.5,  # 上げる
        'reg_lambda': 0.5,  # 上げる
        'bagging_fraction': 0.7,  # 下げる
        'bagging_freq': 5,
        'feature_fraction': 0.7,  # 下げる
        'verbosity': -1
    }
    
    train_set = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_set, num_boost_round=200)  # 減らす
    
    # 予測
    train_preds = model.predict(X_train)
    test_preds = model.predict(X_test)
    
    train_data['pred'] = train_preds
    test_data['pred'] = test_preds
    
    return model, train_data, test_data, available_cols


def calc_roi(df, top_n=1):
    """ROI計算"""
    df = df.copy()
    df['race_rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    df['selected'] = df['race_rank'] <= top_n
    df['is_win'] = df['finish_position'] == 1
    
    selected = df[df['selected']]
    if len(selected) == 0:
        return 0, 0, 0
    
    bets = len(selected)
    wins = selected['is_win'].sum()
    payout = (selected['is_win'] * selected['win_odds']).sum()
    
    hit_rate = wins / bets * 100
    roi = payout / bets * 100
    
    return roi, hit_rate, bets


def main():
    logger.info("=" * 60)
    logger.info("V11.1モデル学習・評価（保守的バージョン）")
    logger.info("=" * 60)
    
    # データ読み込み
    races, pedigrees, corners, horses = load_data()
    
    # データ分割
    train = races[races['race_date'] < '2024-07-01'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"Train: {len(train):,}件")
    logger.info(f"Test:  {len(test):,}件")
    
    # V11.1特徴量生成
    engine = LeakFreeFeatureEngineerV11_1()
    engine.fit(
        races_df=train,
        pedigrees_df=pedigrees,
        corners_df=corners,
        horses_df=horses
    )
    
    train_features = engine.transform(train)
    test_features = engine.transform(test)
    
    feature_cols = engine.get_feature_columns()
    logger.info(f"特徴量数: {len(feature_cols)}")
    
    # 学習・評価
    model, train_eval, test_eval, used_cols = train_and_evaluate(
        engine, train_features, test_features, feature_cols
    )
    
    # ROI計算
    logger.info("\n" + "=" * 60)
    logger.info("結果")
    logger.info("=" * 60)
    
    train_roi, train_hit, train_bets = calc_roi(train_eval, top_n=1)
    test_roi, test_hit, test_bets = calc_roi(test_eval, top_n=1)
    gap = train_roi - test_roi
    
    logger.info(f"\nTrain ROI: {train_roi:.1f}% (Hit: {train_hit:.1f}%, Bets: {train_bets:,})")
    logger.info(f"Test ROI:  {test_roi:.1f}% (Hit: {test_hit:.1f}%, Bets: {test_bets:,})")
    logger.info(f"Gap:       {gap:.1f}%")
    
    # 過学習チェック
    if gap < 25:
        logger.info("\n✓ Train-Test Gap < 25%: 過学習リスク低")
    else:
        logger.warning(f"\n⚠ Train-Test Gap {gap:.1f}% > 25%: 過学習の可能性")
    
    # 特徴量重要度（Top 20）
    importance = pd.DataFrame({
        'feature': used_cols,
        'importance': model.feature_importance()
    }).sort_values('importance', ascending=False)
    
    logger.info("\n--- Top 20 Features ---")
    print(importance.head(20).to_string(index=False))
    
    # basis_weight_light_flagの重要度
    if 'basis_weight_light_flag' in importance['feature'].values:
        row = importance[importance['feature'] == 'basis_weight_light_flag'].iloc[0]
        rank = importance.index.get_loc(row.name) + 1
        logger.info(f"\n--- V11.1 新特徴量 ---")
        logger.info(f"  basis_weight_light_flag: Rank {rank}, Importance {row['importance']}")


if __name__ == "__main__":
    main()

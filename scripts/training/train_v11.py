"""
V11モデル学習・評価スクリプト

V10.2との比較でROI向上を検証
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb
from sklearn.model_selection import train_test_split

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v11 import LeakFreeFeatureEngineerV11

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
    
    # LightGBM学習
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'max_depth': 6,
        'min_child_samples': 100,
        'reg_alpha': 0.1,
        'reg_lambda': 0.1,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'feature_fraction': 0.8,
        'verbosity': -1
    }
    
    train_set = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_set, num_boost_round=300)
    
    # 予測
    train_preds = model.predict(X_train)
    test_preds = model.predict(X_test)
    
    train_data['pred'] = train_preds
    test_data['pred'] = test_preds
    
    return model, train_data, test_data


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
    logger.info("V11モデル学習・評価")
    logger.info("=" * 60)
    
    # データ読み込み
    races, pedigrees, corners, horses = load_data()
    
    # データ分割
    train = races[races['race_date'] < '2024-07-01'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"Train: {len(train):,}件")
    logger.info(f"Test:  {len(test):,}件")
    
    # V11特徴量生成
    engine = LeakFreeFeatureEngineerV11()
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
    model, train_eval, test_eval = train_and_evaluate(
        engine, train_features, test_features, feature_cols
    )
    
    # ROI計算
    logger.info("\n" + "=" * 60)
    logger.info("結果")
    logger.info("=" * 60)
    
    train_roi, train_hit, train_bets = calc_roi(train_eval, top_n=1)
    test_roi, test_hit, test_bets = calc_roi(test_eval, top_n=1)
    
    logger.info(f"\nTrain ROI: {train_roi:.1f}% (Hit: {train_hit:.1f}%, Bets: {train_bets:,})")
    logger.info(f"Test ROI:  {test_roi:.1f}% (Hit: {test_hit:.1f}%, Bets: {test_bets:,})")
    logger.info(f"Gap:       {train_roi - test_roi:.1f}%")
    
    # 特徴量重要度
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance()
    }).sort_values('importance', ascending=False)
    
    logger.info("\n--- Top 15 Features ---")
    print(importance.head(15).to_string(index=False))
    
    # V11新特徴量の重要度
    v11_new = ['prev_finish_category', 'distance_change', 'distance_extend_flag', 
               'surface_switch_flag', 'basis_weight_light_flag']
    logger.info("\n--- V11新特徴量の重要度 ---")
    for col in v11_new:
        if col in importance['feature'].values:
            rank = importance[importance['feature'] == col].index[0] + 1
            imp = importance[importance['feature'] == col]['importance'].values[0]
            logger.info(f"  {col}: Rank {rank}, Importance {imp}")


if __name__ == "__main__":
    main()

"""
V18 vs V17 ROI比較テスト

目的:
1. V18とV17の特徴量を使用してモデルを学習
2. テストセットでのROI、的中率を比較
3. 新特徴量の効果を検証

使用方法:
python scripts/training/compare_v18_v17_roi.py
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb
from sklearn.model_selection import train_test_split

# パスを追加
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v17 import (
    LeakFreeFeatureEngineerV17, 
    FeatureConfigV17
)
from keibaai.src.features.leak_free_feature_engineer_v18 import (
    LeakFreeFeatureEngineerV18, 
    FeatureConfigV18
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    logger.info("=" * 60)
    logger.info("データ読み込み開始")
    logger.info("=" * 60)
    
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    
    # race_detailsを読み込み
    rd_path = 'keibaai/data/parsed/parquet/race_details/race_details.parquet'
    try:
        race_details = pd.read_parquet(rd_path)
    except FileNotFoundError:
        race_details = None
    
    logger.info(f"  レース: {len(races):,}件")
    
    return races, pedigrees, corners, horses, race_details


def prepare_data(races):
    """データ分割"""
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[races['finish_position'].notna()].copy()
    
    # Train/Validation/Test分割
    train = races[races['race_date'] < '2024-01-01'].copy()
    valid = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2024-07-01')].copy()
    test = races[(races['race_date'] >= '2024-07-01') & (races['race_date'] < '2025-01-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Valid: {len(valid):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    return train, valid, test


def train_and_evaluate(train_features, valid_features, test_features, feature_cols, version_name):
    """モデル学習と評価"""
    logger.info(f"\n--- {version_name} モデル学習と評価 ---")
    
    # ラベル作成
    train_features['is_win'] = (train_features['finish_position'] == 1).astype(int)
    valid_features['is_win'] = (valid_features['finish_position'] == 1).astype(int)
    test_features['is_win'] = (test_features['finish_position'] == 1).astype(int)
    
    # 使用可能な特徴量のみ選択
    available_cols = [c for c in feature_cols if c in train_features.columns]
    logger.info(f"  使用特徴量: {len(available_cols)}個")
    
    X_train = train_features[available_cols].fillna(0)
    y_train = train_features['is_win']
    X_valid = valid_features[available_cols].fillna(0)
    y_valid = valid_features['is_win']
    X_test = test_features[available_cols].fillna(0)
    y_test = test_features['is_win']
    
    # LightGBM Ranker用のグループ
    train_group = train_features.groupby('race_id').size().values
    valid_group = valid_features.groupby('race_id').size().values
    test_group = test_features.groupby('race_id').size().values
    
    # モデル学習（シンプルなバイナリ分類）
    model = lgb.LGBMClassifier(
        objective='binary',
        n_estimators=500,
        learning_rate=0.02,
        max_depth=3,
        num_leaves=8,
        min_child_samples=100,
        reg_alpha=2.0,
        reg_lambda=3.0,
        subsample=0.7,
        colsample_bytree=0.7,
        random_state=42,
        verbose=-1
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_valid, y_valid)],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    
    # 予測
    train_proba = model.predict_proba(X_train)[:, 1]
    valid_proba = model.predict_proba(X_valid)[:, 1]
    test_proba = model.predict_proba(X_test)[:, 1]
    
    # ROI計算
    def calc_roi(df, proba, top_n=1):
        df = df.copy()
        df['pred_proba'] = proba
        df['pred_rank'] = df.groupby('race_id')['pred_proba'].rank(ascending=False)
        
        top_picks = df[df['pred_rank'] <= top_n]
        total_bet = len(top_picks)
        total_return = (top_picks['is_win'] * top_picks['win_odds']).sum()
        roi = total_return / total_bet * 100 if total_bet > 0 else 0
        hit_rate = top_picks['is_win'].mean() * 100
        
        return roi, hit_rate
    
    train_roi, train_hit = calc_roi(train_features, train_proba)
    valid_roi, valid_hit = calc_roi(valid_features, valid_proba)
    test_roi, test_hit = calc_roi(test_features, test_proba)
    
    logger.info(f"  Train ROI: {train_roi:.1f}%, Hit: {train_hit:.1f}%")
    logger.info(f"  Valid ROI: {valid_roi:.1f}%, Hit: {valid_hit:.1f}%")
    logger.info(f"  Test ROI:  {test_roi:.1f}%, Hit: {test_hit:.1f}%")
    
    # 特徴量重要度（Top 10）
    importance = pd.DataFrame({
        'feature': available_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    logger.info(f"\n  特徴量重要度 Top 10:")
    for _, row in importance.head(10).iterrows():
        logger.info(f"    {row['feature']}: {row['importance']:.0f}")
    
    return {
        'version': version_name,
        'train_roi': train_roi,
        'valid_roi': valid_roi,
        'test_roi': test_roi,
        'train_hit': train_hit,
        'valid_hit': valid_hit,
        'test_hit': test_hit,
        'importance': importance,
    }


def main():
    """メイン処理"""
    logger.info("V18 vs V17 ROI比較テスト開始")
    
    # データ読み込み
    races, pedigrees, corners, horses, race_details = load_data()
    train, valid, test = prepare_data(races)
    
    # V17
    logger.info("\n" + "=" * 60)
    logger.info("V17 Feature Engineer")
    logger.info("=" * 60)
    
    engine_v17 = LeakFreeFeatureEngineerV17(FeatureConfigV17())
    engine_v17.fit(train, pedigrees, corners, race_details, None, horses)
    
    train_v17 = engine_v17.transform(train)
    valid_v17 = engine_v17.transform(valid)
    test_v17 = engine_v17.transform(test)
    
    result_v17 = train_and_evaluate(
        train_v17, valid_v17, test_v17, 
        engine_v17.get_feature_columns(), 
        "V17"
    )
    
    # V18
    logger.info("\n" + "=" * 60)
    logger.info("V18 Feature Engineer")
    logger.info("=" * 60)
    
    engine_v18 = LeakFreeFeatureEngineerV18(FeatureConfigV18())
    engine_v18.fit(train, pedigrees, corners, race_details, None, horses)
    
    train_v18 = engine_v18.transform(train)
    valid_v18 = engine_v18.transform(valid)
    test_v18 = engine_v18.transform(test)
    
    result_v18 = train_and_evaluate(
        train_v18, valid_v18, test_v18, 
        engine_v18.get_feature_columns(), 
        "V18"
    )
    
    # 比較結果
    logger.info("\n" + "=" * 60)
    logger.info("比較結果サマリー")
    logger.info("=" * 60)
    
    logger.info(f"\n{'Metric':<20} {'V17':>10} {'V18':>10} {'Diff':>10}")
    logger.info("-" * 50)
    logger.info(f"{'Train ROI':<20} {result_v17['train_roi']:>10.1f}% {result_v18['train_roi']:>10.1f}% {result_v18['train_roi']-result_v17['train_roi']:>+10.1f}%")
    logger.info(f"{'Valid ROI':<20} {result_v17['valid_roi']:>10.1f}% {result_v18['valid_roi']:>10.1f}% {result_v18['valid_roi']-result_v17['valid_roi']:>+10.1f}%")
    logger.info(f"{'Test ROI':<20} {result_v17['test_roi']:>10.1f}% {result_v18['test_roi']:>10.1f}% {result_v18['test_roi']-result_v17['test_roi']:>+10.1f}%")
    logger.info(f"{'Train Hit':<20} {result_v17['train_hit']:>10.1f}% {result_v18['train_hit']:>10.1f}% {result_v18['train_hit']-result_v17['train_hit']:>+10.1f}%")
    logger.info(f"{'Valid Hit':<20} {result_v17['valid_hit']:>10.1f}% {result_v18['valid_hit']:>10.1f}% {result_v18['valid_hit']-result_v17['valid_hit']:>+10.1f}%")
    logger.info(f"{'Test Hit':<20} {result_v17['test_hit']:>10.1f}% {result_v18['test_hit']:>10.1f}% {result_v18['test_hit']-result_v17['test_hit']:>+10.1f}%")
    
    # V18新特徴量の重要度
    v18_new_features = [
        'elo_rating', 'recency_bias_indicator',
        'gap_c1_avg', 'gap_c2_avg', 'gap_c3_avg',
        'gap_reduction_c1_c4', 'gap_reduction_c3_c4',
        'senkou_rate', 'sashi_rate'
    ]
    
    logger.info("\n--- V18新特徴量の重要度 ---")
    v18_importance = result_v18['importance']
    for feat in v18_new_features:
        if feat in v18_importance['feature'].values:
            imp = v18_importance[v18_importance['feature'] == feat]['importance'].values[0]
            logger.info(f"  {feat}: {imp:.0f}")
        else:
            logger.info(f"  {feat}: N/A")
    
    logger.info("\n" + "=" * 60)
    logger.info("比較テスト完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

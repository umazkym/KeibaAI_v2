"""
V16 穴馬発掘専用モデル訓練＆テストスクリプト

目的:
- V15特徴量をそのまま使用
- ターゲットを「穴馬勝利」（is_win AND win_odds >= 10）に変更
- 不均衡データに対応（class_weight）
- V15+オッズフィルタ戦略との比較
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # Train: 2020-2024, Test: 2025
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    return train, test, pedigrees, corners, horses, race_details


def create_upset_target(df: pd.DataFrame, min_odds: float = 10.0, max_odds: float = 100.0) -> pd.Series:
    """
    穴馬勝利ターゲット作成
    
    条件: 1着 AND オッズ10-100倍
    
    注意: 
    - オッズは確定オッズ（バックテスト用）
    - 運用時は朝オッズで代替
    """
    is_upset_win = (
        (df['finish_position'] == 1) & 
        (df['win_odds'] >= min_odds) & 
        (df['win_odds'] < max_odds)
    ).astype(int)
    return is_upset_win


def calc_roi(df: pd.DataFrame, preds: np.ndarray, strategy: str = "top1") -> dict:
    """ROI計算
    
    strategy:
    - "top1": モデル予測1位を購入
    - "top1_filtered": モデル予測1位 かつ オッズ10-50倍
    """
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    if strategy == "top1":
        bets = df[df['rank'] == 1]
    elif strategy == "top1_filtered":
        bets = df[(df['rank'] == 1) & (df['win_odds'] >= 10) & (df['win_odds'] < 50)]
    else:
        bets = df[df['rank'] == 1]
    
    if len(bets) == 0:
        return {'roi': 0, 'hit_rate': 0, 'n_bets': 0, 'avg_odds': 0}
    
    hits = bets[bets['finish_position'] == 1]
    
    return {
        'roi': hits['win_odds'].sum() / len(bets) * 100,
        'hit_rate': len(hits) / len(bets) * 100,
        'n_bets': len(bets),
        'avg_odds': bets['win_odds'].mean()
    }


def train_upset_model():
    """穴馬発掘モデル訓練"""
    logger.info("=" * 70)
    logger.info("V16 Upset Horse Model Training")
    logger.info("=" * 70)
    
    train, test, pedigrees, corners, horses, race_details = load_data()
    logger.info(f"Train: {len(train):,}, Test: {len(test):,}")
    
    # 特徴量生成（V15を使用）
    logger.info("\n--- Feature Engineering (V15) ---")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"Features: {len(feature_cols)}")
    
    # ターゲット作成
    y_train_upset = create_upset_target(train_f)
    y_test_upset = create_upset_target(test_f)
    
    # 標準ターゲット（is_win）も作成（比較用）
    y_train_win = (train_f['finish_position'] == 1).astype(int)
    y_test_win = (test_f['finish_position'] == 1).astype(int)
    
    logger.info(f"\n--- Target Distribution ---")
    logger.info(f"Train upset wins: {y_train_upset.sum():,} ({y_train_upset.mean()*100:.2f}%)")
    logger.info(f"Test upset wins: {y_test_upset.sum():,} ({y_test_upset.mean()*100:.2f}%)")
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    
    # モデル訓練パラメータ（強正則化）
    base_params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
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
    
    results = []
    
    # ===== Model 1: V15 (is_win) - ベースライン =====
    logger.info("\n" + "=" * 50)
    logger.info("Training Model 1: V15 Baseline (is_win target)")
    logger.info("=" * 50)
    
    train_ds = lgb.Dataset(X_train, y_train_win)
    model_v15 = lgb.train(base_params, train_ds, num_boost_round=200)
    
    train_pred_v15 = model_v15.predict(X_train)
    test_pred_v15 = model_v15.predict(X_test)
    
    # V15 Top1購入
    v15_train_roi = calc_roi(train_f, train_pred_v15, "top1")
    v15_test_roi = calc_roi(test_f, test_pred_v15, "top1")
    
    logger.info(f"V15 Top1: Train ROI={v15_train_roi['roi']:.1f}%, Test ROI={v15_test_roi['roi']:.1f}%")
    
    # V15 + オッズフィルタ（現行最良戦略）
    v15_train_filtered = calc_roi(train_f, train_pred_v15, "top1_filtered")
    v15_test_filtered = calc_roi(test_f, test_pred_v15, "top1_filtered")
    
    logger.info(f"V15 Top1+Filter: Train ROI={v15_train_filtered['roi']:.1f}%, Test ROI={v15_test_filtered['roi']:.1f}%, Bets={v15_test_filtered['n_bets']}")
    
    results.append({
        'model': 'V15 Top1',
        'train_roi': v15_train_roi['roi'],
        'test_roi': v15_test_roi['roi'],
        'test_bets': v15_test_roi['n_bets'],
        'test_hit_rate': v15_test_roi['hit_rate']
    })
    results.append({
        'model': 'V15 Top1+Filter (10-50x)',
        'train_roi': v15_train_filtered['roi'],
        'test_roi': v15_test_filtered['roi'],
        'test_bets': v15_test_filtered['n_bets'],
        'test_hit_rate': v15_test_filtered['hit_rate']
    })
    
    # ===== Model 2: V16 (upset win target) - 穴馬発掘モデル =====
    logger.info("\n" + "=" * 50)
    logger.info("Training Model 2: V16 Upset Model (upset win target)")
    logger.info("=" * 50)
    
    # 不均衡データ対策: scale_pos_weight
    neg_count = (y_train_upset == 0).sum()
    pos_count = (y_train_upset == 1).sum()
    scale_pos_weight = neg_count / pos_count
    logger.info(f"scale_pos_weight: {scale_pos_weight:.1f}")
    
    upset_params = base_params.copy()
    upset_params['scale_pos_weight'] = scale_pos_weight
    
    train_ds_upset = lgb.Dataset(X_train, y_train_upset)
    model_v16 = lgb.train(upset_params, train_ds_upset, num_boost_round=200)
    
    train_pred_v16 = model_v16.predict(X_train)
    test_pred_v16 = model_v16.predict(X_test)
    
    # V16 Top1購入（穴馬モデルの上位を購入）
    v16_train_roi = calc_roi(train_f, train_pred_v16, "top1")
    v16_test_roi = calc_roi(test_f, test_pred_v16, "top1")
    
    logger.info(f"V16 Top1: Train ROI={v16_train_roi['roi']:.1f}%, Test ROI={v16_test_roi['roi']:.1f}%")
    
    # V16 + オッズフィルタ
    v16_train_filtered = calc_roi(train_f, train_pred_v16, "top1_filtered")
    v16_test_filtered = calc_roi(test_f, test_pred_v16, "top1_filtered")
    
    logger.info(f"V16 Top1+Filter: Train ROI={v16_train_filtered['roi']:.1f}%, Test ROI={v16_test_filtered['roi']:.1f}%, Bets={v16_test_filtered['n_bets']}")
    
    results.append({
        'model': 'V16 Top1',
        'train_roi': v16_train_roi['roi'],
        'test_roi': v16_test_roi['roi'],
        'test_bets': v16_test_roi['n_bets'],
        'test_hit_rate': v16_test_roi['hit_rate']
    })
    results.append({
        'model': 'V16 Top1+Filter (10-50x)',
        'train_roi': v16_train_filtered['roi'],
        'test_roi': v16_test_filtered['roi'],
        'test_bets': v16_test_filtered['n_bets'],
        'test_hit_rate': v16_test_filtered['hit_rate']
    })
    
    # ===== 年別分析 =====
    logger.info("\n" + "=" * 50)
    logger.info("Year-by-Year Analysis")
    logger.info("=" * 50)
    
    for year in [2022, 2023, 2024]:  # 2022年問題の確認
        year_mask = train_f['race_date'].dt.year == year
        if year_mask.sum() == 0:
            continue
        
        year_df = train_f[year_mask]
        year_pred_v15 = train_pred_v15[year_mask]
        year_pred_v16 = train_pred_v16[year_mask]
        
        v15_year = calc_roi(year_df, year_pred_v15, "top1_filtered")
        v16_year = calc_roi(year_df, year_pred_v16, "top1_filtered")
        
        logger.info(f"{year}: V15+Filter ROI={v15_year['roi']:.1f}%, V16+Filter ROI={v16_year['roi']:.1f}%")
    
    # ===== 結果サマリー =====
    logger.info("\n" + "=" * 70)
    logger.info("FINAL RESULTS")
    logger.info("=" * 70)
    
    df_results = pd.DataFrame(results)
    print(df_results.to_string(index=False))
    
    # 結論
    logger.info("\n" + "=" * 70)
    logger.info("CONCLUSION")
    logger.info("=" * 70)
    
    v15_filtered_roi = v15_test_filtered['roi']
    v16_top1_roi = v16_test_roi['roi']
    v16_filtered_roi = v16_test_filtered['roi']
    
    if v16_top1_roi > 100:
        logger.info("✅ V16 Top1 achieves ROI > 100% !")
        logger.info(f"   Test ROI: {v16_top1_roi:.1f}%, Bets: {v16_test_roi['n_bets']}")
    elif v16_top1_roi > v15_filtered_roi:
        logger.info(f"⚠️ V16 Top1 ({v16_top1_roi:.1f}%) > V15+Filter ({v15_filtered_roi:.1f}%) but ROI < 100%")
    else:
        logger.info(f"❌ V16 did not outperform V15+Filter")
        logger.info(f"   V15+Filter: {v15_filtered_roi:.1f}%")
        logger.info(f"   V16 Top1: {v16_top1_roi:.1f}%")
    
    # 特徴量重要度
    logger.info("\n--- V16 Feature Importance (Top 10) ---")
    imp = pd.DataFrame({
        'feature': feature_cols,
        'gain': model_v16.feature_importance(importance_type='gain')
    })
    imp = imp.sort_values('gain', ascending=False).head(10)
    for i, row in imp.iterrows():
        logger.info(f"  {row['feature']}: {row['gain']:.1f}")
    
    return model_v15, model_v16, results


if __name__ == "__main__":
    train_upset_model()

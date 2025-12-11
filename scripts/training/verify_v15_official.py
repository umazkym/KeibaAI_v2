# -*- coding: utf-8 -*-
"""
V15公式設定での再現テスト

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
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    data_dir = Path("keibaai/data/parsed/parquet")
    
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


def calc_place_roi(df, preds, returns_df):
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1][['race_id', 'horse_number', 'finish_position']].copy()
    top1 = top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    
    total_bets = len(top1) * 100
    total_payout = top1['payout'].fillna(0).sum()
    
    hit_rate = top1['payout'].notna().sum() / len(top1) * 100 if len(top1) > 0 else 0
    roi = total_payout / total_bets * 100 if total_bets > 0 else 0
    
    return roi, hit_rate


def main():
    logger.info("=" * 60)
    logger.info("V15公式設定での再現テスト")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns_df = load_data()
    
    # V15公式の期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2024-12-31)")
    logger.info(f"  Test:  {len(test):,}件 (2025-01-01~2025-10-31)")
    logger.info(f"  Trainレース数: {train['race_id'].nunique():,}")
    logger.info(f"  Testレース数: {test['race_id'].nunique():,}")
    
    if len(test) == 0:
        logger.warning("Testデータがありません。2025年のデータが存在しません。")
        logger.info("代替: 2024年データでテスト")
        train = races[races['race_date'] <= '2023-12-31'].copy()
        test = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2025-01-01')].copy()
        logger.info(f"  Train(代替): {len(train):,}件 (~2023-12-31)")
        logger.info(f"  Test(代替):  {len(test):,}件 (2024-01-01~)")
    
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
    
    # V15公式パラメータ (Binary Classification)
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
    logger.info("=" * 60)
    logger.info("Binary Classification (V15公式設定)")
    logger.info("=" * 60)
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    train_roi, train_hit, _ = calc_roi(train_f, train_pred)
    test_roi, test_hit, n_bets = calc_roi(test_f, test_pred)
    
    logger.info(f"  Train: 的中率={train_hit:.1f}%, ROI={train_roi:.1f}%")
    logger.info(f"  Test:  的中率={test_hit:.1f}%, ROI={test_roi:.1f}%")
    logger.info(f"  Train-Test Gap: {train_roi - test_roi:.1f}%")
    logger.info(f"  Test ベット数: {n_bets:,}")
    
    # 複勝ROI
    train_place_roi, train_place_hit = calc_place_roi(train_f, train_pred, returns_df)
    test_place_roi, test_place_hit = calc_place_roi(test_f, test_pred, returns_df)
    
    logger.info("")
    logger.info("【複勝ROI】")
    logger.info(f"  Train: 的中率={train_place_hit:.1f}%, ROI={train_place_roi:.1f}%")
    logger.info(f"  Test:  的中率={test_place_hit:.1f}%, ROI={test_place_roi:.1f}%")
    
    # 結果サマリー
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリー")
    logger.info("=" * 60)
    logger.info(f"  【単勝】Test ROI: {test_roi:.1f}% (公式目標: 91.8%)")
    logger.info(f"  【複勝】Test ROI: {test_place_roi:.1f}%")
    
    target = 91.8
    if test_roi >= target * 0.95:  # 5%許容
        logger.info(f"  ✅ V15ベースライン近似（誤差{abs(target - test_roi):.1f}%）")
    else:
        logger.info(f"  ❌ 差分: {target - test_roi:.1f}%")
    
    # 特徴量重要度
    logger.info("")
    logger.info("Top 10 特徴量:")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for i, row in importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")
    
    return model, engine, test_roi


if __name__ == "__main__":
    main()

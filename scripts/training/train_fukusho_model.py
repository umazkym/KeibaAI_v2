# -*- coding: utf-8 -*-
"""
複勝専用モデル (is_place ラベル)

【概要】
- ラベルを`is_win`→`is_place`（3着以内）に変更
- 複勝は的中率が高く、安定したROIが期待できる

【期待効果】
- 複勝のROI向上（現状83.7%→目標88%+）
- 2着・3着馬の予測精度向上 → 馬連向けにも応用可能
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
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns


def calc_tansho_roi(df, preds):
    """単勝ROI（Top1予測の1着率×オッズ）"""
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def calc_fukusho_roi(df, preds, returns_df):
    """複勝ROI（Top1予測の3着以内×複勝配当）"""
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1][['race_id', 'horse_number', 'finish_position']].copy()
    
    # 複勝払戻を取得
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']]
    fukusho = fukusho.rename(columns={'payout': 'fukusho_payout'})
    
    bets = bets.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    bets['fukusho_payout'] = bets['fukusho_payout'].fillna(0)
    
    hits = bets[bets['finish_position'] <= 3]
    
    total_bet = len(bets) * 100
    total_payout = bets['fukusho_payout'].sum()
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    
    return roi, hit_rate


def main():
    logger.info("=" * 60)
    logger.info("複勝専用モデル (is_place ラベル)")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # V15公式期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2024-12-31)")
    logger.info(f"  Test:  {len(test):,}件 (2025-01-01~)")
    
    # 特徴量
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    X_train = train_f[feature_cols].fillna(0)
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
    
    # ========== 1. ベースライン（is_win = V15相当） ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. ベースライン（is_win = V15相当）")
    logger.info("=" * 60)
    
    y_train_win = (train_f['finish_position'] == 1).astype(int)
    train_ds_win = lgb.Dataset(X_train, y_train_win)
    model_win = lgb.train(params, train_ds_win, num_boost_round=200)
    
    pred_train_win = model_win.predict(X_train)
    pred_test_win = model_win.predict(X_test)
    
    train_tansho_roi, train_tansho_hit = calc_tansho_roi(train_f, pred_train_win)
    test_tansho_roi, test_tansho_hit = calc_tansho_roi(test_f, pred_test_win)
    
    train_fukusho_roi, train_fukusho_hit = calc_fukusho_roi(train_f, pred_train_win, returns)
    test_fukusho_roi, test_fukusho_hit = calc_fukusho_roi(test_f, pred_test_win, returns)
    
    logger.info(f"  [単勝] Train: 的中率={train_tansho_hit:.1f}%, ROI={train_tansho_roi:.1f}%")
    logger.info(f"  [単勝] Test:  的中率={test_tansho_hit:.1f}%, ROI={test_tansho_roi:.1f}%")
    logger.info(f"  [複勝] Train: 的中率={train_fukusho_hit:.1f}%, ROI={train_fukusho_roi:.1f}%")
    logger.info(f"  [複勝] Test:  的中率={test_fukusho_hit:.1f}%, ROI={test_fukusho_roi:.1f}%")
    
    # ========== 2. 複勝専用（is_place） ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. 複勝専用モデル（is_place = 3着以内）")
    logger.info("=" * 60)
    
    y_train_place = (train_f['finish_position'] <= 3).astype(int)
    train_ds_place = lgb.Dataset(X_train, y_train_place)
    model_place = lgb.train(params, train_ds_place, num_boost_round=200)
    
    pred_train_place = model_place.predict(X_train)
    pred_test_place = model_place.predict(X_test)
    
    train_tansho_roi2, train_tansho_hit2 = calc_tansho_roi(train_f, pred_train_place)
    test_tansho_roi2, test_tansho_hit2 = calc_tansho_roi(test_f, pred_test_place)
    
    train_fukusho_roi2, train_fukusho_hit2 = calc_fukusho_roi(train_f, pred_train_place, returns)
    test_fukusho_roi2, test_fukusho_hit2 = calc_fukusho_roi(test_f, pred_test_place, returns)
    
    logger.info(f"  [単勝] Train: 的中率={train_tansho_hit2:.1f}%, ROI={train_tansho_roi2:.1f}%")
    logger.info(f"  [単勝] Test:  的中率={test_tansho_hit2:.1f}%, ROI={test_tansho_roi2:.1f}%")
    logger.info(f"  [複勝] Train: 的中率={train_fukusho_hit2:.1f}%, ROI={train_fukusho_roi2:.1f}%")
    logger.info(f"  [複勝] Test:  的中率={test_fukusho_hit2:.1f}%, ROI={test_fukusho_roi2:.1f}%")
    
    # ========== 3. 2着以内モデル ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. 2着以内モデル（is_top2）")
    logger.info("=" * 60)
    
    y_train_top2 = (train_f['finish_position'] <= 2).astype(int)
    train_ds_top2 = lgb.Dataset(X_train, y_train_top2)
    model_top2 = lgb.train(params, train_ds_top2, num_boost_round=200)
    
    pred_train_top2 = model_top2.predict(X_train)
    pred_test_top2 = model_top2.predict(X_test)
    
    train_tansho_roi3, train_tansho_hit3 = calc_tansho_roi(train_f, pred_train_top2)
    test_tansho_roi3, test_tansho_hit3 = calc_tansho_roi(test_f, pred_test_top2)
    
    train_fukusho_roi3, train_fukusho_hit3 = calc_fukusho_roi(train_f, pred_train_top2, returns)
    test_fukusho_roi3, test_fukusho_hit3 = calc_fukusho_roi(test_f, pred_test_top2, returns)
    
    logger.info(f"  [単勝] Train: 的中率={train_tansho_hit3:.1f}%, ROI={train_tansho_roi3:.1f}%")
    logger.info(f"  [単勝] Test:  的中率={test_tansho_hit3:.1f}%, ROI={test_tansho_roi3:.1f}%")
    logger.info(f"  [複勝] Train: 的中率={train_fukusho_hit3:.1f}%, ROI={train_fukusho_roi3:.1f}%")
    logger.info(f"  [複勝] Test:  的中率={test_fukusho_hit3:.1f}%, ROI={test_fukusho_roi3:.1f}%")
    
    # ========== 比較 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較")
    logger.info("=" * 60)
    
    logger.info("")
    logger.info("【単勝 Test ROI】")
    logger.info(f"  is_win (V15):    {test_tansho_roi:.1f}%")
    logger.info(f"  is_place (3着):  {test_tansho_roi2:.1f}%")
    logger.info(f"  is_top2 (2着):   {test_tansho_roi3:.1f}%")
    
    logger.info("")
    logger.info("【複勝 Test ROI】")
    logger.info(f"  is_win (V15):    {test_fukusho_roi:.1f}%")
    logger.info(f"  is_place (3着):  {test_fukusho_roi2:.1f}%")
    logger.info(f"  is_top2 (2着):   {test_fukusho_roi3:.1f}%")
    
    # 最良結果
    best_tansho = max(test_tansho_roi, test_tansho_roi2, test_tansho_roi3)
    best_fukusho = max(test_fukusho_roi, test_fukusho_roi2, test_fukusho_roi3)
    
    logger.info("")
    logger.info(f"Best単勝ROI: {best_tansho:.1f}%")
    logger.info(f"Best複勝ROI: {best_fukusho:.1f}%")
    
    return model_win, model_place, model_top2


if __name__ == "__main__":
    main()

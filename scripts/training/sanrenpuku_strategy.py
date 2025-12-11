# -*- coding: utf-8 -*-
"""
三連複BOX戦略検証

三連複ROI 45.2%（Top3の1点買い）は低すぎる。
しかし、上位5頭BOX（10点）などの多点買いなら的中率とROIが向上する可能性がある。

【検証パターン】
1. Top3-6のBOX購入（1点～20点買い）
2. Top1軸流し（相手Top2-6）
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import itertools
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


def calc_box_roi(df, returns_df, n_box):
    """
    n頭BOX買いのROI計算
    n=3: 1点
    n=4: 4点
    n=5: 10点
    n=6: 20点
    """
    df = df.copy()
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    # 払戻データの準備
    sanrenpuku = returns_df[returns_df['bet_type'] == 'sanrenpuku'].copy()
    sanrenpuku = sanrenpuku.dropna(subset=['horse_1', 'horse_2', 'horse_3'])
    sanrenpuku['winner_set'] = sanrenpuku.apply(
        lambda x: frozenset([int(x['horse_1']), int(x['horse_2']), int(x['horse_3'])]),
        axis=1
    )
    payout_dict = dict(zip(
        zip(sanrenpuku['race_id'], sanrenpuku['winner_set']),
        sanrenpuku['payout']
    ))
    
    race_ids = df['race_id'].unique()
    total_bet = 0
    total_payout = 0
    hit_count = 0
    
    for race_id in race_ids:
        race_df = df[df['race_id'] == race_id]
        
        # 予測TopN頭を取得
        top_n = race_df[race_df['rank'] <= n_box]['horse_number'].astype(int).tolist()
        
        if len(top_n) < 3:
            continue
            
        # BOXの組み合わせ（nC3通り）
        combinations = list(itertools.combinations(top_n, 3))
        n_bets = len(combinations)
        
        total_bet += n_bets * 100
        
        # 的中判定
        payout = 0
        for combo in combinations:
            key = (race_id, frozenset(combo))
            if key in payout_dict:
                payout += payout_dict[key]
        
        total_payout += payout
        if payout > 0:
            hit_count += 1
            
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hit_count / len(race_ids) * 100 if len(race_ids) > 0 else 0
    avg_bets = total_bet / 100 / len(race_ids)
    
    return roi, hit_rate, avg_bets


def calc_nagashi_roi(df, returns_df, n_aite):
    """
    1頭軸流しのROI計算
    軸: Top1
    相手: Top2 ～ Top(1+n_aite)
    点数: n_aite C 2 通り
    """
    df = df.copy()
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    sanrenpuku = returns_df[returns_df['bet_type'] == 'sanrenpuku'].copy()
    sanrenpuku = sanrenpuku.dropna(subset=['horse_1', 'horse_2', 'horse_3'])
    sanrenpuku['winner_set'] = sanrenpuku.apply(
        lambda x: frozenset([int(x['horse_1']), int(x['horse_2']), int(x['horse_3'])]),
        axis=1
    )
    payout_dict = dict(zip(
        zip(sanrenpuku['race_id'], sanrenpuku['winner_set']),
        sanrenpuku['payout']
    ))
    
    race_ids = df['race_id'].unique()
    total_bet = 0
    total_payout = 0
    hit_count = 0
    
    for race_id in race_ids:
        race_df = df[df['race_id'] == race_id]
        
        # 軸馬と相手馬
        try:
            jiku = race_df[race_df['rank'] == 1]['horse_number'].astype(int).iloc[0]
            aite = race_df[(race_df['rank'] > 1) & (race_df['rank'] <= 1 + n_aite)]['horse_number'].astype(int).tolist()
        except IndexError:
            continue
            
        if len(aite) < 2:
            continue
            
        # 流しの組み合わせ（jiku + aiteから2頭）
        aite_combos = list(itertools.combinations(aite, 2))
        combinations = [tuple(sorted([jiku] + list(c))) for c in aite_combos]
        
        n_bets = len(combinations)
        total_bet += n_bets * 100
        
        payout = 0
        for combo in combinations:
            key = (race_id, frozenset(combo))
            if key in payout_dict:
                payout += payout_dict[key]
        
        total_payout += payout
        if payout > 0:
            hit_count += 1
            
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hit_count / len(race_ids) * 100 if len(race_ids) > 0 else 0
    avg_bets = total_bet / 100 / len(race_ids)
    
    return roi, hit_rate, avg_bets


def main():
    logger.info("=" * 60)
    logger.info("三連複BOX/流し 戦略検証")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # V15特徴量
    logger.info("")
    logger.info("V15特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
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
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    test_f['pred'] = model.predict(X_test)
    
    # ========== BOX戦略 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("BOX買い戦略 (Top N頭 BOX)")
    logger.info("=" * 60)
    
    for n in range(3, 8):
        roi, hit_rate, avg_bets = calc_box_roi(test_f, returns, n)
        n_combos = int(n * (n-1) * (n-2) / 6)
        logger.info(f"  Top{n} BOX ({n_combos:2}点): ROI={roi:5.1f}%, 的中率={hit_rate:4.1f}%")
        
    # ========== 軸流し戦略 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("軸流し戦略 (Top1軸 相手Top2～N)")
    logger.info("=" * 60)
    
    for n in range(2, 9):
        # 相手頭数n → 全体頭数 n+1
        roi, hit_rate, avg_bets = calc_nagashi_roi(test_f, returns, n)
        n_combos = int(n * (n-1) / 2)
        logger.info(f"  Top1 - 相手{n}頭 ({n_combos:2}点): ROI={roi:5.1f}%, 的中率={hit_rate:4.1f}%")


if __name__ == "__main__":
    main()

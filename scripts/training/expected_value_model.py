#!/usr/bin/env python3
"""
期待値モデルによる馬券戦略

目的関数を「着順予測」から「勝率予測」に変更し、
期待値 = 予測勝率 × オッズ > 1.0 の馬のみを購入する戦略
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
from itertools import combinations

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_all_data():
    """全データ読み込み"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races = races[(races['finish_position'].notna()) & (races['finish_position'] > 0)].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    try:
        pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    except:
        pedigrees = None
    
    try:
        race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    except:
        race_details = None
    
    # 払い戻しデータ
    returns = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
    
    # コーナーデータを構築
    corners = []
    for corner in [1, 2, 3, 4]:
        col = f'passing_order_{corner}'
        if col in races.columns:
            temp = races[['race_id', 'horse_number', col]].copy()
            temp = temp[temp[col].notna()]
            temp['corner'] = corner
            temp['position'] = temp[col]
            temp['gap_from_leader'] = 0
            corners.append(temp[['race_id', 'horse_number', 'corner', 'position', 'gap_from_leader']])
    
    corners_df = pd.concat(corners, ignore_index=True) if corners else None
    
    return races, pedigrees, corners_df, race_details, returns


def calc_expected_value_roi(pred_df, returns_df, ev_threshold=1.0):
    """
    期待値ベースの単勝ROI計算
    
    期待値 = 予測勝率 × オッズ
    ev_threshold以上の馬のみ購入
    """
    tansho = returns_df[returns_df['bet_type'] == 'tansho'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        # 期待値が閾値以上の馬を選択
        ev_bets = group[group['expected_value'] >= ev_threshold]
        
        if len(ev_bets) == 0:
            continue
        
        # このレースの単勝払い戻し
        race_tansho = tansho[tansho['race_id'] == race_id]
        if len(race_tansho) == 0:
            continue
        
        winning_horse = race_tansho['horse_number'].values[0]
        payout = race_tansho['payout'].values[0]
        
        total_bet = len(ev_bets) * 100
        total_return = 0
        
        for _, horse in ev_bets.iterrows():
            if horse['horse_number'] == winning_horse:
                total_return += payout
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'hit': total_return > 0,
            'n_bets': len(ev_bets)
        })
    
    if not results:
        return {
            'total_bet': 0, 'total_return': 0, 'roi': 0, 
            'hit_rate': 0, 'n_races': 0, 'avg_bets_per_race': 0
        }
    
    df = pd.DataFrame(results)
    return {
        'total_bet': df['total_bet'].sum(),
        'total_return': df['total_return'].sum(),
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if df['total_bet'].sum() > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100 if len(df) > 0 else 0,
        'n_races': len(df),
        'avg_bets_per_race': df['n_bets'].mean()
    }


def calc_expected_value_fukusho_roi(pred_df, returns_df, ev_threshold=1.0):
    """
    期待値ベースの複勝ROI計算
    """
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        # 期待値が閾値以上の馬を選択
        ev_bets = group[group['expected_value_fukusho'] >= ev_threshold]
        
        if len(ev_bets) == 0:
            continue
        
        # このレースの複勝払い戻し
        race_fukusho = fukusho[fukusho['race_id'] == race_id]
        if len(race_fukusho) == 0:
            continue
        
        fukusho_map = dict(zip(race_fukusho['horse_number'], race_fukusho['payout']))
        
        total_bet = len(ev_bets) * 100
        total_return = 0
        
        for _, horse in ev_bets.iterrows():
            if horse['horse_number'] in fukusho_map:
                total_return += fukusho_map[horse['horse_number']]
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'hit': total_return > 0,
            'n_bets': len(ev_bets)
        })
    
    if not results:
        return {
            'total_bet': 0, 'total_return': 0, 'roi': 0, 
            'hit_rate': 0, 'n_races': 0, 'avg_bets_per_race': 0
        }
    
    df = pd.DataFrame(results)
    return {
        'total_bet': df['total_bet'].sum(),
        'total_return': df['total_return'].sum(),
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if df['total_bet'].sum() > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100 if len(df) > 0 else 0,
        'n_races': len(df),
        'avg_bets_per_race': df['n_bets'].mean()
    }


def run_expected_value_analysis():
    """期待値モデル分析実行"""
    from keibaai.src.features.leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17
    
    races, pedigrees, corners_df, race_details, returns = load_all_data()
    
    logger.info(f"Total races: {len(races):,}")
    
    # 複数年で検証
    years = [2022, 2023, 2024]
    all_results = []
    
    for year in years:
        logger.info(f"\n{'='*60}")
        logger.info(f"Year {year}")
        logger.info(f"{'='*60}")
        
        train_start = f"{year - 8}-01-01"
        train_end = f"{year - 1}-12-31"
        test_start = f"{year}-01-01"
        test_end = f"{year}-12-31"
        
        train_df = races[(races['race_date'] >= train_start) & (races['race_date'] <= train_end)].copy()
        test_df = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        test_race_ids = set(test_df['race_id'].unique())
        test_returns = returns[returns['race_id'].isin(test_race_ids)]
        
        logger.info(f"Train: {len(train_df):,}, Test: {len(test_df):,}")
        
        # V17 Feature Engineering
        v17 = LeakFreeFeatureEngineerV17()
        v17.fit(train_df, pedigrees, corners_df, race_details)
        
        train_feat = v17.transform(train_df)
        test_feat = v17.transform(test_df)
        
        # 勝利フラグを作成（目的変数）- transform後に元のfinish_positionを使用
        train_feat['is_win'] = (train_feat['finish_position'] == 1).astype(int)
        test_feat['is_win'] = (test_feat['finish_position'] == 1).astype(int)
        
        # 複勝フラグ（3着以内）
        train_feat['is_place'] = (train_feat['finish_position'] <= 3).astype(int)
        test_feat['is_place'] = (test_feat['finish_position'] <= 3).astype(int)
        
        feature_cols = v17.get_feature_columns()
        available_cols = [c for c in feature_cols if c in train_feat.columns and c in test_feat.columns]
        
        X_train = train_feat[available_cols].fillna(-999)
        X_test = test_feat[available_cols].fillna(-999)
        
        # === 勝率予測モデル（分類） ===
        y_train_win = train_feat['is_win']
        
        params_cls = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'learning_rate': 0.05,
            'num_leaves': 63,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
            'seed': 42,
            'is_unbalance': True  # 不均衡データ対応
        }
        
        train_data_win = lgb.Dataset(X_train, label=y_train_win)
        model_win = lgb.train(params_cls, train_data_win, num_boost_round=500)
        
        # 勝率予測
        test_feat['pred_win_prob'] = model_win.predict(X_test)
        
        # === 複勝予測モデル（分類） ===
        y_train_place = train_feat['is_place']
        
        train_data_place = lgb.Dataset(X_train, label=y_train_place)
        model_place = lgb.train(params_cls, train_data_place, num_boost_round=500)
        
        # 複勝予測
        test_feat['pred_place_prob'] = model_place.predict(X_test)
        
        # === 期待値計算 ===
        # 単勝期待値 = 予測勝率 × 単勝オッズ
        test_feat['expected_value'] = test_feat['pred_win_prob'] * test_feat['win_odds']
        
        # 複勝期待値（複勝オッズは単勝オッズから推定）
        # 複勝オッズ ≈ (単勝オッズ - 1) / 3 + 1
        test_feat['place_odds_est'] = (test_feat['win_odds'] - 1) / 3 + 1
        test_feat['place_odds_est'] = test_feat['place_odds_est'].clip(lower=1.1)
        test_feat['expected_value_fukusho'] = test_feat['pred_place_prob'] * test_feat['place_odds_est']
        
        # === 期待値分布を確認 ===
        ev_stats = test_feat['expected_value'].describe()
        logger.info(f"期待値分布: mean={ev_stats['mean']:.3f}, max={ev_stats['max']:.3f}")
        logger.info(f"EV >= 1.0: {(test_feat['expected_value'] >= 1.0).sum()}件")
        logger.info(f"EV >= 1.2: {(test_feat['expected_value'] >= 1.2).sum()}件")
        logger.info(f"EV >= 1.5: {(test_feat['expected_value'] >= 1.5).sum()}件")
        
        # === 各閾値でのROI計算 ===
        year_results = []
        
        for ev_thresh in [0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5]:
            # 単勝
            roi = calc_expected_value_roi(test_feat, test_returns, ev_thresh)
            year_results.append({
                'year': year,
                'strategy': f'単勝 EV>={ev_thresh}',
                'ev_threshold': ev_thresh,
                'bet_type': 'tansho',
                'total_bet': roi['total_bet'],
                'total_return': roi['total_return'],
                'roi': roi['roi'],
                'hit_rate': roi['hit_rate'],
                'n_races': roi['n_races'],
                'avg_bets': roi['avg_bets_per_race']
            })
            if roi['n_races'] > 0:
                logger.info(f"単勝 EV>={ev_thresh}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%, "
                           f"レース数 {roi['n_races']}, 平均購入 {roi['avg_bets_per_race']:.1f}点")
        
        for ev_thresh in [0.8, 0.9, 1.0, 1.1, 1.2]:
            # 複勝
            roi = calc_expected_value_fukusho_roi(test_feat, test_returns, ev_thresh)
            year_results.append({
                'year': year,
                'strategy': f'複勝 EV>={ev_thresh}',
                'ev_threshold': ev_thresh,
                'bet_type': 'fukusho',
                'total_bet': roi['total_bet'],
                'total_return': roi['total_return'],
                'roi': roi['roi'],
                'hit_rate': roi['hit_rate'],
                'n_races': roi['n_races'],
                'avg_bets': roi['avg_bets_per_race']
            })
            if roi['n_races'] > 0:
                logger.info(f"複勝 EV>={ev_thresh}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%, "
                           f"レース数 {roi['n_races']}, 平均購入 {roi['avg_bets_per_race']:.1f}点")
        
        all_results.extend(year_results)
    
    # 全体サマリー
    logger.info(f"\n{'='*60}")
    logger.info("年度別ROIサマリー（期待値モデル）")
    logger.info(f"{'='*60}")
    
    results_df = pd.DataFrame(all_results)
    
    # 単勝のピボット
    tansho_df = results_df[results_df['bet_type'] == 'tansho']
    if len(tansho_df) > 0:
        pivot = tansho_df.pivot_table(
            index='strategy',
            columns='year',
            values='roi',
            aggfunc='mean'
        )
        pivot['平均'] = pivot.mean(axis=1)
        pivot = pivot.sort_values('平均', ascending=False)
        print("\n単勝 年度別ROI (%):")
        print(pivot.round(1).to_string())
    
    # 複勝のピボット
    fukusho_df = results_df[results_df['bet_type'] == 'fukusho']
    if len(fukusho_df) > 0:
        pivot = fukusho_df.pivot_table(
            index='strategy',
            columns='year',
            values='roi',
            aggfunc='mean'
        )
        pivot['平均'] = pivot.mean(axis=1)
        pivot = pivot.sort_values('平均', ascending=False)
        print("\n複勝 年度別ROI (%):")
        print(pivot.round(1).to_string())
    
    # 最良戦略
    best = results_df.groupby('strategy')['roi'].mean().sort_values(ascending=False)
    logger.info(f"\n最良戦略: {best.index[0]} (平均ROI: {best.values[0]:.1f}%)")


if __name__ == "__main__":
    run_expected_value_analysis()

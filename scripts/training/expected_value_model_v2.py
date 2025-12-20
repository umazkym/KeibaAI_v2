#!/usr/bin/env python3
"""
期待値モデル修正版

問題点:
- 旧版: EV>=閾値の馬を全て購入 → 1レース10点以上購入 → ROI低下

修正:
- 各レースで期待値TOP-N点のみ購入
- 購入点数を適切に制限

各馬券の特性:
- 単勝: 1レース1点のみ的中 → Top1のみ購入が最適
- 複勝: 1レース最大3点的中 → Top1-3を購入
- 馬連: 1レース1点のみ的中 → Top2のBOX（1点）が最適
- ワイド: 1レース最大3点的中 → Top3のBOX（3点）が最適
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
    
    returns = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
    
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


def calc_tansho_roi_ev_rank(pred_df, returns_df, ev_col='expected_value', top_n=1):
    """
    単勝: EV上位N点を購入（通常Top1のみ）
    """
    tansho = returns_df[returns_df['bet_type'] == 'tansho'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        # EV上位N点を選択
        top_horses = group.nlargest(top_n, ev_col)
        
        race_tansho = tansho[tansho['race_id'] == race_id]
        if len(race_tansho) == 0:
            continue
        
        winning_horse = race_tansho['horse_number'].values[0]
        payout = race_tansho['payout'].values[0]
        
        total_bet = len(top_horses) * 100
        total_return = 0
        
        for _, horse in top_horses.iterrows():
            if horse['horse_number'] == winning_horse:
                total_return += payout
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'hit': total_return > 0
        })
    
    if not results:
        return {'roi': 0, 'hit_rate': 0, 'n_races': 0}
    
    df = pd.DataFrame(results)
    return {
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if df['total_bet'].sum() > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100,
        'n_races': len(df)
    }


def calc_fukusho_roi_ev_rank(pred_df, returns_df, ev_col='expected_value_fukusho', top_n=1):
    """
    複勝: EV上位N点を購入
    """
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        # EV上位N点を選択
        top_horses = group.nlargest(top_n, ev_col)
        
        race_fukusho = fukusho[fukusho['race_id'] == race_id]
        if len(race_fukusho) == 0:
            continue
        
        fukusho_map = dict(zip(race_fukusho['horse_number'], race_fukusho['payout']))
        
        total_bet = len(top_horses) * 100
        total_return = 0
        
        for _, horse in top_horses.iterrows():
            if horse['horse_number'] in fukusho_map:
                total_return += fukusho_map[horse['horse_number']]
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'hit': total_return > 0
        })
    
    if not results:
        return {'roi': 0, 'hit_rate': 0, 'n_races': 0}
    
    df = pd.DataFrame(results)
    return {
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if df['total_bet'].sum() > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100,
        'n_races': len(df)
    }


def calc_umaren_roi_ev_rank(pred_df, returns_df, ev_col='expected_value', top_n=2):
    """
    馬連BOX: EV上位N頭のBOX（Top2なら1点、Top3なら3点）
    """
    umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        if len(group) < top_n:
            continue
        
        top_horses = group.nlargest(top_n, ev_col)
        top_horse_numbers = set(top_horses['horse_number'].values)
        
        race_umaren = umaren[umaren['race_id'] == race_id]
        if len(race_umaren) == 0:
            continue
        
        # 馬連BOXの点数
        n_bets = len(list(combinations(range(top_n), 2)))  # nC2
        
        winning = race_umaren.iloc[0]
        w1, w2 = winning['horse_1'], winning['horse_2']
        payout = winning['payout']
        
        total_bet = n_bets * 100
        total_return = 0
        
        if w1 in top_horse_numbers and w2 in top_horse_numbers:
            total_return = payout
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'hit': total_return > 0
        })
    
    if not results:
        return {'roi': 0, 'hit_rate': 0, 'n_races': 0}
    
    df = pd.DataFrame(results)
    return {
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if df['total_bet'].sum() > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100,
        'n_races': len(df)
    }


def calc_wide_roi_ev_rank(pred_df, returns_df, ev_col='expected_value', top_n=2):
    """
    ワイドBOX: EV上位N頭のBOX（Top2なら1点、Top3なら3点）
    """
    wide = returns_df[returns_df['bet_type'] == 'wide'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        if len(group) < top_n:
            continue
        
        top_horses = group.nlargest(top_n, ev_col)
        top_horse_numbers = set(top_horses['horse_number'].values)
        
        race_wide = wide[wide['race_id'] == race_id]
        if len(race_wide) == 0:
            continue
        
        # ワイドBOXの点数
        n_bets = len(list(combinations(range(top_n), 2)))  # nC2
        
        total_bet = n_bets * 100
        total_return = 0
        
        for _, row in race_wide.iterrows():
            w1, w2 = row['horse_1'], row['horse_2']
            if w1 in top_horse_numbers and w2 in top_horse_numbers:
                total_return += row['payout']
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'hit': total_return > 0
        })
    
    if not results:
        return {'roi': 0, 'hit_rate': 0, 'n_races': 0}
    
    df = pd.DataFrame(results)
    return {
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if df['total_bet'].sum() > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100,
        'n_races': len(df)
    }


def run_corrected_ev_analysis():
    """修正版期待値モデル分析"""
    from keibaai.src.features.leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17
    
    races, pedigrees, corners_df, race_details, returns = load_all_data()
    
    logger.info(f"Total races: {len(races):,}")
    
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
        
        # 勝利フラグ・複勝フラグ
        train_feat['is_win'] = (train_feat['finish_position'] == 1).astype(int)
        test_feat['is_win'] = (test_feat['finish_position'] == 1).astype(int)
        train_feat['is_place'] = (train_feat['finish_position'] <= 3).astype(int)
        test_feat['is_place'] = (test_feat['finish_position'] <= 3).astype(int)
        
        feature_cols = v17.get_feature_columns()
        available_cols = [c for c in feature_cols if c in train_feat.columns and c in test_feat.columns]
        
        X_train = train_feat[available_cols].fillna(-999)
        X_test = test_feat[available_cols].fillna(-999)
        
        # 勝率予測モデル
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
            'is_unbalance': True
        }
        
        y_train_win = train_feat['is_win']
        train_data_win = lgb.Dataset(X_train, label=y_train_win)
        model_win = lgb.train(params_cls, train_data_win, num_boost_round=500)
        test_feat['pred_win_prob'] = model_win.predict(X_test)
        
        # 複勝予測モデル
        y_train_place = train_feat['is_place']
        train_data_place = lgb.Dataset(X_train, label=y_train_place)
        model_place = lgb.train(params_cls, train_data_place, num_boost_round=500)
        test_feat['pred_place_prob'] = model_place.predict(X_test)
        
        # 期待値計算
        test_feat['expected_value'] = test_feat['pred_win_prob'] * test_feat['win_odds']
        test_feat['place_odds_est'] = (test_feat['win_odds'] - 1) / 3 + 1
        test_feat['place_odds_est'] = test_feat['place_odds_est'].clip(lower=1.1)
        test_feat['expected_value_fukusho'] = test_feat['pred_place_prob'] * test_feat['place_odds_est']
        
        # === 修正版: EV上位N点のみ購入 ===
        year_results = []
        
        logger.info(f"\n--- 単勝 (EV上位N点のみ購入) ---")
        for top_n in [1, 2, 3]:
            roi = calc_tansho_roi_ev_rank(test_feat, test_returns, 'expected_value', top_n)
            logger.info(f"単勝 EV-Top{top_n}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%")
            year_results.append({
                'year': year, 'strategy': f'単勝 EV-Top{top_n}',
                'roi': roi['roi'], 'hit_rate': roi['hit_rate'], 'n_races': roi['n_races']
            })
        
        logger.info(f"\n--- 複勝 (EV上位N点のみ購入) ---")
        for top_n in [1, 2, 3]:
            roi = calc_fukusho_roi_ev_rank(test_feat, test_returns, 'expected_value_fukusho', top_n)
            logger.info(f"複勝 EV-Top{top_n}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%")
            year_results.append({
                'year': year, 'strategy': f'複勝 EV-Top{top_n}',
                'roi': roi['roi'], 'hit_rate': roi['hit_rate'], 'n_races': roi['n_races']
            })
        
        logger.info(f"\n--- 馬連BOX (EV上位N頭) ---")
        for top_n in [2, 3]:
            roi = calc_umaren_roi_ev_rank(test_feat, test_returns, 'expected_value', top_n)
            logger.info(f"馬連BOX EV-Top{top_n}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%")
            year_results.append({
                'year': year, 'strategy': f'馬連BOX EV-Top{top_n}',
                'roi': roi['roi'], 'hit_rate': roi['hit_rate'], 'n_races': roi['n_races']
            })
        
        logger.info(f"\n--- ワイドBOX (EV上位N頭) ---")
        for top_n in [2, 3]:
            roi = calc_wide_roi_ev_rank(test_feat, test_returns, 'expected_value', top_n)
            logger.info(f"ワイドBOX EV-Top{top_n}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%")
            year_results.append({
                'year': year, 'strategy': f'ワイドBOX EV-Top{top_n}',
                'roi': roi['roi'], 'hit_rate': roi['hit_rate'], 'n_races': roi['n_races']
            })
        
        all_results.extend(year_results)
    
    # 全体サマリー
    logger.info(f"\n{'='*60}")
    logger.info("年度別ROIサマリー（修正版期待値モデル）")
    logger.info(f"{'='*60}")
    
    results_df = pd.DataFrame(all_results)
    pivot = results_df.pivot_table(
        index='strategy',
        columns='year',
        values='roi',
        aggfunc='mean'
    )
    pivot['平均'] = pivot.mean(axis=1)
    pivot['標準偏差'] = pivot[[2022, 2023, 2024]].std(axis=1)
    pivot = pivot.sort_values('平均', ascending=False)
    
    print("\n年度別ROI (%):")
    print(pivot.round(1).to_string())
    
    logger.info(f"\n最良戦略: {pivot.index[0]} (平均ROI: {pivot['平均'].iloc[0]:.1f}%)")


if __name__ == "__main__":
    run_corrected_ev_analysis()

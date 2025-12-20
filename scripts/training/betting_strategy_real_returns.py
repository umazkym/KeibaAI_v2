#!/usr/bin/env python3
"""
馬券戦略最適化スクリプト（実払い戻しデータ版）

V17モデルの予測と実際の払い戻しデータを使って正確なROIを計算
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


def calc_tansho_roi(pred_df, returns_df, top_n=1):
    """単勝ROI計算（実払い戻しデータ使用）"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        top_horses = group[group['pred_rank'] <= top_n]
        
        # このレースの単勝払い戻し
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
    
    df = pd.DataFrame(results)
    return {
        'total_bet': df['total_bet'].sum(),
        'total_return': df['total_return'].sum(),
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if len(df) > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100 if len(df) > 0 else 0,
        'n_races': len(df)
    }


def calc_fukusho_roi(pred_df, returns_df, top_n=1):
    """複勝ROI計算（実払い戻しデータ使用）"""
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        top_horses = group[group['pred_rank'] <= top_n]
        
        # このレースの複勝払い戻し
        race_fukusho = fukusho[fukusho['race_id'] == race_id]
        if len(race_fukusho) == 0:
            continue
        
        # 複勝的中馬番と払い戻しをマッピング
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
    
    df = pd.DataFrame(results)
    return {
        'total_bet': df['total_bet'].sum(),
        'total_return': df['total_return'].sum(),
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if len(df) > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100 if len(df) > 0 else 0,
        'n_races': len(df)
    }


def calc_wide_box_roi(pred_df, returns_df, top_n=3):
    """ワイドBOX ROI計算（実払い戻しデータ使用）"""
    wide = returns_df[returns_df['bet_type'] == 'wide'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        if len(group) < top_n:
            continue
        
        top_horses = group[group['pred_rank'] <= top_n].sort_values('pred_rank')
        if len(top_horses) < top_n:
            continue
        
        # このレースのワイド払い戻し
        race_wide = wide[wide['race_id'] == race_id]
        if len(race_wide) == 0:
            continue
        
        # ワイド的中ペアをセットとして保存（順番を無視）
        wide_map = {}
        for _, row in race_wide.iterrows():
            pair = frozenset([int(row['horse_1']), int(row['horse_2'])])
            wide_map[pair] = row['payout']
        
        # 予測上位N頭のすべての組み合わせをチェック
        pred_horses = [int(h) for h in top_horses['horse_number'].values]
        
        num_bets = len(list(combinations(pred_horses, 2)))
        total_bet = num_bets * 100
        total_return = 0
        
        for combo in combinations(pred_horses, 2):
            pair = frozenset(combo)
            if pair in wide_map:
                total_return += wide_map[pair]
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'hit': total_return > 0
        })
    
    df = pd.DataFrame(results)
    return {
        'total_bet': df['total_bet'].sum(),
        'total_return': df['total_return'].sum(),
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if len(df) > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100 if len(df) > 0 else 0,
        'n_races': len(df)
    }


def calc_umaren_box_roi(pred_df, returns_df, top_n=2):
    """馬連BOX ROI計算（実払い戻しデータ使用）"""
    umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        if len(group) < top_n:
            continue
        
        top_horses = group[group['pred_rank'] <= top_n].sort_values('pred_rank')
        if len(top_horses) < top_n:
            continue
        
        # このレースの馬連払い戻し
        race_umaren = umaren[umaren['race_id'] == race_id]
        if len(race_umaren) == 0:
            continue
        
        # 馬連的中ペア
        winning_pair = frozenset([
            int(race_umaren['horse_1'].values[0]),
            int(race_umaren['horse_2'].values[0])
        ])
        payout = race_umaren['payout'].values[0]
        
        # 予測上位N頭のすべての組み合わせをチェック
        pred_horses = [int(h) for h in top_horses['horse_number'].values]
        
        num_bets = len(list(combinations(pred_horses, 2)))
        total_bet = num_bets * 100
        total_return = 0
        
        for combo in combinations(pred_horses, 2):
            pair = frozenset(combo)
            if pair == winning_pair:
                total_return += payout
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'hit': total_return > 0
        })
    
    df = pd.DataFrame(results)
    return {
        'total_bet': df['total_bet'].sum(),
        'total_return': df['total_return'].sum(),
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if len(df) > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100 if len(df) > 0 else 0,
        'n_races': len(df)
    }


def run_betting_strategy_analysis():
    """馬券戦略分析実行（実払い戻しデータ版）"""
    from keibaai.src.features.leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17
    
    races, pedigrees, corners_df, race_details, returns = load_all_data()
    
    logger.info(f"Total races: {len(races):,}")
    logger.info(f"Returns data: {len(returns):,}")
    
    # 複数年で検証して再現性を確認
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
        
        # テスト期間の払い戻しデータ
        test_race_ids = set(test_df['race_id'].unique())
        test_returns = returns[returns['race_id'].isin(test_race_ids)]
        
        logger.info(f"Train: {len(train_df):,}, Test: {len(test_df):,}")
        logger.info(f"Test returns: {len(test_returns):,}")
        
        # V17 Feature Engineering
        v17 = LeakFreeFeatureEngineerV17()
        v17.fit(train_df, pedigrees, corners_df, race_details)
        
        train_feat = v17.transform(train_df)
        test_feat = v17.transform(test_df)
        
        # 特徴量カラム
        feature_cols = v17.get_feature_columns()
        available_cols = [c for c in feature_cols if c in train_feat.columns and c in test_feat.columns]
        
        # 学習
        X_train = train_feat[available_cols].fillna(-999)
        y_train = train_feat['finish_position']
        X_test = test_feat[available_cols].fillna(-999)
        
        params = {
            'objective': 'regression',
            'metric': 'rmse',
            'boosting_type': 'gbdt',
            'learning_rate': 0.05,
            'num_leaves': 63,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
            'seed': 42
        }
        
        train_data = lgb.Dataset(X_train, label=y_train)
        model = lgb.train(params, train_data, num_boost_round=500)
        
        # 予測
        test_feat['pred'] = model.predict(X_test)
        test_feat['pred_rank'] = test_feat.groupby('race_id')['pred'].rank()
        
        # 馬券戦略別ROI計算
        year_results = []
        
        # 単勝
        for top_n in [1, 2, 3]:
            roi = calc_tansho_roi(test_feat, test_returns, top_n)
            year_results.append({
                'year': year,
                'strategy': f'単勝 Top{top_n}',
                'total_bet': roi['total_bet'],
                'total_return': roi['total_return'],
                'roi': roi['roi'],
                'hit_rate': roi['hit_rate'],
                'n_races': roi['n_races']
            })
            logger.info(f"単勝 Top{top_n}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%")
        
        # 複勝
        for top_n in [1, 2, 3]:
            roi = calc_fukusho_roi(test_feat, test_returns, top_n)
            year_results.append({
                'year': year,
                'strategy': f'複勝 Top{top_n}',
                'total_bet': roi['total_bet'],
                'total_return': roi['total_return'],
                'roi': roi['roi'],
                'hit_rate': roi['hit_rate'],
                'n_races': roi['n_races']
            })
            logger.info(f"複勝 Top{top_n}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%")
        
        # 馬連BOX
        for top_n in [2, 3]:
            roi = calc_umaren_box_roi(test_feat, test_returns, top_n)
            year_results.append({
                'year': year,
                'strategy': f'馬連BOX Top{top_n}',
                'total_bet': roi['total_bet'],
                'total_return': roi['total_return'],
                'roi': roi['roi'],
                'hit_rate': roi['hit_rate'],
                'n_races': roi['n_races']
            })
            logger.info(f"馬連BOX Top{top_n}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%")
        
        # ワイドBOX
        for top_n in [2, 3]:
            roi = calc_wide_box_roi(test_feat, test_returns, top_n)
            year_results.append({
                'year': year,
                'strategy': f'ワイドBOX Top{top_n}',
                'total_bet': roi['total_bet'],
                'total_return': roi['total_return'],
                'roi': roi['roi'],
                'hit_rate': roi['hit_rate'],
                'n_races': roi['n_races']
            })
            logger.info(f"ワイドBOX Top{top_n}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%")
        
        all_results.extend(year_results)
    
    # 全体サマリー
    logger.info(f"\n{'='*60}")
    logger.info("年度別ROIサマリー")
    logger.info(f"{'='*60}")
    
    results_df = pd.DataFrame(all_results)
    
    # ピボットテーブルで年度別に表示
    pivot = results_df.pivot_table(
        index='strategy',
        columns='year',
        values='roi',
        aggfunc='mean'
    )
    pivot['平均'] = pivot.mean(axis=1)
    pivot['標準偏差'] = results_df.groupby('strategy')['roi'].std()
    pivot = pivot.sort_values('平均', ascending=False)
    
    print("\n年度別ROI (%):")
    print(pivot.round(1).to_string())
    
    # 再現性評価
    logger.info(f"\n{'='*60}")
    logger.info("再現性評価")
    logger.info(f"{'='*60}")
    
    for strategy in pivot.index:
        yearly_roi = results_df[results_df['strategy'] == strategy]['roi'].values
        mean_roi = yearly_roi.mean()
        std_roi = yearly_roi.std()
        cv = std_roi / mean_roi * 100 if mean_roi > 0 else 0
        logger.info(f"{strategy}: 平均ROI {mean_roi:.1f}%, 標準偏差 {std_roi:.1f}%, 変動係数 {cv:.1f}%")


if __name__ == "__main__":
    run_betting_strategy_analysis()

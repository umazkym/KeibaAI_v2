#!/usr/bin/env python3
"""
馬券戦略最適化スクリプト

V17モデルの予測を使って、様々な馬券戦略のROIを検証
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging

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
    try:
        returns = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
    except:
        returns = None
    
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


def calc_win_roi(pred_df, top_n=1):
    """単勝ROI計算（Top N馬を購入）"""
    top_horses = pred_df[pred_df['pred_rank'] <= top_n].copy()
    total_bet = len(top_horses) * 100
    
    wins = top_horses[top_horses['finish_position'] == 1]
    total_return = (wins['win_odds'] * 100).sum() if len(wins) > 0 else 0
    
    return {
        'total_bet': total_bet,
        'total_return': total_return,
        'roi': total_return / total_bet * 100 if total_bet > 0 else 0,
        'hit_rate': len(wins) / len(top_horses) * 100 if len(top_horses) > 0 else 0
    }


def calc_place_roi(pred_df, top_n=1):
    """複勝ROI計算（Top N馬を購入、3着以内で的中）
    
    複勝オッズは正確なデータがないため、単勝オッズから推定
    推定複勝オッズ = (単勝オッズ - 1) / 3 + 1
    """
    top_horses = pred_df[pred_df['pred_rank'] <= top_n].copy()
    total_bet = len(top_horses) * 100
    
    # 複勝的中 = 3着以内
    places = top_horses[top_horses['finish_position'] <= 3]
    
    # 複勝オッズを推定
    places = places.copy()
    places['place_odds_est'] = (places['win_odds'] - 1) / 3 + 1
    places['place_odds_est'] = places['place_odds_est'].clip(lower=1.1)
    
    total_return = (places['place_odds_est'] * 100).sum() if len(places) > 0 else 0
    
    return {
        'total_bet': total_bet,
        'total_return': total_return,
        'roi': total_return / total_bet * 100 if total_bet > 0 else 0,
        'hit_rate': len(places) / len(top_horses) * 100 if len(top_horses) > 0 else 0
    }


def calc_exacta_roi(pred_df):
    """馬連BOX ROI計算（Top2のBOX購入）
    
    馬連オッズは払い戻しデータがないと計算できないため、
    単勝オッズの積から推定: 馬連オッズ ≈ 単勝1位オッズ × 単勝2位オッズ × 0.5
    """
    results = []
    
    for race_id, group in pred_df.groupby('race_id'):
        if len(group) < 2:
            continue
        
        top2 = group[group['pred_rank'] <= 2].sort_values('pred_rank')
        if len(top2) < 2:
            continue
        
        # 馬連BOX: 1点購入
        total_bet = 100
        
        # 的中判定: Top2 の両方が1-2着に入っている
        top2_finish = set(top2[top2['finish_position'] <= 2]['horse_number'].tolist())
        top2_pred = set(top2['horse_number'].tolist())
        
        if len(top2_finish) == 2 and top2_finish == top2_pred:
            # 的中: 馬連オッズを推定
            odds1 = top2.iloc[0]['win_odds']
            odds2 = top2.iloc[1]['win_odds']
            umaren_odds_est = odds1 * odds2 * 0.4  # 推定係数
            total_return = umaren_odds_est * 100
        else:
            total_return = 0
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'hit': total_return > 0
        })
    
    results_df = pd.DataFrame(results)
    
    return {
        'total_bet': results_df['total_bet'].sum(),
        'total_return': results_df['total_return'].sum(),
        'roi': results_df['total_return'].sum() / results_df['total_bet'].sum() * 100 if len(results_df) > 0 else 0,
        'hit_rate': results_df['hit'].sum() / len(results_df) * 100 if len(results_df) > 0 else 0
    }


def calc_wide_box_roi(pred_df, top_n=3):
    """ワイドBOX ROI計算（Top N馬のBOX購入）
    
    ワイドオッズは払い戻しデータがないため、単勝オッズから推定
    """
    results = []
    
    for race_id, group in pred_df.groupby('race_id'):
        if len(group) < top_n:
            continue
        
        top_horses = group[group['pred_rank'] <= top_n].sort_values('pred_rank')
        if len(top_horses) < top_n:
            continue
        
        # ワイドBOX: C(n, 2)点購入
        from math import comb
        num_bets = comb(top_n, 2)
        total_bet = num_bets * 100
        
        # 的中判定: 選んだ馬のうち2頭以上が3着以内
        top_horses_in_top3 = top_horses[top_horses['finish_position'] <= 3]
        num_in_top3 = len(top_horses_in_top3)
        
        if num_in_top3 >= 2:
            # 的中した組み合わせ数
            num_hits = comb(num_in_top3, 2)
            
            # ワイドオッズを推定（単勝オッズの積 × 0.15）
            avg_odds = top_horses_in_top3['win_odds'].mean()
            wide_odds_est = avg_odds * avg_odds * 0.15
            wide_odds_est = max(wide_odds_est, 1.1)
            
            total_return = num_hits * wide_odds_est * 100
        else:
            total_return = 0
            num_hits = 0
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'num_hits': num_hits
        })
    
    results_df = pd.DataFrame(results)
    
    return {
        'total_bet': results_df['total_bet'].sum(),
        'total_return': results_df['total_return'].sum(),
        'roi': results_df['total_return'].sum() / results_df['total_bet'].sum() * 100 if len(results_df) > 0 else 0,
        'hit_rate': (results_df['num_hits'] > 0).sum() / len(results_df) * 100 if len(results_df) > 0 else 0
    }


def run_betting_strategy_analysis():
    """馬券戦略分析実行"""
    from keibaai.src.features.leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17
    
    races, pedigrees, corners_df, race_details, returns = load_all_data()
    
    logger.info(f"Total races: {len(races):,}")
    
    # 2024年のデータで検証
    year = 2024
    train_start = f"{year - 8}-01-01"
    train_end = f"{year - 1}-12-31"
    test_start = f"{year}-01-01"
    test_end = f"{year}-12-31"
    
    train_df = races[(races['race_date'] >= train_start) & (races['race_date'] <= train_end)].copy()
    test_df = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
    
    logger.info(f"Train: {len(train_df):,}, Test: {len(test_df):,}")
    
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
    logger.info(f"\n{'='*60}")
    logger.info(f"馬券戦略別ROI分析 ({year}年)")
    logger.info(f"{'='*60}")
    
    results = []
    
    # 単勝戦略
    for top_n in [1, 2, 3]:
        roi = calc_win_roi(test_feat, top_n)
        results.append({
            'strategy': f'単勝 Top{top_n}',
            'total_bet': roi['total_bet'],
            'total_return': roi['total_return'],
            'roi': roi['roi'],
            'hit_rate': roi['hit_rate']
        })
        logger.info(f"単勝 Top{top_n}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%")
    
    # 複勝戦略
    for top_n in [1, 2, 3]:
        roi = calc_place_roi(test_feat, top_n)
        results.append({
            'strategy': f'複勝 Top{top_n}',
            'total_bet': roi['total_bet'],
            'total_return': roi['total_return'],
            'roi': roi['roi'],
            'hit_rate': roi['hit_rate']
        })
        logger.info(f"複勝 Top{top_n}: ROI {roi['roi']:.1f}% (推定), 的中率 {roi['hit_rate']:.1f}%")
    
    # 馬連BOX
    roi = calc_exacta_roi(test_feat)
    results.append({
        'strategy': '馬連BOX Top2',
        'total_bet': roi['total_bet'],
        'total_return': roi['total_return'],
        'roi': roi['roi'],
        'hit_rate': roi['hit_rate']
    })
    logger.info(f"馬連BOX Top2: ROI {roi['roi']:.1f}% (推定), 的中率 {roi['hit_rate']:.1f}%")
    
    # ワイドBOX
    for top_n in [2, 3]:
        roi = calc_wide_box_roi(test_feat, top_n)
        results.append({
            'strategy': f'ワイドBOX Top{top_n}',
            'total_bet': roi['total_bet'],
            'total_return': roi['total_return'],
            'roi': roi['roi'],
            'hit_rate': roi['hit_rate']
        })
        logger.info(f"ワイドBOX Top{top_n}: ROI {roi['roi']:.1f}% (推定), 的中率 {roi['hit_rate']:.1f}%")
    
    # 結果表示
    logger.info(f"\n{'='*60}")
    logger.info("結果サマリー")
    logger.info(f"{'='*60}")
    
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('roi', ascending=False)
    print(results_df.to_string(index=False))
    
    # 最良戦略
    best = results_df.iloc[0]
    logger.info(f"\n最良戦略: {best['strategy']} (ROI: {best['roi']:.1f}%)")


if __name__ == "__main__":
    run_betting_strategy_analysis()

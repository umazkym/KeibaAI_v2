#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ワイド・馬連 5年間Walk-forwardバックテスト

【目的】
1. ワイドのデータ形式を修正して正しく計算
2. 馬連 stable Top1-Top2 戦略の5年間再現性を検証
3. リーク・過学習リスクを評価

作成日: 2026-01-11
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from itertools import combinations
import logging
import warnings

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def load_base_data():
    """ベースデータ読み込み"""
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 新馬・障害除外
    new_horse_mask = races['race_name'].str.contains('新馬', na=False)
    obstacle_mask = (
        races['track_surface'].str.contains('障', na=False) |
        races['race_name'].str.contains('障害', na=False)
    )
    races = races[~(new_horse_mask | obstacle_mask)].copy()
    
    return races, pedigrees, corners, race_details, horses, returns


def process_wide_returns(returns_df):
    """ワイド払戻データを正しい形式に変換"""
    wide = returns_df[returns_df['bet_type'] == 'wide'].copy()
    
    # horse_1とhorse_2から馬番ペアを作成
    wide['pair'] = wide.apply(
        lambda x: tuple(sorted([int(x['horse_1']), int(x['horse_2'])])) 
        if pd.notna(x['horse_1']) and pd.notna(x['horse_2']) else None, 
        axis=1
    )
    wide = wide[wide['pair'].notna()]
    
    return wide


def process_umaren_returns(returns_df):
    """馬連払戻データを処理"""
    umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    
    # horse_1とhorse_2から馬番ペアを作成
    umaren['pair'] = umaren.apply(
        lambda x: tuple(sorted([int(x['horse_1']), int(x['horse_2'])])) 
        if pd.notna(x['horse_1']) and pd.notna(x['horse_2']) else None, 
        axis=1
    )
    umaren = umaren[umaren['pair'].notna()]
    
    return umaren


def run_single_year(races, pedigrees, corners, race_details, horses, returns, test_year):
    """1年分の予測・計算を実行"""
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    from keibaai.src.models.multi_target_predictor import MultiTargetPredictor
    from keibaai.src.features.time_margin_features import TimeMarginFeatureEngineer
    
    test_start = f'{test_year}-01-01'
    test_end = f'{test_year}-12-31'
    test_start_dt = pd.to_datetime(test_start)
    test_end_dt = pd.to_datetime(test_end)
    train_end = test_start_dt - timedelta(days=1)
    valid_start = train_end - timedelta(days=365)
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
    
    if len(test) < 5000:
        return None
    
    # 特徴量エンジン
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    all_data = pd.concat([train, test], ignore_index=True)
    all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
    all_data_f = engine.transform(all_data)
    
    train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
    valid_f = train_f[(train_f['race_date'] > valid_start)].copy()
    test_f = all_data_f[(all_data_f['race_date'] >= test_start_dt) & (all_data_f['race_date'] <= test_end_dt)].copy()
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # タイム差特徴量
    margin_engineer = TimeMarginFeatureEngineer()
    margin_engineer.fit(train)
    test_f = margin_engineer.transform(test_f)
    
    # モデル学習
    predictor = MultiTargetPredictor(
        surface_specific=True,
        use_v44_residual=True,
        regularization_level='strong',
        use_early_stopping=False,
        fixed_iterations=50
    )
    predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
    
    # 予測
    test_preds = predictor.predict(test_f)
    
    # 実績データをマージ
    test_preds = test_preds.merge(
        test_f[['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    test_preds['year'] = test_year
    
    # 荒れ度分類
    test_preds['vol_type'] = np.where(
        test_preds['race_place_prob_std'] < 0.08, 'volatile',
        np.where(test_preds['race_place_prob_std'] < 0.20, 'moderate', 'stable')
    )
    
    # ランキング
    test_preds['rank_adj'] = test_preds.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    
    return test_preds


def calc_wide_umaren_roi(preds_df, wide_df, umaren_df, vol_filter=None):
    """ワイド・馬連のROIを計算"""
    df = preds_df.copy()
    
    if vol_filter:
        df = df[df['vol_type'] == vol_filter]
    
    results = {
        'wide_1_2': {'bets': 0, 'hits': 0, 'return': 0},
        'wide_1_23': {'bets': 0, 'hits': 0, 'return': 0},
        'umaren_1_2': {'bets': 0, 'hits': 0, 'return': 0},
        'umaren_1_23': {'bets': 0, 'hits': 0, 'return': 0},
    }
    
    for race_id in df['race_id'].unique():
        race_df = df[df['race_id'] == race_id]
        
        # 予測Top1, Top2, Top3
        top1 = race_df[race_df['rank_adj'] == 1]['horse_number'].values
        top2 = race_df[race_df['rank_adj'] == 2]['horse_number'].values
        top3 = race_df[race_df['rank_adj'] == 3]['horse_number'].values
        
        if len(top1) == 0 or len(top2) == 0:
            continue
        
        top1, top2 = int(top1[0]), int(top2[0])
        top3 = int(top3[0]) if len(top3) > 0 else None
        
        # 実際の着順
        actual_top2 = race_df[race_df['finish_position'] <= 2]['horse_number'].astype(int).values
        actual_top3 = race_df[race_df['finish_position'] <= 3]['horse_number'].astype(int).values
        
        # ワイド払戻を取得
        race_wide = wide_df[wide_df['race_id'] == race_id]
        # 馬連払戻を取得
        race_umaren = umaren_df[umaren_df['race_id'] == race_id]
        
        # === ワイド Top1-Top2 ===
        results['wide_1_2']['bets'] += 1
        pair_12 = tuple(sorted([top1, top2]))
        if top1 in actual_top3 and top2 in actual_top3:
            # ワイド的中
            match = race_wide[race_wide['pair'] == pair_12]
            if len(match) > 0:
                results['wide_1_2']['hits'] += 1
                results['wide_1_2']['return'] += match.iloc[0]['payout'] / 100
        
        # === ワイド Top1-Top2,3 (2点買い) ===
        if top3:
            pairs = [(top1, top2), (top1, top3)]
            for h1, h2 in pairs:
                results['wide_1_23']['bets'] += 1
                pair = tuple(sorted([h1, h2]))
                if h1 in actual_top3 and h2 in actual_top3:
                    match = race_wide[race_wide['pair'] == pair]
                    if len(match) > 0:
                        results['wide_1_23']['hits'] += 1
                        results['wide_1_23']['return'] += match.iloc[0]['payout'] / 100
        
        # === 馬連 Top1-Top2 ===
        results['umaren_1_2']['bets'] += 1
        if len(actual_top2) >= 2:
            actual_pair = tuple(sorted(actual_top2[:2]))
            if pair_12 == actual_pair:
                if len(race_umaren) > 0:
                    results['umaren_1_2']['hits'] += 1
                    results['umaren_1_2']['return'] += race_umaren.iloc[0]['payout'] / 100
        
        # === 馬連 Top1-Top2,3 (2点買い) ===
        if top3:
            pairs = [tuple(sorted([top1, top2])), tuple(sorted([top1, top3]))]
            for pair in pairs:
                results['umaren_1_23']['bets'] += 1
                if len(actual_top2) >= 2:
                    actual_pair = tuple(sorted(actual_top2[:2]))
                    if pair == actual_pair:
                        if len(race_umaren) > 0:
                            results['umaren_1_23']['hits'] += 1
                            results['umaren_1_23']['return'] += race_umaren.iloc[0]['payout'] / 100
    
    # ROI計算
    for key in results:
        if results[key]['bets'] > 0:
            results[key]['roi'] = results[key]['return'] / results[key]['bets'] * 100
            results[key]['hit_rate'] = results[key]['hits'] / results[key]['bets'] * 100
        else:
            results[key]['roi'] = 0
            results[key]['hit_rate'] = 0
    
    return results


def main():
    logger.info("="*70)
    logger.info("ワイド・馬連 5年間Walk-forwardバックテスト")
    logger.info("="*70)
    
    # データ読み込み
    races, pedigrees, corners, race_details, horses, returns = load_base_data()
    logger.info(f"データ件数: {len(races):,}")
    
    # 払戻データを処理
    wide_df = process_wide_returns(returns)
    umaren_df = process_umaren_returns(returns)
    logger.info(f"ワイド払戻: {len(wide_df):,}件")
    logger.info(f"馬連払戻: {len(umaren_df):,}件")
    
    # 年別検証
    test_years = [2020, 2021, 2022, 2023, 2024]
    all_results = []
    
    for year in test_years:
        logger.info(f"\n{'='*60}")
        logger.info(f"[{year}年] 検証")
        logger.info(f"{'='*60}")
        
        preds = run_single_year(races, pedigrees, corners, race_details, horses, returns, year)
        if preds is None:
            continue
        
        # 全体のROI
        all_results_year = calc_wide_umaren_roi(preds, wide_df, umaren_df, vol_filter=None)
        # stableのROI
        stable_results = calc_wide_umaren_roi(preds, wide_df, umaren_df, vol_filter='stable')
        # moderateのROI
        moderate_results = calc_wide_umaren_roi(preds, wide_df, umaren_df, vol_filter='moderate')
        
        logger.info(f"\n【全体】")
        logger.info(f"  ワイド Top1-Top2: ROI {all_results_year['wide_1_2']['roi']:.1f}%, 的中率 {all_results_year['wide_1_2']['hit_rate']:.1f}% (n={all_results_year['wide_1_2']['bets']})")
        logger.info(f"  馬連 Top1-Top2:   ROI {all_results_year['umaren_1_2']['roi']:.1f}%, 的中率 {all_results_year['umaren_1_2']['hit_rate']:.1f}% (n={all_results_year['umaren_1_2']['bets']})")
        
        logger.info(f"\n【stableレース】")
        logger.info(f"  ワイド Top1-Top2: ROI {stable_results['wide_1_2']['roi']:.1f}%, 的中率 {stable_results['wide_1_2']['hit_rate']:.1f}% (n={stable_results['wide_1_2']['bets']})")
        logger.info(f"  馬連 Top1-Top2:   ROI {stable_results['umaren_1_2']['roi']:.1f}%, 的中率 {stable_results['umaren_1_2']['hit_rate']:.1f}% (n={stable_results['umaren_1_2']['bets']})")
        
        logger.info(f"\n【moderateレース】")
        logger.info(f"  ワイド Top1-Top2: ROI {moderate_results['wide_1_2']['roi']:.1f}%, 的中率 {moderate_results['wide_1_2']['hit_rate']:.1f}% (n={moderate_results['wide_1_2']['bets']})")
        logger.info(f"  馬連 Top1-Top2:   ROI {moderate_results['umaren_1_2']['roi']:.1f}%, 的中率 {moderate_results['umaren_1_2']['hit_rate']:.1f}% (n={moderate_results['umaren_1_2']['bets']})")
        
        all_results.append({
            'year': year,
            'wide_all_roi': all_results_year['wide_1_2']['roi'],
            'wide_all_bets': all_results_year['wide_1_2']['bets'],
            'umaren_all_roi': all_results_year['umaren_1_2']['roi'],
            'umaren_all_bets': all_results_year['umaren_1_2']['bets'],
            'wide_stable_roi': stable_results['wide_1_2']['roi'],
            'wide_stable_bets': stable_results['wide_1_2']['bets'],
            'umaren_stable_roi': stable_results['umaren_1_2']['roi'],
            'umaren_stable_bets': stable_results['umaren_1_2']['bets'],
            'wide_moderate_roi': moderate_results['wide_1_2']['roi'],
            'umaren_moderate_roi': moderate_results['umaren_1_2']['roi'],
        })
    
    # サマリー
    results_df = pd.DataFrame(all_results)
    
    logger.info("\n" + "="*70)
    logger.info("5年間サマリー")
    logger.info("="*70)
    
    logger.info("\n【ワイド Top1-Top2】")
    logger.info(f"  全体 平均ROI: {results_df['wide_all_roi'].mean():.1f}%")
    logger.info(f"  stable 平均ROI: {results_df['wide_stable_roi'].mean():.1f}%")
    logger.info(f"  moderate 平均ROI: {results_df['wide_moderate_roi'].mean():.1f}%")
    
    logger.info("\n【馬連 Top1-Top2】")
    logger.info(f"  全体 平均ROI: {results_df['umaren_all_roi'].mean():.1f}%")
    logger.info(f"  stable 平均ROI: {results_df['umaren_stable_roi'].mean():.1f}%")
    logger.info(f"  moderate 平均ROI: {results_df['umaren_moderate_roi'].mean():.1f}%")
    
    logger.info("\n【年別推移】")
    logger.info(f"{'年':>6} | {'ワイドstable':>12} | {'馬連stable':>12} | {'stable件数':>10}")
    logger.info("-" * 55)
    for _, row in results_df.iterrows():
        logger.info(f"{int(row['year']):>6} | {row['wide_stable_roi']:>11.1f}% | {row['umaren_stable_roi']:>11.1f}% | {int(row['wide_stable_bets']):>10}")
    
    # 標準偏差（再現性指標）
    wide_std = results_df['wide_stable_roi'].std()
    umaren_std = results_df['umaren_stable_roi'].std()
    
    logger.info(f"\n【再現性評価】")
    logger.info(f"  ワイドstable ROI標準偏差: {wide_std:.1f}pt")
    logger.info(f"  馬連stable ROI標準偏差: {umaren_std:.1f}pt")
    
    if umaren_std < 30:
        logger.info("  → 馬連stable戦略は比較的再現性あり ✅")
    else:
        logger.info("  → 馬連stable戦略は変動が大きい ⚠️")
    
    # 結果保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(output_dir / "wide_umaren_5year_backtest.csv", index=False, encoding='utf-8-sig')
    
    logger.info("\n結果保存完了")


if __name__ == "__main__":
    main()

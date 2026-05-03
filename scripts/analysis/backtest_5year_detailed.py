#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
5年間バックテスト + 的中レース詳細ログ + 連続値閾値最適化

【目的】
1. 2020-2024年で全組み合わせをバックテスト（再現性検証）
2. 的中レースの詳細を記録（外れ値・ラッキー的中の検出）
3. place_prob_std連続値での最適閾値を探索

作成日: 2026-01-11
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import warnings
import json

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
    
    # ワイド・馬連払戻処理
    wide = returns[returns['bet_type'] == 'wide'].copy()
    wide['pair'] = wide.apply(
        lambda x: tuple(sorted([int(x['horse_1']), int(x['horse_2'])])) 
        if pd.notna(x['horse_1']) and pd.notna(x['horse_2']) else None, 
        axis=1
    )
    wide = wide[wide['pair'].notna()]
    
    umaren = returns[returns['bet_type'] == 'umaren'].copy()
    umaren['pair'] = umaren.apply(
        lambda x: tuple(sorted([int(x['horse_1']), int(x['horse_2'])])) 
        if pd.notna(x['horse_1']) and pd.notna(x['horse_2']) else None, 
        axis=1
    )
    umaren = umaren[umaren['pair'].notna()]
    
    return races, pedigrees, corners, race_details, horses, wide, umaren


def run_single_year_prediction(races, pedigrees, corners, race_details, horses, test_year):
    """1年分の予測を実行"""
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
        test_f[['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity', 
                'race_date', 'venue', 'race_name']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    test_preds['year'] = test_year
    test_preds['rank'] = test_preds.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    
    return test_preds


def calc_umaren_roi_with_hits(preds_df, umaren_df, axis_ranks, partner_ranks, std_min, std_max):
    """馬連ROIを計算し、的中レースの詳細を返す"""
    df = preds_df.copy()
    
    # place_prob_stdでフィルタ
    df = df[(df['race_place_prob_std'] >= std_min) & (df['race_place_prob_std'] < std_max)]
    
    total_bet = 0
    total_return = 0
    hits = 0
    hit_details = []
    
    for race_id in df['race_id'].unique():
        race_df = df[df['race_id'] == race_id]
        
        # 実際の1-2着
        actual_top2 = race_df[race_df['finish_position'] <= 2]['horse_number'].astype(int).values
        if len(actual_top2) < 2:
            continue
        actual_pair = tuple(sorted(actual_top2[:2]))
        
        # 馬連払戻
        race_umaren = umaren_df[umaren_df['race_id'] == race_id]
        
        # 軸馬と相手馬
        axis_horses = race_df[race_df['rank'].isin(axis_ranks)]['horse_number'].astype(int).values
        partner_horses = race_df[race_df['rank'].isin(partner_ranks)]['horse_number'].astype(int).values
        
        bet_pairs = set()
        for a in axis_horses:
            for p in partner_horses:
                if a != p:
                    pair = tuple(sorted([a, p]))
                    bet_pairs.add(pair)
        
        for pair in bet_pairs:
            total_bet += 1
            if pair == actual_pair:
                if len(race_umaren) > 0:
                    payout = race_umaren.iloc[0]['payout'] / 100
                    hits += 1
                    total_return += payout
                    
                    # 的中詳細を記録
                    meta = race_df.iloc[0]
                    hit_details.append({
                        'race_id': race_id,
                        'race_date': str(meta['race_date'])[:10],
                        'venue': meta['venue'],
                        'race_name': str(meta['race_name'])[:20],
                        'pair': f"{pair[0]}-{pair[1]}",
                        'payout': payout,
                        'place_prob_std': meta['race_place_prob_std'],
                        'year': meta['year'],
                    })
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hits / total_bet * 100 if total_bet > 0 else 0
    
    return {
        'roi': roi,
        'hit_rate': hit_rate,
        'bets': total_bet,
        'hits': hits,
        'total_return': total_return,
        'hit_details': hit_details,
    }


def analyze_continuous_threshold(all_preds, umaren_df):
    """連続値でplace_prob_stdの最適閾値を探索"""
    logger.info("\n" + "="*70)
    logger.info("【連続値閾値最適化】place_prob_std")
    logger.info("="*70)
    
    # 閾値の候補を細かく設定
    thresholds = np.arange(0.05, 0.35, 0.02)
    
    results = []
    
    for th in thresholds:
        # th以上のレースで馬連Top1-Top2
        result = calc_umaren_roi_with_hits(all_preds, umaren_df, [1], [2], th, 1.0)
        
        results.append({
            'threshold': th,
            'threshold_type': f'>= {th:.2f}',
            'roi': result['roi'],
            'hit_rate': result['hit_rate'],
            'bets': result['bets'],
            'hits': result['hits'],
        })
    
    results_df = pd.DataFrame(results)
    
    logger.info("\n【馬連 Top1-Top2】place_prob_std >= X の ROI")
    logger.info("-" * 55)
    logger.info(f"{'閾値':>10} | {'ROI':>8} | {'的中率':>8} | {'件数':>8}")
    logger.info("-" * 55)
    
    for _, row in results_df.iterrows():
        logger.info(f"{row['threshold']:.2f}以上 | {row['roi']:7.1f}% | {row['hit_rate']:7.1f}% | {int(row['bets']):8}")
    
    # 最適閾値を特定
    best_row = results_df.loc[results_df['roi'].idxmax()]
    logger.info(f"\n最適閾値: {best_row['threshold']:.2f} (ROI {best_row['roi']:.1f}%)")
    
    return results_df


def main():
    logger.info("="*70)
    logger.info("5年間バックテスト + 的中詳細ログ + 連続値閾値最適化")
    logger.info("="*70)
    
    # データ読み込み
    races, pedigrees, corners, race_details, horses, wide_df, umaren_df = load_base_data()
    logger.info(f"データ件数: {len(races):,}")
    
    # 5年分の予測を実行
    test_years = [2020, 2021, 2022, 2023, 2024]
    all_preds_list = []
    
    for year in test_years:
        logger.info(f"\n[{year}年] 予測実行中...")
        preds = run_single_year_prediction(races, pedigrees, corners, race_details, horses, year)
        if preds is not None:
            all_preds_list.append(preds)
    
    all_preds = pd.concat(all_preds_list, ignore_index=True)
    logger.info(f"\n全予測データ: {len(all_preds):,}件")
    
    # === 主要戦略の5年間バックテスト ===
    logger.info("\n" + "="*70)
    logger.info("【主要戦略 5年間バックテスト】")
    logger.info("="*70)
    
    strategies = [
        ("馬連 Top1-Top2 (stable: std>=0.20)", [1], [2], 0.20, 1.0),
        ("馬連 Top3-Top4 (moderate: 0.08-0.20)", [3], [4], 0.08, 0.20),
        ("馬連 Top1-Top2,3 (stable)", [1], [2, 3], 0.20, 1.0),
        ("馬連 Top3-Top4,5 (moderate)", [3], [4, 5], 0.08, 0.20),
    ]
    
    all_hit_details = []
    yearly_results = []
    
    for name, axis, partner, std_min, std_max in strategies:
        logger.info(f"\n■ {name}")
        
        # 年別ROI
        for year in test_years:
            year_preds = all_preds[all_preds['year'] == year]
            result = calc_umaren_roi_with_hits(year_preds, umaren_df, axis, partner, std_min, std_max)
            
            yearly_results.append({
                'strategy': name,
                'year': year,
                'roi': result['roi'],
                'hit_rate': result['hit_rate'],
                'bets': result['bets'],
                'hits': result['hits'],
            })
            
            # 的中詳細を保存
            for hit in result['hit_details']:
                hit['strategy'] = name
                all_hit_details.append(hit)
            
            logger.info(f"  {year}年: ROI {result['roi']:.1f}% (的中{result['hits']}/{result['bets']}件)")
        
        # 全体
        result_all = calc_umaren_roi_with_hits(all_preds, umaren_df, axis, partner, std_min, std_max)
        logger.info(f"  5年平均: ROI {result_all['roi']:.1f}%")
        
        # 標準偏差（再現性指標）
        year_rois = [r['roi'] for r in yearly_results if r['strategy'] == name]
        roi_std = np.std(year_rois)
        logger.info(f"  ROI標準偏差: {roi_std:.1f}pt")
    
    # === 的中レース詳細分析 ===
    logger.info("\n" + "="*70)
    logger.info("【的中レース詳細分析】")
    logger.info("="*70)
    
    hits_df = pd.DataFrame(all_hit_details)
    
    # 馬連 Top1-Top2 stableの的中を分析
    stable_hits = hits_df[hits_df['strategy'].str.contains('Top1-Top2') & 
                          hits_df['strategy'].str.contains('stable')]
    
    if len(stable_hits) > 0:
        logger.info(f"\n■ 馬連 Top1-Top2 (stable) 的中詳細: {len(stable_hits)}件")
        
        # 高配当的中（外れ値候補）
        high_payout = stable_hits[stable_hits['payout'] >= 30]
        logger.info(f"\n  30倍以上の高配当的中: {len(high_payout)}件")
        for _, row in high_payout.iterrows():
            logger.info(f"    {row['race_date']} {row['venue']} {row['race_name']}: {row['payout']:.1f}倍")
        
        # 低配当的中（堅実）
        low_payout = stable_hits[stable_hits['payout'] < 10]
        logger.info(f"\n  10倍未満の低配当的中: {len(low_payout)}件")
        
        # 年別分布
        logger.info(f"\n  年別的中件数:")
        for year in test_years:
            year_hits = stable_hits[stable_hits['year'] == year]
            if len(year_hits) > 0:
                avg_payout = year_hits['payout'].mean()
                logger.info(f"    {year}年: {len(year_hits)}件 (平均配当{avg_payout:.1f}倍)")
    
    # === 連続値閾値最適化 ===
    threshold_results = analyze_continuous_threshold(all_preds, umaren_df)
    
    # === 結果保存 ===
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 年別結果
    yearly_df = pd.DataFrame(yearly_results)
    yearly_df.to_csv(output_dir / "5year_backtest_yearly.csv", index=False, encoding='utf-8-sig')
    
    # 的中詳細
    hits_df.to_csv(output_dir / "5year_hit_details.csv", index=False, encoding='utf-8-sig')
    
    # 閾値最適化結果
    threshold_results.to_csv(output_dir / "threshold_optimization.csv", index=False, encoding='utf-8-sig')
    
    logger.info("\n" + "="*70)
    logger.info("結果保存完了")
    logger.info("="*70)
    logger.info(f"  - 5year_backtest_yearly.csv")
    logger.info(f"  - 5year_hit_details.csv")
    logger.info(f"  - threshold_optimization.csv")


if __name__ == "__main__":
    main()

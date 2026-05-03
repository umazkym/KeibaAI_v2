#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【完全版】5年間バックテスト - 単勝・ワイド・馬連 全組み合わせ

【目的】
1. Top1-5の全組み合わせを5年間でバックテスト
2. 各的中の詳細ログを記録
3. 外れ値（ラッキー的中・爆穴）を検出
4. 連続値でplace_prob_stdの最適閾値を探索

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
    """データ読み込み"""
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    new_horse_mask = races['race_name'].str.contains('新馬', na=False)
    obstacle_mask = (
        races['track_surface'].str.contains('障', na=False) |
        races['race_name'].str.contains('障害', na=False)
    )
    races = races[~(new_horse_mask | obstacle_mask)].copy()
    
    # ワイド払戻
    wide = returns[returns['bet_type'] == 'wide'].copy()
    wide['pair'] = wide.apply(
        lambda x: tuple(sorted([int(x['horse_1']), int(x['horse_2'])])) 
        if pd.notna(x['horse_1']) and pd.notna(x['horse_2']) else None, 
        axis=1
    )
    wide = wide[wide['pair'].notna()]
    
    # 馬連払戻
    umaren = returns[returns['bet_type'] == 'umaren'].copy()
    umaren['pair'] = umaren.apply(
        lambda x: tuple(sorted([int(x['horse_1']), int(x['horse_2'])])) 
        if pd.notna(x['horse_1']) and pd.notna(x['horse_2']) else None, 
        axis=1
    )
    umaren = umaren[umaren['pair'].notna()]
    
    return races, pedigrees, corners, race_details, horses, wide, umaren


def run_year_prediction(races, pedigrees, corners, race_details, horses, test_year):
    """1年分の予測"""
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    from keibaai.src.models.multi_target_predictor import MultiTargetPredictor
    from keibaai.src.features.time_margin_features import TimeMarginFeatureEngineer
    
    test_start_dt = pd.to_datetime(f'{test_year}-01-01')
    test_end_dt = pd.to_datetime(f'{test_year}-12-31')
    train_end = test_start_dt - timedelta(days=1)
    valid_start = train_end - timedelta(days=365)
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
    
    if len(test) < 5000:
        return None
    
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    all_data = pd.concat([train, test], ignore_index=True)
    all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
    all_data_f = engine.transform(all_data)
    
    train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
    valid_f = train_f[(train_f['race_date'] > valid_start)].copy()
    test_f = all_data_f[(all_data_f['race_date'] >= test_start_dt) & (all_data_f['race_date'] <= test_end_dt)].copy()
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    margin_engineer = TimeMarginFeatureEngineer()
    margin_engineer.fit(train)
    test_f = margin_engineer.transform(test_f)
    
    predictor = MultiTargetPredictor(
        surface_specific=True,
        use_v44_residual=True,
        regularization_level='strong',
        use_early_stopping=False,
        fixed_iterations=50
    )
    predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
    
    test_preds = predictor.predict(test_f)
    
    test_preds = test_preds.merge(
        test_f[['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity',
                'race_date', 'venue', 'race_name']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    test_preds['year'] = test_year
    test_preds['rank'] = test_preds.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    
    return test_preds


def generate_patterns():
    """全パターン生成"""
    # 単勝パターン
    tansho_patterns = []
    for size in range(1, 6):
        for start in range(1, 6 - size + 1):
            ranks = list(range(start, start + size))
            name = f"Top{'-'.join(map(str, ranks))}" if size > 1 else f"Top{start}"
            tansho_patterns.append((name, ranks))
    
    # 連勝パターン
    rentan_patterns = []
    
    # 軸-相手パターン
    for axis in range(1, 5):
        for partner_size in range(1, 6 - axis):
            partners = list(range(axis + 1, axis + 1 + partner_size))
            if partners and max(partners) <= 5:
                name = f"Top{axis}-Top{','.join(map(str, partners))}"
                rentan_patterns.append((name, [axis], partners))
    
    # 複数軸パターン
    for axis_size in range(2, 4):
        for axis_start in range(1, 6 - axis_size):
            axes = list(range(axis_start, axis_start + axis_size))
            for partner_size in range(1, 6 - max(axes)):
                partners = list(range(max(axes) + 1, max(axes) + 1 + partner_size))
                if partners and max(partners) <= 5:
                    name = f"Top{','.join(map(str, axes))}-Top{','.join(map(str, partners))}"
                    rentan_patterns.append((name, axes, partners))
    
    # BOXパターン
    for size in range(2, 6):
        for start in range(1, 6 - size + 1):
            ranks = list(range(start, start + size))
            if max(ranks) <= 5:
                name = f"Top{'-'.join(map(str, ranks))}BOX"
                rentan_patterns.append((name, ranks, ranks))
    
    return tansho_patterns, rentan_patterns


def calc_tansho(preds_df, target_ranks, std_min, std_max):
    """単勝ROI計算"""
    df = preds_df.copy()
    df = df[(df['race_place_prob_std'] >= std_min) & (df['race_place_prob_std'] < std_max)]
    
    targets = df[df['rank'].isin(target_ranks)]
    hits = targets[targets['finish_position'] == 1]
    
    total_bet = len(targets)
    total_return = hits['win_odds'].sum()
    
    hit_details = []
    for _, row in hits.iterrows():
        hit_details.append({
            'bet_type': 'tansho',
            'race_id': row['race_id'],
            'race_date': str(row['race_date'])[:10],
            'venue': row['venue'],
            'race_name': str(row['race_name'])[:20],
            'payout': row['win_odds'],
            'year': row['year'],
            'std': row['race_place_prob_std'],
        })
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    return {'roi': roi, 'bets': total_bet, 'hits': len(hits), 'hit_details': hit_details}


def calc_wide(preds_df, wide_df, axis_ranks, partner_ranks, std_min, std_max):
    """ワイドROI計算"""
    df = preds_df.copy()
    df = df[(df['race_place_prob_std'] >= std_min) & (df['race_place_prob_std'] < std_max)]
    
    total_bet = 0
    total_return = 0
    hits = 0
    hit_details = []
    
    for race_id in df['race_id'].unique():
        race_df = df[df['race_id'] == race_id]
        actual_top3 = set(race_df[race_df['finish_position'] <= 3]['horse_number'].astype(int).values)
        race_wide = wide_df[wide_df['race_id'] == race_id]
        
        axis_horses = race_df[race_df['rank'].isin(axis_ranks)]['horse_number'].astype(int).values
        partner_horses = race_df[race_df['rank'].isin(partner_ranks)]['horse_number'].astype(int).values
        
        bet_pairs = set()
        for a in axis_horses:
            for p in partner_horses:
                if a != p:
                    bet_pairs.add(tuple(sorted([a, p])))
        
        for pair in bet_pairs:
            total_bet += 1
            h1, h2 = pair
            if h1 in actual_top3 and h2 in actual_top3:
                match = race_wide[race_wide['pair'] == pair]
                if len(match) > 0:
                    payout = match.iloc[0]['payout'] / 100
                    hits += 1
                    total_return += payout
                    
                    meta = race_df.iloc[0]
                    hit_details.append({
                        'bet_type': 'wide',
                        'race_id': race_id,
                        'race_date': str(meta['race_date'])[:10],
                        'venue': meta['venue'],
                        'race_name': str(meta['race_name'])[:20],
                        'pair': f"{pair[0]}-{pair[1]}",
                        'payout': payout,
                        'year': meta['year'],
                        'std': meta['race_place_prob_std'],
                    })
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    return {'roi': roi, 'bets': total_bet, 'hits': hits, 'hit_details': hit_details}


def calc_umaren(preds_df, umaren_df, axis_ranks, partner_ranks, std_min, std_max):
    """馬連ROI計算"""
    df = preds_df.copy()
    df = df[(df['race_place_prob_std'] >= std_min) & (df['race_place_prob_std'] < std_max)]
    
    total_bet = 0
    total_return = 0
    hits = 0
    hit_details = []
    
    for race_id in df['race_id'].unique():
        race_df = df[df['race_id'] == race_id]
        actual_top2 = race_df[race_df['finish_position'] <= 2]['horse_number'].astype(int).values
        if len(actual_top2) < 2:
            continue
        actual_pair = tuple(sorted(actual_top2[:2]))
        
        race_umaren = umaren_df[umaren_df['race_id'] == race_id]
        
        axis_horses = race_df[race_df['rank'].isin(axis_ranks)]['horse_number'].astype(int).values
        partner_horses = race_df[race_df['rank'].isin(partner_ranks)]['horse_number'].astype(int).values
        
        bet_pairs = set()
        for a in axis_horses:
            for p in partner_horses:
                if a != p:
                    bet_pairs.add(tuple(sorted([a, p])))
        
        for pair in bet_pairs:
            total_bet += 1
            if pair == actual_pair:
                if len(race_umaren) > 0:
                    payout = race_umaren.iloc[0]['payout'] / 100
                    hits += 1
                    total_return += payout
                    
                    meta = race_df.iloc[0]
                    hit_details.append({
                        'bet_type': 'umaren',
                        'race_id': race_id,
                        'race_date': str(meta['race_date'])[:10],
                        'venue': meta['venue'],
                        'race_name': str(meta['race_name'])[:20],
                        'pair': f"{pair[0]}-{pair[1]}",
                        'payout': payout,
                        'year': meta['year'],
                        'std': meta['race_place_prob_std'],
                    })
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    return {'roi': roi, 'bets': total_bet, 'hits': hits, 'hit_details': hit_details}


def main():
    logger.info("="*70)
    logger.info("【完全版】5年間バックテスト - 全組み合わせ")
    logger.info("="*70)
    
    # データ読み込み
    races, pedigrees, corners, race_details, horses, wide_df, umaren_df = load_base_data()
    logger.info(f"データ件数: {len(races):,}")
    
    # 5年分の予測
    test_years = [2020, 2021, 2022, 2023, 2024]
    all_preds_list = []
    
    for year in test_years:
        logger.info(f"\n[{year}年] 予測中...")
        preds = run_year_prediction(races, pedigrees, corners, race_details, horses, year)
        if preds is not None:
            all_preds_list.append(preds)
    
    all_preds = pd.concat(all_preds_list, ignore_index=True)
    logger.info(f"\n全予測: {len(all_preds):,}件")
    
    # パターン生成
    tansho_patterns, rentan_patterns = generate_patterns()
    logger.info(f"単勝パターン: {len(tansho_patterns)}")
    logger.info(f"連勝パターン: {len(rentan_patterns)}")
    
    # 条件
    conditions = [
        ('all', 0.0, 1.0),
        ('stable', 0.20, 1.0),
        ('moderate', 0.08, 0.20),
        ('std>=0.09', 0.09, 1.0),  # 最適化候補
        ('std>=0.15', 0.15, 1.0),
    ]
    
    all_results = []
    all_hits = []
    
    # === 単勝 ===
    logger.info("\n" + "="*70)
    logger.info("【単勝】全パターン × 5年間")
    logger.info("="*70)
    
    for name, ranks in tansho_patterns:
        for cond_name, std_min, std_max in conditions:
            yearly_rois = []
            pattern_hits = []
            
            for year in test_years:
                year_preds = all_preds[all_preds['year'] == year]
                result = calc_tansho(year_preds, ranks, std_min, std_max)
                yearly_rois.append(result['roi'])
                
                for hit in result['hit_details']:
                    hit['pattern'] = name
                    hit['condition'] = cond_name
                    pattern_hits.append(hit)
            
            # 全体
            result_all = calc_tansho(all_preds, ranks, std_min, std_max)
            
            # 外れ値検出
            payouts = [h['payout'] for h in pattern_hits]
            max_payout = max(payouts) if payouts else 0
            outlier_count = len([p for p in payouts if p >= 30])
            
            all_results.append({
                'bet_type': 'tansho',
                'pattern': name,
                'condition': cond_name,
                'roi_5yr': result_all['roi'],
                'roi_std': np.std(yearly_rois) if len(yearly_rois) > 1 else 0,
                'bets': result_all['bets'],
                'hits': result_all['hits'],
                'max_payout': max_payout,
                'outliers_30x': outlier_count,
                'roi_2020': yearly_rois[0] if len(yearly_rois) > 0 else 0,
                'roi_2021': yearly_rois[1] if len(yearly_rois) > 1 else 0,
                'roi_2022': yearly_rois[2] if len(yearly_rois) > 2 else 0,
                'roi_2023': yearly_rois[3] if len(yearly_rois) > 3 else 0,
                'roi_2024': yearly_rois[4] if len(yearly_rois) > 4 else 0,
            })
            all_hits.extend(pattern_hits)
    
    # === ワイド ===
    logger.info("\n" + "="*70)
    logger.info("【ワイド】全パターン × 5年間")
    logger.info("="*70)
    
    for name, axis, partner in rentan_patterns:
        for cond_name, std_min, std_max in conditions:
            yearly_rois = []
            pattern_hits = []
            
            for year in test_years:
                year_preds = all_preds[all_preds['year'] == year]
                result = calc_wide(year_preds, wide_df, axis, partner, std_min, std_max)
                yearly_rois.append(result['roi'])
                
                for hit in result['hit_details']:
                    hit['pattern'] = name
                    hit['condition'] = cond_name
                    pattern_hits.append(hit)
            
            result_all = calc_wide(all_preds, wide_df, axis, partner, std_min, std_max)
            
            payouts = [h['payout'] for h in pattern_hits]
            max_payout = max(payouts) if payouts else 0
            outlier_count = len([p for p in payouts if p >= 30])
            
            all_results.append({
                'bet_type': 'wide',
                'pattern': name,
                'condition': cond_name,
                'roi_5yr': result_all['roi'],
                'roi_std': np.std(yearly_rois) if len(yearly_rois) > 1 else 0,
                'bets': result_all['bets'],
                'hits': result_all['hits'],
                'max_payout': max_payout,
                'outliers_30x': outlier_count,
                'roi_2020': yearly_rois[0] if len(yearly_rois) > 0 else 0,
                'roi_2021': yearly_rois[1] if len(yearly_rois) > 1 else 0,
                'roi_2022': yearly_rois[2] if len(yearly_rois) > 2 else 0,
                'roi_2023': yearly_rois[3] if len(yearly_rois) > 3 else 0,
                'roi_2024': yearly_rois[4] if len(yearly_rois) > 4 else 0,
            })
            all_hits.extend(pattern_hits)
        
        logger.info(f"  {name} 完了")
    
    # === 馬連 ===
    logger.info("\n" + "="*70)
    logger.info("【馬連】全パターン × 5年間")
    logger.info("="*70)
    
    for name, axis, partner in rentan_patterns:
        for cond_name, std_min, std_max in conditions:
            yearly_rois = []
            pattern_hits = []
            
            for year in test_years:
                year_preds = all_preds[all_preds['year'] == year]
                result = calc_umaren(year_preds, umaren_df, axis, partner, std_min, std_max)
                yearly_rois.append(result['roi'])
                
                for hit in result['hit_details']:
                    hit['pattern'] = name
                    hit['condition'] = cond_name
                    pattern_hits.append(hit)
            
            result_all = calc_umaren(all_preds, umaren_df, axis, partner, std_min, std_max)
            
            payouts = [h['payout'] for h in pattern_hits]
            max_payout = max(payouts) if payouts else 0
            outlier_count = len([p for p in payouts if p >= 30])
            
            all_results.append({
                'bet_type': 'umaren',
                'pattern': name,
                'condition': cond_name,
                'roi_5yr': result_all['roi'],
                'roi_std': np.std(yearly_rois) if len(yearly_rois) > 1 else 0,
                'bets': result_all['bets'],
                'hits': result_all['hits'],
                'max_payout': max_payout,
                'outliers_30x': outlier_count,
                'roi_2020': yearly_rois[0] if len(yearly_rois) > 0 else 0,
                'roi_2021': yearly_rois[1] if len(yearly_rois) > 1 else 0,
                'roi_2022': yearly_rois[2] if len(yearly_rois) > 2 else 0,
                'roi_2023': yearly_rois[3] if len(yearly_rois) > 3 else 0,
                'roi_2024': yearly_rois[4] if len(yearly_rois) > 4 else 0,
            })
            all_hits.extend(pattern_hits)
        
        logger.info(f"  {name} 完了")
    
    # 結果をDataFrameに
    results_df = pd.DataFrame(all_results)
    hits_df = pd.DataFrame(all_hits)
    
    # === サマリー ===
    logger.info("\n" + "="*70)
    logger.info("【サマリー】ROI 80%以上 かつ 件数100以上 かつ 標準偏差30以下")
    logger.info("="*70)
    
    good = results_df[(results_df['roi_5yr'] >= 80) & 
                       (results_df['bets'] >= 100) &
                       (results_df['roi_std'] <= 30)]
    good = good.sort_values('roi_5yr', ascending=False)
    
    for bet_type in ['tansho', 'wide', 'umaren']:
        subset = good[good['bet_type'] == bet_type].head(10)
        if len(subset) > 0:
            logger.info(f"\n■ {bet_type} Top10:")
            for _, row in subset.iterrows():
                logger.info(f"  {row['pattern']} ({row['condition']}): "
                           f"ROI {row['roi_5yr']:.1f}% (std={row['roi_std']:.1f}, n={row['bets']}, "
                           f"max={row['max_payout']:.1f}x, 外れ値={row['outliers_30x']}件)")
    
    # 外れ値警告
    logger.info("\n" + "="*70)
    logger.info("【外れ値警告】max_payout >= 100倍")
    logger.info("="*70)
    
    outlier_strategies = results_df[results_df['max_payout'] >= 100]
    if len(outlier_strategies) > 0:
        for _, row in outlier_strategies.iterrows():
            logger.info(f"  {row['bet_type']} {row['pattern']} ({row['condition']}): "
                       f"max {row['max_payout']:.1f}倍")
    
    # 保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results_df.to_csv(output_dir / "full_5year_backtest_results.csv", index=False, encoding='utf-8-sig')
    hits_df.to_csv(output_dir / "full_5year_hit_details.csv", index=False, encoding='utf-8-sig')
    
    logger.info("\n" + "="*70)
    logger.info("結果保存完了")
    logger.info("="*70)
    logger.info(f"  - full_5year_backtest_results.csv ({len(results_df)}パターン)")
    logger.info(f"  - full_5year_hit_details.csv ({len(hits_df)}件の的中)")


if __name__ == "__main__":
    main()

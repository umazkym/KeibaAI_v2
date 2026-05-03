#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【高速版】5年間バックテスト - ベクトル化 + 進捗表示

【高速化ポイント】
1. 予測データを事前にマージしてレースループを排除
2. pandasのベクトル演算でROI計算
3. 進捗バーで残り時間を可視化

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
from tqdm import tqdm
import time

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
    
    return races, pedigrees, corners, race_details, horses, returns


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


def prepare_race_data(preds_df, returns_df):
    """レースデータを事前準備（高速化のため）"""
    # 各レースのTop1-5とfinish_position情報を抽出
    race_info = []
    
    for race_id in tqdm(preds_df['race_id'].unique(), desc="レースデータ準備"):
        race_df = preds_df[preds_df['race_id'] == race_id]
        
        top_horses = {}
        for r in range(1, 6):
            h = race_df[race_df['rank'] == r]
            if len(h) > 0:
                top_horses[f'top{r}'] = int(h.iloc[0]['horse_number'])
                top_horses[f'top{r}_pos'] = int(h.iloc[0]['finish_position'])
                top_horses[f'top{r}_odds'] = h.iloc[0]['win_odds']
            else:
                top_horses[f'top{r}'] = None
                top_horses[f'top{r}_pos'] = None
                top_horses[f'top{r}_odds'] = None
        
        # 実際の1-2-3着を取得
        top3_actual = race_df[race_df['finish_position'] <= 3]['horse_number'].astype(int).tolist()
        top2_actual = race_df[race_df['finish_position'] <= 2]['horse_number'].astype(int).tolist()
        
        meta = race_df.iloc[0]
        race_info.append({
            'race_id': race_id,
            'year': meta['year'],
            'std': meta['race_place_prob_std'],
            'venue': meta['venue'],
            'race_name': str(meta['race_name'])[:20],
            'race_date': str(meta['race_date'])[:10],
            'actual_top2': tuple(sorted(top2_actual[:2])) if len(top2_actual) >= 2 else None,
            'actual_top3': set(top3_actual),
            **top_horses
        })
    
    race_df = pd.DataFrame(race_info)
    
    # ワイド払戻を辞書化
    wide = returns_df[returns_df['bet_type'] == 'wide'].copy()
    wide['pair'] = wide.apply(
        lambda x: tuple(sorted([int(x['horse_1']), int(x['horse_2'])])) 
        if pd.notna(x['horse_1']) and pd.notna(x['horse_2']) else None, 
        axis=1
    )
    wide = wide[wide['pair'].notna()]
    wide_dict = dict(zip(zip(wide['race_id'], wide['pair']), wide['payout']))
    
    # 馬連払戻を辞書化
    umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    umaren_dict = {}
    for _, row in umaren.iterrows():
        if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
            pair = tuple(sorted([int(row['horse_1']), int(row['horse_2'])]))
            umaren_dict[(row['race_id'], pair)] = row['payout']
    
    return race_df, wide_dict, umaren_dict


def generate_patterns():
    """全パターン生成"""
    tansho_patterns = []
    for size in range(1, 6):
        for start in range(1, 6 - size + 1):
            ranks = list(range(start, start + size))
            name = f"Top{'-'.join(map(str, ranks))}" if size > 1 else f"Top{start}"
            tansho_patterns.append((name, ranks))
    
    rentan_patterns = []
    for axis in range(1, 5):
        for partner_size in range(1, 6 - axis):
            partners = list(range(axis + 1, axis + 1 + partner_size))
            if partners and max(partners) <= 5:
                name = f"Top{axis}-Top{','.join(map(str, partners))}"
                rentan_patterns.append((name, [axis], partners))
    
    for axis_size in range(2, 4):
        for axis_start in range(1, 6 - axis_size):
            axes = list(range(axis_start, axis_start + axis_size))
            for partner_size in range(1, 6 - max(axes)):
                partners = list(range(max(axes) + 1, max(axes) + 1 + partner_size))
                if partners and max(partners) <= 5:
                    name = f"Top{','.join(map(str, axes))}-Top{','.join(map(str, partners))}"
                    rentan_patterns.append((name, axes, partners))
    
    for size in range(2, 6):
        for start in range(1, 6 - size + 1):
            ranks = list(range(start, start + size))
            if max(ranks) <= 5:
                name = f"Top{'-'.join(map(str, ranks))}BOX"
                rentan_patterns.append((name, ranks, ranks))
    
    return tansho_patterns, rentan_patterns


def calc_tansho_vectorized(race_df, target_ranks, std_min, std_max):
    """単勝ROI計算（ベクトル化）"""
    df = race_df[(race_df['std'] >= std_min) & (race_df['std'] < std_max)].copy()
    
    total_bet = 0
    total_return = 0
    hits = []
    
    for r in target_ranks:
        # 購入対象
        bets = len(df)
        total_bet += bets
        
        # 的中（1着）
        hit_mask = df[f'top{r}_pos'] == 1
        hit_df = df[hit_mask]
        
        total_return += hit_df[f'top{r}_odds'].sum()
        
        for _, row in hit_df.iterrows():
            hits.append({
                'race_id': row['race_id'],
                'payout': row[f'top{r}_odds'],
                'year': row['year'],
            })
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    return {'roi': roi, 'bets': total_bet, 'hits': len(hits), 'hit_details': hits}


def calc_wide_vectorized(race_df, wide_dict, axis_ranks, partner_ranks, std_min, std_max):
    """ワイドROI計算（ベクトル化）"""
    df = race_df[(race_df['std'] >= std_min) & (race_df['std'] < std_max)].copy()
    
    total_bet = 0
    total_return = 0
    hits = []
    
    for _, row in df.iterrows():
        actual_top3 = row['actual_top3']
        if not actual_top3:
            continue
        
        # 軸馬と相手馬を取得
        axis_horses = [row[f'top{r}'] for r in axis_ranks if row.get(f'top{r}') is not None]
        partner_horses = [row[f'top{r}'] for r in partner_ranks if row.get(f'top{r}') is not None]
        
        bet_pairs = set()
        for a in axis_horses:
            for p in partner_horses:
                if a and p and a != p:
                    bet_pairs.add(tuple(sorted([a, p])))
        
        for pair in bet_pairs:
            total_bet += 1
            h1, h2 = pair
            if h1 in actual_top3 and h2 in actual_top3:
                payout = wide_dict.get((row['race_id'], pair), 0)
                if payout > 0:
                    total_return += payout / 100
                    hits.append({
                        'race_id': row['race_id'],
                        'payout': payout / 100,
                        'year': row['year'],
                    })
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    return {'roi': roi, 'bets': total_bet, 'hits': len(hits), 'hit_details': hits}


def calc_umaren_vectorized(race_df, umaren_dict, axis_ranks, partner_ranks, std_min, std_max):
    """馬連ROI計算（ベクトル化）"""
    df = race_df[(race_df['std'] >= std_min) & (race_df['std'] < std_max)].copy()
    
    total_bet = 0
    total_return = 0
    hits = []
    
    for _, row in df.iterrows():
        actual_pair = row['actual_top2']
        if not actual_pair:
            continue
        
        # 軸馬と相手馬を取得
        axis_horses = [row[f'top{r}'] for r in axis_ranks if row.get(f'top{r}') is not None]
        partner_horses = [row[f'top{r}'] for r in partner_ranks if row.get(f'top{r}') is not None]
        
        bet_pairs = set()
        for a in axis_horses:
            for p in partner_horses:
                if a and p and a != p:
                    bet_pairs.add(tuple(sorted([a, p])))
        
        for pair in bet_pairs:
            total_bet += 1
            if pair == actual_pair:
                payout = umaren_dict.get((row['race_id'], pair), 0)
                if payout > 0:
                    total_return += payout / 100
                    hits.append({
                        'race_id': row['race_id'],
                        'payout': payout / 100,
                        'year': row['year'],
                    })
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    return {'roi': roi, 'bets': total_bet, 'hits': len(hits), 'hit_details': hits}


def main():
    start_time = time.time()
    
    logger.info("="*70)
    logger.info("【高速版】5年間バックテスト - ベクトル化")
    logger.info("="*70)
    
    # データ読み込み
    races, pedigrees, corners, race_details, horses, returns = load_base_data()
    logger.info(f"データ件数: {len(races):,}")
    
    # 5年分の予測
    test_years = [2020, 2021, 2022, 2023, 2024]
    all_preds_list = []
    
    for year in test_years:
        logger.info(f"\n{'='*40}")
        logger.info(f"[{year}年] 予測中...")
        logger.info(f"{'='*40}")
        preds = run_year_prediction(races, pedigrees, corners, race_details, horses, year)
        if preds is not None:
            all_preds_list.append(preds)
            logger.info(f"  予測完了: {len(preds):,}件")
    
    all_preds = pd.concat(all_preds_list, ignore_index=True)
    logger.info(f"\n全予測: {len(all_preds):,}件")
    
    # レースデータを事前準備（高速化の鍵）
    logger.info("\n" + "="*70)
    logger.info("レースデータ準備中...")
    race_df, wide_dict, umaren_dict = prepare_race_data(all_preds, returns)
    logger.info(f"レース数: {len(race_df):,}")
    
    # パターン生成
    tansho_patterns, rentan_patterns = generate_patterns()
    logger.info(f"\n単勝パターン: {len(tansho_patterns)}")
    logger.info(f"連勝パターン: {len(rentan_patterns)}")
    
    # 条件
    conditions = [
        ('all', 0.0, 1.0),
        ('stable', 0.20, 1.0),
        ('moderate', 0.08, 0.20),
        ('std>=0.09', 0.09, 1.0),
        ('std>=0.15', 0.15, 1.0),
    ]
    
    all_results = []
    all_hits = []
    
    # === 単勝 ===
    logger.info("\n" + "="*70)
    logger.info("【単勝】全パターン × 5年間")
    
    for name, ranks in tqdm(tansho_patterns, desc="単勝"):
        for cond_name, std_min, std_max in conditions:
            yearly_rois = []
            pattern_hits = []
            
            for year in test_years:
                year_df = race_df[race_df['year'] == year]
                result = calc_tansho_vectorized(year_df, ranks, std_min, std_max)
                yearly_rois.append(result['roi'])
                pattern_hits.extend(result['hit_details'])
            
            result_all = calc_tansho_vectorized(race_df, ranks, std_min, std_max)
            
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
            
            for hit in pattern_hits:
                hit['bet_type'] = 'tansho'
                hit['pattern'] = name
                hit['condition'] = cond_name
                all_hits.append(hit)
    
    # === ワイド ===
    logger.info("\n" + "="*70)
    logger.info("【ワイド】全パターン × 5年間")
    
    for name, axis, partner in tqdm(rentan_patterns, desc="ワイド"):
        for cond_name, std_min, std_max in conditions:
            yearly_rois = []
            pattern_hits = []
            
            for year in test_years:
                year_df = race_df[race_df['year'] == year]
                result = calc_wide_vectorized(year_df, wide_dict, axis, partner, std_min, std_max)
                yearly_rois.append(result['roi'])
                pattern_hits.extend(result['hit_details'])
            
            result_all = calc_wide_vectorized(race_df, wide_dict, axis, partner, std_min, std_max)
            
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
            
            for hit in pattern_hits:
                hit['bet_type'] = 'wide'
                hit['pattern'] = name
                hit['condition'] = cond_name
                all_hits.append(hit)
    
    # === 馬連 ===
    logger.info("\n" + "="*70)
    logger.info("【馬連】全パターン × 5年間")
    
    for name, axis, partner in tqdm(rentan_patterns, desc="馬連"):
        for cond_name, std_min, std_max in conditions:
            yearly_rois = []
            pattern_hits = []
            
            for year in test_years:
                year_df = race_df[race_df['year'] == year]
                result = calc_umaren_vectorized(year_df, umaren_dict, axis, partner, std_min, std_max)
                yearly_rois.append(result['roi'])
                pattern_hits.extend(result['hit_details'])
            
            result_all = calc_umaren_vectorized(race_df, umaren_dict, axis, partner, std_min, std_max)
            
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
            
            for hit in pattern_hits:
                hit['bet_type'] = 'umaren'
                hit['pattern'] = name
                hit['condition'] = cond_name
                all_hits.append(hit)
    
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
                           f"max={row['max_payout']:.1f}x)")
    
    # 外れ値警告
    logger.info("\n" + "="*70)
    logger.info("【外れ値警告】max_payout >= 100倍")
    logger.info("="*70)
    
    outlier_strategies = results_df[results_df['max_payout'] >= 100]
    for _, row in outlier_strategies.iterrows():
        logger.info(f"  {row['bet_type']} {row['pattern']} ({row['condition']}): max {row['max_payout']:.1f}倍")
    
    # 保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results_df.to_csv(output_dir / "full_5year_backtest_results.csv", index=False, encoding='utf-8-sig')
    hits_df.to_csv(output_dir / "full_5year_hit_details.csv", index=False, encoding='utf-8-sig')
    
    elapsed = time.time() - start_time
    logger.info("\n" + "="*70)
    logger.info(f"完了! 処理時間: {elapsed/60:.1f}分")
    logger.info("="*70)
    logger.info(f"  - full_5year_backtest_results.csv ({len(results_df)}パターン)")
    logger.info(f"  - full_5year_hit_details.csv ({len(hits_df)}件)")


if __name__ == "__main__":
    main()

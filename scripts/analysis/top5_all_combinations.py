#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Top1-5 全組み合わせ買い方検証

【目的】
単勝・ワイド・馬連でTop1-5のすべての組み合わせを網羅的に検証し、
最適な買い方を特定する。

【検証対象】
- 単勝: Top1, Top2, Top3, ..., Top1-3, Top2-4, etc.
- ワイド: Top1-2, Top1-3, ..., Top2-5, Top1-Top2,3, etc.
- 馬連: Top1-2, Top1-3, ..., Top2-5, Top1-Top2,3, etc.

作成日: 2026-01-11
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from itertools import combinations, product
import logging
import warnings

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def load_and_predict_2024():
    """2024年データで予測を実行"""
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    from keibaai.src.models.multi_target_predictor import MultiTargetPredictor
    from keibaai.src.features.time_margin_features import TimeMarginFeatureEngineer
    
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
    
    # 2024年
    test_year = 2024
    test_start_dt = pd.to_datetime('2024-01-01')
    test_end_dt = pd.to_datetime('2024-12-31')
    train_end = test_start_dt - timedelta(days=1)
    valid_start = train_end - timedelta(days=365)
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
    
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
    
    # 荒れ度分類
    test_preds['vol_type'] = np.where(
        test_preds['race_place_prob_std'] < 0.08, 'volatile',
        np.where(test_preds['race_place_prob_std'] < 0.20, 'moderate', 'stable')
    )
    
    # ランキング
    test_preds['rank'] = test_preds.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    
    # ワイド・馬連払戻
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
    
    return test_preds, wide, umaren


def calc_tansho_roi(preds_df, target_ranks, vol_filter=None):
    """
    単勝ROI計算
    
    Args:
        preds_df: 予測データ
        target_ranks: 購入対象の予測順位リスト [1], [1,2], [2,3] など
        vol_filter: 荒れ度フィルタ ('stable', 'moderate', 'volatile', None)
    """
    df = preds_df.copy()
    if vol_filter:
        df = df[df['vol_type'] == vol_filter]
    
    # 対象馬を抽出
    targets = df[df['rank'].isin(target_ranks)]
    
    total_bet = len(targets)
    hits = targets[targets['finish_position'] == 1]
    total_return = hits['win_odds'].sum()
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = len(hits) / total_bet * 100 if total_bet > 0 else 0
    
    return {
        'roi': roi,
        'hit_rate': hit_rate,
        'bets': total_bet,
        'hits': len(hits),
        'avg_odds': hits['win_odds'].mean() if len(hits) > 0 else 0,
    }


def calc_wide_roi(preds_df, wide_df, axis_ranks, partner_ranks, vol_filter=None):
    """
    ワイドROI計算
    
    Args:
        preds_df: 予測データ
        wide_df: ワイド払戻データ
        axis_ranks: 軸馬の予測順位リスト [1], [1,2] など
        partner_ranks: 相手馬の予測順位リスト [2,3], [3,4,5] など
        vol_filter: 荒れ度フィルタ
    """
    df = preds_df.copy()
    if vol_filter:
        df = df[df['vol_type'] == vol_filter]
    
    total_bet = 0
    total_return = 0
    hits = 0
    
    for race_id in df['race_id'].unique():
        race_df = df[df['race_id'] == race_id]
        
        # 実際の3着以内
        actual_top3 = set(race_df[race_df['finish_position'] <= 3]['horse_number'].astype(int).values)
        
        # ワイド払戻
        race_wide = wide_df[wide_df['race_id'] == race_id]
        
        # 軸馬を取得
        axis_horses = race_df[race_df['rank'].isin(axis_ranks)]['horse_number'].astype(int).values
        # 相手馬を取得
        partner_horses = race_df[race_df['rank'].isin(partner_ranks)]['horse_number'].astype(int).values
        
        # 組み合わせを生成（重複排除）
        bet_pairs = set()
        for a in axis_horses:
            for p in partner_horses:
                if a != p:
                    pair = tuple(sorted([a, p]))
                    bet_pairs.add(pair)
        
        for pair in bet_pairs:
            total_bet += 1
            h1, h2 = pair
            if h1 in actual_top3 and h2 in actual_top3:
                # 的中
                match = race_wide[race_wide['pair'] == pair]
                if len(match) > 0:
                    hits += 1
                    total_return += match.iloc[0]['payout'] / 100
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hits / total_bet * 100 if total_bet > 0 else 0
    
    return {
        'roi': roi,
        'hit_rate': hit_rate,
        'bets': total_bet,
        'hits': hits,
    }


def calc_umaren_roi(preds_df, umaren_df, axis_ranks, partner_ranks, vol_filter=None):
    """
    馬連ROI計算
    """
    df = preds_df.copy()
    if vol_filter:
        df = df[df['vol_type'] == vol_filter]
    
    total_bet = 0
    total_return = 0
    hits = 0
    
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
                    hits += 1
                    total_return += race_umaren.iloc[0]['payout'] / 100
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hits / total_bet * 100 if total_bet > 0 else 0
    
    return {
        'roi': roi,
        'hit_rate': hit_rate,
        'bets': total_bet,
        'hits': hits,
    }


def generate_all_combinations():
    """Top1-5のすべての買い方パターンを生成"""
    
    # 単勝パターン (Top1のみ, Top2のみ, ..., Top1-2, Top2-3, ..., Top1-5)
    tansho_patterns = []
    for size in range(1, 6):  # 1点買いから5点買いまで
        for start in range(1, 6 - size + 1):
            ranks = list(range(start, start + size))
            name = f"Top{'-'.join(map(str, ranks))}" if size > 1 else f"Top{start}のみ"
            tansho_patterns.append((name, ranks))
    
    # ワイド・馬連パターン (軸-相手の組み合わせ)
    rentan_patterns = []
    
    # パターン1: 1軸流し (Top1-Top2, Top1-Top2,3, ...)
    for axis in range(1, 6):
        for num_partners in range(1, 6 - axis + 1):
            partners = list(range(axis + 1, axis + 1 + num_partners))
            if partners and max(partners) <= 5:
                if num_partners == 1:
                    name = f"Top{axis}-Top{partners[0]}"
                else:
                    name = f"Top{axis}-Top{','.join(map(str, partners))}"
                rentan_patterns.append((name, [axis], partners))
    
    # パターン2: 複数軸流し (Top1,2-Top3,4, ...)
    for axis_size in range(2, 4):  # 2軸、3軸
        for axis_start in range(1, 6 - axis_size + 1):
            axes = list(range(axis_start, axis_start + axis_size))
            for partner_size in range(1, 6 - max(axes) + 1):
                partners = list(range(max(axes) + 1, max(axes) + 1 + partner_size))
                if partners and max(partners) <= 5:
                    name = f"Top{','.join(map(str, axes))}-Top{','.join(map(str, partners))}"
                    rentan_patterns.append((name, axes, partners))
    
    # パターン3: BOX買い (Top1-2-3 BOX, Top2-3-4 BOX, ...)
    for size in range(2, 6):  # 2頭からTop5までのBOX
        for start in range(1, 6 - size + 1):
            ranks = list(range(start, start + size))
            if max(ranks) <= 5:
                name = f"Top{'-'.join(map(str, ranks))} BOX"
                # BOXは全組み合わせなので軸=相手
                rentan_patterns.append((name, ranks, ranks))
    
    return tansho_patterns, rentan_patterns


def main():
    logger.info("="*70)
    logger.info("Top1-5 全組み合わせ買い方検証")
    logger.info("="*70)
    
    # データ読み込み・予測
    logger.info("\n2024年データで予測を実行中...")
    preds_df, wide_df, umaren_df = load_and_predict_2024()
    
    logger.info(f"予測データ: {len(preds_df):,}件")
    logger.info(f"レース数: {preds_df['race_id'].nunique():,}件")
    
    # パターン生成
    tansho_patterns, rentan_patterns = generate_all_combinations()
    
    logger.info(f"\n単勝パターン数: {len(tansho_patterns)}")
    logger.info(f"連勝パターン数: {len(rentan_patterns)}")
    
    # === 単勝検証 ===
    logger.info("\n" + "="*70)
    logger.info("【単勝】全パターン検証")
    logger.info("="*70)
    
    tansho_results = []
    
    for name, ranks in tansho_patterns:
        for vol_filter in [None, 'stable', 'moderate']:
            filter_name = vol_filter if vol_filter else 'all'
            result = calc_tansho_roi(preds_df, ranks, vol_filter)
            
            tansho_results.append({
                'name': name,
                'filter': filter_name,
                'ranks': str(ranks),
                **result
            })
    
    tansho_df = pd.DataFrame(tansho_results)
    
    # Top10を表示
    logger.info("\n【単勝 Top10 (全体)】")
    top10_all = tansho_df[tansho_df['filter'] == 'all'].nlargest(10, 'roi')
    for _, row in top10_all.iterrows():
        logger.info(f"  {row['name']}: ROI {row['roi']:.1f}% (的中{row['hit_rate']:.1f}%, n={row['bets']})")
    
    logger.info("\n【単勝 Top10 (stable)】")
    top10_stable = tansho_df[tansho_df['filter'] == 'stable'].nlargest(10, 'roi')
    for _, row in top10_stable.iterrows():
        logger.info(f"  {row['name']}: ROI {row['roi']:.1f}% (的中{row['hit_rate']:.1f}%, n={row['bets']})")
    
    # === ワイド検証 ===
    logger.info("\n" + "="*70)
    logger.info("【ワイド】全パターン検証")
    logger.info("="*70)
    
    wide_results = []
    
    for name, axis_ranks, partner_ranks in rentan_patterns:
        for vol_filter in [None, 'stable', 'moderate']:
            filter_name = vol_filter if vol_filter else 'all'
            result = calc_wide_roi(preds_df, wide_df, axis_ranks, partner_ranks, vol_filter)
            
            wide_results.append({
                'name': name,
                'filter': filter_name,
                'axis': str(axis_ranks),
                'partner': str(partner_ranks),
                **result
            })
    
    wide_df_result = pd.DataFrame(wide_results)
    
    logger.info("\n【ワイド Top10 (全体)】")
    top10_all = wide_df_result[wide_df_result['filter'] == 'all'].nlargest(10, 'roi')
    for _, row in top10_all.iterrows():
        logger.info(f"  {row['name']}: ROI {row['roi']:.1f}% (的中{row['hit_rate']:.1f}%, n={row['bets']})")
    
    logger.info("\n【ワイド Top10 (stable)】")
    top10_stable = wide_df_result[wide_df_result['filter'] == 'stable'].nlargest(10, 'roi')
    for _, row in top10_stable.iterrows():
        logger.info(f"  {row['name']}: ROI {row['roi']:.1f}% (的中{row['hit_rate']:.1f}%, n={row['bets']})")
    
    # === 馬連検証 ===
    logger.info("\n" + "="*70)
    logger.info("【馬連】全パターン検証")
    logger.info("="*70)
    
    umaren_results = []
    
    for name, axis_ranks, partner_ranks in rentan_patterns:
        for vol_filter in [None, 'stable', 'moderate']:
            filter_name = vol_filter if vol_filter else 'all'
            result = calc_umaren_roi(preds_df, umaren_df, axis_ranks, partner_ranks, vol_filter)
            
            umaren_results.append({
                'name': name,
                'filter': filter_name,
                'axis': str(axis_ranks),
                'partner': str(partner_ranks),
                **result
            })
    
    umaren_df_result = pd.DataFrame(umaren_results)
    
    logger.info("\n【馬連 Top10 (全体)】")
    top10_all = umaren_df_result[umaren_df_result['filter'] == 'all'].nlargest(10, 'roi')
    for _, row in top10_all.iterrows():
        logger.info(f"  {row['name']}: ROI {row['roi']:.1f}% (的中{row['hit_rate']:.1f}%, n={row['bets']})")
    
    logger.info("\n【馬連 Top10 (stable)】")
    top10_stable = umaren_df_result[umaren_df_result['filter'] == 'stable'].nlargest(10, 'roi')
    for _, row in top10_stable.iterrows():
        logger.info(f"  {row['name']}: ROI {row['roi']:.1f}% (的中{row['hit_rate']:.1f}%, n={row['bets']})")
    
    # === 総合サマリー ===
    logger.info("\n" + "="*70)
    logger.info("【総合サマリー】")
    logger.info("="*70)
    
    # 全券種でROI100%超えを抽出
    logger.info("\n■ ROI 100%超え戦略:")
    
    over100_tansho = tansho_df[tansho_df['roi'] >= 100]
    over100_wide = wide_df_result[wide_df_result['roi'] >= 100]
    over100_umaren = umaren_df_result[umaren_df_result['roi'] >= 100]
    
    if len(over100_tansho) > 0:
        logger.info("\n  【単勝】")
        for _, row in over100_tansho.sort_values('roi', ascending=False).head(5).iterrows():
            logger.info(f"    {row['name']} ({row['filter']}): ROI {row['roi']:.1f}% (n={row['bets']})")
    
    if len(over100_wide) > 0:
        logger.info("\n  【ワイド】")
        for _, row in over100_wide.sort_values('roi', ascending=False).head(5).iterrows():
            logger.info(f"    {row['name']} ({row['filter']}): ROI {row['roi']:.1f}% (n={row['bets']})")
    
    if len(over100_umaren) > 0:
        logger.info("\n  【馬連】")
        for _, row in over100_umaren.sort_values('roi', ascending=False).head(5).iterrows():
            logger.info(f"    {row['name']} ({row['filter']}): ROI {row['roi']:.1f}% (n={row['bets']})")
    
    # 結果保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    tansho_df.to_csv(output_dir / "top5_tansho_all_patterns.csv", index=False, encoding='utf-8-sig')
    wide_df_result.to_csv(output_dir / "top5_wide_all_patterns.csv", index=False, encoding='utf-8-sig')
    umaren_df_result.to_csv(output_dir / "top5_umaren_all_patterns.csv", index=False, encoding='utf-8-sig')
    
    logger.info("\n結果保存完了")


if __name__ == "__main__":
    main()

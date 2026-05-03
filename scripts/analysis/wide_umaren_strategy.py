#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ワイド・馬連 戦略開発 & V4.4調整検証

【目的】
1. ワイド・馬連の効果的な買い方を探索
2. stableレースでV4.4ウェイト削減を検証
3. moderate穴馬狙いモードを開発
4. リーク・過適合に注意しつつ検証

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


def load_data_and_predict():
    """データ読み込みと予測"""
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
    
    # 2024年データで分析
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
    merge_cols = ['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity',
                  'race_name', 'venue', 'race_date']
    for col in ['horse_close_loss_rate', 'horse_margin_std']:
        if col in test_f.columns:
            merge_cols.append(col)
    
    test_preds = test_preds.merge(test_f[merge_cols], on=['race_id', 'horse_number'], how='left')
    
    # ワイド・馬連払戻をマージ
    wide = returns[returns['bet_type'] == 'wide'].copy()
    umaren = returns[returns['bet_type'] == 'umaren'].copy()
    
    return test_preds, wide, umaren


def apply_volatility_and_v44_adjustment(df, stable_v44_weight=0.15):
    """荒れ度分類とV4.4調整を適用"""
    df = df.copy()
    
    # 荒れ度分類
    df['vol_type'] = np.where(
        df['race_place_prob_std'] < 0.08, 'volatile',
        np.where(df['race_place_prob_std'] < 0.20, 'moderate', 'stable')
    )
    
    # V4.4調整ウェイト（stableでは削減）
    df['adj_v44_weight'] = np.where(
        df['vol_type'] == 'stable', stable_v44_weight,
        np.where(df['vol_type'] == 'moderate', 0.35, 0.40)  # moderate穴馬狙い
    )
    
    # 調整後のアンサンブルスコア
    df['adj_ensemble_score'] = (
        df['win_prob'] * (1 - df['adj_v44_weight'] - 0.20) +
        df['v44_score'] * df['adj_v44_weight'] +
        df['top2_prob'] * 0.10 +
        df['place_prob'] * 0.10
    )
    
    # moderate穴馬狙いスコア（V4.4を重視）
    df['longshot_score'] = (
        df['win_prob'] * 0.25 +
        df['v44_score'] * 0.45 +
        df['top2_prob'] * 0.15 +
        df['place_prob'] * 0.15
    )
    
    # ランキング
    df['rank_ensemble'] = df.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    df['rank_adj'] = df.groupby('race_id')['adj_ensemble_score'].rank(ascending=False, method='first')
    df['rank_longshot'] = df.groupby('race_id')['longshot_score'].rank(ascending=False, method='first')
    df['rank_top2prob'] = df.groupby('race_id')['top2_prob'].rank(ascending=False, method='first')
    df['rank_place'] = df.groupby('race_id')['place_prob'].rank(ascending=False, method='first')
    
    return df


def calc_wide_roi(preds_df, wide_df, horse1_ranks, horse2_ranks, filter_func=None):
    """
    ワイドROIを計算
    
    Args:
        preds_df: 予測データ
        wide_df: ワイド払戻データ
        horse1_ranks: 1頭目の予測順位リスト
        horse2_ranks: 2頭目の予測順位リスト
        filter_func: レースフィルタ関数（オプション）
    """
    df = preds_df.copy()
    
    if filter_func:
        # フィルタを適用（レース単位）
        race_ids = df.groupby('race_id').apply(filter_func)
        race_ids = race_ids[race_ids].index
        df = df[df['race_id'].isin(race_ids)]
    
    total_bet = 0
    total_return = 0
    hits = 0
    
    for race_id in df['race_id'].unique():
        race_df = df[df['race_id'] == race_id]
        
        # 指定順位の馬を取得
        horses1 = race_df[race_df['rank_adj'].isin(horse1_ranks)]['horse_number'].values
        horses2 = race_df[race_df['rank_adj'].isin(horse2_ranks)]['horse_number'].values
        
        # 全組み合わせを生成
        bet_pairs = set()
        for h1 in horses1:
            for h2 in horses2:
                if h1 != h2:
                    pair = tuple(sorted([h1, h2]))
                    bet_pairs.add(pair)
        
        if not bet_pairs:
            continue
        
        total_bet += len(bet_pairs)
        
        # 実際の3着以内
        actual_top3 = race_df[race_df['finish_position'] <= 3]['horse_number'].values
        
        # 的中判定
        for h1, h2 in bet_pairs:
            if h1 in actual_top3 and h2 in actual_top3:
                # ワイド払戻を取得
                race_wide = wide_df[wide_df['race_id'] == race_id]
                for _, row in race_wide.iterrows():
                    # horse_numberは "1-3" のような形式
                    if 'horse_number' in row:
                        try:
                            nums = str(row['horse_number']).split('-')
                            if len(nums) == 2:
                                w1, w2 = int(nums[0]), int(nums[1])
                                if (h1 == w1 and h2 == w2) or (h1 == w2 and h2 == w1):
                                    total_return += row['payout'] / 100
                                    hits += 1
                                    break
                        except:
                            pass
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hits / total_bet * 100 if total_bet > 0 else 0
    
    return {
        'roi': roi,
        'hit_rate': hit_rate,
        'bets': total_bet,
        'hits': hits,
        'return': total_return,
    }


def calc_umaren_roi(preds_df, umaren_df, horse1_ranks, horse2_ranks, filter_func=None):
    """
    馬連ROIを計算
    """
    df = preds_df.copy()
    
    if filter_func:
        race_ids = df.groupby('race_id').apply(filter_func)
        race_ids = race_ids[race_ids].index
        df = df[df['race_id'].isin(race_ids)]
    
    total_bet = 0
    total_return = 0
    hits = 0
    
    for race_id in df['race_id'].unique():
        race_df = df[df['race_id'] == race_id]
        
        horses1 = race_df[race_df['rank_adj'].isin(horse1_ranks)]['horse_number'].values
        horses2 = race_df[race_df['rank_adj'].isin(horse2_ranks)]['horse_number'].values
        
        bet_pairs = set()
        for h1 in horses1:
            for h2 in horses2:
                if h1 != h2:
                    pair = tuple(sorted([h1, h2]))
                    bet_pairs.add(pair)
        
        if not bet_pairs:
            continue
        
        total_bet += len(bet_pairs)
        
        # 実際の1-2着
        actual_top2 = race_df[race_df['finish_position'] <= 2]['horse_number'].values
        if len(actual_top2) < 2:
            continue
        
        actual_pair = tuple(sorted(actual_top2[:2]))
        
        for pair in bet_pairs:
            if pair == actual_pair:
                race_umaren = umaren_df[umaren_df['race_id'] == race_id]
                if len(race_umaren) > 0:
                    total_return += race_umaren.iloc[0]['payout'] / 100
                    hits += 1
                break
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hits / total_bet * 100 if total_bet > 0 else 0
    
    return {
        'roi': roi,
        'hit_rate': hit_rate,
        'bets': total_bet,
        'hits': hits,
    }


def analyze_wide_strategies(preds_df, wide_df):
    """ワイド戦略の分析"""
    logger.info("\n" + "="*70)
    logger.info("【ワイド戦略分析】")
    logger.info("="*70)
    
    strategies = [
        # (名前, 1頭目順位, 2頭目順位, フィルタ)
        ("Top1-Top2", [1], [2], None),
        ("Top1-Top3", [1], [3], None),
        ("Top1-Top2,3", [1], [2, 3], None),
        ("Top1-Top2,3,4", [1], [2, 3, 4], None),
        ("Top1,2-Top3,4", [1, 2], [3, 4], None),
        ("Top2-Top3", [2], [3], None),
        ("Top1-穴馬(人気6-10)", [1], [6, 7, 8, 9, 10], None),
        ("stable: Top1-Top2", [1], [2], lambda x: (x['vol_type'] == 'stable').any()),
        ("moderate: Top1-Top3", [1], [2, 3], lambda x: (x['vol_type'] == 'moderate').any()),
        ("moderate: 穴馬Top1-Top2", None, None, 'longshot'),  # 特殊処理
    ]
    
    results = []
    
    for name, h1_ranks, h2_ranks, filter_func in strategies:
        if name == "moderate: 穴馬Top1-Top2":
            # 穴馬モード: longshot_scoreでランキング
            df = preds_df.copy()
            df = df[df['vol_type'] == 'moderate']
            df['rank_adj'] = df.groupby('race_id')['longshot_score'].rank(ascending=False, method='first')
            result = calc_wide_roi(df, wide_df, [1], [2], None)
        else:
            result = calc_wide_roi(preds_df, wide_df, h1_ranks, h2_ranks, filter_func)
        
        results.append({
            'strategy': name,
            **result
        })
        
        logger.info(f"  {name}: ROI {result['roi']:.1f}%, 的中率 {result['hit_rate']:.1f}% (n={result['bets']})")
    
    return pd.DataFrame(results)


def analyze_umaren_strategies(preds_df, umaren_df):
    """馬連戦略の分析"""
    logger.info("\n" + "="*70)
    logger.info("【馬連戦略分析】")
    logger.info("="*70)
    
    strategies = [
        ("Top1-Top2", [1], [2], None),
        ("Top1-Top2,3", [1], [2, 3], None),
        ("Top1-Top2,3,4", [1], [2, 3, 4], None),
        ("Top1,2-Top3", [1, 2], [3], None),
        ("Top1-穴馬(人気6-10)", [1], [6, 7, 8, 9, 10], None),
        ("stable: Top1-Top2", [1], [2], lambda x: (x['vol_type'] == 'stable').any()),
        ("moderate: Top1-Top3", [1], [2, 3], lambda x: (x['vol_type'] == 'moderate').any()),
    ]
    
    results = []
    
    for name, h1_ranks, h2_ranks, filter_func in strategies:
        result = calc_umaren_roi(preds_df, umaren_df, h1_ranks, h2_ranks, filter_func)
        
        results.append({
            'strategy': name,
            **result
        })
        
        logger.info(f"  {name}: ROI {result['roi']:.1f}%, 的中率 {result['hit_rate']:.1f}% (n={result['bets']})")
    
    return pd.DataFrame(results)


def analyze_v44_adjustment_effect(preds_df, wide_df, umaren_df):
    """V4.4調整の効果検証"""
    logger.info("\n" + "="*70)
    logger.info("【V4.4調整効果検証】（stable=0.15 vs stable=0.30）")
    logger.info("="*70)
    
    # baseline: V4.4=0.30
    df_baseline = apply_volatility_and_v44_adjustment(preds_df, stable_v44_weight=0.30)
    # adjusted: V4.4=0.15
    df_adjusted = apply_volatility_and_v44_adjustment(preds_df, stable_v44_weight=0.15)
    
    # stableレースのみでワイドROI比較
    stable_filter = lambda x: (x['vol_type'] == 'stable').any()
    
    baseline_result = calc_wide_roi(df_baseline, wide_df, [1], [2], stable_filter)
    adjusted_result = calc_wide_roi(df_adjusted, wide_df, [1], [2], stable_filter)
    
    logger.info(f"\n  stable + V4.4=0.30: ROI {baseline_result['roi']:.1f}%")
    logger.info(f"  stable + V4.4=0.15: ROI {adjusted_result['roi']:.1f}%")
    logger.info(f"  差: {adjusted_result['roi'] - baseline_result['roi']:+.1f}pt")
    
    return {
        'baseline': baseline_result,
        'adjusted': adjusted_result,
    }


def analyze_moderate_longshot_mode(preds_df, wide_df, umaren_df):
    """moderate穴馬狙いモード分析"""
    logger.info("\n" + "="*70)
    logger.info("【moderate穴馬狙いモード分析】")
    logger.info("="*70)
    
    df = preds_df.copy()
    moderate_df = df[df['vol_type'] == 'moderate'].copy()
    
    # 穴馬スコア（V4.4重視）でランキング
    moderate_df['rank_longshot'] = moderate_df.groupby('race_id')['longshot_score'].rank(ascending=False, method='first')
    
    # 従来スコアとの比較
    strategies = [
        ("通常(ensemble): Top1-Top2", 'rank_adj'),
        ("穴馬狙い(longshot): Top1-Top2", 'rank_longshot'),
        ("穴馬狙い: Top1-Top3 (3点買い)", 'rank_longshot_3'),
    ]
    
    for name, rank_col in strategies:
        if rank_col == 'rank_longshot_3':
            # 3点買い
            temp = moderate_df.copy()
            temp['rank_adj'] = temp['rank_longshot']
            result = calc_wide_roi(temp, wide_df, [1], [2, 3], None)
        else:
            temp = moderate_df.copy()
            temp['rank_adj'] = temp[rank_col.replace('_3', '')]
            result = calc_wide_roi(temp, wide_df, [1], [2], None)
        
        logger.info(f"  {name}: ROI {result['roi']:.1f}%, 的中 {result['hit_rate']:.1f}% (n={result['bets']})")


def analyze_creative_strategies(preds_df, wide_df, umaren_df):
    """創造的な買い方の探索"""
    logger.info("\n" + "="*70)
    logger.info("【創造的な買い方探索】")
    logger.info("="*70)
    
    df = preds_df.copy()
    
    # 戦略1: 本命軸に穴馬流し（Top1 → 人気4-8番）
    logger.info("\n■ 戦略1: 本命軸に穴馬流し")
    
    # 人気4-8番をplace_probでランキング
    df['pop_rank'] = df.groupby('race_id')['popularity'].rank(method='first')
    
    total_bet = 0
    total_return = 0
    hits = 0
    
    for race_id in df['race_id'].unique():
        race_df = df[df['race_id'] == race_id]
        
        top1 = race_df[race_df['rank_adj'] == 1]['horse_number'].values
        if len(top1) == 0:
            continue
        top1 = top1[0]
        
        # 人気4-8番かつplace_probが高い馬
        mild_longshots = race_df[(race_df['popularity'] >= 4) & (race_df['popularity'] <= 8)]
        if len(mild_longshots) == 0:
            continue
        
        mild_longshots = mild_longshots.nlargest(2, 'place_prob')['horse_number'].values
        
        for h2 in mild_longshots:
            if h2 != top1:
                total_bet += 1
                
                # ワイド的中判定
                actual_top3 = race_df[race_df['finish_position'] <= 3]['horse_number'].values
                if top1 in actual_top3 and h2 in actual_top3:
                    race_wide = wide_df[wide_df['race_id'] == race_id]
                    for _, row in race_wide.iterrows():
                        try:
                            nums = str(row['horse_number']).split('-')
                            if len(nums) == 2:
                                w1, w2 = int(nums[0]), int(nums[1])
                                pair = tuple(sorted([top1, h2]))
                                wide_pair = tuple(sorted([w1, w2]))
                                if pair == wide_pair:
                                    total_return += row['payout'] / 100
                                    hits += 1
                                    break
                        except:
                            pass
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    logger.info(f"  ROI: {roi:.1f}%, 的中率: {hits/total_bet*100:.1f}% (n={total_bet})")
    
    # 戦略2: V4.4上位×place_prob上位のクロス
    logger.info("\n■ 戦略2: V4.4上位×place_prob上位のクロス")
    
    df['rank_v44'] = df.groupby('race_id')['v44_score'].rank(ascending=False, method='first')
    df['rank_place'] = df.groupby('race_id')['place_prob'].rank(ascending=False, method='first')
    
    total_bet = 0
    total_return = 0
    hits = 0
    
    for race_id in df['race_id'].unique():
        race_df = df[df['race_id'] == race_id]
        
        # V4.4 Top3
        v44_top3 = race_df[race_df['rank_v44'] <= 3]['horse_number'].values
        # place_prob Top3
        place_top3 = race_df[race_df['rank_place'] <= 3]['horse_number'].values
        
        # 共通の馬（両方で高評価）
        common = set(v44_top3) & set(place_top3)
        
        if len(common) >= 2:
            # 共通馬でワイド
            common_list = list(common)
            for h1, h2 in combinations(common_list, 2):
                total_bet += 1
                
                actual_top3 = race_df[race_df['finish_position'] <= 3]['horse_number'].values
                if h1 in actual_top3 and h2 in actual_top3:
                    race_wide = wide_df[wide_df['race_id'] == race_id]
                    for _, row in race_wide.iterrows():
                        try:
                            nums = str(row['horse_number']).split('-')
                            if len(nums) == 2:
                                w1, w2 = int(nums[0]), int(nums[1])
                                pair = tuple(sorted([h1, h2]))
                                wide_pair = tuple(sorted([w1, w2]))
                                if pair == wide_pair:
                                    total_return += row['payout'] / 100
                                    hits += 1
                                    break
                        except:
                            pass
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    logger.info(f"  ROI: {roi:.1f}%, 的中率: {hits/total_bet*100:.1f}% (n={total_bet})")


def main():
    logger.info("="*70)
    logger.info("ワイド・馬連 戦略開発 & V4.4調整検証")
    logger.info("="*70)
    
    # データ読み込みと予測
    logger.info("\nデータ読み込み・予測中...")
    preds_df, wide_df, umaren_df = load_data_and_predict()
    
    # V4.4調整を適用
    preds_df = apply_volatility_and_v44_adjustment(preds_df, stable_v44_weight=0.15)
    
    logger.info(f"予測データ: {len(preds_df):,}件")
    logger.info(f"ワイド払戻: {len(wide_df):,}件")
    logger.info(f"馬連払戻: {len(umaren_df):,}件")
    
    # ワイド戦略分析
    wide_results = analyze_wide_strategies(preds_df, wide_df)
    
    # 馬連戦略分析
    umaren_results = analyze_umaren_strategies(preds_df, umaren_df)
    
    # V4.4調整効果検証
    v44_effect = analyze_v44_adjustment_effect(preds_df, wide_df, umaren_df)
    
    # moderate穴馬狙いモード分析
    analyze_moderate_longshot_mode(preds_df, wide_df, umaren_df)
    
    # 創造的な買い方探索
    analyze_creative_strategies(preds_df, wide_df, umaren_df)
    
    # サマリー
    logger.info("\n" + "="*70)
    logger.info("【サマリー】")
    logger.info("="*70)
    
    logger.info("\n■ ワイド Best 3:")
    wide_sorted = wide_results.sort_values('roi', ascending=False).head(3)
    for _, row in wide_sorted.iterrows():
        logger.info(f"  {row['strategy']}: ROI {row['roi']:.1f}%")
    
    logger.info("\n■ 馬連 Best 3:")
    umaren_sorted = umaren_results.sort_values('roi', ascending=False).head(3)
    for _, row in umaren_sorted.iterrows():
        logger.info(f"  {row['strategy']}: ROI {row['roi']:.1f}%")
    
    # 結果保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    wide_results.to_csv(output_dir / "wide_strategy_results.csv", index=False, encoding='utf-8-sig')
    umaren_results.to_csv(output_dir / "umaren_strategy_results.csv", index=False, encoding='utf-8-sig')
    
    logger.info("\n結果保存完了")


if __name__ == "__main__":
    main()

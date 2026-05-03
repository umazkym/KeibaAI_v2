#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
クロスバリデーションによる閾値・パラメータ最適化

【目的】
1. 荒れ度閾値のLeave-One-Year-Out CV最適化
2. V4.4ウェイト調整幅の最適化
3. 券種別（単勝/複勝/馬連）の最適戦略探索

【設計思想】
- 過適合を防ぐため、各年を順番にテストセットとして留め置き
- 残りの年で閾値を最適化
- Out-of-sample性能で評価

作成日: 2026-01-11
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from itertools import product
import logging
import warnings

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def load_data():
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
    
    # 新馬・障害除外
    new_horse_mask = races['race_name'].str.contains('新馬', na=False)
    obstacle_mask = (
        races['track_surface'].str.contains('障', na=False) |
        races['race_name'].str.contains('障害', na=False)
    )
    races = races[~(new_horse_mask | obstacle_mask)].copy()
    
    return races, pedigrees, corners, race_details, horses, returns


def run_single_year_prediction(races, pedigrees, corners, race_details, horses, returns, test_year):
    """
    1年分の予測を実行して予測結果を返す
    """
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
    
    # モデル学習（ベース設定）
    predictor = MultiTargetPredictor(
        surface_specific=True,
        use_v44_residual=True,
        regularization_level='strong',
        use_early_stopping=False,
        fixed_iterations=50
    )
    predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
    
    # 予測（荒れ度分析を含む）
    test_preds = predictor.predict(test_f)
    
    # 実績データをマージ
    test_preds = test_preds.merge(
        test_f[['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity',
                'horse_close_loss_rate', 'horse_margin_std']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # 払戻をマージ
    fukusho = returns[returns['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']]
    test_preds = test_preds.merge(fukusho, on=['race_id', 'horse_number'], how='left', suffixes=('', '_fukusho'))
    test_preds['fukusho_payout'] = test_preds['payout'].fillna(0) / 100
    
    umaren = returns[returns['bet_type'] == 'umaren'][['race_id', 'payout']].drop_duplicates('race_id')
    umaren.columns = ['race_id', 'umaren_payout']
    test_preds = test_preds.merge(umaren, on='race_id', how='left')
    test_preds['umaren_payout'] = test_preds['umaren_payout'].fillna(0) / 100
    
    test_preds['year'] = test_year
    
    return test_preds


def calc_roi_with_params(preds_df, volatile_th, moderate_th, v44_volatile_w, v44_moderate_w, v44_stable_w):
    """
    指定パラメータでROIを計算
    """
    df = preds_df.copy()
    
    # 荒れ度を再分類
    df['vol_type'] = df['race_place_prob_std'].apply(
        lambda x: 'volatile' if x < volatile_th 
                  else ('moderate' if x < moderate_th else 'stable')
    )
    
    # V4.4ウェイトマップ
    weight_map = {
        'volatile': v44_volatile_w,
        'moderate': v44_moderate_w,
        'stable': v44_stable_w
    }
    df['v44_w'] = df['vol_type'].map(weight_map)
    
    # 適応型スコアを再計算
    df['adaptive_score'] = (
        df['win_prob'] * (1 - df['v44_w'] - 0.20) +
        df['v44_score'] * df['v44_w'] +
        df['top2_prob'] * 0.10 +
        df['place_prob'] * 0.10
    )
    
    results = {}
    
    # === 単勝ROI（adaptive_score Top1） ===
    df['rank_adaptive'] = df.groupby('race_id')['adaptive_score'].rank(ascending=False, method='first')
    top1 = df[df['rank_adaptive'] == 1].copy()
    
    hits = top1[top1['finish_position'] == 1]
    results['tansho_roi'] = hits['win_odds'].sum() / len(top1) * 100 if len(top1) > 0 else 0
    results['tansho_hit_rate'] = len(hits) / len(top1) * 100 if len(top1) > 0 else 0
    
    # === 複勝ROI（place_prob Top1） ===
    df['rank_place'] = df.groupby('race_id')['place_prob'].rank(ascending=False, method='first')
    top1_place = df[df['rank_place'] == 1].copy()
    
    results['fukusho_roi'] = top1_place['fukusho_payout'].sum() / len(top1_place) * 100 if len(top1_place) > 0 else 0
    results['fukusho_hit_rate'] = (top1_place['finish_position'] <= 3).mean() * 100
    
    # === 荒れ度別ROI ===
    for vol_type in ['volatile', 'moderate', 'stable']:
        subset = top1[top1['vol_type'] == vol_type]
        if len(subset) > 0:
            type_hits = subset[subset['finish_position'] == 1]
            results[f'{vol_type}_roi'] = type_hits['win_odds'].sum() / len(subset) * 100
            results[f'{vol_type}_count'] = len(subset)
        else:
            results[f'{vol_type}_roi'] = 0
            results[f'{vol_type}_count'] = 0
    
    # === 馬連ROI（top2_prob Top2） ===
    df['rank_top2'] = df.groupby('race_id')['top2_prob'].rank(ascending=False, method='first')
    
    umaren_hits = 0
    umaren_total_payout = 0
    race_count = 0
    
    for race_id in df['race_id'].unique():
        race_df = df[df['race_id'] == race_id]
        top2_pred = race_df[race_df['rank_top2'] <= 2]
        
        if len(top2_pred) < 2:
            continue
        
        race_count += 1
        pred_horses = set(top2_pred['horse_number'].values)
        actual_top2 = race_df[race_df['finish_position'] <= 2]
        actual_horses = set(actual_top2['horse_number'].values)
        
        if pred_horses == actual_horses:
            umaren_hits += 1
            umaren_total_payout += race_df['umaren_payout'].iloc[0]
    
    results['umaren_roi'] = umaren_total_payout / race_count * 100 if race_count > 0 else 0
    results['umaren_hit_rate'] = umaren_hits / race_count * 100 if race_count > 0 else 0
    results['umaren_races'] = race_count
    
    return results


def leave_one_year_out_cv(all_predictions, param_grid):
    """
    Leave-One-Year-Out Cross Validation
    
    各年を順番にテストセットとして留め置き、
    残りの年でパラメータを最適化
    """
    years = sorted(all_predictions['year'].unique())
    
    cv_results = []
    
    for test_year in years:
        logger.info(f"\n=== Leave-Out Year: {test_year} ===")
        
        # 訓練年（テスト年以外）
        train_years = [y for y in years if y != test_year]
        train_preds = all_predictions[all_predictions['year'].isin(train_years)]
        test_preds = all_predictions[all_predictions['year'] == test_year]
        
        # 訓練データで最適パラメータを探索
        best_params = None
        best_train_roi = 0
        
        for params in param_grid:
            train_result = calc_roi_with_params(train_preds, *params)
            train_roi = train_result['tansho_roi']
            
            if train_roi > best_train_roi:
                best_train_roi = train_roi
                best_params = params
        
        # テストデータで評価
        test_result = calc_roi_with_params(test_preds, *best_params)
        
        cv_results.append({
            'test_year': test_year,
            'best_volatile_th': best_params[0],
            'best_moderate_th': best_params[1],
            'best_v44_volatile_w': best_params[2],
            'best_v44_moderate_w': best_params[3],
            'best_v44_stable_w': best_params[4],
            'train_roi': best_train_roi,
            'test_tansho_roi': test_result['tansho_roi'],
            'test_fukusho_roi': test_result['fukusho_roi'],
            'test_umaren_roi': test_result['umaren_roi'],
            'test_volatile_roi': test_result['volatile_roi'],
            'test_stable_roi': test_result['stable_roi'],
        })
        
        logger.info(f"  最適パラメータ: volatile_th={best_params[0]:.2f}, moderate_th={best_params[1]:.2f}")
        logger.info(f"  V4.4ウェイト: volatile={best_params[2]:.2f}, moderate={best_params[3]:.2f}, stable={best_params[4]:.2f}")
        logger.info(f"  Train ROI: {best_train_roi:.1f}% → Test ROI: {test_result['tansho_roi']:.1f}%")
    
    return pd.DataFrame(cv_results)


def grid_search_best_strategy(all_predictions):
    """
    グリッドサーチで最適戦略を探索
    """
    logger.info("\n" + "="*60)
    logger.info("グリッドサーチ: 最適戦略探索")
    logger.info("="*60)
    
    # パラメータグリッド
    volatile_ths = [0.06, 0.08, 0.10, 0.12, 0.15]
    moderate_ths = [0.15, 0.18, 0.20, 0.22]
    v44_volatile_ws = [0.30, 0.40, 0.50]
    v44_moderate_ws = [0.30, 0.35, 0.40]
    v44_stable_ws = [0.25, 0.30, 0.35]
    
    param_grid = list(product(volatile_ths, moderate_ths, v44_volatile_ws, v44_moderate_ws, v44_stable_ws))
    
    # 有効なパラメータのみ（volatile_th < moderate_th）
    param_grid = [p for p in param_grid if p[0] < p[1]]
    
    logger.info(f"  パラメータ組み合わせ数: {len(param_grid)}")
    
    # Leave-One-Year-Out CV
    cv_results = leave_one_year_out_cv(all_predictions, param_grid)
    
    return cv_results


def analyze_bet_type_strategies(all_predictions):
    """
    券種別の最適戦略を分析
    """
    logger.info("\n" + "="*60)
    logger.info("券種別 最適戦略分析")
    logger.info("="*60)
    
    df = all_predictions.copy()
    
    # === 戦略1: 単勝（従来） ===
    df['rank_ensemble'] = df.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    top1_ensemble = df[df['rank_ensemble'] == 1]
    
    ensemble_roi = top1_ensemble[top1_ensemble['finish_position'] == 1]['win_odds'].sum() / len(top1_ensemble) * 100
    
    # === 戦略2: 複勝（place_prob Top1） ===
    df['rank_place'] = df.groupby('race_id')['place_prob'].rank(ascending=False, method='first')
    top1_place = df[df['rank_place'] == 1]
    
    place_roi = top1_place['fukusho_payout'].sum() / len(top1_place) * 100
    
    # === 戦略3: 複勝（place_prob Top2も購入） ===
    top2_place = df[df['rank_place'] <= 2]
    place_top2_roi = top2_place['fukusho_payout'].sum() / len(top2_place) * 100
    
    # === 戦略4: 条件付き複勝（stableレースのみ） ===
    stable_races = df[df['race_volatility_type'] == 'stable']
    stable_top1 = stable_races[stable_races['rank_place'] == 1]
    stable_place_roi = stable_top1['fukusho_payout'].sum() / len(stable_top1) * 100 if len(stable_top1) > 0 else 0
    
    # === 戦略5: 惜敗馬狙い（close_loss_rate高い馬をフィルタ） ===
    # close_loss_rateが上位25%の馬のみを対象
    if 'horse_close_loss_rate' in df.columns:
        threshold = df['horse_close_loss_rate'].quantile(0.75)
        close_losers = df[df['horse_close_loss_rate'] >= threshold]
        close_losers['rank_in_close'] = close_losers.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
        top1_close = close_losers[close_losers['rank_in_close'] == 1]
        
        if len(top1_close) > 0:
            close_loser_roi = top1_close[top1_close['finish_position'] == 1]['win_odds'].sum() / len(top1_close) * 100
        else:
            close_loser_roi = 0
    else:
        close_loser_roi = 0
    
    # 結果出力
    logger.info("\n【券種別ROI比較】")
    logger.info("-" * 40)
    logger.info(f"  1. 単勝（ensemble Top1）:        {ensemble_roi:.1f}%")
    logger.info(f"  2. 複勝（place_prob Top1）:      {place_roi:.1f}%")
    logger.info(f"  3. 複勝（place_prob Top2も）:    {place_top2_roi:.1f}%")
    logger.info(f"  4. 複勝（stableレースのみ）:     {stable_place_roi:.1f}%")
    logger.info(f"  5. 単勝（惜敗馬フィルタ）:       {close_loser_roi:.1f}%")
    
    return {
        'ensemble_tansho': ensemble_roi,
        'place_top1': place_roi,
        'place_top2': place_top2_roi,
        'stable_place': stable_place_roi,
        'close_loser': close_loser_roi,
    }


def find_optimal_combined_strategy(all_predictions):
    """
    複合戦略の最適化
    
    stableレースは単勝、volatileレースは複勝など
    """
    logger.info("\n" + "="*60)
    logger.info("複合戦略 最適化")
    logger.info("="*60)
    
    df = all_predictions.copy()
    
    # 各レースタイプ別に最適券種を選択
    strategies = [
        ('all_tansho', {'volatile': 'tansho', 'moderate': 'tansho', 'stable': 'tansho'}),
        ('all_fukusho', {'volatile': 'fukusho', 'moderate': 'fukusho', 'stable': 'fukusho'}),
        ('stable_tansho_else_fukusho', {'volatile': 'fukusho', 'moderate': 'fukusho', 'stable': 'tansho'}),
        ('volatile_fukusho_else_tansho', {'volatile': 'fukusho', 'moderate': 'tansho', 'stable': 'tansho'}),
        ('moderate_tansho_only', {'volatile': 'skip', 'moderate': 'tansho', 'stable': 'skip'}),
        ('stable_tansho_only', {'volatile': 'skip', 'moderate': 'skip', 'stable': 'tansho'}),
    ]
    
    df['rank_ensemble'] = df.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    df['rank_place'] = df.groupby('race_id')['place_prob'].rank(ascending=False, method='first')
    
    results = []
    
    for strategy_name, bet_map in strategies:
        total_bet = 0
        total_return = 0
        
        for vol_type in ['volatile', 'moderate', 'stable']:
            subset = df[(df['race_volatility_type'] == vol_type) & (df['rank_ensemble'] == 1)]
            bet_type = bet_map[vol_type]
            
            if bet_type == 'skip':
                continue
            elif bet_type == 'tansho':
                total_bet += len(subset)
                total_return += subset[subset['finish_position'] == 1]['win_odds'].sum()
            elif bet_type == 'fukusho':
                # place_prob Top1を使用
                place_subset = df[(df['race_volatility_type'] == vol_type) & (df['rank_place'] == 1)]
                total_bet += len(place_subset)
                total_return += place_subset['fukusho_payout'].sum()
        
        roi = total_return / total_bet * 100 if total_bet > 0 else 0
        results.append({
            'strategy': strategy_name,
            'bet_count': total_bet,
            'roi': roi,
        })
        
        logger.info(f"  {strategy_name}: ROI {roi:.1f}% (n={total_bet})")
    
    return pd.DataFrame(results)


def main():
    logger.info("="*60)
    logger.info("クロスバリデーション最適化 & 戦略分析")
    logger.info("="*60)
    
    # データ読み込み
    races, pedigrees, corners, race_details, horses, returns = load_data()
    logger.info(f"データ件数: {len(races):,}")
    
    # 各年の予測を生成
    test_years = [2020, 2021, 2022, 2023, 2024]
    all_predictions = []
    
    for year in test_years:
        logger.info(f"\n{year}年の予測を生成中...")
        preds = run_single_year_prediction(races, pedigrees, corners, race_details, horses, returns, year)
        if preds is not None:
            all_predictions.append(preds)
    
    all_predictions = pd.concat(all_predictions, ignore_index=True)
    logger.info(f"\n全予測件数: {len(all_predictions):,}")
    
    # グリッドサーチCV
    cv_results = grid_search_best_strategy(all_predictions)
    
    # 券種別戦略分析
    bet_type_results = analyze_bet_type_strategies(all_predictions)
    
    # 複合戦略分析
    combined_results = find_optimal_combined_strategy(all_predictions)
    
    # サマリー
    logger.info("\n" + "="*60)
    logger.info("最終サマリー")
    logger.info("="*60)
    
    logger.info("\n【Leave-One-Year-Out CV結果】")
    logger.info(f"  平均Test単勝ROI: {cv_results['test_tansho_roi'].mean():.1f}%")
    logger.info(f"  平均Test複勝ROI: {cv_results['test_fukusho_roi'].mean():.1f}%")
    logger.info(f"  平均Test馬連ROI: {cv_results['test_umaren_roi'].mean():.1f}%")
    
    logger.info("\n【最適閾値（CV平均）】")
    logger.info(f"  volatile_th: {cv_results['best_volatile_th'].mean():.2f}")
    logger.info(f"  moderate_th: {cv_results['best_moderate_th'].mean():.2f}")
    logger.info(f"  V4.4 volatile: {cv_results['best_v44_volatile_w'].mean():.2f}")
    logger.info(f"  V4.4 moderate: {cv_results['best_v44_moderate_w'].mean():.2f}")
    logger.info(f"  V4.4 stable: {cv_results['best_v44_stable_w'].mean():.2f}")
    
    # 結果保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    cv_results.to_csv(output_dir / "cv_optimization_results.csv", index=False, encoding='utf-8-sig')
    combined_results.to_csv(output_dir / "combined_strategy_results.csv", index=False, encoding='utf-8-sig')
    
    logger.info(f"\n結果保存完了")


if __name__ == "__main__":
    main()

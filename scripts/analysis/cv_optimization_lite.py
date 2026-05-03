#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
クロスバリデーション最適化（軽量版）

【変更点】
- 馬連計算を省略（重すぎる）
- パラメータグリッドを削減
- ベクトル化を徹底

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


def load_predictions():
    """既存の予測結果を読み込み（前回のバックテストで生成済み）"""
    output_dir = project_root / "outputs/analysis"
    csv_path = output_dir / "volatility_backtest_results.csv"
    
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return None


def calc_roi_with_params_fast(preds_df, volatile_th, moderate_th, v44_volatile_w, v44_moderate_w, v44_stable_w):
    """
    高速版ROI計算（ベクトル化徹底）
    """
    df = preds_df.copy()
    
    # 荒れ度を再分類
    df['vol_type'] = np.where(
        df['race_place_prob_std'] < volatile_th, 'volatile',
        np.where(df['race_place_prob_std'] < moderate_th, 'moderate', 'stable')
    )
    
    # V4.4ウェイトを直接計算
    df['v44_w'] = np.where(
        df['vol_type'] == 'volatile', v44_volatile_w,
        np.where(df['vol_type'] == 'moderate', v44_moderate_w, v44_stable_w)
    )
    
    # 適応型スコアを再計算
    df['adaptive_score'] = (
        df['win_prob'] * (1 - df['v44_w'] - 0.20) +
        df['v44_score'] * df['v44_w'] +
        df['top2_prob'] * 0.10 +
        df['place_prob'] * 0.10
    )
    
    # ランキング（ベクトル化）
    df['rank_adaptive'] = df.groupby('race_id')['adaptive_score'].rank(ascending=False, method='first')
    df['rank_place'] = df.groupby('race_id')['place_prob'].rank(ascending=False, method='first')
    
    results = {}
    
    # 単勝ROI（adaptive_score Top1）
    top1 = df[df['rank_adaptive'] == 1]
    hits = top1[top1['finish_position'] == 1]
    results['tansho_roi'] = hits['win_odds'].sum() / len(top1) * 100 if len(top1) > 0 else 0
    results['tansho_hit_rate'] = len(hits) / len(top1) * 100 if len(top1) > 0 else 0
    
    # 複勝ROI（place_prob Top1）
    top1_place = df[df['rank_place'] == 1]
    results['fukusho_roi'] = top1_place['fukusho_payout'].sum() / len(top1_place) * 100 if len(top1_place) > 0 else 0
    
    # 荒れ度別ROI
    for vol_type in ['volatile', 'moderate', 'stable']:
        subset = top1[top1['vol_type'] == vol_type]
        if len(subset) > 0:
            type_hits = subset[subset['finish_position'] == 1]
            results[f'{vol_type}_roi'] = type_hits['win_odds'].sum() / len(subset) * 100
            results[f'{vol_type}_count'] = len(subset)
        else:
            results[f'{vol_type}_roi'] = 0
            results[f'{vol_type}_count'] = 0
    
    return results


def run_cv_optimization(all_predictions):
    """
    Leave-One-Year-Out CV（軽量版）
    """
    years = sorted(all_predictions['year'].unique())
    
    # パラメータグリッド（削減版）
    volatile_ths = [0.08, 0.10, 0.12]
    moderate_ths = [0.16, 0.18, 0.20]
    v44_volatile_ws = [0.35, 0.45, 0.55]
    v44_moderate_ws = [0.30, 0.35, 0.40]
    v44_stable_ws = [0.25, 0.30]
    
    param_grid = [(v, m, vv, mv, sv) 
                  for v in volatile_ths 
                  for m in moderate_ths 
                  for vv in v44_volatile_ws 
                  for mv in v44_moderate_ws 
                  for sv in v44_stable_ws
                  if v < m]
    
    logger.info(f"パラメータ組み合わせ数: {len(param_grid)}")
    
    cv_results = []
    
    for test_year in years:
        logger.info(f"\n=== Leave-Out Year: {test_year} ===")
        
        train_years = [y for y in years if y != test_year]
        train_preds = all_predictions[all_predictions['year'].isin(train_years)]
        test_preds = all_predictions[all_predictions['year'] == test_year]
        
        # 訓練データで最適パラメータを探索
        best_params = None
        best_train_roi = 0
        
        for params in param_grid:
            train_result = calc_roi_with_params_fast(train_preds, *params)
            train_roi = train_result['tansho_roi']
            
            if train_roi > best_train_roi:
                best_train_roi = train_roi
                best_params = params
        
        # テストデータで評価
        test_result = calc_roi_with_params_fast(test_preds, *best_params)
        
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
            'test_volatile_roi': test_result['volatile_roi'],
            'test_moderate_roi': test_result['moderate_roi'],
            'test_stable_roi': test_result['stable_roi'],
        })
        
        logger.info(f"  最適パラメータ: volatile_th={best_params[0]:.2f}, moderate_th={best_params[1]:.2f}")
        logger.info(f"  V4.4ウェイト: volatile={best_params[2]:.2f}, moderate={best_params[3]:.2f}, stable={best_params[4]:.2f}")
        logger.info(f"  Train ROI: {best_train_roi:.1f}% → Test ROI: {test_result['tansho_roi']:.1f}%")
    
    return pd.DataFrame(cv_results)


def analyze_combined_strategies(all_predictions):
    """
    複合戦略分析
    """
    logger.info("\n" + "="*60)
    logger.info("複合戦略分析")
    logger.info("="*60)
    
    df = all_predictions.copy()
    
    # ランキング
    df['rank_ensemble'] = df.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    df['rank_place'] = df.groupby('race_id')['place_prob'].rank(ascending=False, method='first')
    
    strategies = [
        ('全単勝', {'volatile': 'tansho', 'moderate': 'tansho', 'stable': 'tansho'}),
        ('全複勝', {'volatile': 'fukusho', 'moderate': 'fukusho', 'stable': 'fukusho'}),
        ('stable→単勝、他→複勝', {'volatile': 'fukusho', 'moderate': 'fukusho', 'stable': 'tansho'}),
        ('volatile→複勝、他→単勝', {'volatile': 'fukusho', 'moderate': 'tansho', 'stable': 'tansho'}),
        ('moderate単勝のみ', {'volatile': 'skip', 'moderate': 'tansho', 'stable': 'skip'}),
        ('stable単勝のみ', {'volatile': 'skip', 'moderate': 'skip', 'stable': 'tansho'}),
        ('stable複勝のみ', {'volatile': 'skip', 'moderate': 'skip', 'stable': 'fukusho'}),
    ]
    
    results = []
    
    for strategy_name, bet_map in strategies:
        total_bet = 0
        total_return = 0
        
        for vol_type in ['volatile', 'moderate', 'stable']:
            bet_type = bet_map[vol_type]
            
            if bet_type == 'skip':
                continue
            elif bet_type == 'tansho':
                subset = df[(df['race_volatility_type'] == vol_type) & (df['rank_ensemble'] == 1)]
                total_bet += len(subset)
                total_return += subset[subset['finish_position'] == 1]['win_odds'].sum()
            elif bet_type == 'fukusho':
                subset = df[(df['race_volatility_type'] == vol_type) & (df['rank_place'] == 1)]
                total_bet += len(subset)
                total_return += subset['fukusho_payout'].sum()
        
        roi = total_return / total_bet * 100 if total_bet > 0 else 0
        results.append({
            'strategy': strategy_name,
            'bet_count': total_bet,
            'roi': roi,
        })
        
        logger.info(f"  {strategy_name}: ROI {roi:.1f}% (n={total_bet})")
    
    return pd.DataFrame(results)


def analyze_feature_impact(all_predictions):
    """
    特徴量の影響分析
    """
    logger.info("\n" + "="*60)
    logger.info("特徴量影響分析")
    logger.info("="*60)
    
    df = all_predictions.copy()
    df['rank_ensemble'] = df.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    top1 = df[df['rank_ensemble'] == 1]
    
    # close_loss_rate が高い馬のパフォーマンス
    if 'horse_close_loss_rate' in df.columns:
        valid_data = df[df['horse_close_loss_rate'].notna()]['horse_close_loss_rate']
        if len(valid_data) > 100:
            try:
                # 四分位に分割（重複がある場合は自動調整）
                df['close_loss_quartile'] = pd.qcut(
                    df['horse_close_loss_rate'].fillna(-1), 
                    q=4, 
                    labels=False,
                    duplicates='drop'
                )
                
                logger.info("\n  close_loss_rate別ROI:")
                for q in sorted(df['close_loss_quartile'].dropna().unique()):
                    subset = top1[top1['close_loss_quartile'] == q]
                    if len(subset) > 0:
                        hits = subset[subset['finish_position'] == 1]
                        roi = hits['win_odds'].sum() / len(subset) * 100
                        label = {0: 'Q1(低)', 1: 'Q2', 2: 'Q3', 3: 'Q4(高)'}.get(q, f'Q{q}')
                        logger.info(f"    {label}: ROI {roi:.1f}% (n={len(subset)})")
            except Exception as e:
                logger.warning(f"  close_loss_rate分析エラー: {e}")
    
    # margin_std が低い馬（安定した馬）
    if 'horse_margin_std' in df.columns:
        valid_data = df[df['horse_margin_std'].notna()]['horse_margin_std']
        if len(valid_data) > 100:
            try:
                df['margin_std_quartile'] = pd.qcut(
                    df['horse_margin_std'].fillna(df['horse_margin_std'].max()), 
                    q=4, 
                    labels=False,
                    duplicates='drop'
                )
                
                logger.info("\n  margin_std別ROI:")
                for q in sorted(df['margin_std_quartile'].dropna().unique()):
                    subset = top1[top1['margin_std_quartile'] == q]
                    if len(subset) > 0:
                        hits = subset[subset['finish_position'] == 1]
                        roi = hits['win_odds'].sum() / len(subset) * 100
                        label = {0: 'Q1(安定)', 1: 'Q2', 2: 'Q3', 3: 'Q4(不安定)'}.get(q, f'Q{q}')
                        logger.info(f"    {label}: ROI {roi:.1f}% (n={len(subset)})")
            except Exception as e:
                logger.warning(f"  margin_std分析エラー: {e}")


def main():
    logger.info("="*60)
    logger.info("クロスバリデーション最適化（軽量版）")
    logger.info("="*60)
    
    # 前回のバックテストで生成した予測結果を読み込む
    # ない場合は新規生成（重いので省略）
    
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
    
    logger.info(f"データ件数: {len(races):,}")
    
    # 2024年のみで軽量検証
    test_year = 2024
    test_start_dt = pd.to_datetime('2024-01-01')
    test_end_dt = pd.to_datetime('2024-12-31')
    train_end = test_start_dt - timedelta(days=1)
    valid_start = train_end - timedelta(days=365)
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
    
    logger.info(f"Train: {len(train):,} / Test: {len(test):,}")
    
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
                'horse_close_loss_rate', 'horse_margin_std']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # 複勝払戻をマージ
    fukusho = returns[returns['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']]
    test_preds = test_preds.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    test_preds['fukusho_payout'] = test_preds['payout'].fillna(0) / 100
    
    test_preds['year'] = 2024
    
    # 複合戦略分析
    strategy_results = analyze_combined_strategies(test_preds)
    
    # 特徴量影響分析
    analyze_feature_impact(test_preds)
    
    # パラメータグリッドサーチ（2024年内でtrain/test分割）
    logger.info("\n" + "="*60)
    logger.info("パラメータグリッドサーチ（2024年前半→後半）")
    logger.info("="*60)
    
    test_preds['month'] = pd.to_datetime(test_preds['race_id'].str[:8]).dt.month
    train_2024 = test_preds[test_preds['month'] <= 6]
    test_2024 = test_preds[test_preds['month'] > 6]
    
    logger.info(f"Train: {len(train_2024):,} / Test: {len(test_2024):,}")
    
    volatile_ths = [0.08, 0.10, 0.12]
    moderate_ths = [0.16, 0.18, 0.20]
    v44_volatile_ws = [0.35, 0.45, 0.55]
    v44_moderate_ws = [0.30, 0.35, 0.40]
    v44_stable_ws = [0.25, 0.30]
    
    param_grid = [(v, m, vv, mv, sv) 
                  for v in volatile_ths 
                  for m in moderate_ths 
                  for vv in v44_volatile_ws 
                  for mv in v44_moderate_ws 
                  for sv in v44_stable_ws
                  if v < m]
    
    best_params = None
    best_train_roi = 0
    
    for params in param_grid:
        train_result = calc_roi_with_params_fast(train_2024, *params)
        if train_result['tansho_roi'] > best_train_roi:
            best_train_roi = train_result['tansho_roi']
            best_params = params
    
    test_result = calc_roi_with_params_fast(test_2024, *best_params)
    
    logger.info(f"  最適パラメータ: volatile_th={best_params[0]:.2f}, moderate_th={best_params[1]:.2f}")
    logger.info(f"  V4.4ウェイト: volatile={best_params[2]:.2f}, moderate={best_params[3]:.2f}, stable={best_params[4]:.2f}")
    logger.info(f"  Train ROI: {best_train_roi:.1f}% → Test ROI: {test_result['tansho_roi']:.1f}%")
    logger.info(f"  Test volatile ROI: {test_result['volatile_roi']:.1f}%")
    logger.info(f"  Test stable ROI: {test_result['stable_roi']:.1f}%")
    
    # 結果保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    strategy_results.to_csv(output_dir / "combined_strategy_results.csv", index=False, encoding='utf-8-sig')
    
    logger.info(f"\n結果保存完了")


if __name__ == "__main__":
    main()

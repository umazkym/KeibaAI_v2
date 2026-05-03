#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
荒れ度検出閾値最適化 & 戦略効果バックテスト

【目的】
実際のMultiTargetPredictor予測結果を使用して：
1. 荒れ度検出の最適閾値を決定
2. 適応型戦略のROI効果を検証

【検証期間】
- Train: 2019年以前
- Test: 2020-2024年（5年間Walk-forward）

作成日: 2026-01-11
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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


def run_backtest(races, pedigrees, corners, race_details, horses, returns):
    """
    5年間のWalk-forward検証
    """
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    from keibaai.src.models.multi_target_predictor import MultiTargetPredictor
    from keibaai.src.features.time_margin_features import TimeMarginFeatureEngineer
    
    test_years = [
        ('2020', '2020-01-01', '2020-12-31'),
        ('2021', '2021-01-01', '2021-12-31'),
        ('2022', '2022-01-01', '2022-12-31'),
        ('2023', '2023-01-01', '2023-12-31'),
        ('2024', '2024-01-01', '2024-12-31'),
    ]
    
    all_results = []
    
    for year_idx, (test_year, test_start, test_end) in enumerate(test_years):
        logger.info(f"\n{'='*60}")
        logger.info(f"[{year_idx+1}/5] {test_year}年 検証")
        logger.info("="*60)
        
        test_start_dt = pd.to_datetime(test_start)
        test_end_dt = pd.to_datetime(test_end)
        train_end = test_start_dt - timedelta(days=1)
        valid_start = train_end - timedelta(days=365)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
        
        if len(test) < 5000:
            logger.warning(f"  テストデータ不足: {len(test)}")
            continue
        
        logger.info(f"  Train: {len(train):,} / Test: {len(test):,}")
        
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
        
        # モデル学習（V4.4なし、強正則化、固定イテレーション）
        predictor = MultiTargetPredictor(
            surface_specific=True,
            use_v44_residual=True,  # V4.4を含める
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
        
        # 複勝払戻をマージ
        fukusho = returns[returns['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']]
        test_preds = test_preds.merge(fukusho, on=['race_id', 'horse_number'], how='left')
        test_preds['fukusho_payout'] = test_preds['payout'].fillna(0) / 100
        
        # 各ROI計算
        year_result = calc_roi_by_strategy(test_preds, test_year)
        all_results.append(year_result)
        
        # 結果出力
        logger.info(f"\n  【{test_year}年 結果】")
        logger.info(f"    固定戦略（ensemble）:   単勝ROI {year_result['fixed_tansho_roi']:.1f}%")
        logger.info(f"    適応型戦略（adaptive）: 単勝ROI {year_result['adaptive_tansho_roi']:.1f}%")
        logger.info(f"    差: {year_result['adaptive_tansho_roi'] - year_result['fixed_tansho_roi']:+.1f}pt")
        
        # 荒れ度別の内訳
        for vol_type in ['volatile', 'moderate', 'stable']:
            count = year_result.get(f'{vol_type}_count', 0)
            roi = year_result.get(f'{vol_type}_roi', 0)
            if count > 0:
                logger.info(f"      {vol_type}: ROI {roi:.1f}% (n={count})")
    
    return pd.DataFrame(all_results)


def calc_roi_by_strategy(preds_df, year):
    """
    戦略別ROIを計算
    
    1. 固定戦略: ensemble_score Top1 → 単勝
    2. 適応型戦略: 
       - volatile: adaptive_ensemble_score Top1 → 複勝（または単勝）
       - stable: adaptive_ensemble_score Top1 → 単勝
    """
    result = {'year': year}
    
    # === 固定戦略（ensemble_score） ===
    preds_df['rank_ensemble'] = preds_df.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    top1_ensemble = preds_df[preds_df['rank_ensemble'] == 1].copy()
    
    # 単勝ROI
    tansho_hits = top1_ensemble[top1_ensemble['finish_position'] == 1]
    result['fixed_tansho_roi'] = tansho_hits['win_odds'].sum() / len(top1_ensemble) * 100 if len(top1_ensemble) > 0 else 0
    result['fixed_tansho_hit_rate'] = len(tansho_hits) / len(top1_ensemble) * 100 if len(top1_ensemble) > 0 else 0
    
    # === 適応型戦略（adaptive_ensemble_score） ===
    preds_df['rank_adaptive'] = preds_df.groupby('race_id')['adaptive_ensemble_score'].rank(ascending=False, method='first')
    top1_adaptive = preds_df[preds_df['rank_adaptive'] == 1].copy()
    
    # 単勝ROI（adaptive）
    tansho_hits_adaptive = top1_adaptive[top1_adaptive['finish_position'] == 1]
    result['adaptive_tansho_roi'] = tansho_hits_adaptive['win_odds'].sum() / len(top1_adaptive) * 100 if len(top1_adaptive) > 0 else 0
    
    # 荒れ度別の内訳
    for vol_type in ['volatile', 'moderate', 'stable']:
        subset = top1_adaptive[top1_adaptive['race_volatility_type'] == vol_type]
        if len(subset) > 0:
            hits = subset[subset['finish_position'] == 1]
            roi = hits['win_odds'].sum() / len(subset) * 100
            result[f'{vol_type}_roi'] = roi
            result[f'{vol_type}_count'] = len(subset)
            result[f'{vol_type}_hit_rate'] = len(hits) / len(subset) * 100
    
    # 複勝戦略（volatileレースのみ）
    volatile_subset = top1_adaptive[top1_adaptive['race_volatility_type'] == 'volatile']
    if len(volatile_subset) > 0:
        fukusho_total = volatile_subset['fukusho_payout'].sum()
        result['volatile_fukusho_roi'] = fukusho_total / len(volatile_subset) * 100
    
    # place_prob Top1の複勝ROI（System E方式）
    preds_df['rank_place'] = preds_df.groupby('race_id')['place_prob'].rank(ascending=False, method='first')
    top1_place = preds_df[preds_df['rank_place'] == 1].copy()
    fukusho_total = top1_place['fukusho_payout'].sum()
    result['place_prob_fukusho_roi'] = fukusho_total / len(top1_place) * 100 if len(top1_place) > 0 else 0
    
    return result


def analyze_threshold_sensitivity(preds_df):
    """
    荒れ度閾値の感度分析
    """
    logger.info("\n" + "="*60)
    logger.info("荒れ度閾値感度分析")
    logger.info("="*60)
    
    # place_prob_std の分布を確認
    race_level = preds_df.groupby('race_id').first()[['race_place_prob_std']].reset_index()
    
    logger.info(f"  place_prob_std統計:")
    logger.info(f"    平均: {race_level['race_place_prob_std'].mean():.4f}")
    logger.info(f"    中央: {race_level['race_place_prob_std'].median():.4f}")
    logger.info(f"    25%ile: {race_level['race_place_prob_std'].quantile(0.25):.4f}")
    logger.info(f"    75%ile: {race_level['race_place_prob_std'].quantile(0.75):.4f}")
    
    # 1番人気が飛んだレースとの相関
    preds_df['fav1_upset'] = (
        (preds_df['popularity'] == 1) & 
        (preds_df['finish_position'] > 3)
    ).astype(int)
    
    race_upset = preds_df.groupby('race_id')['fav1_upset'].max().reset_index()
    race_level = race_level.merge(race_upset, on='race_id')
    
    # 閾値別の荒れ予測精度
    thresholds = [0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20]
    
    logger.info("\n  閾値別 荒れ予測精度:")
    logger.info("  閾値   | Precision | Recall  | 予測数")
    logger.info("  -------|-----------|---------|-------")
    
    for th in thresholds:
        predicted = race_level['race_place_prob_std'] < th
        
        tp = ((predicted) & (race_level['fav1_upset'] == 1)).sum()
        fp = ((predicted) & (race_level['fav1_upset'] == 0)).sum()
        fn = ((~predicted) & (race_level['fav1_upset'] == 1)).sum()
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        
        logger.info(f"  {th:.2f}    | {precision*100:5.1f}%    | {recall*100:5.1f}%  | {predicted.sum():,}")


def main():
    logger.info("="*60)
    logger.info("荒れ度検出閾値最適化 & 戦略効果バックテスト")
    logger.info("="*60)
    
    # データ読み込み
    races, pedigrees, corners, race_details, horses, returns = load_data()
    logger.info(f"データ件数: {len(races):,}")
    
    # バックテスト実行
    results_df = run_backtest(races, pedigrees, corners, race_details, horses, returns)
    
    # サマリー
    logger.info("\n" + "="*60)
    logger.info("5年間サマリー")
    logger.info("="*60)
    
    logger.info(f"\n  固定戦略（ensemble）:")
    logger.info(f"    平均単勝ROI: {results_df['fixed_tansho_roi'].mean():.1f}%")
    
    logger.info(f"\n  適応型戦略（adaptive）:")
    logger.info(f"    平均単勝ROI: {results_df['adaptive_tansho_roi'].mean():.1f}%")
    
    logger.info(f"\n  place_prob複勝戦略:")
    logger.info(f"    平均複勝ROI: {results_df['place_prob_fukusho_roi'].mean():.1f}%")
    
    diff = results_df['adaptive_tansho_roi'].mean() - results_df['fixed_tansho_roi'].mean()
    logger.info(f"\n  適応型 vs 固定: {diff:+.1f}pt")
    
    # 結果保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "volatility_backtest_results.csv"
    results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"\n結果保存: {output_path}")


if __name__ == "__main__":
    main()

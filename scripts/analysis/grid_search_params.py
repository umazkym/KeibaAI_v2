#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3指数モデル パラメータグリッドサーチ

【テスト対象】
1. iterations: 50, 100, 150
2. V4.4ウェイト: 0%, 5%, 10%
→ 9パターンの組み合わせを全てテスト

【深い分析ポイント】
1. ROI × Gap の2軸で最適パラメータを特定
2. 年別のパフォーマンス推移
3. 芝/ダート別の特性

【リーク防止チェック】
- finish_position: shift(1)適用
- win_odds: V4.4のみ使用
- 学習データ < テスト開始日
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
from datetime import datetime, timedelta
import sys
import warnings
import logging
import time
import itertools

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# ===== ログ設定 =====
log_dir = project_root / "outputs/logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"grid_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(message)s'))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.models.multi_target_predictor import MultiTargetPredictor, calc_multi_roi, calc_roi_by_prob


# ===== グリッドサーチパラメータ =====
ITERATIONS_LIST = [50, 100, 150]
V44_WEIGHTS = [0.00, 0.05, 0.10]


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


def compute_v16_features(df, train_end_date):
    """V16特徴量をリークフリーで計算"""
    df = df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
    
    if 'is_win' not in df.columns:
        df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # shift(1)で前レースのみ使用
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    df['horse_prev_winrate'] = df.groupby('horse_id')['is_win'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    
    df = df.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
    df['jockey_prev_winrate'] = df.groupby('jockey_id')['is_win'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    
    df = df.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
    df['trainer_prev_winrate'] = df.groupby('trainer_id')['is_win'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
    
    return df


def calc_ensemble_score(pred_df, v44_weight):
    """指定V4.4ウェイトでensemble_scoreを計算"""
    remaining = 1.0 - v44_weight
    return (
        pred_df['win_prob'] * (remaining * 0.50) +
        pred_df['v44_score'] * v44_weight +
        pred_df['top2_prob'] * (remaining * 0.30) +
        pred_df['place_prob'] * (remaining * 0.20)
    )


def run_single_test(races, pedigrees, corners, race_details, horses, returns,
                    iterations, v44_weight, engine_cache=None):
    """
    単一パラメータセットでのテスト実行
    
    Returns:
        dict: 年別結果のリスト
    """
    test_years = [
        ('2020', '2020-01-01', '2020-12-31'),
        ('2021', '2021-01-01', '2021-12-31'),
        ('2022', '2022-01-01', '2022-12-31'),
        ('2023', '2023-01-01', '2023-12-31'),
        ('2024', '2024-01-01', '2024-12-31'),
        ('2025', '2025-01-01', '2025-12-31'),
    ]
    
    results = []
    
    for test_year, test_start, test_end in test_years:
        test_start_dt = pd.to_datetime(test_start)
        test_end_dt = pd.to_datetime(test_end)
        train_end = test_start_dt - timedelta(days=1)
        valid_start = train_end - timedelta(days=365)
        
        # データ分割
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
        
        if len(test) < 5000 or len(train) < 100000:
            continue
        
        # 特徴量エンジン fit
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        # 一括transform
        all_data = pd.concat([train, test], ignore_index=True)
        all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
        all_data_f = engine.transform(all_data)
        all_data_f = compute_v16_features(all_data_f, train_end)
        
        # 分割
        train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
        valid_f = train_f[(train_f['race_date'] > valid_start) & (train_f['race_date'] <= train_end)].copy()
        test_f = all_data_f[(all_data_f['race_date'] >= test_start_dt) & (all_data_f['race_date'] <= test_end_dt)].copy()
        
        # 特徴量列
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        v16_features = ['horse_prev_winrate', 'jockey_prev_winrate', 'trainer_prev_winrate', 'prev_finish']
        feature_cols = list(dict.fromkeys(base_features + [f for f in v16_features if f in train_f.columns]))
        
        # モデル学習
        predictor = MultiTargetPredictor(
            surface_specific=True,
            use_v44_residual=True,
            regularization_level='strong',
            use_early_stopping=False,
            fixed_iterations=iterations
        )
        predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
        
        # 予測
        valid_preds = predictor.predict(valid_f)
        test_preds = predictor.predict(test_f)
        
        # ROI計算
        valid_roi_prob = calc_roi_by_prob(valid_preds, valid_f, returns)
        test_roi_prob = calc_roi_by_prob(test_preds, test_f, returns)
        
        # ensemble score計算
        valid_preds['ensemble'] = calc_ensemble_score(valid_preds, v44_weight)
        test_preds['ensemble'] = calc_ensemble_score(test_preds, v44_weight)
        
        valid_roi_ens = calc_multi_roi(valid_preds, valid_f, returns, score_col='ensemble')
        test_roi_ens = calc_multi_roi(test_preds, test_f, returns, score_col='ensemble')
        
        result = {
            'year': test_year,
            'iterations': iterations,
            'v44_weight': v44_weight,
            'win_valid_roi': valid_roi_prob['win_prob']['tansho_roi'],
            'win_test_roi': test_roi_prob['win_prob']['tansho_roi'],
            'win_gap': abs(valid_roi_prob['win_prob']['tansho_roi'] - test_roi_prob['win_prob']['tansho_roi']),
            'top2_valid_roi': valid_roi_prob['top2_prob']['tansho_roi'],
            'top2_test_roi': test_roi_prob['top2_prob']['tansho_roi'],
            'top2_gap': abs(valid_roi_prob['top2_prob']['tansho_roi'] - test_roi_prob['top2_prob']['tansho_roi']),
            'ensemble_valid_roi': valid_roi_ens['tansho_roi'],
            'ensemble_test_roi': test_roi_ens['tansho_roi'],
            'ensemble_gap': abs(valid_roi_ens['tansho_roi'] - test_roi_ens['tansho_roi']),
        }
        results.append(result)
    
    return results


def main():
    logger.info("=" * 70)
    logger.info("3指数モデル パラメータグリッドサーチ")
    logger.info("=" * 70)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    logger.info(f"データ: {len(races):,}件")
    
    all_results = []
    total_patterns = len(ITERATIONS_LIST) * len(V44_WEIGHTS)
    pattern_idx = 0
    
    total_start = time.time()
    
    for iterations in ITERATIONS_LIST:
        for v44_weight in V44_WEIGHTS:
            pattern_idx += 1
            pattern_start = time.time()
            
            logger.info(f"\n[{pattern_idx}/{total_patterns}] iterations={iterations}, V4.4={v44_weight*100:.0f}%")
            
            results = run_single_test(
                races, pedigrees, corners, race_details, horses, returns,
                iterations, v44_weight
            )
            all_results.extend(results)
            
            pattern_time = time.time() - pattern_start
            
            # 途中経過
            if results:
                avg_roi = np.mean([r['win_test_roi'] for r in results])
                avg_gap = np.mean([r['win_gap'] for r in results])
                logger.info(f"  → 平均ROI: {avg_roi:.1f}%, Gap: {avg_gap:.1f}% ({pattern_time/60:.1f}分)")
    
    total_time = time.time() - total_start
    
    # 結果保存
    results_df = pd.DataFrame(all_results)
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "grid_search_results.csv"
    results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    # ===== 深い分析 =====
    logger.info("\n" + "=" * 70)
    logger.info("【深い分析】パラメータ別サマリー")
    logger.info("=" * 70)
    
    summary_data = []
    for iterations in ITERATIONS_LIST:
        for v44_weight in V44_WEIGHTS:
            mask = (results_df['iterations'] == iterations) & (results_df['v44_weight'] == v44_weight)
            sub = results_df[mask]
            if len(sub) > 0:
                summary = {
                    'iterations': iterations,
                    'v44_weight': v44_weight,
                    'avg_win_roi': sub['win_test_roi'].mean(),
                    'avg_win_gap': sub['win_gap'].mean(),
                    'avg_top2_roi': sub['top2_test_roi'].mean(),
                    'avg_top2_gap': sub['top2_gap'].mean(),
                    'avg_ensemble_roi': sub['ensemble_test_roi'].mean(),
                    'avg_ensemble_gap': sub['ensemble_gap'].mean(),
                    'roi_gap_ratio': sub['win_test_roi'].mean() / (sub['win_gap'].mean() + 1),
                }
                summary_data.append(summary)
    
    summary_df = pd.DataFrame(summary_data)
    summary_df = summary_df.sort_values('roi_gap_ratio', ascending=False)
    
    logger.info("\n【ROI/Gap比率でソート（高いほど良い）】")
    logger.info(f"{'Iter':>6} | {'V4.4':>5} | {'Win ROI':>8} | {'Win Gap':>8} | {'Top2 ROI':>8} | {'Ratio':>6}")
    logger.info("-" * 60)
    
    for _, row in summary_df.iterrows():
        logger.info(f"{row['iterations']:>6} | {row['v44_weight']*100:>4.0f}% | {row['avg_win_roi']:>7.1f}% | {row['avg_win_gap']:>7.1f}% | {row['avg_top2_roi']:>7.1f}% | {row['roi_gap_ratio']:>6.2f}")
    
    # 最適パラメータ
    best = summary_df.iloc[0]
    logger.info(f"\n★ 推奨パラメータ: iterations={int(best['iterations'])}, V4.4={best['v44_weight']*100:.0f}%")
    logger.info(f"  ROI: {best['avg_win_roi']:.1f}%, Gap: {best['avg_win_gap']:.1f}%, Ratio: {best['roi_gap_ratio']:.2f}")
    
    # 年別分析
    logger.info("\n" + "=" * 70)
    logger.info("【年別パフォーマンス（最適パラメータ）】")
    logger.info("=" * 70)
    
    best_mask = (results_df['iterations'] == best['iterations']) & (results_df['v44_weight'] == best['v44_weight'])
    best_results = results_df[best_mask]
    
    for _, row in best_results.iterrows():
        logger.info(f"  {row['year']}: win {row['win_test_roi']:.1f}% (Gap {row['win_gap']:.1f}%), top2 {row['top2_test_roi']:.1f}%")
    
    # 既存システムとの比較
    logger.info("\n【既存システムとの比較】")
    logger.info(f"  既存System A+: 80.2%")
    logger.info(f"  最適パラメータ: {best['avg_win_roi']:.1f}%")
    logger.info(f"  差: {best['avg_win_roi'] - 80.2:+.1f}pt")
    
    logger.info(f"\n結果保存: {output_path}")
    logger.info(f"総処理時間: {total_time/60:.1f}分")
    logger.info("\n完了")


if __name__ == "__main__":
    main()

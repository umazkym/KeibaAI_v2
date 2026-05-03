#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3指数モデル改善版 - 年単位学習 + strong正則化 + iterations増加

【改善点】
1. 正則化: extreme → strong（アンダーフィット解消）
2. iterations: 80 → 150（予測力向上）
3. 学習単位: 四半期 → 年単位（既存System A+と同じ）

【リーク防止の厳格ルール】
1. 学習データ: race_date < テスト年開始日
2. V16特徴量: shift(1)で前レースのみ使用
3. Valid期間: テスト開始の1年前
4. early_stopping: 無効（Valid過適合防止）

【V4.4ウェイト】
15%に制限（Gap増大防止）
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
import argparse

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# ===== ログ設定 =====
log_dir = project_root / "outputs/logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"system_e_improved_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(logging.Formatter('%(message)s'))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

progress_logger = logging.getLogger('progress')
progress_logger.setLevel(logging.INFO)
progress_console = logging.StreamHandler()
progress_console.setFormatter(logging.Formatter('%(message)s'))
progress_logger.addHandler(progress_console)
progress_logger.addHandler(file_handler)

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.models.multi_target_predictor import MultiTargetPredictor, calc_multi_roi, calc_roi_by_prob


# ===== Ensembleウェイト（V4.4を15%に制限） =====
ENSEMBLE_WEIGHTS = {
    'v1_v44_15':      {'win': 0.50, 'v44': 0.15, 'top2': 0.20, 'place': 0.15},
    'v2_v44_10':      {'win': 0.50, 'v44': 0.10, 'top2': 0.25, 'place': 0.15},
    'v3_no_v44':      {'win': 0.50, 'v44': 0.00, 'top2': 0.30, 'place': 0.20},
    'v4_top2_focus':  {'win': 0.35, 'v44': 0.10, 'top2': 0.35, 'place': 0.20},
    'v5_balanced':    {'win': 0.40, 'v44': 0.10, 'top2': 0.30, 'place': 0.20},
}


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


def compute_v16_features_leakfree(df, train_end_date):
    """
    V16特徴量をリークフリーで計算
    
    【ロジック】
    - shift(1)で前レースの情報のみ使用
    - train_end以降のデータは累積に含めない
    """
    df = df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
    
    if 'is_win' not in df.columns:
        df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # ===== 馬の累積勝率（shift(1)でリークフリー） =====
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    df['horse_prev_winrate'] = df.groupby('horse_id')['is_win'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    
    # ===== 騎手の累積勝率 =====
    df = df.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
    df['jockey_prev_winrate'] = df.groupby('jockey_id')['is_win'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    
    # ===== 調教師の累積勝率 =====
    df = df.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
    df['trainer_prev_winrate'] = df.groupby('trainer_id')['is_win'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    
    # ===== 前走着順 =====
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
    
    return df


def calc_ensemble_score(pred_df, weights):
    """指定ウェイトでensemble_scoreを計算"""
    return (
        pred_df['win_prob'] * weights['win'] +
        pred_df['v44_score'] * weights['v44'] +
        pred_df['top2_prob'] * weights['top2'] +
        pred_df['place_prob'] * weights['place']
    )


def calc_surface_roi(pred_df, test_df, returns_df, score_col='ensemble_score'):
    """芝/ダート別ROIを計算"""
    merged = pred_df.merge(
        test_df[['race_id', 'horse_number', 'finish_position', 'win_odds', 'track_surface']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    merged['rank_pred'] = merged.groupby('race_id')[score_col].rank(ascending=False, method='first')
    
    results = {}
    
    for surface, pattern in [('turf', '芝'), ('dirt', 'ダート')]:
        mask = merged['track_surface'].str.contains(pattern, na=False)
        sub = merged[mask]
        
        top1 = sub[sub['rank_pred'] == 1].copy()
        if len(top1) == 0:
            results[surface] = {'roi': 0, 'count': 0, 'hit_rate': 0}
            continue
        
        tansho_hits = top1[top1['finish_position'] == 1]
        tansho_roi = tansho_hits['win_odds'].sum() / len(top1) * 100 if len(top1) > 0 else 0
        hit_rate = len(tansho_hits) / len(top1) * 100 if len(top1) > 0 else 0
        
        results[surface] = {'roi': tansho_roi, 'count': len(top1), 'hit_rate': hit_rate}
    
    return results


def run_yearly_validation(races, pedigrees, corners, race_details, horses, returns):
    """
    年単位Walk-forward検証（改善版）
    
    【改善点】
    1. 正則化: strong（極端からの改善）
    2. iterations: 150（80からの増加）
    3. 学習単位: 年単位（System A+と同じ）
    """
    progress_logger.info("=" * 60)
    progress_logger.info("System E 改善版 (年単位学習 + strong正則化)")
    progress_logger.info(f"ログファイル: {log_file}")
    progress_logger.info("=" * 60)
    
    # 年単位テスト期間
    test_years = [
        ('2020', '2020-01-01', '2020-12-31'),
        ('2021', '2021-01-01', '2021-12-31'),
        ('2022', '2022-01-01', '2022-12-31'),
        ('2023', '2023-01-01', '2023-12-31'),
        ('2024', '2024-01-01', '2024-12-31'),
        ('2025', '2025-01-01', '2025-12-31'),
    ]
    
    all_results = []
    total_start_time = time.time()
    
    for year_idx, (test_year, test_start, test_end) in enumerate(test_years):
        year_start_time = time.time()
        
        test_start_dt = pd.to_datetime(test_start)
        test_end_dt = pd.to_datetime(test_end)
        train_end = test_start_dt - timedelta(days=1)
        valid_start = train_end - timedelta(days=365)  # 1年前からValid
        
        # データ分割（厳格なリーク防止）
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] > valid_start) & (races['race_date'] <= train_end)].copy()
        test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
        
        if len(test) < 5000 or len(train) < 100000:
            logger.warning(f"{test_year}: データ不足 → スキップ")
            continue
        
        progress_logger.info(f"\n[{year_idx+1}/6] {test_year}年 (Train:{len(train):,} Test:{len(test):,})")
        
        # ===== 特徴量エンジン fit =====
        logger.info(f"=== {test_year}年 特徴量エンジン fit開始 ===")
        
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        # ===== 一括transform（train + test） =====
        logger.info("一括transform開始")
        all_data = pd.concat([train, test], ignore_index=True)
        all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
        all_data_f = engine.transform(all_data)
        
        # V16特徴量を計算（リークフリー）
        all_data_f = compute_v16_features_leakfree(all_data_f, train_end)
        
        # 分割
        train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
        valid_f = train_f[(train_f['race_date'] > valid_start) & (train_f['race_date'] <= train_end)].copy()
        test_f = all_data_f[(all_data_f['race_date'] >= test_start_dt) & (all_data_f['race_date'] <= test_end_dt)].copy()
        
        # 特徴量列
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        v16_features = ['horse_prev_winrate', 'jockey_prev_winrate', 'trainer_prev_winrate', 'prev_finish']
        feature_cols = list(dict.fromkeys(base_features + [f for f in v16_features if f in train_f.columns]))
        
        # ===== モデル学習（改善版パラメータ） =====
        logger.info("モデル学習開始（strong正則化 + iterations=150）")
        
        predictor = MultiTargetPredictor(
            surface_specific=True,
            use_v44_residual=True,
            regularization_level='strong',      # 改善1: extreme → strong
            use_early_stopping=False,
            fixed_iterations=150                # 改善2: 80 → 150
        )
        predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
        
        # ===== 予測 =====
        logger.info("予測開始")
        
        valid_preds = predictor.predict(valid_f)
        test_preds = predictor.predict(test_f)
        
        # ===== ROI評価 =====
        valid_roi_by_prob = calc_roi_by_prob(valid_preds, valid_f, returns)
        test_roi_by_prob = calc_roi_by_prob(test_preds, test_f, returns)
        
        # 複数ensembleウェイトを評価
        for weight_name, weights in ENSEMBLE_WEIGHTS.items():
            test_preds[f'ensemble_{weight_name}'] = calc_ensemble_score(test_preds, weights)
            valid_preds[f'ensemble_{weight_name}'] = calc_ensemble_score(valid_preds, weights)
        
        # 結果記録
        year_result = {
            'year': test_year,
            'train_count': len(train),
            'test_count': len(test),
        }
        
        # 各指数のROI
        for prob_col in ['win_prob', 'top2_prob', 'place_prob']:
            year_result[f'{prob_col}_valid_roi'] = valid_roi_by_prob[prob_col]['tansho_roi']
            year_result[f'{prob_col}_test_roi'] = test_roi_by_prob[prob_col]['tansho_roi']
            year_result[f'{prob_col}_gap'] = abs(
                valid_roi_by_prob[prob_col]['tansho_roi'] - test_roi_by_prob[prob_col]['tansho_roi']
            )
        
        # 各ensembleウェイトのROI
        for weight_name in ENSEMBLE_WEIGHTS.keys():
            score_col = f'ensemble_{weight_name}'
            valid_roi = calc_multi_roi(valid_preds, valid_f, returns, score_col=score_col)
            test_roi = calc_multi_roi(test_preds, test_f, returns, score_col=score_col)
            
            year_result[f'{weight_name}_valid_roi'] = valid_roi['tansho_roi']
            year_result[f'{weight_name}_test_roi'] = test_roi['tansho_roi']
            year_result[f'{weight_name}_gap'] = abs(valid_roi['tansho_roi'] - test_roi['tansho_roi'])
            
            # 芝/ダート別
            surface_roi = calc_surface_roi(test_preds, test_f, returns, score_col=score_col)
            year_result[f'{weight_name}_turf_roi'] = surface_roi['turf']['roi']
            year_result[f'{weight_name}_dirt_roi'] = surface_roi['dirt']['roi']
        
        all_results.append(year_result)
        
        year_time = time.time() - year_start_time
        
        # 結果表示
        win_roi = year_result['win_prob_test_roi']
        win_gap = year_result['win_prob_gap']
        top2_roi = year_result['top2_prob_test_roi']
        top2_gap = year_result['top2_prob_gap']
        progress_logger.info(f"  win:{win_roi:.1f}%(Gap:{win_gap:.1f}%) top2:{top2_roi:.1f}%(Gap:{top2_gap:.1f}%) [{year_time/60:.1f}分]")
    
    # ===== 全期間サマリー =====
    total_time = time.time() - total_start_time
    
    if all_results:
        results_df = pd.DataFrame(all_results)
        
        # CSV出力
        output_dir = project_root / "outputs/analysis"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "system_e_improved_yearly_results.csv"
        results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        # サマリー出力
        progress_logger.info("\n" + "=" * 60)
        progress_logger.info("【全期間サマリー（年単位学習 + strong正則化）】")
        progress_logger.info("=" * 60)
        
        progress_logger.info("\n--- 各指数 平均成績 ---")
        for prob_col in ['win_prob', 'top2_prob', 'place_prob']:
            avg_roi = results_df[f'{prob_col}_test_roi'].mean()
            avg_gap = results_df[f'{prob_col}_gap'].mean()
            progress_logger.info(f"  {prob_col:15s}: ROI {avg_roi:.1f}%, Gap {avg_gap:.1f}%")
        
        progress_logger.info("\n--- Ensembleウェイト比較 ---")
        for weight_name in ENSEMBLE_WEIGHTS.keys():
            avg_roi = results_df[f'{weight_name}_test_roi'].mean()
            avg_gap = results_df[f'{weight_name}_gap'].mean()
            turf_roi = results_df[f'{weight_name}_turf_roi'].mean()
            dirt_roi = results_df[f'{weight_name}_dirt_roi'].mean()
            progress_logger.info(f"  {weight_name:15s}: ROI {avg_roi:.1f}%, Gap {avg_gap:.1f}% (芝{turf_roi:.1f}%/ダ{dirt_roi:.1f}%)")
        
        progress_logger.info("\n--- 既存システムとの比較 ---")
        progress_logger.info(f"  既存System A+:        80.2%")
        progress_logger.info(f"  今回(win_prob):       {results_df['win_prob_test_roi'].mean():.1f}%")
        progress_logger.info(f"  今回(top2_prob):      {results_df['top2_prob_test_roi'].mean():.1f}%")
        
        progress_logger.info(f"\n結果保存: {output_path}")
        progress_logger.info(f"詳細ログ: {log_file}")
        progress_logger.info(f"総処理時間: {total_time/60:.1f}分")
    
    return all_results


def main():
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    progress_logger.info(f"データ: {len(races):,}件")
    
    results = run_yearly_validation(
        races, pedigrees, corners, race_details, horses, returns
    )
    
    progress_logger.info("\n完了")


if __name__ == "__main__":
    main()

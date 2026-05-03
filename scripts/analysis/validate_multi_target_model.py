#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3指数モデル Walk-forward検証スクリプト

【目的】
1着/2着以内/3着以内の3指数モデルを6年間Walk-forwardで検証し、
各指数の特性と最適な使い分けを明らかにする。

【評価項目】
1. 各指数による単勝/複勝ROI
2. 芝/ダート別パフォーマンス
3. Train-Test Gap（過学習チェック）
4. アンサンブル効果

【リスクオフ設計】
- Gap > 30%の場合は警告を出力
- 年度間変動が大きい指数は使用を控える
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
import logging

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.models.multi_target_predictor import MultiTargetPredictor, calc_multi_roi, calc_roi_by_prob


def load_data():
    """データ読み込み（新馬・障害除外）"""
    logger.info("=" * 70)
    logger.info("3指数モデル Walk-forward検証")
    logger.info("=" * 70)
    
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
    initial_count = len(races)
    new_horse_mask = races['race_name'].str.contains('新馬', na=False)
    obstacle_mask = (
        races['track_surface'].str.contains('障', na=False) |
        races['race_name'].str.contains('障害', na=False)
    )
    
    races = races[~(new_horse_mask | obstacle_mask)].copy()
    
    logger.info(f"\n【データ概要】")
    logger.info(f"  全レコード: {initial_count:,} → {len(races):,} (除外: {initial_count - len(races):,})")
    
    return races, pedigrees, corners, race_details, horses, returns


def add_v16_features(df):
    """V16新特徴量を追加（shift(1)でリーク防止）"""
    df = df.copy()
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    if 'is_win' not in df.columns:
        df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # 馬累積勝率
    df['_horse_cumwins'] = df.groupby('horse_id')['is_win'].cumsum().shift(1).fillna(0)
    df['_horse_cumraces'] = df.groupby('horse_id').cumcount()
    df['horse_prev_winrate'] = df['_horse_cumwins'] / df['_horse_cumraces'].replace(0, np.nan)
    df.drop(columns=['_horse_cumwins', '_horse_cumraces'], inplace=True)
    
    # 騎手累積勝率
    df = df.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
    df['_jockey_cumwins'] = df.groupby('jockey_id')['is_win'].cumsum().shift(1).fillna(0)
    df['_jockey_cumraces'] = df.groupby('jockey_id').cumcount()
    df['jockey_prev_winrate'] = df['_jockey_cumwins'] / df['_jockey_cumraces'].replace(0, np.nan)
    df.drop(columns=['_jockey_cumwins', '_jockey_cumraces'], inplace=True)
    
    # 調教師累積勝率
    df = df.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
    df['_trainer_cumwins'] = df.groupby('trainer_id')['is_win'].cumsum().shift(1).fillna(0)
    df['_trainer_cumraces'] = df.groupby('trainer_id').cumcount()
    df['trainer_prev_winrate'] = df['_trainer_cumwins'] / df['_trainer_cumraces'].replace(0, np.nan)
    df.drop(columns=['_trainer_cumwins', '_trainer_cumraces'], inplace=True)
    
    # 前走着順
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
    
    return df


def run_walkforward_validation(races, pedigrees, corners, race_details, horses, returns):
    """6年間Walk-forward検証"""
    
    # 検証期間設定
    test_periods = [
        ('2020', '2018-12-31', '2019-01-01', '2019-12-31', '2020-01-01', '2020-12-31'),
        ('2021', '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2021-12-31'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2022-12-31'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2023-12-31'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2024-12-31'),
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-12-31'),
    ]
    
    all_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        logger.info(f"\n{'='*70}")
        logger.info(f"[{year}年] Walk-forward検証")
        logger.info("=" * 70)
        
        # データ分割
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] <= valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(test) < 5000 or len(train) < 50000:
            logger.warning(f"  データ不足: Train={len(train)}, Test={len(test)} → スキップ")
            continue
        
        logger.info(f"  Train: {len(train):,} / Valid: {len(valid):,} / Test: {len(test):,}")
        
        # 特徴量エンジニアリング
        logger.info("  特徴量生成中...")
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        # V16特徴量追加
        train_f = add_v16_features(train_f)
        valid_f = add_v16_features(valid_f)
        test_f = add_v16_features(test_f)
        
        # 特徴量列
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        v16_features = ['horse_prev_winrate', 'jockey_prev_winrate', 'trainer_prev_winrate', 'prev_finish']
        feature_cols = list(dict.fromkeys(base_features + [f for f in v16_features if f in train_f.columns]))
        
        logger.info(f"  特徴量数: {len(feature_cols)}個")
        
        # ====================================================================
        # 3指数モデル学習（芝/ダート別、V4.4 Residual含む）
        # ====================================================================
        logger.info("\n  3指数モデル学習開始...")
        
        predictor = MultiTargetPredictor(
            surface_specific=True,
            use_v44_residual=True,
            regularization_level='strong'
        )
        
        predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
        
        # ====================================================================
        # 予測
        # ====================================================================
        logger.info("\n  予測実行中...")
        
        # Valid予測（Gap計算用）
        valid_preds = predictor.predict(valid_f)
        
        # Test予測
        test_preds = predictor.predict(test_f)
        
        # ====================================================================
        # ROI評価
        # ====================================================================
        logger.info("\n  ROI評価中...")
        
        # 各指数でのROI
        valid_roi_by_prob = calc_roi_by_prob(valid_preds, valid_f, returns)
        test_roi_by_prob = calc_roi_by_prob(test_preds, test_f, returns)
        
        # 結果出力
        logger.info(f"\n  【{year}年 結果サマリー】")
        logger.info(f"  {'指数':<20} | {'Valid単勝ROI':>12} | {'Test単勝ROI':>12} | {'Gap':>8} | {'的中率':>8}")
        logger.info("  " + "-" * 75)
        
        year_result = {'year': year}
        
        for prob_col in ['win_prob', 'top2_prob', 'place_prob', 'ensemble_score']:
            valid_roi = valid_roi_by_prob[prob_col]['tansho_roi']
            test_roi = test_roi_by_prob[prob_col]['tansho_roi']
            gap = abs(valid_roi - test_roi)
            hit_rate = test_roi_by_prob[prob_col]['tansho_hit_rate']
            
            gap_marker = "⚠️" if gap > 30 else ""
            
            logger.info(f"  {prob_col:<20} | {valid_roi:>10.1f}% | {test_roi:>10.1f}% | {gap:>6.1f}% | {hit_rate:>6.1f}% {gap_marker}")
            
            year_result[f'{prob_col}_valid_roi'] = valid_roi
            year_result[f'{prob_col}_test_roi'] = test_roi
            year_result[f'{prob_col}_gap'] = gap
            year_result[f'{prob_col}_hit_rate'] = hit_rate
        
        # 複勝ROI
        logger.info(f"\n  【複勝ROI】")
        for prob_col in ['win_prob', 'top2_prob', 'place_prob', 'ensemble_score']:
            fukusho_roi = test_roi_by_prob[prob_col]['fukusho_roi']
            fukusho_hit_rate = test_roi_by_prob[prob_col]['fukusho_hit_rate']
            logger.info(f"  {prob_col:<20} | Test ROI: {fukusho_roi:>6.1f}% | 的中率: {fukusho_hit_rate:>6.1f}%")
            
            year_result[f'{prob_col}_fukusho_roi'] = fukusho_roi
        
        # 芝/ダート別分析
        logger.info(f"\n  【芝/ダート別 Test単勝ROI】")
        
        for surface, surface_name in [('芝', 'turf'), ('ダート', 'dirt')]:
            surface_mask = test_f['track_surface'].str.contains(surface, na=False)
            if surface_mask.sum() == 0:
                continue
            
            test_f_surface = test_f[surface_mask]
            test_preds_surface = test_preds[test_preds['race_id'].isin(test_f_surface['race_id'])]
            
            if len(test_preds_surface) > 0:
                surface_roi = calc_multi_roi(test_preds_surface, test_f_surface, returns, 'ensemble_score')
                logger.info(f"  {surface}: 単勝ROI {surface_roi['tansho_roi']:.1f}%, 的中率 {surface_roi['tansho_hit_rate']:.1f}%")
                
                year_result[f'{surface_name}_tansho_roi'] = surface_roi['tansho_roi']
        
        all_results.append(year_result)
    
    return all_results


def print_summary(results: list):
    """検証結果サマリー出力"""
    logger.info("\n" + "=" * 80)
    logger.info("【6年間 Walk-forward検証 サマリー】")
    logger.info("=" * 80)
    
    if not results:
        logger.warning("結果がありません")
        return
    
    # 平均計算
    prob_cols = ['win_prob', 'top2_prob', 'place_prob', 'ensemble_score']
    
    logger.info(f"\n{'指数':<20} | {'平均Test単勝ROI':>14} | {'平均Gap':>10} | {'平均的中率':>10}")
    logger.info("-" * 70)
    
    for prob_col in prob_cols:
        avg_roi = np.mean([r[f'{prob_col}_test_roi'] for r in results if f'{prob_col}_test_roi' in r])
        avg_gap = np.mean([r[f'{prob_col}_gap'] for r in results if f'{prob_col}_gap' in r])
        avg_hit = np.mean([r[f'{prob_col}_hit_rate'] for r in results if f'{prob_col}_hit_rate' in r])
        
        gap_marker = "⚠️" if avg_gap > 25 else "✅"
        
        logger.info(f"{prob_col:<20} | {avg_roi:>12.1f}% | {avg_gap:>8.1f}% | {avg_hit:>8.1f}% {gap_marker}")
    
    # 芝/ダート別平均
    logger.info(f"\n【芝/ダート別 平均単勝ROI】")
    for surface in ['turf', 'dirt']:
        key = f'{surface}_tansho_roi'
        if any(key in r for r in results):
            avg_surface_roi = np.mean([r[key] for r in results if key in r])
            logger.info(f"  {surface.upper()}: {avg_surface_roi:.1f}%")
    
    # 複勝平均
    logger.info(f"\n【複勝ROI 平均】")
    for prob_col in prob_cols:
        key = f'{prob_col}_fukusho_roi'
        if any(key in r for r in results):
            avg_fukusho = np.mean([r[key] for r in results if key in r])
            logger.info(f"  {prob_col:<20}: {avg_fukusho:.1f}%")
    
    # 推奨指数
    logger.info("\n" + "=" * 80)
    logger.info("【推論と推奨】")
    logger.info("=" * 80)
    
    # 各指数の平均ROIを計算
    roi_summary = {}
    for prob_col in prob_cols:
        avg_roi = np.mean([r[f'{prob_col}_test_roi'] for r in results if f'{prob_col}_test_roi' in r])
        avg_gap = np.mean([r[f'{prob_col}_gap'] for r in results if f'{prob_col}_gap' in r])
        roi_summary[prob_col] = {'roi': avg_roi, 'gap': avg_gap}
    
    # 単勝向け推奨
    best_tansho = max(roi_summary.items(), key=lambda x: x[1]['roi'] if x[1]['gap'] < 30 else 0)
    logger.info(f"\n  【単勝】推奨指数: {best_tansho[0]} (ROI {best_tansho[1]['roi']:.1f}%, Gap {best_tansho[1]['gap']:.1f}%)")
    
    # 複勝向け推奨
    fukusho_avg = {}
    for prob_col in prob_cols:
        key = f'{prob_col}_fukusho_roi'
        if any(key in r for r in results):
            fukusho_avg[prob_col] = np.mean([r[key] for r in results if key in r])
    
    if fukusho_avg:
        best_fukusho = max(fukusho_avg.items(), key=lambda x: x[1])
        logger.info(f"  【複勝】推奨指数: {best_fukusho[0]} (ROI {best_fukusho[1]:.1f}%)")
    
    # 警告
    logger.info("\n  【リスク警告】")
    for prob_col, data in roi_summary.items():
        if data['gap'] > 30:
            logger.warning(f"  ⚠️ {prob_col}: Gap {data['gap']:.1f}% → 過学習リスク高")


def main():
    # データ読み込み
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # Walk-forward検証
    results = run_walkforward_validation(races, pedigrees, corners, race_details, horses, returns)
    
    # サマリー出力
    print_summary(results)
    
    # CSV保存
    if results:
        output_dir = project_root / "outputs/analysis"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        results_df = pd.DataFrame(results)
        output_path = output_dir / "multi_target_walkforward_results.csv"
        results_df.to_csv(output_path, index=False)
        logger.info(f"\n結果保存: {output_path}")
    
    logger.info("\n検証完了")
    

if __name__ == "__main__":
    main()

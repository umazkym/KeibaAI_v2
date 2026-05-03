#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3指数統合モデル 全24四半期検証スクリプト (System E)

【機能】
1. 全24四半期（2020Q1〜2025Q4）のWalk-forward検証
2. 複数ensembleウェイトの探索
3. 芝/ダート別ROI集計
4. 結果をCSV出力

【リーク防止の厳格ルール】
1. 学習データ: race_date < 予測四半期開始日
2. 特徴量計算: 各レース日の前日以前のデータのみ使用
3. 同日レースは使用禁止（レース順序が不明）

【実行時間】
約13時間（24四半期 × 約33分/四半期）
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.models.multi_target_predictor import MultiTargetPredictor, calc_multi_roi, calc_roi_by_prob


# ===== Ensembleウェイト候補 =====
ENSEMBLE_WEIGHTS = {
    'v1_original': {'win': 0.50, 'v44': 0.30, 'top2': 0.10, 'place': 0.10},
    'v2_balanced': {'win': 0.40, 'v44': 0.20, 'top2': 0.25, 'place': 0.15},
    'v3_low_v44':  {'win': 0.50, 'v44': 0.15, 'top2': 0.20, 'place': 0.15},
    'v4_no_v44':   {'win': 0.50, 'v44': 0.00, 'top2': 0.30, 'place': 0.20},
    'v5_top2_focus': {'win': 0.30, 'v44': 0.10, 'top2': 0.40, 'place': 0.20},
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


def compute_daily_features(all_data, target_date):
    """
    特定日時点での特徴量を計算（リークフリー）
    
    【ロジック】
    - target_date より前のレース結果のみを使用
    - target_date 当日のレースに対する特徴量を返す
    """
    df = all_data.copy()
    df = df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
    
    if 'is_win' not in df.columns:
        df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # target_dateより前のデータのみを有効として計算
    past_mask = df['race_date'] < target_date
    
    # ===== 馬の累積勝率 =====
    df['_valid_win'] = df['is_win'].where(past_mask, np.nan)
    df['_valid_race'] = past_mask.astype(float)
    
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    df['_horse_cumwins'] = df.groupby('horse_id')['_valid_win'].cumsum()
    df['_horse_cumraces'] = df.groupby('horse_id')['_valid_race'].cumsum()
    df['horse_prev_winrate'] = df['_horse_cumwins'] / df['_horse_cumraces'].replace(0, np.nan)
    
    # ===== 騎手の累積勝率 =====
    df = df.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
    df['_jockey_cumwins'] = df.groupby('jockey_id')['_valid_win'].cumsum()
    df['_jockey_cumraces'] = df.groupby('jockey_id')['_valid_race'].cumsum()
    df['jockey_prev_winrate'] = df['_jockey_cumwins'] / df['_jockey_cumraces'].replace(0, np.nan)
    
    # ===== 調教師の累積勝率 =====
    df = df.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
    df['_trainer_cumwins'] = df.groupby('trainer_id')['_valid_win'].cumsum()
    df['_trainer_cumraces'] = df.groupby('trainer_id')['_valid_race'].cumsum()
    df['trainer_prev_winrate'] = df['_trainer_cumwins'] / df['_trainer_cumraces'].replace(0, np.nan)
    
    # ===== 前走着順 =====
    past_data = df[df['race_date'] < target_date]
    if len(past_data) > 0:
        last_finish = past_data.groupby('horse_id')['finish_position'].last().to_dict()
        df['prev_finish'] = df['horse_id'].map(last_finish)
    else:
        df['prev_finish'] = np.nan
    
    # 不要列削除
    drop_cols = ['_valid_win', '_valid_race', '_horse_cumwins', '_horse_cumraces',
                 '_jockey_cumwins', '_jockey_cumraces', '_trainer_cumwins', '_trainer_cumraces']
    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)
    
    # target_date のレースのみ抽出
    target_df = df[df['race_date'] == target_date].copy()
    
    return target_df


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


def run_full_validation(races, pedigrees, corners, race_details, horses, returns,
                        start_quarter=None, end_quarter=None):
    """
    全24四半期Walk-forward検証
    """
    logger.info("=" * 70)
    logger.info("3指数統合モデル 全期間Walk-forward検証 (System E)")
    logger.info("=" * 70)
    
    # 四半期リスト生成
    all_quarters = []
    for year in range(2020, 2026):
        for q in range(1, 5):
            if q == 1:
                start, end = f'{year}-01-01', f'{year}-03-31'
            elif q == 2:
                start, end = f'{year}-04-01', f'{year}-06-30'
            elif q == 3:
                start, end = f'{year}-07-01', f'{year}-09-30'
            else:
                start, end = f'{year}-10-01', f'{year}-12-31'
            all_quarters.append((f'{year}Q{q}', start, end))
    
    # 開始/終了四半期のフィルタリング
    if start_quarter:
        all_quarters = [q for q in all_quarters if q[0] >= start_quarter]
    if end_quarter:
        all_quarters = [q for q in all_quarters if q[0] <= end_quarter]
    
    all_results = []
    total_start_time = time.time()
    
    for quarter_idx, (quarter_name, test_start, test_end) in enumerate(all_quarters):
        quarter_start_time = time.time()
        
        test_start_dt = pd.to_datetime(test_start)
        test_end_dt = pd.to_datetime(test_end)
        train_end = test_start_dt - timedelta(days=1)
        valid_start = train_end - timedelta(days=365)
        
        # データ分割
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] > valid_start) & (races['race_date'] <= train_end)].copy()
        test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
        
        if len(test) < 1000 or len(train) < 50000:
            logger.warning(f"  {quarter_name}: データ不足 → スキップ")
            continue
        
        logger.info(f"\n{'='*70}")
        logger.info(f"[{quarter_idx+1}/{len(all_quarters)}] {quarter_name}")
        logger.info(f"  学習: ~{train_end.strftime('%Y-%m-%d')} ({len(train):,}件)")
        logger.info(f"  テスト: {test_start}~{test_end} ({len(test):,}件)")
        logger.info("=" * 70)
        
        # ===== 特徴量エンジン fit =====
        logger.info("  [1/3] 特徴量エンジン fit...")
        fit_start = time.time()
        
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        # 学習データ変換
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        
        # V16特徴量（shift(1)でリークフリー）
        for df in [train_f, valid_f]:
            df_sorted = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
            df['is_win'] = (df['finish_position'] == 1).astype(int)
            df['horse_prev_winrate'] = df.groupby('horse_id')['is_win'].transform(
                lambda x: x.shift(1).expanding().mean()
            )
            df['jockey_prev_winrate'] = df.groupby('jockey_id')['is_win'].transform(
                lambda x: x.shift(1).expanding().mean()
            )
            df['trainer_prev_winrate'] = df.groupby('trainer_id')['is_win'].transform(
                lambda x: x.shift(1).expanding().mean()
            )
            df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
        
        fit_time = time.time() - fit_start
        logger.info(f"    完了 ({fit_time:.1f}秒)")
        
        # 特徴量列
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        v16_features = ['horse_prev_winrate', 'jockey_prev_winrate', 'trainer_prev_winrate', 'prev_finish']
        feature_cols = list(dict.fromkeys(base_features + [f for f in v16_features if f in train_f.columns]))
        
        # ===== モデル学習 =====
        logger.info("  [2/3] モデル学習中...")
        model_start = time.time()
        
        predictor = MultiTargetPredictor(
            surface_specific=True,
            use_v44_residual=True,
            regularization_level='extreme',
            use_early_stopping=False,
            fixed_iterations=80
        )
        predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
        
        model_time = time.time() - model_start
        logger.info(f"    完了 ({model_time:.1f}秒)")
        
        # ===== テストデータ予測（日次特徴量更新） =====
        logger.info("  [3/3] テストデータ予測（日次特徴量更新）...")
        predict_start = time.time()
        
        test_dates = sorted(test['race_date'].unique())
        all_test_preds = []
        all_test_actual = []
        
        all_data = pd.concat([train, test], ignore_index=True)
        all_data = all_data.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        
        for i, target_date in enumerate(test_dates):
            if i > 0 and i % 10 == 0:
                elapsed = time.time() - predict_start
                remaining = (len(test_dates) - i) / i * elapsed
                logger.info(f"    {i}/{len(test_dates)}日完了 (残り約{remaining/60:.1f}分)")
            
            target_df = compute_daily_features(all_data, target_date)
            
            if len(target_df) == 0:
                continue
            
            target_f = engine.transform(target_df)
            
            if len(target_f) > 0 and all(c in target_f.columns for c in feature_cols):
                target_preds = predictor.predict(target_f)
                all_test_preds.append(target_preds)
                all_test_actual.append(target_f)
        
        predict_time = time.time() - predict_start
        logger.info(f"    完了 ({predict_time:.1f}秒, {len(test_dates)}日間)")
        
        if not all_test_preds:
            continue
        
        test_preds = pd.concat(all_test_preds, ignore_index=True)
        test_f = pd.concat(all_test_actual, ignore_index=True)
        
        # ===== ROI評価 =====
        valid_preds = predictor.predict(valid_f)
        
        # 各指数のROI
        valid_roi_by_prob = calc_roi_by_prob(valid_preds, valid_f, returns)
        test_roi_by_prob = calc_roi_by_prob(test_preds, test_f, returns)
        
        # 複数ensembleウェイトを評価
        for weight_name, weights in ENSEMBLE_WEIGHTS.items():
            test_preds[f'ensemble_{weight_name}'] = calc_ensemble_score(test_preds, weights)
            valid_preds[f'ensemble_{weight_name}'] = calc_ensemble_score(valid_preds, weights)
        
        # 結果記録
        quarter_result = {
            'quarter': quarter_name,
            'train_count': len(train),
            'test_count': len(test),
        }
        
        # 各指数のROI
        for prob_col in ['win_prob', 'top2_prob', 'place_prob']:
            quarter_result[f'{prob_col}_valid_roi'] = valid_roi_by_prob[prob_col]['tansho_roi']
            quarter_result[f'{prob_col}_test_roi'] = test_roi_by_prob[prob_col]['tansho_roi']
            quarter_result[f'{prob_col}_gap'] = abs(
                valid_roi_by_prob[prob_col]['tansho_roi'] - test_roi_by_prob[prob_col]['tansho_roi']
            )
        
        # 各ensembleウェイトのROI
        for weight_name in ENSEMBLE_WEIGHTS.keys():
            score_col = f'ensemble_{weight_name}'
            valid_roi = calc_multi_roi(valid_preds, valid_f, returns, score_col=score_col)
            test_roi = calc_multi_roi(test_preds, test_f, returns, score_col=score_col)
            
            quarter_result[f'{weight_name}_valid_roi'] = valid_roi['tansho_roi']
            quarter_result[f'{weight_name}_test_roi'] = test_roi['tansho_roi']
            quarter_result[f'{weight_name}_gap'] = abs(valid_roi['tansho_roi'] - test_roi['tansho_roi'])
        
        # 芝/ダート別ROI（best ensembleで）
        for weight_name in ENSEMBLE_WEIGHTS.keys():
            surface_roi = calc_surface_roi(test_preds, test_f, returns, score_col=f'ensemble_{weight_name}')
            quarter_result[f'{weight_name}_turf_roi'] = surface_roi['turf']['roi']
            quarter_result[f'{weight_name}_dirt_roi'] = surface_roi['dirt']['roi']
        
        all_results.append(quarter_result)
        
        quarter_time = time.time() - quarter_start_time
        logger.info(f"\n  四半期処理時間: {quarter_time/60:.1f}分")
        
        # 結果出力
        logger.info(f"\n  【{quarter_name} 結果】")
        logger.info(f"  {'指数':<20} | {'Valid ROI':>10} | {'Test ROI':>10} | {'Gap':>8}")
        logger.info("  " + "-" * 55)
        
        for prob_col in ['win_prob', 'top2_prob', 'place_prob']:
            v_roi = quarter_result[f'{prob_col}_valid_roi']
            t_roi = quarter_result[f'{prob_col}_test_roi']
            gap = quarter_result[f'{prob_col}_gap']
            marker = "✅" if gap <= 30 else "⚠️"
            logger.info(f"  {prob_col:<20} | {v_roi:>8.1f}% | {t_roi:>8.1f}% | {gap:>6.1f}% {marker}")
        
        logger.info("  " + "-" * 55)
        for weight_name in ENSEMBLE_WEIGHTS.keys():
            v_roi = quarter_result[f'{weight_name}_valid_roi']
            t_roi = quarter_result[f'{weight_name}_test_roi']
            gap = quarter_result[f'{weight_name}_gap']
            marker = "✅" if gap <= 30 else "⚠️"
            logger.info(f"  {weight_name:<20} | {v_roi:>8.1f}% | {t_roi:>8.1f}% | {gap:>6.1f}% {marker}")
    
    # ===== 全期間サマリー =====
    total_time = time.time() - total_start_time
    
    if all_results:
        results_df = pd.DataFrame(all_results)
        
        # CSV出力
        output_dir = project_root / "outputs/analysis"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "system_e_full_validation_results.csv"
        results_df.to_csv(output_path, index=False)
        logger.info(f"\n結果保存: {output_path}")
        
        # サマリー出力
        logger.info("\n" + "=" * 70)
        logger.info("【全期間サマリー】")
        logger.info("=" * 70)
        
        logger.info(f"\n--- 各指数 平均Test ROI ---")
        for prob_col in ['win_prob', 'top2_prob', 'place_prob']:
            avg_roi = results_df[f'{prob_col}_test_roi'].mean()
            avg_gap = results_df[f'{prob_col}_gap'].mean()
            logger.info(f"  {prob_col:<15}: ROI {avg_roi:.1f}%, Gap {avg_gap:.1f}%")
        
        logger.info(f"\n--- Ensembleウェイト比較 ---")
        for weight_name in ENSEMBLE_WEIGHTS.keys():
            avg_roi = results_df[f'{weight_name}_test_roi'].mean()
            avg_gap = results_df[f'{weight_name}_gap'].mean()
            turf_roi = results_df[f'{weight_name}_turf_roi'].mean()
            dirt_roi = results_df[f'{weight_name}_dirt_roi'].mean()
            logger.info(f"  {weight_name:<15}: ROI {avg_roi:.1f}%, Gap {avg_gap:.1f}% (芝{turf_roi:.1f}%/ダ{dirt_roi:.1f}%)")
        
        logger.info(f"\n総処理時間: {total_time/3600:.1f}時間")
    
    return all_results


def main():
    parser = argparse.ArgumentParser(description='3指数統合モデル 全期間検証')
    parser.add_argument('--start', type=str, default=None, help='開始四半期 (例: 2020Q1)')
    parser.add_argument('--end', type=str, default=None, help='終了四半期 (例: 2025Q4)')
    args = parser.parse_args()
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    logger.info(f"\n【データ概要】")
    logger.info(f"  レコード数: {len(races):,}")
    
    results = run_full_validation(
        races, pedigrees, corners, race_details, horses, returns,
        start_quarter=args.start,
        end_quarter=args.end
    )
    
    logger.info("\n処理完了")


if __name__ == "__main__":
    main()

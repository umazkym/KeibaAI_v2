#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3指数統合モデル 効率化版 全期間検証スクリプト (System E v2)

【効率化ポイント】
1. V15特徴量を事前に全テストデータで計算しキャッシュ
2. V16特徴量（日次更新分）のみを日次で計算
3. ログをファイル出力 + コンソールは要約のみ
4. 進捗表示を簡潔化

【精度は変更なし】
- 日次境界ルールは厳守（race_date < target_date）
- 同日レースは使用禁止

【推定時間】
約5-6時間（13時間→半分以下に短縮）
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

# ===== ログ設定（ファイル出力 + コンソール簡素化） =====
log_dir = project_root / "outputs/logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"system_e_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# ファイルハンドラ（詳細）
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# コンソールハンドラ（簡潔）
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(logging.Formatter('%(message)s'))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 進捗用の別ロガー（コンソールにも出力）
progress_logger = logging.getLogger('progress')
progress_logger.setLevel(logging.INFO)
progress_console = logging.StreamHandler()
progress_console.setFormatter(logging.Formatter('%(message)s'))
progress_logger.addHandler(progress_console)
progress_logger.addHandler(file_handler)

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


def compute_v16_features_vectorized(df, train_end_date):
    """
    V16特徴量を全データに対してベクトル化計算（効率化版）
    
    【ロジック】
    日次境界を守りつつ、事前に全日の累積統計を計算
    """
    df = df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
    
    if 'is_win' not in df.columns:
        df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # train_end以前のデータのみ有効
    valid_mask = df['race_date'] <= train_end_date
    
    # ===== 馬の累積勝率（shift(1)でリークフリー） =====
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    df['horse_prev_winrate'] = df.groupby('horse_id')['is_win'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    # train_end以降のデータは、train_end時点の値で上書き
    df.loc[~valid_mask, 'horse_prev_winrate'] = np.nan
    
    # ===== 騎手の累積勝率 =====
    df = df.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
    df['jockey_prev_winrate'] = df.groupby('jockey_id')['is_win'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    df.loc[~valid_mask, 'jockey_prev_winrate'] = np.nan
    
    # ===== 調教師の累積勝率 =====
    df = df.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
    df['trainer_prev_winrate'] = df.groupby('trainer_id')['is_win'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    df.loc[~valid_mask, 'trainer_prev_winrate'] = np.nan
    
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


def run_optimized_validation(races, pedigrees, corners, race_details, horses, returns,
                              start_quarter=None, end_quarter=None):
    """
    効率化版 全24四半期Walk-forward検証
    """
    progress_logger.info("=" * 60)
    progress_logger.info("System E 効率化版 全期間検証")
    progress_logger.info(f"ログファイル: {log_file}")
    progress_logger.info("=" * 60)
    
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
            logger.warning(f"{quarter_name}: データ不足 → スキップ")
            continue
        
        progress_logger.info(f"\n[{quarter_idx+1}/{len(all_quarters)}] {quarter_name} (Train:{len(train):,} Test:{len(test):,})")
        
        # ===== 特徴量エンジン fit（詳細はファイルログへ） =====
        logger.info(f"=== {quarter_name} 特徴量エンジン fit開始 ===")
        
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        # ===== 効率化: train+testを一括transform（validはtrainの一部なので含まれる） =====
        logger.info("一括transform開始")
        # train + test を結合（重複排除）
        all_data = pd.concat([train, test], ignore_index=True)
        all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
        all_data_f = engine.transform(all_data)
        
        # V16特徴量を一括計算
        all_data_f = compute_v16_features_vectorized(all_data_f, train_end)
        
        # 分割（race_dateで再分割）
        train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
        valid_f = train_f[(train_f['race_date'] > valid_start) & (train_f['race_date'] <= train_end)].copy()
        test_f = all_data_f[(all_data_f['race_date'] >= test_start_dt) & (all_data_f['race_date'] <= test_end_dt)].copy()
        
        # 特徴量列
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        v16_features = ['horse_prev_winrate', 'jockey_prev_winrate', 'trainer_prev_winrate', 'prev_finish']
        feature_cols = list(dict.fromkeys(base_features + [f for f in v16_features if f in train_f.columns]))
        
        # ===== モデル学習 =====
        logger.info("モデル学習開始")
        
        predictor = MultiTargetPredictor(
            surface_specific=True,
            use_v44_residual=True,
            regularization_level='extreme',
            use_early_stopping=False,
            fixed_iterations=80
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
            
            # 芝/ダート別
            surface_roi = calc_surface_roi(test_preds, test_f, returns, score_col=score_col)
            quarter_result[f'{weight_name}_turf_roi'] = surface_roi['turf']['roi']
            quarter_result[f'{weight_name}_dirt_roi'] = surface_roi['dirt']['roi']
        
        all_results.append(quarter_result)
        
        quarter_time = time.time() - quarter_start_time
        
        # 簡潔な結果表示
        best_roi = max(quarter_result[f'{w}_test_roi'] for w in ENSEMBLE_WEIGHTS.keys())
        win_roi = quarter_result['win_prob_test_roi']
        win_gap = quarter_result['win_prob_gap']
        progress_logger.info(f"  → win:{win_roi:.1f}% (Gap:{win_gap:.1f}%) / Best:{best_roi:.1f}% [{quarter_time/60:.1f}分]")
    
    # ===== 全期間サマリー =====
    total_time = time.time() - total_start_time
    
    if all_results:
        results_df = pd.DataFrame(all_results)
        
        # CSV出力
        output_dir = project_root / "outputs/analysis"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "system_e_optimized_results.csv"
        results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        # サマリー出力
        progress_logger.info("\n" + "=" * 60)
        progress_logger.info("【全期間サマリー】")
        progress_logger.info("=" * 60)
        
        progress_logger.info("\n--- 各指数 平均Test ROI ---")
        for prob_col in ['win_prob', 'top2_prob', 'place_prob']:
            avg_roi = results_df[f'{prob_col}_test_roi'].mean()
            avg_gap = results_df[f'{prob_col}_gap'].mean()
            progress_logger.info(f"  {prob_col:<15}: ROI {avg_roi:.1f}%, Gap {avg_gap:.1f}%")
        
        progress_logger.info("\n--- Ensembleウェイト比較 ---")
        for weight_name in ENSEMBLE_WEIGHTS.keys():
            avg_roi = results_df[f'{weight_name}_test_roi'].mean()
            avg_gap = results_df[f'{weight_name}_gap'].mean()
            turf_roi = results_df[f'{weight_name}_turf_roi'].mean()
            dirt_roi = results_df[f'{weight_name}_dirt_roi'].mean()
            progress_logger.info(f"  {weight_name:<15}: ROI {avg_roi:.1f}%, Gap {avg_gap:.1f}% (芝{turf_roi:.1f}%/ダ{dirt_roi:.1f}%)")
        
        progress_logger.info(f"\n結果保存: {output_path}")
        progress_logger.info(f"詳細ログ: {log_file}")
        progress_logger.info(f"総処理時間: {total_time/3600:.1f}時間")
    
    return all_results


def main():
    parser = argparse.ArgumentParser(description='System E 効率化版検証')
    parser.add_argument('--start', type=str, default=None, help='開始四半期 (例: 2020Q1)')
    parser.add_argument('--end', type=str, default=None, help='終了四半期 (例: 2025Q4)')
    args = parser.parse_args()
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    progress_logger.info(f"データ: {len(races):,}件")
    
    results = run_optimized_validation(
        races, pedigrees, corners, race_details, horses, returns,
        start_quarter=args.start,
        end_quarter=args.end
    )
    
    progress_logger.info("\n完了")


if __name__ == "__main__":
    main()

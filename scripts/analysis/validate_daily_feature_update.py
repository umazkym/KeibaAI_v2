#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日次特徴量更新 Walk-forward 検証スクリプト（A案）

【改善点】
- モデル: 四半期ごとに再学習（計算コスト削減）
- 特徴量: 各レース日の前日までのデータで計算（最新情報を反映）

【リーク防止の厳格ルール】
1. 学習データ: race_date < 予測四半期開始日
2. 特徴量計算: 各レース日の前日以前のデータのみ使用
3. 同日レースは使用禁止（レース順序が不明）

【計算時間計測】
まず1四半期分で実測し、全体の時間を推定
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

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.models.multi_target_predictor import MultiTargetPredictor, calc_multi_roi, calc_roi_by_prob


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


def compute_daily_features(all_data, target_date, feature_result_cache=None):
    """
    特定日時点での特徴量を計算
    
    【ロジック】
    - target_date より前のレース結果のみを使用
    - target_date 当日のレースに対する特徴量を返す
    
    Args:
        all_data: 全レースデータ（特徴量計算のベース）
        target_date: 予測対象日（この日のレースを予測）
        feature_result_cache: 計算済み馬/騎手/調教師の累積統計（オプション）
    
    Returns:
        target_date 当日のレースデータに特徴量を付与したDataFrame
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
    
    # 累積計算（馬ごと）
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
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    # 過去データのみで前走を取得
    def get_prev_finish_for_date(group):
        result = pd.Series(index=group.index, dtype=float)
        past_races = group[group['race_date'] < target_date]
        
        for idx, row in group.iterrows():
            if len(past_races) > 0:
                result.loc[idx] = past_races.iloc[-1]['finish_position']
            else:
                result.loc[idx] = np.nan
        return result
    
    # 高速化のため、target_date以前の最終レースを事前計算
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


def run_daily_feature_validation(races, pedigrees, corners, race_details, horses, returns,
                                  test_quarters=None, measure_time_only=False):
    """
    日次特徴量更新Walk-forward検証
    
    Args:
        test_quarters: 検証する四半期のリスト（Noneの場合は全期間）
        measure_time_only: Trueの場合、1四半期分のみ実行して時間計測
    """
    logger.info("=" * 70)
    logger.info("日次特徴量更新 Walk-forward検証（A案: 特徴量日次更新）")
    logger.info("=" * 70)
    
    # 四半期リスト
    if test_quarters is None:
        test_quarters = []
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
                test_quarters.append((f'{year}Q{q}', start, end))
    
    if measure_time_only:
        test_quarters = test_quarters[:1]  # 1四半期のみ
        logger.info("\n【時間計測モード】1四半期分のみ実行")
    
    all_results = []
    total_time = 0
    
    for quarter_name, test_start, test_end in test_quarters:
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
        logger.info(f"[{quarter_name}] 日次特徴量更新検証")
        logger.info(f"  学習期間: ~{train_end.strftime('%Y-%m-%d')} ({len(train):,}件)")
        logger.info(f"  予測期間: {test_start}~{test_end} ({len(test):,}件)")
        logger.info("=" * 70)
        
        # ===== モデル学習（四半期開始時点） =====
        logger.info("  [1/3] 特徴量エンジン fit...")
        fit_start = time.time()
        
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        # 学習データ変換
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        
        # V16特徴量（学習データ用、train_end時点）
        # 学習データはtrain_end以前なので、シンプルなshift(1)で問題なし
        train_f = train_f.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        train_f['is_win'] = (train_f['finish_position'] == 1).astype(int)
        train_f['horse_prev_winrate'] = train_f.groupby('horse_id')['is_win'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        train_f['jockey_prev_winrate'] = train_f.groupby('jockey_id')['is_win'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        train_f['trainer_prev_winrate'] = train_f.groupby('trainer_id')['is_win'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        train_f['prev_finish'] = train_f.groupby('horse_id')['finish_position'].shift(1)
        
        # Valid用も同様
        valid_f = valid_f.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        valid_f['is_win'] = (valid_f['finish_position'] == 1).astype(int)
        valid_f['horse_prev_winrate'] = valid_f.groupby('horse_id')['is_win'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        valid_f['jockey_prev_winrate'] = valid_f.groupby('jockey_id')['is_win'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        valid_f['trainer_prev_winrate'] = valid_f.groupby('trainer_id')['is_win'].transform(
            lambda x: x.shift(1).expanding().mean()
        )
        valid_f['prev_finish'] = valid_f.groupby('horse_id')['finish_position'].shift(1)
        
        fit_time = time.time() - fit_start
        logger.info(f"    完了 ({fit_time:.1f}秒)")
        
        # 特徴量列
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        v16_features = ['horse_prev_winrate', 'jockey_prev_winrate', 'trainer_prev_winrate', 'prev_finish']
        feature_cols = list(dict.fromkeys(base_features + [f for f in v16_features if f in train_f.columns]))
        
        logger.info(f"  [2/3] モデル学習中...")
        model_start = time.time()
        
        predictor = MultiTargetPredictor(
            surface_specific=True,
            use_v44_residual=True,
            regularization_level='extreme',  # 極端正則化
            use_early_stopping=False,         # early_stopping無効化（過学習防止）
            fixed_iterations=80               # 固定イテレーション数
        )
        predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
        
        model_time = time.time() - model_start
        logger.info(f"    完了 ({model_time:.1f}秒)")
        
        # ===== テストデータ: 日次で特徴量を更新 =====
        logger.info(f"  [3/3] テストデータ予測（日次特徴量更新）...")
        predict_start = time.time()
        
        # テスト期間のユニーク日付
        test_dates = sorted(test['race_date'].unique())
        
        # 予測結果を蓄積
        all_test_preds = []
        all_test_actual = []
        
        # 全データ（学習+テスト）を用意
        # ただし、各日の予測時には当該日より前のデータのみ使用
        all_data = pd.concat([train, test], ignore_index=True)
        all_data = all_data.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        
        # 各テスト日ごとに特徴量を計算して予測
        for i, target_date in enumerate(test_dates):
            # 進捗表示（10日ごと）
            if i > 0 and i % 10 == 0:
                elapsed = time.time() - predict_start
                remaining = (len(test_dates) - i) / i * elapsed
                logger.info(f"    {i}/{len(test_dates)}日完了 (残り約{remaining/60:.1f}分)")
            
            # target_date時点の特徴量を計算
            target_df = compute_daily_features(all_data, target_date)
            
            if len(target_df) == 0:
                continue
            
            # V15特徴量適用（engineのtransform）
            target_f = engine.transform(target_df)
            
            # 予測
            if len(target_f) > 0 and all(c in target_f.columns for c in feature_cols):
                target_preds = predictor.predict(target_f)
                all_test_preds.append(target_preds)
                all_test_actual.append(target_f)
        
        predict_time = time.time() - predict_start
        logger.info(f"    完了 ({predict_time:.1f}秒, {len(test_dates)}日間)")
        
        # 結果統合
        if all_test_preds:
            test_preds = pd.concat(all_test_preds, ignore_index=True)
            test_f = pd.concat(all_test_actual, ignore_index=True)
            
            # ROI評価
            valid_preds = predictor.predict(valid_f)
            valid_roi_by_prob = calc_roi_by_prob(valid_preds, valid_f, returns)
            test_roi_by_prob = calc_roi_by_prob(test_preds, test_f, returns)
            
            # 結果出力
            logger.info(f"\n  【{quarter_name} 結果】")
            logger.info(f"  {'指数':<20} | {'Valid単勝ROI':>12} | {'Test単勝ROI':>12} | {'Gap':>8}")
            logger.info("  " + "-" * 60)
            
            quarter_result = {'quarter': quarter_name}
            
            for prob_col in ['win_prob', 'top2_prob', 'place_prob', 'ensemble_score']:
                valid_roi = valid_roi_by_prob[prob_col]['tansho_roi']
                test_roi = test_roi_by_prob[prob_col]['tansho_roi']
                gap = abs(valid_roi - test_roi)
                
                gap_marker = "⚠️" if gap > 30 else "✅"
                logger.info(f"  {prob_col:<20} | {valid_roi:>10.1f}% | {test_roi:>10.1f}% | {gap:>6.1f}% {gap_marker}")
                
                quarter_result[f'{prob_col}_valid_roi'] = valid_roi
                quarter_result[f'{prob_col}_test_roi'] = test_roi
                quarter_result[f'{prob_col}_gap'] = gap
            
            all_results.append(quarter_result)
        
        quarter_time = time.time() - quarter_start_time
        total_time += quarter_time
        logger.info(f"\n  四半期処理時間: {quarter_time/60:.1f}分")
    
    # 時間推定
    if measure_time_only and len(all_results) > 0:
        estimated_total = total_time * 24  # 24四半期
        logger.info(f"\n" + "=" * 70)
        logger.info(f"【時間推定】")
        logger.info(f"  1四半期処理時間: {total_time/60:.1f}分")
        logger.info(f"  全期間（24四半期）推定: {estimated_total/60:.1f}分 ≒ {estimated_total/3600:.1f}時間")
        logger.info("=" * 70)
    
    return all_results


def main():
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    logger.info(f"\n【データ概要】")
    logger.info(f"  レコード数: {len(races):,}")
    
    # まず時間計測（1四半期のみ）
    logger.info("\n" + "=" * 70)
    logger.info("【ステップ1】時間計測（1四半期分）")
    logger.info("=" * 70)
    
    results = run_daily_feature_validation(
        races, pedigrees, corners, race_details, horses, returns,
        measure_time_only=True
    )
    
    if results:
        logger.info("\n計測完了。全期間実行は別途ご指示ください。")
    
    logger.info("\n処理完了")


if __name__ == "__main__":
    main()

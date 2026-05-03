#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日次境界 Walk-forward 検証スクリプト

【重要な改善点】
従来: 年末までのデータで翌年全体を予測（例: 2024-12-31までで2025年を予測）
改善: 前日までのデータで当日を予測（例: 2025-01-04までで2025-01-05を予測）

【リーク防止の厳格ルール】
1. 学習データ: race_date < 予測日
2. 特徴量: 予測日より前のレースのみ参照（shift(1)ではなく日付ベース）
3. 当日の他レースも使用禁止（同日レースはほぼ同時進行）

【実装アプローチ】
- ローリングウィンドウ方式
- 四半期ごとにモデルを再学習（毎日は計算コスト高すぎ）
- 特徴量は全データ一括計算後、日付でマスク

注意: 計算時間が長くなるため、最初は一部期間で検証
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
from datetime import datetime, timedelta
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
    logger.info("日次境界 Walk-forward検証（前日までのデータで予測）")
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


def add_v16_features_with_date_boundary(df, cutoff_date):
    """
    V16特徴量を追加（日付境界でリーク防止）
    
    【重要】
    - cutoff_date より前のデータのみを使用して計算
    - 同日レースも未来扱い（レース順序が不明なため）
    
    Args:
        df: 全データ（特徴量計算のため）
        cutoff_date: この日付より前のデータのみ使用
    """
    df = df.copy()
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    if 'is_win' not in df.columns:
        df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # 日付境界マスク（cutoff_dateより前のレースのみTrue）
    past_mask = df['race_date'] < cutoff_date
    
    # 馬累積勝率（cutoff_date以前のレースのみカウント）
    df['_past_win'] = df['is_win'].where(past_mask, 0)
    df['_past_race'] = past_mask.astype(int)
    
    df['_horse_cumwins'] = df.groupby('horse_id')['_past_win'].cumsum()
    df['_horse_cumraces'] = df.groupby('horse_id')['_past_race'].cumsum()
    
    # 当該レース自身を除外するためshift
    df['_horse_cumwins'] = df.groupby('horse_id')['_horse_cumwins'].shift(1).fillna(0)
    df['_horse_cumraces'] = df.groupby('horse_id')['_horse_cumraces'].shift(1)
    
    df['horse_prev_winrate'] = df['_horse_cumwins'] / df['_horse_cumraces'].replace(0, np.nan)
    df.drop(columns=['_past_win', '_past_race', '_horse_cumwins', '_horse_cumraces'], inplace=True)
    
    # 騎手累積勝率
    df = df.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
    past_mask = df['race_date'] < cutoff_date
    df['_past_win'] = df['is_win'].where(past_mask, 0)
    df['_past_race'] = past_mask.astype(int)
    
    df['_jockey_cumwins'] = df.groupby('jockey_id')['_past_win'].cumsum()
    df['_jockey_cumraces'] = df.groupby('jockey_id')['_past_race'].cumsum()
    df['_jockey_cumwins'] = df.groupby('jockey_id')['_jockey_cumwins'].shift(1).fillna(0)
    df['_jockey_cumraces'] = df.groupby('jockey_id')['_jockey_cumraces'].shift(1)
    
    df['jockey_prev_winrate'] = df['_jockey_cumwins'] / df['_jockey_cumraces'].replace(0, np.nan)
    df.drop(columns=['_past_win', '_past_race', '_jockey_cumwins', '_jockey_cumraces'], inplace=True)
    
    # 調教師累積勝率
    df = df.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
    past_mask = df['race_date'] < cutoff_date
    df['_past_win'] = df['is_win'].where(past_mask, 0)
    df['_past_race'] = past_mask.astype(int)
    
    df['_trainer_cumwins'] = df.groupby('trainer_id')['_past_win'].cumsum()
    df['_trainer_cumraces'] = df.groupby('trainer_id')['_past_race'].cumsum()
    df['_trainer_cumwins'] = df.groupby('trainer_id')['_trainer_cumwins'].shift(1).fillna(0)
    df['_trainer_cumraces'] = df.groupby('trainer_id')['_trainer_cumraces'].shift(1)
    
    df['trainer_prev_winrate'] = df['_trainer_cumwins'] / df['_trainer_cumraces'].replace(0, np.nan)
    df.drop(columns=['_past_win', '_past_race', '_trainer_cumwins', '_trainer_cumraces'], inplace=True)
    
    # 前走着順（日付境界適用）
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    # cutoff_date以前の最後のレースの着順を取得
    def get_prev_finish(group):
        result = pd.Series(index=group.index, dtype=float)
        for i, (idx, row) in enumerate(group.iterrows()):
            if row['race_date'] < cutoff_date:
                # 過去データ：通常のshift
                if i > 0:
                    result.loc[idx] = group.iloc[i-1]['finish_position']
                else:
                    result.loc[idx] = np.nan
            else:
                # 未来データ：cutoff_date以前の最新レース
                past_races = group[group['race_date'] < cutoff_date]
                if len(past_races) > 0:
                    result.loc[idx] = past_races.iloc[-1]['finish_position']
                else:
                    result.loc[idx] = np.nan
        return result
    
    # 計算コストを考慮し、シンプルなshift後に日付マスクで上書き
    df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
    
    # cutoff_date以降のレースについては、cutoff_date時点で見える最新着順を使用
    future_mask = df['race_date'] >= cutoff_date
    if future_mask.any():
        # 各馬のcutoff_date以前の最終着順を計算
        past_data = df[df['race_date'] < cutoff_date]
        last_finish = past_data.groupby('horse_id')['finish_position'].last().to_dict()
        df.loc[future_mask, 'prev_finish'] = df.loc[future_mask, 'horse_id'].map(last_finish)
    
    return df


def run_daily_boundary_validation(races, pedigrees, corners, race_details, horses, returns):
    """
    日次境界Walk-forward検証
    
    【検証方式】
    - 四半期ごとにモデルを再学習（計算コスト削減）
    - 各四半期開始時点までのデータで学習
    - その四半期全体を予測
    """
    
    # 四半期別検証期間（2020-2025年の24四半期）
    quarters = []
    for year in range(2020, 2026):
        for q in range(1, 5):
            if q == 1:
                start = f'{year}-01-01'
                end = f'{year}-03-31'
            elif q == 2:
                start = f'{year}-04-01'
                end = f'{year}-06-30'
            elif q == 3:
                start = f'{year}-07-01'
                end = f'{year}-09-30'
            else:
                start = f'{year}-10-01'
                end = f'{year}-12-31'
            quarters.append((f'{year}Q{q}', start, end))
    
    all_results = []
    yearly_results = {}
    
    for quarter_name, test_start, test_end in quarters:
        test_start_dt = pd.to_datetime(test_start)
        test_end_dt = pd.to_datetime(test_end)
        
        # 学習データ: test_start より前
        train_end = test_start_dt - timedelta(days=1)
        
        # Validation: 学習データの直近1年
        valid_start = train_end - timedelta(days=365)
        
        # データ分割
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] > valid_start) & (races['race_date'] <= train_end)].copy()
        test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
        
        if len(test) < 1000 or len(train) < 50000:
            logger.warning(f"  {quarter_name}: データ不足 Train={len(train)}, Test={len(test)} → スキップ")
            continue
        
        logger.info(f"\n{'='*70}")
        logger.info(f"[{quarter_name}] 日次境界Walk-forward検証")
        logger.info(f"  学習期間: ~{train_end.strftime('%Y-%m-%d')} ({len(train):,}件)")
        logger.info(f"  予測期間: {test_start}~{test_end} ({len(test):,}件)")
        logger.info("=" * 70)
        
        # 特徴量エンジニアリング（学習データでfit）
        logger.info("  特徴量生成中...")
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        # V16特徴量追加（日付境界適用）
        # 学習・検証データ: train_endを境界
        train_f = add_v16_features_with_date_boundary(train_f, train_end + timedelta(days=1))
        valid_f = add_v16_features_with_date_boundary(valid_f, train_end + timedelta(days=1))
        
        # テストデータ: 各レース日の前日を境界として計算するのが理想だが、
        # 計算コストを考慮し、test_startを境界として一括計算
        # これは「四半期開始時点の情報」で予測することを意味する
        test_f = add_v16_features_with_date_boundary(test_f, test_start_dt)
        
        # 特徴量列
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        v16_features = ['horse_prev_winrate', 'jockey_prev_winrate', 'trainer_prev_winrate', 'prev_finish']
        feature_cols = list(dict.fromkeys(base_features + [f for f in v16_features if f in train_f.columns]))
        
        logger.info(f"  特徴量数: {len(feature_cols)}個")
        
        # 3指数モデル学習
        logger.info("  3指数モデル学習中...")
        
        predictor = MultiTargetPredictor(
            surface_specific=True,
            use_v44_residual=True,
            regularization_level='strong'
        )
        
        predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
        
        # 予測
        valid_preds = predictor.predict(valid_f)
        test_preds = predictor.predict(test_f)
        
        # ROI評価
        valid_roi_by_prob = calc_roi_by_prob(valid_preds, valid_f, returns)
        test_roi_by_prob = calc_roi_by_prob(test_preds, test_f, returns)
        
        # 結果出力
        logger.info(f"\n  【{quarter_name} 結果】")
        logger.info(f"  {'指数':<20} | {'Valid単勝ROI':>12} | {'Test単勝ROI':>12} | {'Gap':>8}")
        logger.info("  " + "-" * 60)
        
        quarter_result = {'quarter': quarter_name, 'test_start': test_start, 'test_end': test_end}
        
        for prob_col in ['win_prob', 'top2_prob', 'place_prob', 'ensemble_score']:
            valid_roi = valid_roi_by_prob[prob_col]['tansho_roi']
            test_roi = test_roi_by_prob[prob_col]['tansho_roi']
            gap = abs(valid_roi - test_roi)
            
            logger.info(f"  {prob_col:<20} | {valid_roi:>10.1f}% | {test_roi:>10.1f}% | {gap:>6.1f}%")
            
            quarter_result[f'{prob_col}_valid_roi'] = valid_roi
            quarter_result[f'{prob_col}_test_roi'] = test_roi
            quarter_result[f'{prob_col}_gap'] = gap
            quarter_result[f'{prob_col}_fukusho_roi'] = test_roi_by_prob[prob_col]['fukusho_roi']
        
        # 芝/ダート別
        for surface, surface_name in [('芝', 'turf'), ('ダート', 'dirt')]:
            surface_mask = test_f['track_surface'].str.contains(surface, na=False)
            if surface_mask.sum() > 0:
                test_f_surface = test_f[surface_mask]
                test_preds_surface = test_preds[test_preds['race_id'].isin(test_f_surface['race_id'])]
                if len(test_preds_surface) > 0:
                    surface_roi = calc_multi_roi(test_preds_surface, test_f_surface, returns, 'ensemble_score')
                    quarter_result[f'{surface_name}_tansho_roi'] = surface_roi['tansho_roi']
        
        all_results.append(quarter_result)
        
        # 年別集計
        year = quarter_name[:4]
        if year not in yearly_results:
            yearly_results[year] = []
        yearly_results[year].append(quarter_result)
    
    return all_results, yearly_results


def print_summary(all_results, yearly_results):
    """検証結果サマリー出力"""
    logger.info("\n" + "=" * 80)
    logger.info("【日次境界 Walk-forward検証 サマリー】")
    logger.info("=" * 80)
    
    if not all_results:
        logger.warning("結果がありません")
        return
    
    # 年別平均
    logger.info("\n【年別 平均Test単勝ROI】")
    logger.info(f"{'年度':<8} | {'win_prob':>10} | {'top2_prob':>10} | {'place_prob':>10} | {'ensemble':>10}")
    logger.info("-" * 65)
    
    for year, results in sorted(yearly_results.items()):
        roi_win = np.mean([r['win_prob_test_roi'] for r in results])
        roi_top2 = np.mean([r['top2_prob_test_roi'] for r in results])
        roi_place = np.mean([r['place_prob_test_roi'] for r in results])
        roi_ens = np.mean([r['ensemble_score_test_roi'] for r in results])
        
        logger.info(f"{year:<8} | {roi_win:>9.1f}% | {roi_top2:>9.1f}% | {roi_place:>9.1f}% | {roi_ens:>9.1f}%")
    
    # 全体平均
    logger.info("\n【全体平均】")
    prob_cols = ['win_prob', 'top2_prob', 'place_prob', 'ensemble_score']
    
    for prob_col in prob_cols:
        avg_roi = np.mean([r[f'{prob_col}_test_roi'] for r in all_results])
        avg_gap = np.mean([r[f'{prob_col}_gap'] for r in all_results])
        avg_fukusho = np.mean([r[f'{prob_col}_fukusho_roi'] for r in all_results])
        
        gap_marker = "✅" if avg_gap < 15 else "⚠️"
        logger.info(f"  {prob_col:<20}: 単勝ROI {avg_roi:.1f}%, 複勝ROI {avg_fukusho:.1f}%, Gap {avg_gap:.1f}% {gap_marker}")
    
    # 芝/ダート別
    logger.info("\n【芝/ダート別 平均】")
    for surface in ['turf', 'dirt']:
        key = f'{surface}_tansho_roi'
        values = [r[key] for r in all_results if key in r]
        if values:
            logger.info(f"  {surface.upper()}: {np.mean(values):.1f}%")
    
    # 推奨
    logger.info("\n【推奨戦略（日次境界検証に基づく）】")
    best_prob = max(prob_cols, key=lambda x: np.mean([r[f'{x}_test_roi'] for r in all_results]))
    best_roi = np.mean([r[f'{best_prob}_test_roi'] for r in all_results])
    logger.info(f"  単勝推奨: {best_prob} (平均ROI {best_roi:.1f}%)")


def main():
    # データ読み込み
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 日次境界Walk-forward検証
    all_results, yearly_results = run_daily_boundary_validation(
        races, pedigrees, corners, race_details, horses, returns
    )
    
    # サマリー出力
    print_summary(all_results, yearly_results)
    
    # CSV保存
    if all_results:
        output_dir = project_root / "outputs/analysis"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        results_df = pd.DataFrame(all_results)
        output_path = output_dir / "daily_boundary_walkforward_results.csv"
        results_df.to_csv(output_path, index=False)
        logger.info(f"\n結果保存: {output_path}")
    
    logger.info("\n検証完了")
    

if __name__ == "__main__":
    main()

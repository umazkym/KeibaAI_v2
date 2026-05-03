#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
各指標の正しいROI評価

【評価方法】
- win_prob → 単勝ROI（1着判定、単勝払戻）
- top2_prob → 馬連ROI（top2が1-2着、馬連払戻）
- place_prob → 複勝ROI（3着以内判定、複勝払戻）
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import sys
import warnings
import logging
import time

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.models.multi_target_predictor import MultiTargetPredictor


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
    """V16特徴量計算"""
    df = df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
    
    if 'is_win' not in df.columns:
        df['is_win'] = (df['finish_position'] == 1).astype(int)
    
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


def calc_tansho_roi(pred_df, test_df, returns_df, score_col='win_prob'):
    """
    単勝ROI計算
    - score_colでtop1の馬を選択
    - その馬が1着なら単勝払戻
    """
    merged = pred_df.merge(
        test_df[['race_id', 'horse_number', 'finish_position', 'win_odds']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    merged['rank_pred'] = merged.groupby('race_id')[score_col].rank(ascending=False, method='first')
    top1 = merged[merged['rank_pred'] == 1].copy()
    
    hits = top1[top1['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(top1) * 100 if len(top1) > 0 else 0
    hit_rate = len(hits) / len(top1) * 100 if len(top1) > 0 else 0
    
    return {'roi': roi, 'hit_rate': hit_rate, 'races': len(top1), 'hits': len(hits)}


def calc_fukusho_roi(pred_df, test_df, returns_df, score_col='place_prob'):
    """
    複勝ROI計算
    - score_colでtop1の馬を選択
    - その馬が3着以内なら複勝払戻
    """
    merged = pred_df.merge(
        test_df[['race_id', 'horse_number', 'finish_position']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    merged['rank_pred'] = merged.groupby('race_id')[score_col].rank(ascending=False, method='first')
    top1 = merged[merged['rank_pred'] == 1].copy()
    
    # 複勝払戻を取得（bet_type = 'fukusho'）
    fukusho_returns = returns_df[returns_df['bet_type'] == 'fukusho'].copy()
    
    # 各レースの複勝払戻をマージ
    hits_list = []
    for _, row in top1.iterrows():
        if row['finish_position'] <= 3:
            # 複勝的中 - 該当馬の払戻を探す
            race_returns = fukusho_returns[fukusho_returns['race_id'] == row['race_id']]
            horse_return = race_returns[race_returns['horse_number'] == row['horse_number']]
            if len(horse_return) > 0:
                hits_list.append({
                    'race_id': row['race_id'],
                    'horse_number': row['horse_number'],
                    'payout': horse_return['payout'].values[0]
                })
    
    hits_df = pd.DataFrame(hits_list)
    total_payout = hits_df['payout'].sum() / 100 if len(hits_df) > 0 else 0  # 100円単位
    roi = total_payout / len(top1) * 100 if len(top1) > 0 else 0
    hit_rate = len(hits_df) / len(top1) * 100 if len(top1) > 0 else 0
    
    return {
        'roi': roi, 
        'hit_rate': hit_rate, 
        'races': len(top1), 
        'hits': len(hits_df),
        'avg_payout': hits_df['payout'].mean() if len(hits_df) > 0 else 0
    }


def calc_umaren_roi(pred_df, test_df, returns_df, score_col='top2_prob'):
    """
    馬連ROI計算
    - score_colでtop2の馬を選択
    - その2頭が1-2着（順不同）なら馬連払戻
    """
    merged = pred_df.merge(
        test_df[['race_id', 'horse_number', 'finish_position']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    merged['rank_pred'] = merged.groupby('race_id')[score_col].rank(ascending=False, method='first')
    
    # 馬連払戻を取得（bet_type = 'umaren'、horse_1/horse_2に馬番）
    umaren_returns = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    
    hits_list = []
    race_ids = merged['race_id'].unique()
    
    for race_id in race_ids:
        race_data = merged[merged['race_id'] == race_id]
        top2 = race_data[race_data['rank_pred'] <= 2]
        
        if len(top2) < 2:
            continue
        
        # 予測top2の馬番（セット、順不同）
        pred_horses = set(top2['horse_number'].values)
        
        # 実際の1-2着
        actual_top2 = race_data[race_data['finish_position'] <= 2]
        actual_horses = set(actual_top2['horse_number'].values)
        
        # 馬連的中判定（順不同で一致）
        if pred_horses == actual_horses:
            race_returns = umaren_returns[umaren_returns['race_id'] == race_id]
            if len(race_returns) > 0:
                hits_list.append({
                    'race_id': race_id,
                    'payout': race_returns['payout'].values[0]
                })
    
    total_races = len(race_ids)
    hits_df = pd.DataFrame(hits_list)
    total_payout = hits_df['payout'].sum() / 100 if len(hits_df) > 0 else 0
    roi = total_payout / total_races * 100 if total_races > 0 else 0
    hit_rate = len(hits_df) / total_races * 100 if total_races > 0 else 0
    
    return {
        'roi': roi, 
        'hit_rate': hit_rate, 
        'races': total_races, 
        'hits': len(hits_df),
        'avg_payout': hits_df['payout'].mean() if len(hits_df) > 0 else 0
    }


def run_evaluation(races, pedigrees, corners, race_details, horses, returns):
    """年単位での評価実行"""
    
    test_years = [
        ('2020', '2020-01-01', '2020-12-31'),
        ('2021', '2021-01-01', '2021-12-31'),
        ('2022', '2022-01-01', '2022-12-31'),
        ('2023', '2023-01-01', '2023-12-31'),
        ('2024', '2024-01-01', '2024-12-31'),
        ('2025', '2025-01-01', '2025-12-31'),
    ]
    
    results = []
    
    for year_idx, (test_year, test_start, test_end) in enumerate(test_years):
        test_start_dt = pd.to_datetime(test_start)
        test_end_dt = pd.to_datetime(test_end)
        train_end = test_start_dt - timedelta(days=1)
        valid_start = train_end - timedelta(days=365)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
        
        if len(test) < 5000:
            continue
        
        logger.info(f"\n[{year_idx+1}/6] {test_year}年 (Train:{len(train):,} Test:{len(test):,})")
        
        # 特徴量エンジン
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        all_data = pd.concat([train, test], ignore_index=True)
        all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
        all_data_f = engine.transform(all_data)
        all_data_f = compute_v16_features(all_data_f, train_end)
        
        train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
        valid_f = train_f[(train_f['race_date'] > valid_start)].copy()
        test_f = all_data_f[(all_data_f['race_date'] >= test_start_dt) & (all_data_f['race_date'] <= test_end_dt)].copy()
        
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        v16_features = ['horse_prev_winrate', 'jockey_prev_winrate', 'trainer_prev_winrate', 'prev_finish']
        feature_cols = list(dict.fromkeys(base_features + [f for f in v16_features if f in train_f.columns]))
        
        # モデル学習（iterations=50, V4.4なし）
        predictor = MultiTargetPredictor(
            surface_specific=True,
            use_v44_residual=False,
            regularization_level='strong',
            use_early_stopping=False,
            fixed_iterations=50
        )
        predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
        
        # 予測
        test_preds = predictor.predict(test_f)
        
        # 各券種ROI計算
        tansho = calc_tansho_roi(test_preds, test_f, returns, 'win_prob')
        fukusho = calc_fukusho_roi(test_preds, test_f, returns, 'place_prob')
        umaren = calc_umaren_roi(test_preds, test_f, returns, 'top2_prob')
        
        result = {
            'year': test_year,
            'tansho_roi': tansho['roi'],
            'tansho_hit_rate': tansho['hit_rate'],
            'fukusho_roi': fukusho['roi'],
            'fukusho_hit_rate': fukusho['hit_rate'],
            'fukusho_avg_payout': fukusho['avg_payout'],
            'umaren_roi': umaren['roi'],
            'umaren_hit_rate': umaren['hit_rate'],
            'umaren_avg_payout': umaren['avg_payout'],
        }
        results.append(result)
        
        logger.info(f"  単勝: ROI {tansho['roi']:.1f}%, 的中率 {tansho['hit_rate']:.1f}%")
        logger.info(f"  複勝: ROI {fukusho['roi']:.1f}%, 的中率 {fukusho['hit_rate']:.1f}%, 平均配当 {fukusho['avg_payout']:.0f}円")
        logger.info(f"  馬連: ROI {umaren['roi']:.1f}%, 的中率 {umaren['hit_rate']:.1f}%, 平均配当 {umaren['avg_payout']:.0f}円")
    
    # サマリ
    logger.info("\n" + "=" * 60)
    logger.info("【6年平均】")
    logger.info("=" * 60)
    
    results_df = pd.DataFrame(results)
    
    logger.info(f"  単勝: ROI {results_df['tansho_roi'].mean():.1f}%, 的中率 {results_df['tansho_hit_rate'].mean():.1f}%")
    logger.info(f"  複勝: ROI {results_df['fukusho_roi'].mean():.1f}%, 的中率 {results_df['fukusho_hit_rate'].mean():.1f}%")
    logger.info(f"  馬連: ROI {results_df['umaren_roi'].mean():.1f}%, 的中率 {results_df['umaren_hit_rate'].mean():.1f}%")
    
    # CSV保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "correct_roi_evaluation.csv"
    results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"\n結果保存: {output_path}")
    
    return results_df


def main():
    logger.info("=" * 60)
    logger.info("各指標の正しいROI評価")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    logger.info(f"データ: {len(races):,}件")
    
    results = run_evaluation(races, pedigrees, corners, race_details, horses, returns)
    
    logger.info("\n完了")


if __name__ == "__main__":
    main()

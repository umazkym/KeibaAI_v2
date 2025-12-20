#!/usr/bin/env python3
"""
V15 Walk-Forward検証

各年度（2022-2025）でTrain/Testを分割し、V15モデルのROIを検証
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data():
    """データ読み込み"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, horses, race_details


def train_and_evaluate(train_df, test_df, pedigrees, corners, horses, race_details, year):
    """V15モデルを学習して評価（複数戦略シミュレーション）"""
    # 特徴量エンジニア
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train_df, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train_df)
    test_f = engine.transform(test_df)
    
    # 特徴量カラム
    feature_cols = engine.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    
    # モデル学習
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'learning_rate': 0.03,
        'max_depth': 3,
        'num_leaves': 20,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'bagging_fraction': 0.7,
        'feature_fraction': 0.7,
        'verbose': -1,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    # 予測
    train_f['pred'] = model.predict(X_train)
    test_f['pred'] = model.predict(X_test)
    
    # ランキング
    train_f['pred_rank'] = train_f.groupby('race_id')['pred'].rank(ascending=False)
    test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False)
    
    # 戦略シミュレーション
    strategies = [
        {'name': 'R1', 'rank': [1], 'min_odds': 0.0},
        {'name': 'R1_O15', 'rank': [1], 'min_odds': 1.5},
        {'name': 'R1_O20', 'rank': [1], 'min_odds': 2.0},
        {'name': 'R2', 'rank': [1, 2], 'min_odds': 0.0},
        {'name': 'R2_O15', 'rank': [1, 2], 'min_odds': 1.5},
        {'name': 'R2_O20', 'rank': [1, 2], 'min_odds': 2.0},
        {'name': 'R3_O20', 'rank': [1, 2, 3], 'min_odds': 2.0},
    ]
    
    results = {}
    
    for strat in strategies:
        def calc_metrics(df):
            # 対象馬抽出
            target = df[
                (df['pred_rank'].isin(strat['rank'])) & 
                (df['win_odds'] >= strat['min_odds'])
            ]
            
            # 購入数
            count = len(target)
            if count == 0:
                return 0.0, 0.0, 0
            
            # 払い戻し
            wins = target[target['finish_position'] == 1]
            return_amount = wins['win_odds'].sum() * 100
            
            # ROI
            roi = return_amount / (count * 100) * 100
            
            # 的中率
            hit_rate = len(wins) / count * 100
            
            return roi, hit_rate, count
        
        train_roi, train_hit, train_count = calc_metrics(train_f)
        test_roi, test_hit, test_count = calc_metrics(test_f)
        
        results[strat['name']] = {
            'year': year,
            'strategy': strat['name'],
            'train_roi': train_roi,
            'test_roi': test_roi,
            'test_count': test_count,
            'test_hit': test_hit,
            'test_total_return': (test_count * 100) * (test_roi / 100) - (test_count * 100)
        }
        
    return results


def main():
    logger.info("=" * 60)
    logger.info("V15 Strategy Optimization (2022-2025)")
    logger.info("=" * 60)
    
    races, pedigrees, corners, horses, race_details = load_data()
    logger.info(f"Total races: {len(races):,}")
    
    # Walk-Forward設定
    periods = [
        {'year': 2022, 'train_end': '2021-12-31', 'test_start': '2022-01-01', 'test_end': '2022-12-31'},
        {'year': 2023, 'train_end': '2022-12-31', 'test_start': '2023-01-01', 'test_end': '2023-12-31'},
        {'year': 2024, 'train_end': '2023-12-31', 'test_start': '2024-01-01', 'test_end': '2024-12-31'},
        {'year': 2025, 'train_end': '2024-12-31', 'test_start': '2025-01-01', 'test_end': '2025-11-01'},
    ]
    
    all_results = []
    
    for period in periods:
        year = period['year']
        logger.info(f"\n{'='*40}")
        logger.info(f"Year {year}")
        logger.info(f"{'='*40}")
        
        train = races[races['race_date'] <= period['train_end']].copy()
        test = races[(races['race_date'] >= period['test_start']) & 
                     (races['race_date'] < period['test_end'])].copy()
        
        if len(test) == 0: continue
        
        logger.info(f"  Train: {len(train):,}, Test: {len(test):,}")
        
        res = train_and_evaluate(train, test, pedigrees, corners, horses, race_details, year)
        
        for strat_name, data in res.items():
            all_results.append(data)
            logger.info(f"  [{strat_name: <8}] ROI: {data['test_roi']:.1f}% (Hit: {data['test_hit']:.1f}%, Count: {data['test_count']:,})")
    
    df = pd.DataFrame(all_results)
    
    # 戦略ごとの集計
    logger.info("\n" + "=" * 60)
    logger.info("STRATEGY SUMMARY (Average across years)")
    logger.info("=" * 60)
    
    summary = df.groupby('strategy').agg({
        'test_roi': ['mean', 'std', 'min', 'max'],
        'test_count': 'sum',
        'test_total_return': 'sum'
    }).sort_values(('test_roi', 'mean'), ascending=False)
    
    print(summary)
    
    best_strat = summary.index[0]
    logger.info(f"\n🏆 Best Strategy: {best_strat}")
    
    return df


if __name__ == "__main__":
    results_df = main()

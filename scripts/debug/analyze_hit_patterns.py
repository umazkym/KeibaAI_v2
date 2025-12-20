#!/usr/bin/env python3
"""
モデル的中馬の特徴分析

予測Top1-2で的中した馬の特徴を深掘りし、
どのようなパターンの馬を正しく予測できているかを分析
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


def analyze_hit_patterns(test_f):
    """的中馬と不的中馬の比較分析"""
    
    # 予測Top2で的中した馬
    top2 = test_f[test_f['pred_rank'] <= 2].copy()
    hits = top2[top2['finish_position'] == 1].copy()
    misses = top2[top2['finish_position'] != 1].copy()
    
    print("\n" + "=" * 70)
    print("【 的中馬 vs 不的中馬 比較分析 】")
    print("=" * 70)
    
    print(f"\n予測Top2での購入対象: {len(top2):,}件")
    print(f"  的中: {len(hits):,}件 ({len(hits)/len(top2)*100:.1f}%)")
    print(f"  不的中: {len(misses):,}件")
    
    # --- 1. オッズ帯別分析 ---
    print("\n" + "-" * 50)
    print("1. オッズ帯別 的中傾向")
    print("-" * 50)
    
    odds_bins = [0, 2, 3, 5, 10, 20, 50, 999]
    odds_labels = ['~2倍', '2-3倍', '3-5倍', '5-10倍', '10-20倍', '20-50倍', '50倍~']
    top2['odds_bin'] = pd.cut(top2['win_odds'], bins=odds_bins, labels=odds_labels)
    
    odds_analysis = top2.groupby('odds_bin', observed=True).agg({
        'finish_position': lambda x: (x == 1).sum(),  # 的中数
        'win_odds': ['count', 'mean'],
    })
    odds_analysis.columns = ['的中数', '購入数', '平均オッズ']
    odds_analysis['的中率'] = odds_analysis['的中数'] / odds_analysis['購入数'] * 100
    odds_analysis['回収額'] = hits.groupby(pd.cut(hits['win_odds'], bins=odds_bins, labels=odds_labels), observed=True)['win_odds'].sum().reindex(odds_analysis.index).fillna(0) * 100
    odds_analysis['投資額'] = odds_analysis['購入数'] * 100
    odds_analysis['ROI'] = odds_analysis['回収額'] / odds_analysis['投資額'] * 100
    
    print(odds_analysis.to_string())
    
    # --- 2. 脚質別分析 ---
    print("\n" + "-" * 50)
    print("2. 脚質別 的中傾向 (horse_front_runner_rate)")
    print("-" * 50)
    
    if 'horse_front_runner_rate' in top2.columns:
        fr_bins = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
        fr_labels = ['差し追込(~0.2)', '差し(0.2-0.4)', '自在(0.4-0.6)', '先行(0.6-0.8)', '逃げ(0.8~)']
        top2['style_bin'] = pd.cut(top2['horse_front_runner_rate'].fillna(0.5), bins=fr_bins, labels=fr_labels)
        
        style_analysis = top2.groupby('style_bin', observed=True).agg({
            'finish_position': lambda x: (x == 1).sum(),
            'win_odds': ['count', 'mean'],
        })
        style_analysis.columns = ['的中数', '購入数', '平均オッズ']
        style_analysis['的中率'] = style_analysis['的中数'] / style_analysis['購入数'] * 100
        print(style_analysis.to_string())
    
    # --- 3. 直線距離×脚質 (交互作用効果確認) ---
    print("\n" + "-" * 50)
    print("3. 直線距離×脚質 交互作用分析")
    print("-" * 50)
    
    if 'is_long_straight' in top2.columns and 'horse_front_runner_rate' in top2.columns:
        top2['is_closer'] = top2['horse_front_runner_rate'] < 0.3
        
        cross_analysis = top2.groupby(['is_long_straight', 'is_closer']).agg({
            'finish_position': lambda x: (x == 1).sum(),
            'win_odds': 'count',
        })
        cross_analysis.columns = ['的中数', '購入数']
        cross_analysis['的中率'] = cross_analysis['的中数'] / cross_analysis['購入数'] * 100
        
        print("is_long_straight × is_closer (差し馬)")
        print(cross_analysis.to_string())
    
    # --- 4. 競馬場別分析 ---
    print("\n" + "-" * 50)
    print("4. 競馬場別 的中傾向")
    print("-" * 50)
    
    venue_analysis = top2.groupby('venue', observed=True).agg({
        'finish_position': lambda x: (x == 1).sum(),
        'win_odds': ['count', 'mean'],
    })
    venue_analysis.columns = ['的中数', '購入数', '平均オッズ']
    venue_analysis['的中率'] = venue_analysis['的中数'] / venue_analysis['購入数'] * 100
    venue_analysis = venue_analysis.sort_values('的中率', ascending=False)
    print(venue_analysis.to_string())
    
    # --- 5. 距離別分析 ---
    print("\n" + "-" * 50)
    print("5. 距離別 的中傾向")
    print("-" * 50)
    
    if 'distance_m' in top2.columns:
        dist_bins = [0, 1200, 1600, 2000, 2400, 9999]
        dist_labels = ['短距離(~1200)', 'マイル(1201-1600)', '中距離(1601-2000)', '中長距離(2001-2400)', '長距離(2401~)']
        top2['dist_bin'] = pd.cut(top2['distance_m'], bins=dist_bins, labels=dist_labels)
        
        dist_analysis = top2.groupby('dist_bin', observed=True).agg({
            'finish_position': lambda x: (x == 1).sum(),
            'win_odds': ['count', 'mean'],
        })
        dist_analysis.columns = ['的中数', '購入数', '平均オッズ']
        dist_analysis['的中率'] = dist_analysis['的中数'] / dist_analysis['購入数'] * 100
        print(dist_analysis.to_string())
    
    # --- 6. 高配当的中例 (オッズ10倍以上で的中) ---
    print("\n" + "-" * 50)
    print("6. 高配当的中例 (オッズ10倍以上で1着)")
    print("-" * 50)
    
    high_odds_hits = hits[hits['win_odds'] >= 10].sort_values('win_odds', ascending=False)
    
    if len(high_odds_hits) > 0:
        cols_to_show = ['race_id', 'horse_name', 'win_odds', 'finish_position', 'venue', 'distance_m']
        cols_to_show = [c for c in cols_to_show if c in high_odds_hits.columns]
        
        print(f"高配当的中数: {len(high_odds_hits)}件")
        print(f"高配当合計: {high_odds_hits['win_odds'].sum():.1f}万円相当")
        print("\nTop 20件:")
        print(high_odds_hits[cols_to_show].head(20).to_string())
    
    # --- 7. 不的中分析 (Top2予測なのに1着取れず) ---
    print("\n" + "-" * 50)
    print("7. 不的中パターン分析")
    print("-" * 50)
    
    # Top1予測で外した馬の実際の着順
    top1_miss = test_f[(test_f['pred_rank'] == 1) & (test_f['finish_position'] != 1)]
    print(f"\nTop1予測の不的中: {len(top1_miss)}件")
    print("実際の着順分布:")
    print(top1_miss['finish_position'].value_counts().sort_index().head(10))
    
    # オッズ帯別の不的中傾向
    print("\nオッズ帯別 不的中数:")
    top1_miss['odds_bin'] = pd.cut(top1_miss['win_odds'], bins=odds_bins, labels=odds_labels)
    print(top1_miss['odds_bin'].value_counts())
    
    return hits, misses


def main():
    logger.info("=" * 60)
    logger.info("的中馬特徴分析 (2024年データ)")
    logger.info("=" * 60)
    
    races, pedigrees, corners, horses, race_details = load_data()
    
    # 2024年のみで分析
    train = races[races['race_date'] <= '2023-12-31'].copy()
    test = races[(races['race_date'] >= '2024-01-01') & 
                 (races['race_date'] < '2024-12-31')].copy()
    
    logger.info(f"Train: {len(train):,}, Test: {len(test):,}")
    
    # 特徴量生成
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    test_f = engine.transform(test)
    
    # モデル学習
    feature_cols = engine.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in test_f.columns]
    
    train_f = engine.transform(train)
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
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
    X_test = test_f[feature_cols].fillna(0)
    test_f['pred'] = model.predict(X_test)
    test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False)
    
    # 分析実行
    hits, misses = analyze_hit_patterns(test_f)
    
    # 特徴量重要度
    print("\n" + "=" * 70)
    print("【 特徴量重要度 Top 20 】")
    print("=" * 70)
    
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    print(importance.head(20).to_string())
    
    return test_f, hits, misses, importance


if __name__ == "__main__":
    test_f, hits, misses, importance = main()

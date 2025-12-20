#!/usr/bin/env python3
"""
jockey_venue_win_rate 分布調査
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import logging

from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data():
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    return races

def check_distribution():
    races = load_data()
    train_df = races[races['race_date'] < '2024-01-01'].copy()
    
    logger.info("Generating features...")
    v16 = LeakFreeFeatureEngineerV16()
    # 簡易fit (親クラスのfitは省略して、必要な統計だけ計算するハックもできるが、正攻法でいく)
    # 時間短縮のため直近3年分だけでfit/transformしてみる
    sample_train = train_df[train_df['race_date'] >= '2020-01-01'].copy()
    
    # 必要なデータだけダミーで渡す（エラー回避）
    v16.fit(sample_train, None, None, None, horses_df=None)
    
    # transform実行（内部で_calc_jockey_venue_statsが呼ばれる）
    # ただしfitで統計が作られるので、transform後のデータを見れば良い
    feat_df = v16.transform(sample_train)
    
    print("\n--- Jockey Venue Win Rate Statistics ---")
    print(feat_df['jockey_venue_win_rate'].describe())
    
    print("\n--- Histogram (Non-zero) ---")
    nonzero = feat_df[feat_df['jockey_venue_win_rate'] > 0]['jockey_venue_win_rate']
    print(pd.cut(nonzero, bins=10).value_counts().sort_index())
    
    print("\n--- Check raw values (Head 20) ---")
    print(feat_df[['race_id', 'jockey_id', 'venue_id', 'jockey_venue_win_rate']].head(20))

if __name__ == "__main__":
    check_distribution()

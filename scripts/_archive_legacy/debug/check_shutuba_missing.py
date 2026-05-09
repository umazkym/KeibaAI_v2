#!/usr/bin/env python3
"""
shutuba.parquet 欠損率再確認スクリプト
結果カラムの欠損率を数値で明確に出力する
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def check_shutuba_missing_rates():
    path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
    if not path.exists():
        return

    logging.info("=" * 80)
    logging.info("Checking shutuba.parquet Missing Rates (Pre-2024)")
    df = pd.read_parquet(path)
    
    df['race_date'] = pd.to_datetime(df['race_date'])
    df_old = df[df['race_date'] < '2024-01-01']
    
    logging.info(f"Target rows: {len(df_old):,}")
    
    target_cols = [
        'finish_position', 
        'passing_order_2', 
        'passing_order_4', 
        'last_3f_time',
        'jockey_id',
        'trainer_id'
    ]
    
    logging.info("-" * 60)
    logging.info(f"{'Column':<20} | {'Null Count':<10} | {'Missing Rate':<10}")
    logging.info("-" * 60)
    
    for col in target_cols:
        if col in df_old.columns:
            null_count = df_old[col].isnull().sum()
            rate = null_count / len(df_old)
            logging.info(f"{col:<20} | {null_count:<10,} | {rate:.1%}")
        else:
            logging.info(f"{col:<20} | {'NOT FOUND':<10} | -")

if __name__ == "__main__":
    check_shutuba_missing_rates()

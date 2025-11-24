#!/usr/bin/env python3
"""
データ詳細検証スクリプト v2
1. horses_performanceのIDと成績データの「空文字」「NaN」を厳密に区別してカウント
2. pedigreesのデータ構造（縦持ちか横持ちか）を確認
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def check_horses_performance():
    path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')
    if not path.exists():
        return

    logging.info("=" * 80)
    logging.info("Checking horses_performance.parquet (Detail)")
    df = pd.read_parquet(path)
    
    # 日付フィルタ（2023年以前）
    df['race_date'] = pd.to_datetime(df['race_date'])
    df_old = df[df['race_date'] < '2024-01-01']
    logging.info(f"Target rows (pre-2024): {len(df_old):,}")
    
    cols = ['jockey_id', 'trainer_id', 'passing_order_2', 'last_3f_time']
    
    for col in cols:
        if col not in df.columns:
            logging.info(f"❌ {col}: Column not found!")
            continue
            
        # NaNチェック
        nan_count = df_old[col].isnull().sum()
        
        # 空文字チェック (文字列型の場合)
        empty_str_count = 0
        if df_old[col].dtype == 'object':
            empty_str_count = (df_old[col] == '').sum()
            
        # 0チェック (数値型の場合)
        zero_count = 0
        if pd.api.types.is_numeric_dtype(df_old[col]):
            zero_count = (df_old[col] == 0).sum()
            
        logging.info(f"Column: {col}")
        logging.info(f"  NaN: {nan_count} ({nan_count/len(df_old):.1%})")
        logging.info(f"  Empty Str: {empty_str_count} ({empty_str_count/len(df_old):.1%})")
        logging.info(f"  Zero: {zero_count} ({zero_count/len(df_old):.1%})")
        logging.info(f"  Sample: {df_old[col].dropna().head(3).tolist()}")

def check_pedigrees():
    path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    if not path.exists():
        return

    logging.info("=" * 80)
    logging.info("Checking pedigrees.parquet (Structure)")
    df = pd.read_parquet(path)
    
    logging.info(f"Columns: {df.columns.tolist()}")
    logging.info(f"Rows: {len(df):,}")
    logging.info(f"Sample:\n{df.head(5)}")
    
    # horse_idごとのレコード数
    counts = df['horse_id'].value_counts()
    logging.info(f"Records per horse (mean): {counts.mean():.1f}")
    logging.info(f"Records per horse (max): {counts.max()}")

if __name__ == "__main__":
    check_horses_performance()
    check_pedigrees()

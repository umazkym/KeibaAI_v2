#!/usr/bin/env python3
"""
過去データ品質診断スクリプト
horses_performance.parquet のカラムとデータ内容を検査
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def inspect_history_data():
    path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')
    if not path.exists():
        logging.error(f"File not found: {path}")
        return

    logging.info(f"Reading {path}...")
    df = pd.read_parquet(path)
    
    logging.info(f"Total rows: {len(df):,}")
    logging.info(f"Columns: {len(df.columns)}")
    
    # ターゲットとなるカラム
    target_cols = [
        'finish_position', 
        'passing_order_2', 
        'passing_order_4', 
        'last_3f_time',
        'horse_id', 
        'race_date'
    ]
    
    logging.info("-" * 50)
    logging.info("Target Columns Inspection:")
    
    for col in target_cols:
        if col not in df.columns:
            logging.warning(f"❌ {col}: MISSING")
            continue
            
        # データ型
        dtype = df[col].dtype
        # 欠損数
        null_count = df[col].isnull().sum()
        # 0の数
        zero_count = (df[col] == 0).sum() if pd.api.types.is_numeric_dtype(dtype) else 0
        # サンプル
        sample = df[col].dropna().head(5).tolist()
        
        logging.info(f"✅ {col:<20} | Type: {str(dtype):<10} | Null: {null_count:>7} ({null_count/len(df):.1%}) | Zero: {zero_count:>7} ({zero_count/len(df):.1%}) | Sample: {sample}")

    # 2023年のデータに限定してチェック
    if 'race_date' in df.columns:
        df['race_date'] = pd.to_datetime(df['race_date'])
        df_2023 = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] <= '2023-12-31')]
        logging.info("-" * 50)
        logging.info(f"2023 Data Inspection ({len(df_2023):,} rows):")
        
        for col in target_cols:
            if col not in df_2023.columns: continue
            null_count = df_2023[col].isnull().sum()
            logging.info(f"{col:<20} | Null: {null_count:>7} ({null_count/len(df_2023):.1%})")

if __name__ == "__main__":
    inspect_history_data()

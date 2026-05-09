#!/usr/bin/env python3
"""
データソース詳細診断スクリプト
horses_performance.parquet と shutuba.parquet の年別データ品質を調査
"""
import pandas as pd
from pathlib import Path
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(message)s')

def check_dataframe_quality(df, name, date_col='race_date'):
    logging.info("=" * 80)
    logging.info(f"Checking {name}...")
    logging.info(f"Total rows: {len(df):,}")
    
    if date_col not in df.columns:
        logging.error(f"Date column '{date_col}' not found!")
        return

    df[date_col] = pd.to_datetime(df[date_col])
    df['year'] = df[date_col].dt.year
    
    # チェック対象のカラム
    target_cols = [
        'passing_order_2', 'passing_order_4', 
        'last_3f_time', 'finish_position',
        'horse_weight', 'jockey_id', 'trainer_id'
    ]
    
    # 存在するカラムのみチェック
    cols_to_check = [c for c in target_cols if c in df.columns]
    
    # 年ごとの欠損率を集計
    years = sorted(df['year'].unique())
    
    logging.info("-" * 80)
    logging.info(f"{'Year':<6} | {'Rows':<8} | " + " | ".join([f"{c[:10]:<10}" for c in cols_to_check]))
    logging.info("-" * 80)
    
    for year in years:
        year_df = df[df['year'] == year]
        row_count = len(year_df)
        
        row_str = f"{year:<6} | {row_count:<8,}"
        
        for col in cols_to_check:
            null_count = year_df[col].isnull().sum()
            zero_count = (year_df[col] == 0).sum() if pd.api.types.is_numeric_dtype(year_df[col]) else 0
            
            # 欠損または0の割合
            missing_rate = (null_count + zero_count) / row_count
            
            # 表示色分け（文字ベース）
            if missing_rate > 0.9:
                val_str = f"!!{missing_rate:.0%}!!"
            elif missing_rate > 0.5:
                val_str = f"!{missing_rate:.0%}!"
            else:
                val_str = f"{missing_rate:.0%}"
                
            row_str += f" | {val_str:<10}"
        
        logging.info(row_str)

def main():
    # 1. horses_performance (過去結果)
    history_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')
    if history_path.exists():
        df_hist = pd.read_parquet(history_path)
        check_dataframe_quality(df_hist, "horses_performance.parquet")
    else:
        logging.error(f"Not found: {history_path}")

    # 2. shutuba (出馬表)
    shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
    if shutuba_path.exists():
        df_shutuba = pd.read_parquet(shutuba_path)
        check_dataframe_quality(df_shutuba, "shutuba.parquet")
    else:
        logging.error(f"Not found: {shutuba_path}")

if __name__ == "__main__":
    main()

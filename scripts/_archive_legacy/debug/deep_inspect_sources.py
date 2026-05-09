#!/usr/bin/env python3
"""
データソース深層分析スクリプト
shutuba.parquet と races.parquet に「レース結果情報」が含まれているかを確認する。
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def inspect_shutuba_detailed():
    path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
    if not path.exists():
        logging.error(f"Not found: {path}")
        return

    logging.info("=" * 80)
    logging.info("🔍 Checking shutuba.parquet for Result Data")
    df = pd.read_parquet(path)
    
    logging.info(f"Total rows: {len(df):,}")
    logging.info(f"Columns: {df.columns.tolist()}")
    
    # 結果に関連しそうなキーワード
    result_keywords = ['finish', 'order', 'time', 'odds', 'pop', 'prize', 'result', 'chakojun']
    result_cols = [c for c in df.columns if any(k in c.lower() for k in result_keywords)]
    
    logging.info("-" * 50)
    logging.info(f"Potential Result Columns found: {result_cols}")
    
    if result_cols:
        # 2023年以前のデータで確認
        df['race_date'] = pd.to_datetime(df['race_date'])
        df_old = df[df['race_date'] < '2024-01-01']
        
        logging.info(f"Checking pre-2024 data ({len(df_old):,} rows)...")
        for col in result_cols:
            null_count = df_old[col].isnull().sum()
            zero_count = (df_old[col] == 0).sum() if pd.api.types.is_numeric_dtype(df_old[col]) else 0
            sample = df_old[col].dropna().head(5).tolist()
            
            logging.info(f"  {col:<20} | Null: {null_count:>7} ({null_count/len(df_old):.1%}) | Zero: {zero_count:>7} | Sample: {sample}")
    else:
        logging.warning("⚠️ No result-related columns found in shutuba.parquet!")

def inspect_races_detailed():
    path = Path('keibaai/data/parsed/parquet/races/races.parquet')
    if not path.exists():
        logging.error(f"Not found: {path}")
        return

    logging.info("=" * 80)
    logging.info("🔍 Checking races.parquet for Result/Pace Data")
    df = pd.read_parquet(path)
    
    logging.info(f"Total rows: {len(df):,}")
    logging.info(f"Columns: {df.columns.tolist()}")
    
    # ペースやラップタイムに関連するカラム
    pace_keywords = ['pace', 'lap', 'time', 'passing']
    pace_cols = [c for c in df.columns if any(k in c.lower() for k in pace_keywords)]
    
    logging.info("-" * 50)
    logging.info(f"Potential Pace/Time Columns found: {pace_cols}")
    
    if pace_cols:
        for col in pace_cols:
            sample = df[col].dropna().head(5).tolist()
            logging.info(f"  {col:<20} | Sample: {sample}")

if __name__ == "__main__":
    inspect_shutuba_detailed()
    inspect_races_detailed()

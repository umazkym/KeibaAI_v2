#!/usr/bin/env python3
"""
races.parquet 詳細分析スクリプト
ラップタイム、ペース、通過順位などのレース単位情報を調査
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def inspect_races_deeply():
    path = Path('keibaai/data/parsed/parquet/races/races.parquet')
    if not path.exists():
        logging.error(f"Not found: {path}")
        return

    logging.info("=" * 80)
    logging.info("🔍 Checking races.parquet (Deep Inspection)")
    df = pd.read_parquet(path)
    
    logging.info(f"Total rows: {len(df):,}")
    logging.info(f"Columns: {df.columns.tolist()}")
    
    # ペース・ラップ関連のカラム候補
    pace_cols = [c for c in df.columns if any(k in c.lower() for k in ['lap', 'pace', 'time', 'passing', 'corner'])]
    
    logging.info("-" * 50)
    logging.info(f"Potential Pace/Time Columns: {pace_cols}")
    
    if pace_cols:
        # 2023年以前のデータで確認
        if 'race_date' in df.columns:
            df['race_date'] = pd.to_datetime(df['race_date'])
            df_old = df[df['race_date'] < '2024-01-01']
            logging.info(f"Checking pre-2024 data ({len(df_old):,} rows)...")
            
            for col in pace_cols:
                null_count = df_old[col].isnull().sum()
                sample = df_old[col].dropna().head(3).tolist()
                logging.info(f"  {col:<25} | Null: {null_count:>6} ({null_count/len(df_old):.1%}) | Sample: {sample}")
        else:
            logging.warning("⚠️ 'race_date' column not found!")

if __name__ == "__main__":
    inspect_races_deeply()

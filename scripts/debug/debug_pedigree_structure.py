#!/usr/bin/env python3
"""
血統データ構造詳細確認スクリプト
generation=1 のデータを確認し、父と母の区別が可能か調査する
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def inspect_pedigree_parents():
    path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    if not path.exists():
        return

    logging.info("=" * 80)
    logging.info("Checking Pedigree Parents (Generation 1)")
    df = pd.read_parquet(path)
    
    # 任意の馬IDをいくつか抽出
    sample_ids = df['horse_id'].unique()[:5]
    
    for hid in sample_ids:
        logging.info("-" * 40)
        logging.info(f"Horse ID: {hid}")
        
        # 1代前（父母）を抽出
        parents = df[(df['horse_id'] == hid) & (df['generation'] == 1)]
        logging.info(f"Generation 1 records: {len(parents)}")
        logging.info(parents[['ancestor_id', 'ancestor_name', 'generation']].to_string(index=False))
        
        # 2代前（祖父母）
        grandparents = df[(df['horse_id'] == hid) & (df['generation'] == 2)]
        logging.info(f"Generation 2 records: {len(grandparents)}")
        # logging.info(grandparents[['ancestor_id', 'ancestor_name']].to_string(index=False))

if __name__ == "__main__":
    inspect_pedigree_parents()

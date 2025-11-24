#!/usr/bin/env python3
"""
血統データ順序検証スクリプト
有名な馬の血統を確認し、順序（父→母）が守られているか検証する
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def verify_pedigree_order():
    path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    if not path.exists():
        return

    logging.info("=" * 80)
    logging.info("Verifying Pedigree Order")
    df = pd.read_parquet(path)
    
    # 検証用リスト（有名な馬とその父・母）
    # ディープインパクト (2002100816): 父サンデーサイレンス, 母ウインドインハーヘア
    # キタサンブラック (2012102013): 父ブラックタイド, 母シュガーハート
    # アーモンドアイ (2015104364): 父ロードカナロア, 母フサイチパンドラ
    
    targets = {
        '2002100816': {'name': 'Deep Impact', 'sire': 'サンデーサイレンス', 'dam': 'ウインドインハーヘア'},
        '2012102013': {'name': 'Kitasan Black', 'sire': 'ブラックタイド', 'dam': 'シュガーハート'},
        '2015104364': {'name': 'Almond Eye', 'sire': 'ロードカナロア', 'dam': 'フサイチパンドラ'}
    }
    
    for hid, info in targets.items():
        logging.info("-" * 50)
        logging.info(f"Checking {info['name']} (ID: {hid})")
        
        # Generation 1 (父母)
        parents = df[(df['horse_id'] == hid) & (df['generation'] == 1)]
        
        if len(parents) == 2:
            p1 = parents.iloc[0]['ancestor_name']
            p2 = parents.iloc[1]['ancestor_name']
            
            logging.info(f"  Index 0 (Sire?): {p1} (Expected: {info['sire']})")
            logging.info(f"  Index 1 (Dam?):  {p2} (Expected: {info['dam']})")
            
            is_sire_match = (p1 == info['sire'])
            is_dam_match = (p2 == info['dam'])
            
            if is_sire_match and is_dam_match:
                logging.info("  ✅ Order is Correct (Sire -> Dam)")
            else:
                logging.warning("  ❌ Order Mismatch!")
        else:
            logging.warning(f"  ⚠️ Unexpected record count: {len(parents)}")

if __name__ == "__main__":
    verify_pedigree_order()

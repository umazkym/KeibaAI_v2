#!/usr/bin/env python3
"""
血統データ詳細検証スクリプト
指定された馬IDの血統データをダンプし、ユーザー提示の正解と比較・分析する
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def deep_inspect_pedigree():
    path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    if not path.exists():
        logging.error("pedigrees.parquet not found")
        return

    df = pd.read_parquet(path)
    
    # ユーザー指定のID
    target_ids = ['2015104364', '2022105300']
    
    # ユーザー提示の正解データ（比較用）
    ground_truth = {
        '2015104364': {
            'gen1': ['2004102753', '2008103040'], # 父, 母
            'gen2': ['1994108729', '1997103540', '1996106512', '1993109739'] # 父父, 父母, 母父, 母母
        },
        '2022105300': {
            'gen1': ['000a014377', '2008103847'],
            'gen2': ['000a0022e1', '000a014376', '000a000006', '000a0000b1']
        }
    }
    
    for hid in target_ids:
        logging.info("=" * 60)
        logging.info(f"Inspecting Horse ID: {hid}")
        
        records = df[df['horse_id'] == hid].copy()
        
        if records.empty:
            logging.warning("  No records found in pedigrees.parquet!")
            continue
            
        # 世代ごとに分析
        for gen in [1, 2]:
            gen_records = records[records['generation'] == gen]
            logging.info(f"  Generation {gen}: {len(gen_records)} records")
            
            # 実際のデータリスト
            actual_ids = gen_records['ancestor_id'].tolist()
            actual_names = gen_records['ancestor_name'].tolist()
            
            # 正解データ
            expected_ids = ground_truth[hid][f'gen{gen}']
            
            # 比較と表示
            for i, (act_id, act_name) in enumerate(zip(actual_ids, actual_names)):
                # 期待される役割（順序に基づく仮定）
                role = "Unknown"
                if gen == 1:
                    role = "Sire" if i == 0 else "Dam"
                elif gen == 2:
                    roles = ["Sire's Sire", "Sire's Dam", "Dam's Sire", "Dam's Dam"]
                    role = roles[i] if i < len(roles) else "Unknown"
                
                # 正解との一致確認
                match_status = "❌ Mismatch"
                expected_id = "N/A"
                if i < len(expected_ids):
                    expected_id = expected_ids[i]
                    if act_id == expected_id:
                        match_status = "✅ Match"
                
                logging.info(f"    [{i}] {role}: ID={act_id} ({act_name}) | Expected={expected_id} -> {match_status}")
            
            # リスト全体の整合性
            if actual_ids == expected_ids:
                logging.info(f"  ✅ Generation {gen} structure is PERFECT.")
            else:
                logging.warning(f"  ⚠️ Generation {gen} structure has discrepancies.")

if __name__ == "__main__":
    deep_inspect_pedigree()

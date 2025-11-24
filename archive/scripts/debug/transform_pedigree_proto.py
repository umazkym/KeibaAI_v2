#!/usr/bin/env python3
"""
血統データ構造変換スクリプト
pedigrees.parquet (縦持ち) を 馬ごとの祖先IDリスト (横持ち) に変換する
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def transform_pedigrees():
    path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    if not path.exists():
        logging.error(f"Not found: {path}")
        return

    logging.info("=" * 80)
    logging.info("Transforming pedigrees.parquet")
    df = pd.read_parquet(path)
    
    logging.info(f"Original rows: {len(df):,}")
    
    # horse_idごとの祖先リストを作成
    # generation 1 (父母), 2 (祖父母) までに限定してみる
    target_gens = [1, 2]
    df_filtered = df[df['generation'].isin(target_gens)].copy()
    
    # 祖先IDのユニーク数
    logging.info(f"Unique Ancestors: {df_filtered['ancestor_id'].nunique():,}")
    
    # 横持ち変換
    # horse_idごとに、generationと出現順序でカラムを作る
    # 例: gen1_1 (父), gen1_2 (母), gen2_1 (父父), gen2_2 (父母)...
    
    peds_list = []
    
    # 処理時間を考慮して、サンプルで実行
    sample_ids = df_filtered['horse_id'].unique()[:1000]
    logging.info(f"Processing sample of {len(sample_ids)} horses...")
    
    for hid in sample_ids:
        records = df_filtered[df_filtered['horse_id'] == hid]
        row = {'horse_id': hid}
        
        # 世代ごとに処理
        for gen in target_gens:
            gen_records = records[records['generation'] == gen]
            for i, (_, rec) in enumerate(gen_records.iterrows()):
                col_name = f"ancestor_gen{gen}_{i+1}"
                row[col_name] = rec['ancestor_id']
                # 名前も参考までに
                # row[f"{col_name}_name"] = rec['ancestor_name']
        
        peds_list.append(row)
    
    pedigree_flat = pd.DataFrame(peds_list)
    
    logging.info("-" * 50)
    logging.info("Transformed Data Sample:")
    logging.info(pedigree_flat.head().to_string())
    
    # カラムごとの欠損率
    logging.info("-" * 50)
    logging.info("Missing Rates:")
    for col in pedigree_flat.columns:
        null_cnt = pedigree_flat[col].isnull().sum()
        logging.info(f"{col}: {null_cnt} ({null_cnt/len(pedigree_flat):.1%})")

if __name__ == "__main__":
    transform_pedigrees()

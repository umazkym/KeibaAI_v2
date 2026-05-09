#!/usr/bin/env python3
"""
データ結合キー検証スクリプト
shutuba, races, horses_performance の結合可能性を確認
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def check_join_keys():
    base_dir = Path('keibaai/data/parsed/parquet')
    
    # 読み込み
    df_shutuba = pd.read_parquet(base_dir / 'shutuba/shutuba.parquet')
    df_races = pd.read_parquet(base_dir / 'races/races.parquet')
    df_perf = pd.read_parquet(base_dir / 'horses_performance/horses_performance.parquet')
    
    logging.info(f"shutuba: {len(df_shutuba):,} rows, cols: {df_shutuba.columns.tolist()}")
    logging.info(f"races:   {len(df_races):,} rows, cols: {df_races.columns.tolist()}")
    logging.info(f"perf:    {len(df_perf):,} rows, cols: {df_perf.columns.tolist()}")
    
    # 共通キーの確認
    join_keys = ['race_id', 'horse_number']
    
    for name, df in [('shutuba', df_shutuba), ('races', df_races), ('perf', df_perf)]:
        missing_keys = [k for k in join_keys if k not in df.columns]
        if missing_keys:
            logging.warning(f"⚠️ {name} is missing keys: {missing_keys}")
        else:
            logging.info(f"✅ {name} has all keys: {join_keys}")
            # キーの重複チェック
            dupes = df.duplicated(subset=join_keys).sum()
            logging.info(f"   Duplicates on keys: {dupes}")

    # racesとshutubaの整合性チェック
    # 行数が同じなので、インデックスまたはキーで完全一致するか
    if len(df_shutuba) == len(df_races):
        logging.info("-" * 50)
        logging.info("Checking shutuba <-> races alignment")
        
        # race_idが一致するか（行順序が同じか）
        if 'race_id' in df_races.columns:
             match_rate = (df_shutuba['race_id'] == df_races['race_id']).mean()
             logging.info(f"Row-wise race_id match rate: {match_rate:.1%}")
        
        # キーでのマージテスト
        if 'horse_number' in df_races.columns:
            merged = pd.merge(df_shutuba, df_races, on=['race_id', 'horse_number'], how='inner', suffixes=('', '_race'))
            logging.info(f"Inner join count: {len(merged):,} (Expected: {len(df_shutuba):,})")

if __name__ == "__main__":
    check_join_keys()

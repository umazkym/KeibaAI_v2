#!/usr/bin/env python3
"""
学習データセット再構築スクリプト
shutuba.parquet (ID情報) + races.parquet (結果詳細) = horses_performance_fixed.parquet
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def rebuild_dataset():
    base_dir = Path('keibaai/data/parsed/parquet')
    output_path = base_dir / 'horses_performance/horses_performance_fixed.parquet'
    
    logging.info("1. データの読み込み...")
    
    # shutuba (ID情報)
    shutuba_path = base_dir / 'shutuba/shutuba.parquet'
    df_shutuba = pd.read_parquet(shutuba_path)
    logging.info(f"  shutuba: {len(df_shutuba):,} rows")
    
    # races (結果詳細)
    races_path = base_dir / 'races/races.parquet'
    df_races = pd.read_parquet(races_path)
    logging.info(f"  races:   {len(df_races):,} rows")
    
    logging.info("2. データの結合...")
    
    # 結合キー
    join_keys = ['race_id', 'horse_number']
    
    # カラムの重複を避けるため、racesから必要なカラムのみを選択
    # shutubaにあるカラムはshutubaを優先（ID系など）
    # racesからは結果系を取得
    
    # shutubaのカラム
    # ['race_id', 'horse_number', 'horse_id', 'jockey_id', 'trainer_id', 'race_date', ...]
    
    # racesのカラムで欲しいもの
    target_races_cols = [
        'race_id', 'horse_number', # キー
        'finish_position', 'finish_time_seconds',
        'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
        'last_3f_time', 'time_except_last3f', 'pace_index',
        'final_corner_to_finish', 'position_change_1_2', 
        'win_probability', 'relative_odds', # これらはリーク注意だが履歴としては必要
        # レース条件（履歴データとして必要）
        'distance_m', 'track_surface', 'weather', 'track_condition', 'venue'
    ]
    
    # 存在するカラムのみ
    target_races_cols = [c for c in target_races_cols if c in df_races.columns]
    
    # 結合
    # shutubaを左側にして、ID情報を保持
    df_merged = pd.merge(
        df_shutuba, 
        df_races[target_races_cols], 
        on=join_keys, 
        how='inner',
        suffixes=('', '_race')
    )
    
    logging.info(f"  結合後: {len(df_merged):,} rows")
    
    # 日付型の変換（念のため）
    if 'race_date' in df_merged.columns:
        df_merged['race_date'] = pd.to_datetime(df_merged['race_date'])
    
    logging.info("3. 保存...")
    # 保存ディレクトリ作成
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df_merged.to_parquet(output_path)
    logging.info(f"✅ 保存完了: {output_path}")
    
    # 欠損率チェック（主要カラム）
    logging.info("4. 品質チェック（2023年以前）...")
    df_old = df_merged[df_merged['race_date'] < '2024-01-01']
    logging.info(f"  対象行数: {len(df_old):,}")
    
    check_cols = ['jockey_id', 'trainer_id', 'passing_order_2', 'last_3f_time', 'finish_position']
    for col in check_cols:
        if col in df_old.columns:
            null_count = df_old[col].isnull().sum()
            logging.info(f"  {col:<20}: 欠損率 {null_count/len(df_old):.1%}")
        else:
            logging.warning(f"  {col:<20}: NOT FOUND")

if __name__ == "__main__":
    rebuild_dataset()

#!/usr/bin/env python3
"""
全データソース包括的品質診断スクリプト
指定された5つのParquetファイルについて、カラム構成、欠損率、データ内容を詳細に分析する。
"""
import pandas as pd
from pathlib import Path
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(message)s')

def analyze_parquet(file_path, name):
    logging.info("=" * 100)
    logging.info(f"📂 分析対象: {name}")
    logging.info(f"   パス: {file_path}")
    
    path = Path(file_path)
    if not path.exists():
        logging.error(f"❌ ファイルが見つかりません: {path}")
        return

    try:
        df = pd.read_parquet(path)
    except Exception as e:
        logging.error(f"❌ 読み込みエラー: {e}")
        return

    logging.info(f"   行数: {len(df):,}")
    logging.info(f"   列数: {len(df.columns)}")
    logging.info(f"   カラム一覧: {', '.join(df.columns)}")
    
    # 日付カラムの特定（年別集計用）
    date_col = None
    for col in ['race_date', 'date', 'birthday']:
        if col in df.columns:
            date_col = col
            break
    
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df['year'] = df[date_col].dt.year
        years = sorted(df['year'].dropna().unique().astype(int))
        logging.info(f"   データ期間: {min(years)}年 〜 {max(years)}年")
    else:
        logging.info("   ⚠️ 日付カラムが見つかりません（年別集計スキップ）")

    # 注目すべき重要カラムの定義（ファイルごとに異なる）
    important_cols = []
    if 'performance' in name:
        important_cols = ['race_id', 'horse_id', 'jockey_id', 'trainer_id', 'finish_position', 'passing_order_2', 'last_3f_time', 'win_odds']
    elif 'shutuba' in name:
        important_cols = ['race_id', 'horse_id', 'jockey_id', 'trainer_id', 'horse_weight']
    elif 'races' in name:
        important_cols = ['race_id', 'course_type', 'distance', 'weather', 'track_condition', 'race_name']
    elif 'horses' in name:
        important_cols = ['horse_id', 'name', 'gender', 'birthday', 'trainer_id']
    elif 'pedigrees' in name:
        important_cols = ['horse_id', 'sire_id', 'dam_id', 'damsire_id']
    
    # 共通してチェックするID
    common_ids = ['race_id', 'horse_id']
    for cid in common_ids:
        if cid in df.columns and cid not in important_cols:
            important_cols.append(cid)

    # 欠損率チェック
    logging.info("-" * 100)
    logging.info(f"{'Column':<25} | {'Type':<10} | {'Null %':<10} | {'Zero %':<10} | {'Unique':<8} | {'Sample'}")
    logging.info("-" * 100)

    for col in df.columns:
        # 重要カラムまたは欠損が多いカラムを表示
        is_important = col in important_cols
        
        null_count = df[col].isnull().sum()
        missing_rate = null_count / len(df)
        
        zero_count = (df[col] == 0).sum() if pd.api.types.is_numeric_dtype(df[col]) else 0
        zero_rate = zero_count / len(df)
        
        # 表示条件: 重要カラム OR 欠損がある OR 0が多い
        if is_important or missing_rate > 0 or zero_rate > 0.1:
            mark = "📌" if is_important else "  "
            if missing_rate > 0.9: mark += "❌" # ほぼ全滅
            elif missing_rate > 0.1: mark += "⚠️" # 注意
            
            sample = str(df[col].dropna().unique()[:3].tolist())
            if len(sample) > 50: sample = sample[:47] + "..."
            
            logging.info(f"{mark:<2} {col:<23} | {str(df[col].dtype):<10} | {missing_rate:>9.1%} | {zero_rate:>9.1%} | {df[col].nunique():<8} | {sample}")

    # 年別欠損率（重要カラムのみ）
    if date_col and important_cols:
        logging.info("-" * 100)
        logging.info("📅 年別欠損率推移 (重要カラムのみ)")
        
        # 最近の5年分 + 最古の年を表示
        target_years = years[-5:]
        if years[0] not in target_years:
            target_years = [years[0]] + target_years
            
        header = f"{'Year':<6} | " + " | ".join([f"{c[:10]:<10}" for c in important_cols if c in df.columns])
        logging.info(header)
        logging.info("-" * len(header))
        
        for year in target_years:
            year_df = df[df['year'] == year]
            if len(year_df) == 0: continue
            
            row_str = f"{year:<6} | "
            for col in important_cols:
                if col not in df.columns: continue
                
                null_count = year_df[col].isnull().sum()
                zero_count = (year_df[col] == 0).sum() if pd.api.types.is_numeric_dtype(year_df[col]) else 0
                rate = (null_count + zero_count) / len(year_df)
                
                val_str = f"{rate:.0%}"
                if rate > 0.9: val_str = f"❌{val_str}"
                elif rate > 0.1: val_str = f"⚠️{val_str}"
                
                row_str += f"{val_str:<10} | "
            logging.info(row_str.rstrip(" | "))

def main():
    base_dir = Path('keibaai/data/parsed/parquet')
    
    files = [
        (base_dir / 'horses_performance/horses_performance.parquet', 'horses_performance (過去走成績)'),
        (base_dir / 'races/races.parquet', 'races (レース情報)'),
        (base_dir / 'shutuba/shutuba.parquet', 'shutuba (出馬表)'),
        (base_dir / 'horses/horses.parquet', 'horses (馬プロフィール)'),
        (base_dir / 'pedigrees/pedigrees.parquet', 'pedigrees (血統)')
    ]
    
    for path, name in files:
        analyze_parquet(path, name)
        print("\n")

if __name__ == "__main__":
    main()

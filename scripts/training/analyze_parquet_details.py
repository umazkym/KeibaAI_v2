# -*- coding: utf-8 -*-
"""
Parquetファイル詳細分析スクリプト
各parquetファイルのカラム、データ型、サンプル値、欠損率を確認
"""

import pandas as pd
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
data_dir = project_root / "keibaai/data/parsed/parquet"

def analyze_parquet(parquet_path):
    print(f"\n{'='*80}")
    print(f"ファイル: {parquet_path.name}")
    print("=" * 80)
    
    df = pd.read_parquet(parquet_path)
    print(f"行数: {len(df):,}, カラム数: {len(df.columns)}")
    
    print(f"\n{'カラム名':30s} {'型':15s} {'非NULL':>10s} {'欠損率':>8s} {'サンプル値'}")
    print("-" * 100)
    
    for col in df.columns:
        dtype = str(df[col].dtype)[:14]
        non_null = df[col].notna().sum()
        null_rate = (1 - non_null / len(df)) * 100
        
        # サンプル値
        sample = df[col].dropna().iloc[0] if non_null > 0 else "N/A"
        if isinstance(sample, str) and len(sample) > 30:
            sample = sample[:30] + "..."
        
        print(f"{col:30s} {dtype:15s} {non_null:>10,} {null_rate:>7.1f}% {sample}")

# 各ディレクトリのparquetファイルを分析
directories = [
    'analysis',
    'corners', 
    'horses',
    'horses_performance',
    'pedigrees',
    'race_details',
    'races',
    'returns',
    'shutuba'
]

for dir_name in directories:
    dir_path = data_dir / dir_name
    if dir_path.exists():
        for parquet_file in dir_path.glob("*.parquet"):
            analyze_parquet(parquet_file)

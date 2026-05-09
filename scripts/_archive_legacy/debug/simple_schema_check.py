#!/usr/bin/env python3
"""
Simple schema checker for parquet files
"""
import pandas as pd
from pathlib import Path

races_dir = Path('keibaai/data/parsed/parquet/races')
files = list(races_dir.glob('*.parquet'))

print(f"Found {len(files)} parquet files\n")

for f in sorted(files):
    print(f"File: {f.name}")
    print(f"Size: {f.stat().st_size:,} bytes")
    try:
        df = pd.read_parquet(f)
        print(f"Rows: {len(df):,}, Columns: {len(df.columns)}")
        
        if 'race_course' in df.columns:
            print("race_course: YES")
            nunique = df['race_course'].nunique()
            print(f"  Unique values: {nunique}")
        else:
            print("race_course: NO")
            
    except Exception as e:
        print(f"ERROR: {e}")
    
    print("-" * 60)

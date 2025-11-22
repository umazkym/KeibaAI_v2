#!/usr/bin/env python3
"""
Check schema of all parquet files in races directory
"""
import pandas as pd
from pathlib import Path

races_dir = Path('keibaai/data/parsed/parquet/races')
files = list(races_dir.glob('*.parquet'))

print(f"Found {len(files)} parquet files in {races_dir}")
print("=" * 80)

for f in sorted(files):
    print(f"\n📁 File: {f.name}")
    print(f"   Size: {f.stat().st_size:,} bytes")
    try:
        df = pd.read_parquet(f)
        print(f"   Rows: {len(df):,}")
        print(f"   Columns: {len(df.columns)}")
        
        # Check for race_course column
        if 'race_course' in df.columns:
            print(f"   ✅ 'race_course' EXISTS")
            unique_count = df['race_course'].nunique()
            print(f"      Unique values: {unique_count}")
            sample = df['race_course'].dropna().unique()[:5]
            print(f"      Sample: {list(sample)}")
        else:
            print(f"   ❌ 'race_course' MISSING")
            
        # Show first few column names
        print(f"   First 10 columns: {df.columns[:10].tolist()}")
        
    except Exception as e:
        print(f"   ⚠️  Error reading file: {e}")
    
    print("-" * 80)

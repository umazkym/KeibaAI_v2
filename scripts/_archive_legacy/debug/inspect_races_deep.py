#!/usr/bin/env python3
"""
Deep inspection of races.parquet columns
"""
import pandas as pd
from pathlib import Path

races_file = Path('keibaai/data/parsed/parquet/races/races.parquet')

print(f"Inspecting: {races_file}")
print(f"File size: {races_file.stat().st_size:,} bytes")
print()

df = pd.read_parquet(races_file)

print(f"Total rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")
print()

# Show ALL column names
print("All columns:")
for i, col in enumerate(df.columns, 1):
    print(f"  {i:2d}. {col}")

print()

# Check for columns containing 'course' or 'venue'
print("Columns containing 'course':")
course_cols = [col for col in df.columns if 'course' in col.lower()]
if course_cols:
    for col in course_cols:
        print(f"  - {col}")
else:
    print("  (none found)")

print()

print("Columns containing 'venue':")
venue_cols = [col for col in df.columns if 'venue' in col.lower()]
if venue_cols:
    for col in venue_cols:
        print(f"  - {col}")
else:
    print("  (none found)")

print()
print("Sample of first row:")
print(df.iloc[0].to_dict())

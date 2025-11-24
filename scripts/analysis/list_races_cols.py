#!/usr/bin/env python3
"""
List all columns in races.parquet - output to file
"""
import pandas as pd

df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')

with open('races_columns_list.txt', 'w', encoding='utf-8') as f:
    f.write(f"Total columns: {len(df.columns)}\n")
    f.write(f"Total rows: {len(df):,}\n\n")
    f.write("All columns:\n")
    for i, col in enumerate(df.columns, 1):
        f.write(f"{i:3d}. {col}\n")
    
    f.write("\n\nChecking for 'course' or 'venue':\n")
    course_cols = [col for col in df.columns if 'course' in col.lower()]
    venue_cols = [col for col in df.columns if 'venue' in col.lower()]
    
    f.write(f"'course' columns: {course_cols if course_cols else 'NONE'}\n")
    f.write(f"'venue' columns: {venue_cols if venue_cols else 'NONE'}\n")

print("Output written to races_columns_list.txt")

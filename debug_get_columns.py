
import pandas as pd
import sys

# Windowsのコンソール出力エンコーディング問題を回避
sys.stdout.reconfigure(encoding='utf-8')

try:
    df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    print("--- races.parquet Columns ---")
    for col in df.columns:
        print(col)
    print("-----------------------------")
except FileNotFoundError:
    print("Error: 'keibaai/data/parsed/parquet/races/races.parquet' not found.")


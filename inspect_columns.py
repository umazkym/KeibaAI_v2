import pandas as pd
import sys

try:
    df = pd.read_parquet('keibaai/models/mu_v2/train_data_mu_v2.parquet')
    print(f"Total columns: {len(df.columns)}")
    print("Columns:")
    for col in sorted(df.columns):
        print(col)
except Exception as e:
    print(f"Error: {e}")

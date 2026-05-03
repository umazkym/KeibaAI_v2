import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "keibaai", "data")
RACE_DETAILS_PARQUET = os.path.join(DATA_DIR, "parsed", "parquet", "race_details", "race_details.parquet")

try:
    df = pd.read_parquet(RACE_DETAILS_PARQUET)
    print("Columns in RACE_DETAILS_PARQUET:")
    print(df.columns.tolist())
    print("\nSample row:")
    if not df.empty:
        sample = df.iloc[0].to_dict()
        for k, v in sample.items():
            print(f"{k}: {type(v)} = {str(v)[:50]}")
except Exception as e:
    print(f"Error: {e}")

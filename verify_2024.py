import pandas as pd
from pathlib import Path
import glob

def verify_2024():
    feature_dir = Path("keibaai/data/features/parquet")
    # Find parquet files for 2024-01
    files = list(feature_dir.glob("year=2024/month=1/*.parquet"))
    
    if not files:
        print("No feature files found for 2024-01")
        return

    print(f"Checking file: {files[0]}")
    df = pd.read_parquet(files[0])
    
    cols = ['distance_m']
    track_cols = [c for c in df.columns if c.startswith('track_')]
    
    for col in cols + track_cols:
        if col in df.columns:
            zeros = (df[col] == 0).sum()
            total = len(df)
            print(f"{col}: Zeros={zeros}/{total} ({zeros/total:.2%})")
        else:
            print(f"{col}: NOT FOUND")

if __name__ == "__main__":
    verify_2024()

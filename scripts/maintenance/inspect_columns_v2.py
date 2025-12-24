
import pandas as pd
from pathlib import Path

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def inspect_parquet(name):
    path = DATA_DIR / name / f"{name}.parquet"
    if not path.exists():
        print(f"[MISSING] {path}")
        return
    
    try:
        df = pd.read_parquet(path)
        print(f"\n--- {name} ({len(df)} rows) ---")
        print(df.dtypes)
        if 'head_count' in name or 'races' in name:
             if 'head_count' in df.columns:
                 print(f"!!! head_count FOUND in {name} !!!")
             else:
                 print(f"!!! head_count NOT FOUND in {name} !!!")
    except Exception as e:
        print(f"Error reading {name}: {e}")

inspect_parquet("races")
inspect_parquet("shutuba")
inspect_parquet("horses")
inspect_parquet("pedigrees")
inspect_parquet("corners") # Folder logic diff?

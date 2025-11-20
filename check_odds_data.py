import pandas as pd
import numpy as np
from pathlib import Path

# Load one sample parquet file
files = list(Path('keibaai/data/features/parquet').glob('**/*.parquet'))
if files:
    df = pd.read_parquet(files[0])
    
    print("Total rows:", len(df))
    print("\n=== Checking relative_odds column ===")
    if 'relative_odds' in df.columns:
        print("Column exists!")
        print("Non-null count:", df['relative_odds'].notna().sum())
        print("Null count:", df['relative_odds'].isna().sum())
        print("Sample values:", df['relative_odds'].head(10).tolist())
        print("Mean:", df['relative_odds'].mean())
        print("Min:", df['relative_odds'].min())
        print("Max:", df['relative_odds'].max())
    else:
        print("relative_odds column NOT found")
        print("Available odds columns:", [c for c in df.columns if 'odds' in c.lower()])
    
    print("\n=== Checking finish_position ===")
    if 'finish_position' in df.columns:
        print("Wins (position=1):", (df['finish_position'] == 1).sum())
        print("Total entries:", len(df))

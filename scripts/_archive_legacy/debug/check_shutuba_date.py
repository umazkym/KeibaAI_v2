import pandas as pd
from pathlib import Path
path = list(Path('keibaai/data/parsed/parquet/shutuba').glob('*.parquet'))[0]
df = pd.read_parquet(path)
print(f"File: {path.name}")
print(f"Date Range: {df['race_date'].min()} - {df['race_date'].max()}")
print(f"Date Type: {df['race_date'].dtype}")

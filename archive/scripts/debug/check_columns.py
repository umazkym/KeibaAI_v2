import pandas as pd
from pathlib import Path
df = pd.read_parquet(list(Path('keibaai/data/parsed/parquet/races').glob('*.parquet'))[0])
cols = df.columns.tolist()
print(f"Total columns: {len(cols)}")
print("Columns related to order/rank/place:")
print([c for c in cols if 'order' in c or 'rank' in c or 'place' in c or 'chakujun' in c])

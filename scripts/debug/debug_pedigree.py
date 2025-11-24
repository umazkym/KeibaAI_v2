import pandas as pd
from pathlib import Path

# pedigreeを確認
pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
print(f"Loading pedigree from: {pedigree_path}")

if pedigree_path.exists():
    df = pd.read_parquet(pedigree_path)
    print(f"Columns: {list(df.columns)}")
else:
    print("File not found")

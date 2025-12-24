import pandas as pd
from pathlib import Path

ped_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
if ped_path.exists():
    df = pd.read_parquet(ped_path)
    print(f"Loaded {len(df)} rows.")
    print("Columns:", df.columns.tolist())
    print("Generations:", df['generation'].unique())
    print("Sample:\n", df.head())
    
    sires = df[df['generation'] == 1]
    print(f"Sires count: {len(sires)}")
    
    damsires = df[(df['generation'] == 2) & (df['ancestor_name'].str.contains('母', na=False))]
    print(f"Damsires count: {len(damsires)}")
    if len(damsires) == 0:
        print("Sample generation 2:\n", df[df['generation'] == 2].head(10))
else:
    print("File not found.")

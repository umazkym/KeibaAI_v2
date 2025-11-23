import pandas as pd
from pathlib import Path

# pedigreeを確認
pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
print(f"Loading pedigree from: {pedigree_path}")

if pedigree_path.exists():
    df = pd.read_parquet(pedigree_path)
    
    # Check generation 1 counts per horse
    gen1 = df[df['generation'] == 1]
    counts = gen1.groupby('horse_id').size()
    
    print(f"Max generation 1 count: {counts.max()}")
    print(f"Min generation 1 count: {counts.min()}")
    
    if counts.max() > 1:
        print("Some horses have multiple generation 1 ancestors!")
        print(counts[counts > 1].head())
    else:
        print("All horses have at most 1 generation 1 ancestor (Sire).")
else:
    print("File not found")

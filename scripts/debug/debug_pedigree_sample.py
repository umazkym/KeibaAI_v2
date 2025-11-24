import pandas as pd
from pathlib import Path

# pedigreeを確認
pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
print(f"Loading pedigree from: {pedigree_path}")

if pedigree_path.exists():
    df = pd.read_parquet(pedigree_path)
    print(f"Sample:\n{df.head(10)}")
    
    # Check one horse
    horse_ids = df['horse_id'].unique()[:1]
    if len(horse_ids) > 0:
        print(f"\nPedigree for horse {horse_ids[0]}:")
        print(df[df['horse_id'] == horse_ids[0]])
else:
    print("File not found")

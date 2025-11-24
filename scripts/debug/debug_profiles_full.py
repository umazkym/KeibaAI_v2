import pandas as pd
from pathlib import Path

# horse_profilesを確認
profiles_path = Path('keibaai/data/parsed/parquet/horses/horses.parquet')
print(f"Loading profiles from: {profiles_path}")

if profiles_path.exists():
    df = pd.read_parquet(profiles_path)
    print(f"Columns: {list(df.columns)}")
    
    # Check for sire/dam related columns
    sire_cols = [c for c in df.columns if 'sire' in c or 'dam' in c or 'father' in c or 'mother' in c]
    print(f"Sire/Dam related columns: {sire_cols}")
else:
    print("File not found")

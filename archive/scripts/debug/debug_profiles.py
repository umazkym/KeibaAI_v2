import pandas as pd
from pathlib import Path

# horse_profilesを確認
profiles_path = Path('keibaai/data/parsed/parquet/horses/horses.parquet')
print(f"Loading profiles from: {profiles_path}")

if profiles_path.exists():
    df = pd.read_parquet(profiles_path)
    print(f"Columns: {list(df.columns)}")
else:
    print("File not found")

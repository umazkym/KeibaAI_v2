import pandas as pd
from pathlib import Path

# shutuba.parquetを確認
shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
print(f"Loading shutuba from: {shutuba_path}")

if shutuba_path.exists():
    df = pd.read_parquet(shutuba_path)
    print(f"Loaded {shutuba_path.name}")
    print(f"Columns: {list(df.columns)[:20]}")
    
    if 'finish_position' in df.columns:
        print(f"\nfinish_position in shutuba:")
        print(df['finish_position'].describe())
        print(f"Sample: {df['finish_position'].head().tolist()}")
    else:
        print("\nfinish_position NOT in shutuba")
else:
    print("File not found")

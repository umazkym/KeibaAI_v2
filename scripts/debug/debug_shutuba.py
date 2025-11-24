import pandas as pd
from pathlib import Path

# 2024年の出馬表データを確認
shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/year=2024/month=1')
print(f"Loading shutuba from: {shutuba_path}")

if shutuba_path.exists():
    # Load first parquet file in directory
    parquet_files = list(shutuba_path.glob('*.parquet'))
    if parquet_files:
        df = pd.read_parquet(parquet_files[0])
        print(f"Loaded {parquet_files[0].name}")
        print(f"Columns: {list(df.columns)[:20]}")
        
        if 'finish_position' in df.columns:
            print(f"\nfinish_position in shutuba:")
            print(df['finish_position'].describe())
            print(f"Sample: {df['finish_position'].head().tolist()}")
        else:
            print("\nfinish_position NOT in shutuba")
    else:
        print("No parquet files found")
else:
    print("Directory not found")

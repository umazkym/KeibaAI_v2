import pandas as pd
from pathlib import Path

# Check races_old
old_path = Path('keibaai/data/parsed/parquet/races/races_old.parquet')
print(f"Loading old results from: {old_path}")

if old_path.exists():
    df = pd.read_parquet(old_path)
    print(f"Loaded {old_path.name}")
    
    if 'finish_position' in df.columns:
        print(f"\nfinish_position stats:")
        print(df['finish_position'].describe())
        print(f"Sample: {df['finish_position'].head().tolist()}")
        
        zeros = df[df['finish_position'] == 0]
        print(f"Count of 0s: {len(zeros)}")
    else:
        print("finish_position NOT found")
else:
    print("File not found")

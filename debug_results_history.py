import pandas as pd
from pathlib import Path

# results_historyを確認
results_path = Path('keibaai/data/parsed/parquet/races/year=2024/month=1')
print(f"Loading results from: {results_path}")

if results_path.exists():
    # Load first parquet
    files = list(results_path.glob('*.parquet'))
    if files:
        df = pd.read_parquet(files[0])
        print(f"Loaded {files[0].name}")
        print(f"Columns: {list(df.columns)[:20]}")
        
        if 'finish_position' in df.columns:
            print(f"\nfinish_position stats:")
            print(df['finish_position'].describe())
            print(f"Sample: {df['finish_position'].head().tolist()}")
            
            # Check race_id format
            print(f"\nRace ID sample: {df['race_id'].head().tolist()}")
        else:
            print("finish_position NOT found")
    else:
        print("No parquet files found")
else:
    print("Directory not found")

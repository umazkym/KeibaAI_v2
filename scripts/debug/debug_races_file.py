import pandas as pd
from pathlib import Path

# results_historyを確認
results_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
print(f"Loading results from: {results_path}")

if results_path.exists():
    df = pd.read_parquet(results_path)
    print(f"Loaded {results_path.name}")
    print(f"Columns: {list(df.columns)[:20]}")
    
    if 'finish_position' in df.columns:
        print(f"\nfinish_position stats:")
        print(df['finish_position'].describe())
        print(f"Sample: {df['finish_position'].head().tolist()}")
        
        # Check race_id format
        print(f"\nRace ID sample: {df['race_id'].head().tolist()}")
        
        # Check 2024 data specifically
        if 'race_date' in df.columns:
            df['race_date'] = pd.to_datetime(df['race_date'])
            df_2024 = df[df['race_date'].dt.year == 2024]
            print(f"\n2024 data count: {len(df_2024)}")
            if not df_2024.empty:
                print(f"2024 finish_position stats:")
                print(df_2024['finish_position'].describe())
    else:
        print("finish_position NOT found")
else:
    print("File not found")

import pandas as pd
from pathlib import Path

def inspect(path):
    print(f"--- Inspecting {path} ---")
    try:
        df = pd.read_parquet(path)
        cols = list(df.columns)
        print("All Columns:", cols)
        odds_cols = [c for c in cols if 'odds' in c or 'win' in c]
        print("Odds/Win Columns:", odds_cols)
        print("Head:", df.head(1))
    except Exception as e:
        print(f"Error: {e}")

# Check races
inspect('keibaai/data/parsed/parquet/races/races.parquet')

# Check one feature file
feature_files = list(Path('keibaai/data/features/parquet').glob('**/*.parquet'))
if feature_files:
    inspect(feature_files[0])
else:
    print("No feature files found.")

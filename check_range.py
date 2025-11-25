import pandas as pd
from pathlib import Path

def check_date_range():
    file_path = Path("keibaai/data/parsed/parquet/races/races.parquet")
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return

    try:
        df = pd.read_parquet(file_path, columns=['race_date'])
        df['race_date'] = pd.to_datetime(df['race_date'])
        min_date = df['race_date'].min()
        max_date = df['race_date'].max()
        print(f"Data Range: {min_date.date()} to {max_date.date()}")
        print(f"Total Rows: {len(df)}")
    except Exception as e:
        print(f"Error reading file: {e}")

if __name__ == "__main__":
    check_date_range()

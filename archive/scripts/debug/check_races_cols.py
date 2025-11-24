import pandas as pd
from pathlib import Path

def check_columns():
    try:
        # ディレクトリ内の最初のparquetファイルを探す
        races_dir = Path('keibaai/data/parsed/parquet/races')
        parquet_files = list(races_dir.glob('**/*.parquet'))
        
        if not parquet_files:
            print(f"No parquet files found in {races_dir}")
            return

        target_file = parquet_files[0]
        with open('check_races_result.txt', 'w', encoding='utf-8') as f:
            f.write(f"Reading: {target_file}\n")
            
            df = pd.read_parquet(target_file)
            f.write(f"Total columns: {len(df.columns)}\n")
            
            has_venue = 'venue' in df.columns
            has_race_course = 'race_course' in df.columns
            has_place = 'place' in df.columns
            
            f.write(f"Has 'venue': {has_venue}\n")
            f.write(f"Has 'race_course': {has_race_course}\n")
            f.write(f"Has 'place': {has_place}\n")
            
            if has_venue:
                f.write(f"Sample 'venue': {df['venue'].dropna().unique()[:5]}\n")
            if has_race_course:
                f.write(f"Sample 'race_course': {df['race_course'].dropna().unique()[:5]}\n")
            if has_place:
                f.write(f"Sample 'place': {df['place'].dropna().unique()[:5]}\n")
                
            f.write(f"Columns: {df.columns.tolist()}\n")
        print("Result written to check_races_result.txt")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_columns()

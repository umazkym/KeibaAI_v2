from pathlib import Path

def count_files():
    base_dir = Path("keibaai/data/features/parquet")
    years = range(2020, 2025)
    
    total_files = 0
    for year in years:
        year_dir = base_dir / f"year={year}"
        if year_dir.exists():
            count = len(list(year_dir.glob("**/*.parquet")))
            print(f"Year {year}: {count} files")
            total_files += count
        else:
            print(f"Year {year}: Directory not found")
            
    print(f"Total files: {total_files}")

if __name__ == "__main__":
    count_files()

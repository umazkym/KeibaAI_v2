import pyarrow.dataset as ds
import pandas as pd
from pathlib import Path

def test_pyarrow_load():
    data_dir = Path('keibaai/data/parsed/parquet/races')
    files = list(data_dir.glob('*.parquet'))
    print(f"Files: {files}")
    
    # Test 1: With partitioning="hive" (Current implementation)
    print("\n--- Test 1: partitioning='hive' ---")
    try:
        dataset = ds.dataset(files, format="parquet", partitioning="hive")
        df = dataset.to_table().to_pandas()
        if 'race_course' in df.columns:
            print("✅ 'race_course' exists")
        else:
            print("❌ 'race_course' MISSING")
            print(f"Columns: {df.columns.tolist()}")
    except Exception as e:
        print(f"Error: {e}")

    # Test 2: Without partitioning
    print("\n--- Test 2: No partitioning ---")
    try:
        dataset = ds.dataset(files, format="parquet")
        df = dataset.to_table().to_pandas()
        if 'race_course' in df.columns:
            print("✅ 'race_course' exists")
        else:
            print("❌ 'race_course' MISSING")
    except Exception as e:
        print(f"Error: {e}")

    # Test 3: Only races.parquet
    print("\n--- Test 3: Only races.parquet ---")
    target_file = [f for f in files if f.name == 'races.parquet']
    if target_file:
        try:
            dataset = ds.dataset(target_file, format="parquet")
            df = dataset.to_table().to_pandas()
            if 'race_course' in df.columns:
                print("✅ 'race_course' exists")
            else:
                print("❌ 'race_course' MISSING")
        except Exception as e:
            print(f"Error: {e}")
    if not target_file:
        print("races.parquet not found in list")

    # Test 4: pd.read_parquet on directory
    print("\n--- Test 4: pd.read_parquet on directory ---")
    try:
        df = pd.read_parquet(data_dir)
        if 'race_course' in df.columns:
            print("✅ 'race_course' exists")
        else:
            print("❌ 'race_course' MISSING")
    except Exception as e:
        print(f"Error: {e}")

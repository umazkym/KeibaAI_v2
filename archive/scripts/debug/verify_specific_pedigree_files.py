import sys
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from keibaai.src.modules.parsers import pedigree_parser

files_to_check = [
    r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\ped\2021102922.bin",
    r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\ped\2019104037.bin"
]

print("--- Starting Verification ---")

for file_path in files_to_check:
    print(f"\nChecking: {file_path}")
    try:
        df = pedigree_parser.parse_pedigree_html(file_path)
        if not df.empty:
            print(f"SUCCESS: Parsed {len(df)} rows.")
            print(df.head(3).to_string())
            
            # Check for mojibake or empty strings in critical columns
            if df['ancestor_name'].isnull().any() or (df['ancestor_name'] == '').any():
                print("WARNING: Found empty ancestor names.")
            
            # Check if names look like valid Japanese or English
            sample_name = df.iloc[0]['ancestor_name']
            print(f"Sample Name: {sample_name}")
            
        else:
            print("FAILURE: DataFrame is empty.")
    except Exception as e:
        print(f"ERROR: {e}")

print("\n--- Verification Complete ---")

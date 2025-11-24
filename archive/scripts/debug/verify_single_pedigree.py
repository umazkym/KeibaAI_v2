import sys
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from keibaai.src.modules.parsers import pedigree_parser

file_path = r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\ped\2021102922.bin"

print(f"\nChecking: {file_path}")
try:
    df = pedigree_parser.parse_pedigree_html(file_path)
    if not df.empty:
        print(f"SUCCESS: Parsed {len(df)} rows.")
        print(df.head(3).to_string())
        print(f"Sample Name: {df.iloc[0]['ancestor_name']}")
    else:
        print("FAILURE: DataFrame is empty.")
except Exception as e:
    print(f"ERROR: {e}")

import pandas as pd
import os

file_path = "race_analysis_20251130_東京.xlsx"

if not os.path.exists(file_path):
    print(f"Error: {file_path} not found.")
    exit(1)

try:
    xls = pd.ExcelFile(file_path)
    sheet_names = xls.sheet_names
    print(f"Sheets found: {sheet_names}")
    
    # Check if Race_05 and Race_06 are missing
    missing_05 = "Race_05" not in sheet_names
    missing_06 = "Race_06" not in sheet_names
    
    if missing_05 and missing_06:
        print("Success: Race_05 and Race_06 are correctly skipped.")
    else:
        print(f"Failure: Race_05 present: {not missing_05}, Race_06 present: {not missing_06}")

except Exception as e:
    print(f"Verification failed: {e}")

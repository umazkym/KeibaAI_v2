import pandas as pd
import os

file_path = 'race_analysis_20251130_東京.xlsx'

if not os.path.exists(file_path):
    print(f"{file_path} not found.")
    exit()

print(f"Reading {file_path}...")
df_dict = pd.read_excel(file_path, sheet_name=None)

total_valid_time = 0
total_valid_l3f = 0
total_rows = 0

for name, sheet in df_dict.items():
    valid_time = sheet['タイム指数'].count()
    valid_l3f = sheet['上り指数'].count()
    rows = len(sheet)
    
    total_valid_time += valid_time
    total_valid_l3f += valid_l3f
    total_rows += rows
    
    print(f"{name}: Rows={rows}, ValidTime={valid_time}, ValidL3F={valid_l3f}")

print("-" * 30)
print(f"Total Rows: {total_rows}")
print(f"Total Valid Time Index: {total_valid_time}")
print(f"Total Valid L3F Index: {total_valid_l3f}")

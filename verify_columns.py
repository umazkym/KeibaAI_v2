import pandas as pd
import os

file_path = "race_analysis_20251130_東京.xlsx"

if not os.path.exists(file_path):
    print(f"Error: {file_path} not found.")
    exit(1)

try:
    xls = pd.ExcelFile(file_path)
    sheet_name = xls.sheet_names[0] # Check first race
    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
    
    # Find header row
    header_row_idx = -1
    for i, row in df.iterrows():
        if '馬場' in row.values and 'ﾀｲﾑ' in row.values:
            header_row_idx = i
            break
            
    if header_row_idx == -1:
        print("Header row not found.")
        exit(1)
        
    # Set header
    df.columns = df.iloc[header_row_idx]
    df = df.iloc[header_row_idx+1:].reset_index(drop=True)
    
    # Check sample rows
    cols_to_check = ['馬場', 'ﾀｲﾑ', '着差', '上り', 'ﾍﾟｰｽ1', '通過', '1C', '2C', '3C', '4C', '馬体重', '厩舎', '平均t', '基準t']
    print(f"Checking columns: {cols_to_check}")
    
    for col in cols_to_check:
        if col in df.columns:
            sample_vals = df[col].dropna().head(5).tolist()
            print(f"{col}: {sample_vals}")
        else:
            print(f"{col}: Not found")

    # Check Parquet Date Range
    parquet_path = r"keibaai\data\processed\races.parquet"
    if os.path.exists(parquet_path):
        df_p = pd.read_parquet(parquet_path, columns=['race_date'])
        df_p['race_date'] = pd.to_datetime(df_p['race_date'])
        print(f"\nRaces Parquet Date Range: {df_p['race_date'].min()} to {df_p['race_date'].max()}")
    else:
        print("\nraces.parquet not found.")

except Exception as e:
    print(f"Verification failed: {e}")

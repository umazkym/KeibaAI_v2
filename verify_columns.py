import pandas as pd
import os

file_path = "race_analysis_20251130_東京.xlsx"

if not os.path.exists(file_path):
    print(f"Error: {file_path} not found.")
    exit(1)

try:
    # Load Excel Sheet 1 (Flat format, header at row 0)
    df = pd.read_excel(file_path, sheet_name=0, header=0)
    print("Loaded Excel Sheet 1")
    print(df.head(5))

    # Check sample rows
    cols_to_check = ['出走枠番', '出走馬番', '乗り替わり', 'グループNo', '基準3F', '基3F差', '通過', '馬場', 'ﾀｲﾑ']
    print(f"Checking columns: {cols_to_check}")
    
    for col in cols_to_check:
        if col in df.columns:
            sample_vals = df[col].dropna().head(5).tolist()
            print(f"{col}: {sample_vals}")
    parquet_path = r"keibaai\data\processed\races.parquet"
    if os.path.exists(parquet_path):
        df_p = pd.read_parquet(parquet_path, columns=['race_date'])
        df_p['race_date'] = pd.to_datetime(df_p['race_date'])
        print(f"\nRaces Parquet Date Range: {df_p['race_date'].min()} to {df_p['race_date'].max()}")
    else:
        print("\nraces.parquet not found.")

except Exception as e:
    print(f"Verification failed: {e}")

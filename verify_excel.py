import pandas as pd
import os

file_path = "race_analysis_20251130_東京.xlsx"

if not os.path.exists(file_path):
    print(f"Error: {file_path} not found.")
    exit(1)

try:
    # Read the first sheet
    xls = pd.ExcelFile(file_path)
    sheet_name = xls.sheet_names[0]
    print(f"Reading sheet: {sheet_name}")
    
    # The file has a header row for each horse, then the table.
    # We need to find the first table header row.
    # It's likely the 2nd row (index 1) if the first row is the horse info.
    
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    
    # Find the row with '日付'
    header_row_idx = -1
    for i, row in df.iterrows():
        if '日付' in row.values:
            header_row_idx = i
            break
            
    if header_row_idx == -1:
        print("Error: Could not find header row with '日付'")
        exit(1)
        
    print(f"Found header at row {header_row_idx}")
    
    # Extract columns
    columns = df.iloc[header_row_idx].tolist()
    print("Columns found:")
    print(columns)
    
    # Check for requested columns
    required_cols = [
        '日付', '場所', '回', '日', 'ｺｰｽ', '距離', 'R', '馬場', '天気', '頭数', 
        '枠番', '馬番', '馬名', '斤量', '騎手', '厩舎', 'ﾀｲﾑ', '着差', '人気', 'ｵｯｽﾞ', 
        '上り', 'ﾍﾟｰｽ1', 'ﾍﾟｰｽ2', '1C', '2C', '3C', '4C', '着順', '馬体重', '増減', 
        'レース名', '性', '年齢', 
        '平均t', '平均場t', '基準t', '基準場t', '基準3F', '基準場3F', 
        '平t差', '平場t差', '基t差', '基場t差', '基3F差', '基場3F差', 
        'horse_id', 'race_id'
    ]
    
    missing = [c for c in required_cols if c not in columns]
    if missing:
        print(f"Missing columns: {missing}")
    else:
        print("All required columns are present.")
        
    # Show a sample data row
    if len(df) > header_row_idx + 1:
        print("Sample data row:")
        print(df.iloc[header_row_idx + 1].tolist())

except Exception as e:
    print(f"Verification failed: {e}")

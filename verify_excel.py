import pandas as pd
import os

def verify_excel():
    file_path = 'race_analysis_20251130_東京.xlsx'

    if not os.path.exists(file_path):
        print(f"{file_path} not found.")
        return

    print(f"Reading {file_path}...")
    df_dict = pd.read_excel(file_path, sheet_name=None)

    total_rows = 0

    for name, sheet in df_dict.items():
        rows = len(sheet)
        total_rows += rows
        
        print(f"Sheet: {name}, Rows={rows}")

        # Check metrics
        metrics = ['タイム指数', '上り指数', 'RPCI', '平均t', '基準t', '基t差', '馬場差']
        for metric in metrics:
            if metric in sheet.columns:
                # Check for non-null and non-empty string
                if sheet[metric].dtype == object:
                    valid_count = sheet[metric].apply(lambda x: pd.notna(x) and str(x).strip() != "").sum()
                else:
                    valid_count = sheet[metric].notna().sum()
                
                print(f"  {metric}: {valid_count} / {rows} valid entries")
            else:
                print(f"  {metric}: Column not found!")
        print("-" * 20)

    print(f"Total Rows: {total_rows}")

if __name__ == "__main__":
    verify_excel()

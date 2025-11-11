import pandas as pd
import os
from pathlib import Path

def check_parquet_files():
    """
    解析済みParquetファイルの中身を確認する
    """
    # このスクリプトが keibaai/scripts ディレクトリにあることを前提とする
    project_root = Path(__file__).resolve().parent.parent
    parsed_dir = project_root / 'data' / 'parsed' / 'parquet'
    
    files_to_check = {
        "races": parsed_dir / 'races' / 'races.parquet',
    }
    
    for name, path in files_to_check.items():
        print(f"--- Checking: {name} ({path}) ---")
        if not path.exists():
            print("ファイルが見つかりません。")
            print("\n" + "="*50 + "\n")
            continue
            
        try:
            df = pd.read_parquet(path)
            print(f"Shape: {df.shape}")
            print("\nInfo:")
            df.info()
            print("\nHead (first 5 rows):")
            print(df.head(5).to_string())
            print("\nTail (last 5 rows):")
            print(df.tail(5).to_string())
        except Exception as e:
            print(f"ファイルの読み込み中にエラーが発生しました: {e}")
        
        print("\n" + "="*50 + "\n")

if __name__ == '__main__':
    check_parquet_files()
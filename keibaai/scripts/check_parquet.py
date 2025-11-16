import pandas as pd
import argparse
from pathlib import Path

def check_parquet_file(file_path: Path):
    """
    指定されたParquetファイルの中身を確認する
    """
    print(f"--- Checking Parquet: {file_path} ---")
    if not file_path.exists():
        print("ファイルが見つかりません。")
        return

    try:
        df = pd.read_parquet(file_path)
        print(f"Shape: {df.shape}")
        print("\nInfo:")
        df.info()
        print("\nColumns:")
        print(df.columns.tolist())
        print("\nHead (first 5 rows):")
        print(df.head(5).to_string())
        print("\nTail (last 5 rows):")
        print(df.tail(5).to_string())
        
        # is_jockey_changed と is_trainer_changed の値を確認
        if 'is_jockey_changed' in df.columns:
            print("\nValue counts for 'is_jockey_changed':")
            print(df['is_jockey_changed'].value_counts())
        
        if 'is_trainer_changed' in df.columns:
            print("\nValue counts for 'is_trainer_changed':")
            print(df['is_trainer_changed'].value_counts())

    except Exception as e:
        print(f"ファイルの読み込み中にエラーが発生しました: {e}")

def check_csv_file(file_path: Path):
    """
    指定されたCSVファイルの中身を確認する
    """
    print(f"--- Checking CSV: {file_path} ---")
    if not file_path.exists():
        print("ファイルが見つかりません。")
        return

    encodings_to_try = ['shift-jis', 'utf-8', 'cp932']
    df = None
    for encoding in encodings_to_try:
        try:
            print(f"Trying to read with encoding: {encoding}")
            # ヘッダーがない可能性を考慮
            df = pd.read_csv(file_path, encoding=encoding, header=None)
            print(f"Successfully read with {encoding}")
            break
        except Exception as e:
            print(f"Failed to read with {encoding}: {e}")
    
    if df is not None:
        print(f"Shape: {df.shape}")
        print("\nHead (first 20 rows):")
        print(df.head(20).to_string())
    else:
        print("Could not read the CSV file with any of the attempted encodings.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ParquetまたはCSVファイルの中身を確認するスクリプト')
    parser.add_argument('file_path', type=str, help='確認するファイルのパス')
    parser.add_argument('--csv', action='store_true', help='CSVファイルとして読み込む場合に指定')
    args = parser.parse_args()

    file_path = Path(args.file_path)

    if args.csv:
        check_csv_file(file_path)
    else:
        check_parquet_file(file_path)
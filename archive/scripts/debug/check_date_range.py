import pandas as pd
import sys

def check_date_range(file_path):
    try:
        df = pd.read_parquet(file_path)
        if 'race_date' not in df.columns:
            print(f"エラー: '{file_path}' に 'race_date' カラムが存在しません。")
            return

        # メモリを節約するため、日付カラムのみをロード
        date_col = pd.to_datetime(df['race_date'])
        min_date = date_col.min().strftime('%Y-%m-%d')
        max_date = date_col.max().strftime('%Y-%m-%d')
        
        print(f"ファイル: {file_path}")
        print(f"日付の範囲: {min_date} から {max_date} まで")
        print(f"ユニークな日付の数: {date_col.nunique()}")

    except Exception as e:
        print(f"ファイルの読み込みまたは処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        check_date_range(file_path)
    else:
        print("使用法: python check_date_range.py <parquet_file_path>")

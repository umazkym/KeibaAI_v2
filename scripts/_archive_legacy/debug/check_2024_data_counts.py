import pandas as pd
from pathlib import Path
import sys

try:
    # shutubaディレクトリ内のparquetファイルを探す
    files = list(Path('keibaai/data/parsed/parquet/shutuba').glob('*.parquet'))
    if not files:
        print("No parquet files found in keibaai/data/parsed/parquet/shutuba")
        sys.exit(1)
        
    path = files[0]
    print(f"Reading {path}...")
    df = pd.read_parquet(path)
    
    # race_dateを文字列に変換して確認
    # floatが混じっている可能性があるので、一旦astype(str)
    df['race_date_str'] = df['race_date'].astype(str)
    
    # 2024年のデータ数を月ごとに集計
    # '2024'を含むものを抽出
    df_2024 = df[df['race_date_str'].str.contains('2024', na=False)].copy()
    
    if df_2024.empty:
        print("No 2024 data found!")
    else:
        print(f"Total 2024 rows: {len(df_2024)}")
        # 月ごとのカウント
        # errors='coerce'でパースできないものはNaTにする
        df_2024['dt'] = pd.to_datetime(df_2024['race_date_str'], errors='coerce')
        df_2024['month'] = df_2024['dt'].dt.month
        print("Rows per month in 2024:")
        print(df_2024['month'].value_counts().sort_index())
        
except Exception as e:
    print(f"Error: {e}")

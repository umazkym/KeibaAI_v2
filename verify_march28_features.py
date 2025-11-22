import pandas as pd
from pathlib import Path

files = list(Path('keibaai/data/features/parquet/year=2020/month=3').rglob('*.parquet'))
print(f"3月の特徴量ファイル数: {len(files)}")

if files:
    df = pd.read_parquet(files[0])
    print(f"サンプル行数: {len(df)}")
    if 'race_date' in df.columns:
        print(f"race_date範囲: {df['race_date'].min()} - {df['race_date'].max()}")
        # 3/28のデータがあるか確認
        df_328 = df[df['race_date'] == '2020-03-28']
        print(f"\n2020-03-28のデータ: {len(df_328)}行")

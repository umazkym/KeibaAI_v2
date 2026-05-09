import pandas as pd
from pathlib import Path

# races.parquetから3/28のデータを確認
races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
df = pd.read_parquet(races_path)
df['race_date'] = pd.to_datetime(df['race_date'])

# 3/28のレースを確認
date_data = df[df['race_date'] == '2020-03-28']
print(f"2020-03-28のレース数: {len(date_data)}")
print(f"\nrace_ids: {date_data['race_id'].tolist()}")

# 必須カラムの欠損状況を確認
required_cols = ['race_id', 'horse_id', 'finish_position', 'finish_time_seconds']
print("\n必須カラムの欠損状況:")
for col in required_cols:
    if col in date_data.columns:
        null_count = date_data[col].isna().sum()
        print(f"  {col}: {null_count}/{len(date_data)} 欠損")
    else:
        print(f"  {col}: カラム自体が存在しない")

# 3/22（成功している日付）と比較
print("\n\n比較: 2020-03-22（成功している日付）")
date_data_322 = df[df['race_date'] == '2020-03-22']
print(f"2020-03-22のレース数: {len(date_data_322)}")
for col in required_cols:
    if col in date_data_322.columns:
        null_count = date_data_322[col].isna().sum()
        print(f"  {col}: {null_count}/{len(date_data_322)} 欠損")
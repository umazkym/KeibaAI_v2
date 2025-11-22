import pandas as pd
from pathlib import Path

races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
df = pd.read_parquet(races_path)
df['race_date'] = pd.to_datetime(df['race_date'])

# 2020-03-28のユニークなrace_idを確認
date_data = df[df['race_date'] == '2020-03-28']
print(f"2020-03-28の総レース数: {len(date_data)}")
print(f"ユニークなrace_id数: {date_data['race_id'].nunique()}")
print(f"\nユニークなrace_id一覧:")
unique_race_ids = sorted(date_data['race_id'].unique())
for race_id in unique_race_ids[:20]:  # 最初の20個だけ表示
    count = len(date_data[date_data['race_id'] == race_id])
    print(f"  {race_id}: {count}エントリ")

# race_idから実際の日付を逆算
print("\n\nrace_idのパターン分析:")
print("race_id形式: YYYYPPNNDDRR")
print("  YYYY=年, PP=場コード, NN=回, DD=日, RR=レース番号")
for race_id in unique_race_ids[:10]:
    venue_code = race_id[4:6]
    round_num = race_id[6:8]
    day_num = race_id[8:10]
    print(f"  {race_id}: 場={venue_code}, 第{round_num}回, {day_num}日目")
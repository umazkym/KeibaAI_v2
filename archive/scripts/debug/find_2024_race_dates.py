import sys
sys.path.insert(0, 'keibaai/src')

import pandas as pd
from pathlib import Path
import pyarrow.dataset as ds

print("=== 2024年 実在レース日付の確認 ===\n")

# PyArrow Datasetを使ってparquetファイルのみを読み込む
features_dir = Path('keibaai/data/features/parquet')
dataset = ds.dataset(features_dir, format='parquet', partitioning='hive')
features_df = dataset.to_table().to_pandas()

# 2024年のデータをフィルタ
features_2024 = features_df[pd.to_datetime(features_df['race_date']).dt.year == 2024]

if len(features_2024) > 0:
    # ユニークな日付を取得
    unique_dates = sorted(pd.to_datetime(features_2024['race_date']).dt.date.unique())
    
    print(f"2024年のレース開催日数: {len(unique_dates)}日")
    print(f"\n最初の10日:")
    for i, date in enumerate(unique_dates[:10], 1):
        weekday = ['月', '火', '水', '木', '金', '土', '日'][date.weekday()]
        date_mask = pd.to_datetime(features_2024['race_date']).dt.date == date
        race_count = features_2024[date_mask]['race_id'].nunique() if 'race_id' in features_2024.columns else len(features_2024[date_mask])
        print(f"  {i}. {date} ({weekday}) - {race_count}レース")
    
    print(f"\n最後の10日:")
    for i, date in enumerate(unique_dates[-10:], len(unique_dates)-9):
        weekday = ['月', '火', '水', '木', '金', '土', '日'][date.weekday()]
        date_mask = pd.to_datetime(features_2024['race_date']).dt.date == date
        race_count = features_2024[date_mask]['race_id'].nunique() if 'race_id' in features_2024.columns else len(features_2024[date_mask])
        print(f"  {i}. {date} ({weekday}) - {race_count}レース")
    
    # 最初の土曜日を特定
    saturdays = [d for d in unique_dates if d.weekday() == 5]  # 5 = 土曜日
    if saturdays:
        print(f"\n【推奨実行日: 最初の土曜日】")
        first_saturday = saturdays[0]
        date_mask = pd.to_datetime(features_2024['race_date']).dt.date == first_saturday
        race_count = features_2024[date_mask]['race_id'].nunique() if 'race_id' in features_2024.columns else len(features_2024[date_mask])
        print(f"  {first_saturday}: {race_count}レース")
        print(f"\npredict.pyコマンド:")
        print(f"  --date {first_saturday}")
else:
    print("2024年のデータが見つかりません")

print("\n" + "=" * 60)

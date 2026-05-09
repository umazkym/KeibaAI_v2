import pandas as pd
from pathlib import Path

# 2020年3月の特徴量データを確認
feature_path = Path('keibaai/data/features/parquet/year=2020/month=3')

if feature_path.exists():
    df = pd.read_parquet(feature_path)
    print(f"総行数: {len(df)}")
    
    df['race_date'] = pd.to_datetime(df['race_date'])
    
    # 全日付を確認
    dates = sorted(df['race_date'].dt.date.unique())
    print(f"\n3月の特徴量がある日付 ({len(dates)}日):")
    for date in dates:
        count = len(df[df['race_date'].dt.date == date])
        print(f"  {date}: {count}エントリ")
    
    # 失敗した日付を確認
    print("\n失敗日付の詳細:")
    failed_dates = ['2020-03-28', '2020-03-29', '2020-03-31']
    for date_str in failed_dates:
        date_data = df[df['race_date'].dt.strftime('%Y-%m-%d') == date_str]
        print(f"  {date_str}: {len(date_data)}エントリ")
else:
    print("特徴量データが見つかりません")
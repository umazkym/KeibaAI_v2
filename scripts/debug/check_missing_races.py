"""shutubaにのみ存在する4レースの詳細調査"""
import pandas as pd
from pathlib import Path

data_dir = Path('keibaai/data/parsed/parquet')
html_dir = Path('keibaai/data/raw/html')

# 問題のrace_id
missing_race_ids = ['201805010304', '201408020304', '201405010404', '201405010508']

print("=" * 80)
print("shutubaにのみ存在する4レースの詳細調査")
print("=" * 80)

# shutubaの内容確認
shutuba = pd.read_parquet(data_dir / 'shutuba/shutuba.parquet')

for race_id in missing_race_ids:
    print(f"\n--- {race_id} ---")
    rows = shutuba[shutuba['race_id'] == race_id]
    print(f"出走頭数: {len(rows)}")
    print(f"race_date: {rows['race_date'].iloc[0] if not rows.empty else 'N/A'}")
    print(f"馬名: {rows['horse_name'].tolist()[:5]}...")
    
    # HTMLファイルが存在するか確認
    race_html = html_dir / 'race' / f'{race_id}.bin'
    shutuba_html = html_dir / 'shutuba' / f'{race_id}.bin'
    
    print(f"race HTML存在: {race_html.exists()}")
    print(f"shutuba HTML存在: {shutuba_html.exists()}")

# cornersに不足している286レースを確認
print("\n" + "=" * 80)
print("cornersに不足している286レースの調査")
print("=" * 80)

races = pd.read_parquet(data_dir / 'races/races.parquet')
corners = pd.read_parquet(data_dir / 'corners/corner_positions.parquet')

races_race_ids = set(races['race_id'].unique())
corners_race_ids = set(corners['race_id'].unique())
missing_corners = races_race_ids - corners_race_ids

print(f"cornersに不足しているレース数: {len(missing_corners)}")
print(f"例: {list(missing_corners)[:10]}")

# 不足レースの特徴を確認
missing_races = races[races['race_id'].isin(missing_corners)]
print(f"\n不足レースの特徴:")
print(f"  期間: {missing_races['race_date'].min()} ~ {missing_races['race_date'].max()}")
print(f"  venue分布:")
print(missing_races['venue'].value_counts().head())
print(f"  track_surface分布:")
print(missing_races['track_surface'].value_counts())

# 直線コースかどうか確認
if 'is_straight_course' in missing_races.columns:
    print(f"  is_straight_course: {missing_races['is_straight_course'].value_counts()}")

"""
V15コース特徴量統合のテスト
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root / 'keibaai'))

from src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15

# テスト用データを作成
test_data = pd.DataFrame({
    'race_id': ['202504030411', '202504030412', '202405010101', '202401010101'],
    'horse_id': ['2019100001', '2019100002', '2019100003', '2019100004'],
    'horse_number': [1, 2, 3, 4],
    'venue': ['新潟', '新潟', '東京', '中山'],
    'track_surface': ['芝', '芝', 'ダート', '芝'],
    'distance_m': [2000, 2000, 1400, 2000],
    'is_outer_course': [True, None, None, None],  # 新潟2000m外/内
    'race_date': pd.to_datetime(['2025-08-01', '2025-08-01', '2025-05-01', '2025-01-01']),
    'finish_position': [1, 2, 3, 4],
    'popularity': [1, 2, 3, 4],
    'horse_front_runner_rate': [0.4, 0.2, 0.1, 0.3],  # V15で使用
})

print("=" * 80)
print("V15 コース特徴量統合テスト")
print("=" * 80)

# V15エンジニアを初期化
engineer = LeakFreeFeatureEngineerV15()

print("\n【テスト1】_calc_course_featuresメソッドのみテスト")
result = engineer._calc_course_features(test_data)

# 結果を表示
print("\n追加されたコース特徴量:")
course_cols = [
    'course_corner_count', 
    'course_start_to_corner_m',
    'course_final_straight_m', 
    'course_slope_percent',
    'course_is_outer',
    'straight_ratio',
    'is_long_straight'
]

for col in course_cols:
    if col in result.columns:
        print(f"  {col}: {result[col].tolist()}")
    else:
        print(f"  {col}: [カラムなし]")

# 期待値チェック
print("\n【検証】")

# 新潟2000m外 (is_outer=True)
niigata_outer = result[result['is_outer_course'] == True].iloc[0]
print(f"新潟2000m外: 直線={niigata_outer['course_final_straight_m']}m (期待値: 658.7m)")

# 新潟2000m内 (is_outer=None)
niigata_inner = result[(result['venue'] == '新潟') & (result['is_outer_course'] != True)].iloc[0]
print(f"新潟2000m内: 直線={niigata_inner['course_final_straight_m']}m (期待値: マスターのdefault)")

# 東京ダート1400m
tokyo = result[result['venue'] == '東京'].iloc[0]
print(f"東京ダ1400m: 直線={tokyo['course_final_straight_m']}m (期待値: 501.6m)")

# 中山芝2000m
nakayama = result[result['venue'] == '中山'].iloc[0]
print(f"中山芝2000m: 直線={nakayama['course_final_straight_m']}m, 坂={nakayama['course_slope_percent']}%")

print("\n" + "=" * 80)
print("テスト完了")
print("=" * 80)

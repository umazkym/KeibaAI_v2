"""
全コースのコーナーデータパターン調査
同一距離・同一馬場でコーナー数のパターンが複数あるコースを抽出する
"""

import pandas as pd
from pathlib import Path

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def main():
    print("=" * 80)
    print("全コース コーナーデータパターン調査")
    print("=" * 80)
    
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    
    # 障害レースを除外
    flat_races = races[~races['race_name'].str.contains('障害', na=False)].copy()
    
    # コーナー有無フラグ
    flat_races['has_c1'] = flat_races['passing_order_1'].notna()
    flat_races['has_c2'] = flat_races['passing_order_2'].notna()
    flat_races['has_c3'] = flat_races['passing_order_3'].notna()
    flat_races['has_c4'] = flat_races['passing_order_4'].notna()
    
    flat_races['corner_pattern'] = flat_races.apply(
        lambda x: ''.join(['1' if x[c] else '0' for c in ['has_c1', 'has_c2', 'has_c3', 'has_c4']]),
        axis=1
    )
    
    # レース単位で集約（多数決または代表値）
    # ここでは1レース内の全馬の最頻パターンを採用
    race_patterns = flat_races.groupby(['venue', 'distance_m', 'track_surface', 'race_id'])['corner_pattern'].agg(lambda x: x.mode()[0]).reset_index()
    
    # コースごとにパターン数を集計
    course_stats = race_patterns.groupby(['venue', 'distance_m', 'track_surface'])['corner_pattern'].nunique().reset_index()
    course_stats.columns = ['venue', 'distance_m', 'track_surface', 'pattern_count']
    
    # パターンが複数あるコースを抽出
    multi_pattern_courses = course_stats[course_stats['pattern_count'] > 1]
    
    print(f"\nパターンが複数あるコース: {len(multi_pattern_courses)}件")
    
    for _, row in multi_pattern_courses.iterrows():
        venue = row['venue']
        dist = row['distance_m']
        surface = row['track_surface']
        
        print(f"\n【{venue} {surface}{int(dist)}m】")
        
        # パターンごとの内訳
        course_races = race_patterns[
            (race_patterns['venue'] == venue) & 
            (race_patterns['distance_m'] == dist) & 
            (race_patterns['track_surface'] == surface)
        ]
        
        counts = course_races['corner_pattern'].value_counts()
        print(counts)
        
        # サンプル表示
        for pat in counts.index:
            sample_ids = course_races[course_races['corner_pattern'] == pat]['race_id'].head(3).tolist()
            sample_names = races[races['race_id'].isin(sample_ids)]['race_name'].drop_duplicates().tolist()
            print(f"  パターン {pat}: {sample_names}")

if __name__ == '__main__':
    main()

"""
コーナー通過順データを用いた内回り/外回りの判別可能性検証
新潟芝2000m, 京都芝1600mなど、同一距離でコースが異なるケースを分析
"""

import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def analyze_course_type(df, venue, distance, surface):
    print(f"\n{'='*60}")
    print(f"分析対象: {venue} {surface}{distance}m")
    print(f"{'='*60}")
    
    target_races = df[
        (df['venue'] == venue) & 
        (df['distance_m'] == distance) &
        (df['track_surface'] == surface)
    ].copy()
    
    if len(target_races) == 0:
        print("データがありません")
        return

    # コーナー通過順の欠損状況を確認
    # passing_order_1, 2, 3, 4
    
    # 1レースごとの非欠損数をカウント（代表して1着馬のデータで確認）
    # レース単位で判定したいため
    race_groups = target_races.groupby('race_id').first()
    
    # コーナーデータの有無
    race_groups['has_c1'] = race_groups['passing_order_1'].notna()
    race_groups['has_c2'] = race_groups['passing_order_2'].notna()
    race_groups['has_c3'] = race_groups['passing_order_3'].notna()
    race_groups['has_c4'] = race_groups['passing_order_4'].notna()
    
    # パターン分類
    race_groups['corner_pattern'] = race_groups.apply(
        lambda x: ''.join(['1' if x[c] else '0' for c in ['has_c1', 'has_c2', 'has_c3', 'has_c4']]),
        axis=1
    )
    
    pattern_counts = race_groups['corner_pattern'].value_counts()
    print("コーナーデータパターン (1=あり, 0=なし):")
    print(pattern_counts)
    
    # パターンごとのレース名サンプル
    for pattern in pattern_counts.index:
        print(f"\nパターン {pattern} ({pattern_counts[pattern]}レース):")
        sample_races = race_groups[race_groups['corner_pattern'] == pattern]['race_name'].head(5).tolist()
        print(f"  サンプル: {sample_races}")
        
    return race_groups

def main():
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    
    # 1. 新潟 芝2000m (内4つ vs 外2つ?)
    analyze_course_type(races, '新潟', 2000, '芝')
    
    # 2. 京都 芝1600m (内? vs 外?)
    analyze_course_type(races, '京都', 1600, '芝')
    
    # 3. 阪神 芝1400m (内? vs 外?)
    analyze_course_type(races, '阪神', 1400, '芝')

if __name__ == '__main__':
    main()

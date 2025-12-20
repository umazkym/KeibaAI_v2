"""
競馬場×距離別のコーナー数を詳細分析

has_4_cornersの判定を正確にするための調査
"""

import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")


def main():
    print("=" * 80)
    print("競馬場×距離別のコーナー数詳細分析")
    print("=" * 80)
    
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 平地レースのみ
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    
    # ===============================
    # 1. 競馬場×距離別のPO4欠損率
    # ===============================
    print("\n" + "=" * 80)
    print("1. 競馬場×距離別のPO4欠損率")
    print("=" * 80)
    
    # PO4欠損フラグ
    flat_races['po4_is_null'] = flat_races['passing_order_4'].isna()
    
    # 競馬場×距離でグループ化
    venue_dist = flat_races.groupby(['venue', 'distance_m']).agg({
        'po4_is_null': 'mean',
        'race_id': 'count'
    }).reset_index()
    
    venue_dist.columns = ['venue', 'distance_m', 'po4_null_rate', 'count']
    venue_dist['po4_null_rate'] = venue_dist['po4_null_rate'] * 100
    
    # 1600m以下で100%ではないケース
    print("\n【1600m以下でPO4欠損率 < 100% のケース】")
    print("（これらは1600m以下でも4コーナーがあるコース）")
    
    short_with_po4 = venue_dist[
        (venue_dist['distance_m'] <= 1600) & 
        (venue_dist['po4_null_rate'] < 100)
    ].sort_values('po4_null_rate')
    
    print(f"\n{'競馬場':<8} {'距離':>6} {'PO4欠損率':>10} {'レコード数':>10}")
    print("-" * 45)
    for _, row in short_with_po4.iterrows():
        print(f"{row['venue']:<8} {int(row['distance_m']):>6}m {row['po4_null_rate']:>9.1f}% {int(row['count']):>10,}")
    
    # 1800m以上で100%欠損のケース
    print("\n【1800m以上でPO4欠損率 = 100% のケース】")
    print("（これらは1800m以上でも4コーナーがないコース）")
    
    long_without_po4 = venue_dist[
        (venue_dist['distance_m'] >= 1800) & 
        (venue_dist['po4_null_rate'] == 100)
    ]
    
    if len(long_without_po4) > 0:
        print(f"\n{'競馬場':<8} {'距離':>6} {'PO4欠損率':>10} {'レコード数':>10}")
        print("-" * 45)
        for _, row in long_without_po4.iterrows():
            print(f"{row['venue']:<8} {int(row['distance_m']):>6}m {row['po4_null_rate']:>9.1f}% {int(row['count']):>10,}")
    else:
        print("  該当なし")
    
    # ===============================
    # 2. コーナー数の実際のパターン
    # ===============================
    print("\n" + "=" * 80)
    print("2. コーナー数の実際のパターン（PO4存在率で判定）")
    print("=" * 80)
    
    # PO4存在率が高い（80%以上）: 4コーナーあり
    # PO4存在率が低い（20%以下）: 4コーナーなし
    
    venue_dist['has_4_corners'] = venue_dist['po4_null_rate'] < 50
    
    corners_4 = venue_dist[venue_dist['has_4_corners']]
    corners_2 = venue_dist[~venue_dist['has_4_corners']]
    
    print(f"\n4コーナーありの競馬場×距離組み合わせ: {len(corners_4)}")
    print(f"4コーナーなしの競馬場×距離組み合わせ: {len(corners_2)}")
    
    # ===============================
    # 3. 正確なhas_4_cornersマッピングを生成
    # ===============================
    print("\n" + "=" * 80)
    print("3. 正確な4コーナー判定マッピング")
    print("=" * 80)
    
    # 競馬場×距離→4コーナー有無
    mapping = venue_dist[['venue', 'distance_m', 'has_4_corners']].copy()
    mapping = mapping.sort_values(['venue', 'distance_m'])
    
    print("\n【4コーナーありのコース】")
    for venue in mapping['venue'].unique():
        venue_data = mapping[(mapping['venue'] == venue) & (mapping['has_4_corners'] == True)]
        if len(venue_data) > 0:
            distances = sorted(venue_data['distance_m'].unique())
            print(f"  {venue}: {[int(d) for d in distances]}")
    
    print("\n【4コーナーなしのコース（1600m以下）】")
    for venue in mapping['venue'].unique():
        venue_data = mapping[(mapping['venue'] == venue) & 
                             (mapping['has_4_corners'] == False) & 
                             (mapping['distance_m'] <= 1600)]
        if len(venue_data) > 0:
            distances = sorted(venue_data['distance_m'].unique())
            print(f"  {venue}: {[int(d) for d in distances]}")
    
    # ===============================
    # 4. 設定ファイル用の出力
    # ===============================
    print("\n" + "=" * 80)
    print("4. 推奨: 競馬場×距離ベースの4コーナー判定")
    print("=" * 80)
    
    print("""
    【結論】
    
    distance_mだけでは正確に判定できない。
    
    例:
    - 京都1600m: 4コーナーあり（外回り）
    - 東京1600m: 3コーナー
    - 新潟1000m芝: 0コーナー（直線）
    
    【推奨対応】
    
    1. venue × distance_m のマッピングテーブルを作成
    2. 実データから自動判定（PO4存在率 > 50% なら4コーナーあり）
    3. 特徴量生成時にこのマッピングを使用
    """)
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == '__main__':
    main()

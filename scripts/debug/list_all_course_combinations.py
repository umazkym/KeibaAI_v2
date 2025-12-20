"""
全競馬場×距離×芝/ダートの組み合わせを生データから抽出
"""

import pandas as pd
from pathlib import Path

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def main():
    print("=" * 100)
    print("全競馬場×距離×芝/ダートの組み合わせ")
    print("=" * 100)
    
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 障害レースを除外
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    
    # 競馬場一覧
    venues = ['札幌', '函館', '福島', '新潟', '東京', '中山', '中京', '京都', '阪神', '小倉']
    
    # 競馬場×距離×馬場の組み合わせを集計
    combinations = flat_races.groupby(['venue', 'distance_m', 'track_surface']).agg({
        'race_id': 'nunique',
        'horse_id': 'count'
    }).reset_index()
    combinations.columns = ['venue', 'distance_m', 'track_surface', 'races', 'records']
    
    # 競馬場でフィルタ
    combinations = combinations[combinations['venue'].isin(venues)]
    combinations = combinations.sort_values(['venue', 'track_surface', 'distance_m'])
    
    # 競馬場ごとに出力
    for venue in venues:
        venue_data = combinations[combinations['venue'] == venue]
        if len(venue_data) == 0:
            continue
        
        print(f"\n{'='*80}")
        print(f"【{venue}競馬場】")
        print(f"{'='*80}")
        
        for surface in ['芝', 'ダート']:
            surface_data = venue_data[venue_data['track_surface'] == surface]
            if len(surface_data) == 0:
                continue
            
            print(f"\n  [{surface}]")
            print(f"  {'距離':>8} {'レース数':>10} {'出走数':>12}")
            print(f"  " + "-" * 35)
            
            for _, row in surface_data.iterrows():
                print(f"  {int(row['distance_m']):>7}m {int(row['races']):>10} {int(row['records']):>12}")
            
            print(f"  " + "-" * 35)
            print(f"  {'合計':>8} {int(surface_data['races'].sum()):>10} {int(surface_data['records'].sum()):>12}")
    
    # 全組み合わせの総数
    print(f"\n\n{'='*100}")
    print(f"総計")
    print(f"{'='*100}")
    
    total_combinations = len(combinations)
    total_races = combinations['races'].sum()
    total_records = combinations['records'].sum()
    
    print(f"総組み合わせ数: {total_combinations}")
    print(f"総レース数: {int(total_races):,}")
    print(f"総出走数: {int(total_records):,}")
    
    # CSVで全データ出力
    print(f"\n\n{'='*100}")
    print(f"全組み合わせ一覧（CSV形式）")
    print(f"{'='*100}")
    print("\nvenue,track_surface,distance_m,races,records")
    for _, row in combinations.iterrows():
        print(f"{row['venue']},{row['track_surface']},{int(row['distance_m'])},{int(row['races'])},{int(row['records'])}")


if __name__ == '__main__':
    main()

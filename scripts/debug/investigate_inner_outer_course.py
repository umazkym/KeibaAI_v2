"""
内回り/外回り情報の調査

Parquetデータに内回り/外回りの情報が含まれているか確認
"""

import pandas as pd
from pathlib import Path

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")


def main():
    print("=" * 80)
    print("内回り/外回り情報の調査")
    print("=" * 80)
    
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    
    # 全カラムを確認
    print("\n【races.parquetのカラム一覧】")
    for col in races.columns:
        print(f"  - {col}")
    
    # 内回り/外回りに関連しそうなカラムを探す
    print("\n" + "=" * 80)
    print("内回り/外回り関連カラムの探索")
    print("=" * 80)
    
    keywords = ['inner', 'outer', '内', '外', 'course', 'コース', 'track']
    for col in races.columns:
        for kw in keywords:
            if kw.lower() in col.lower():
                print(f"  発見: {col}")
                print(f"    ユニーク値: {races[col].dropna().unique()[:20]}")
    
    # race_nameから内回り/外回りを探す
    print("\n" + "=" * 80)
    print("race_nameに「内」「外」を含むレース")
    print("=" * 80)
    
    inner_races = races[races['race_name'].str.contains('内', na=False)]
    outer_races = races[races['race_name'].str.contains('外', na=False)]
    
    print(f"\n「内」を含むrace_name: {len(inner_races):,}件")
    if len(inner_races) > 0:
        print("  サンプル:")
        for name in inner_races['race_name'].drop_duplicates().head(10):
            print(f"    - {name}")
    
    print(f"\n「外」を含むrace_name: {len(outer_races):,}件")
    if len(outer_races) > 0:
        print("  サンプル:")
        for name in outer_races['race_name'].drop_duplicates().head(10):
            print(f"    - {name}")
    
    # 新潟芝2000mに絞って確認
    print("\n" + "=" * 80)
    print("新潟芝2000mの詳細確認")
    print("=" * 80)
    
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    niigata_2000 = flat_races[
        (flat_races['venue'] == '新潟') & 
        (flat_races['distance_m'] == 2000) &
        (flat_races['track_surface'] == '芝')
    ]
    
    print(f"\n新潟芝2000m レース数: {niigata_2000['race_id'].nunique()}")
    print(f"出走レコード数: {len(niigata_2000)}")
    
    # race_nameのパターン確認
    print("\nrace_nameのユニーク値:")
    for name in niigata_2000['race_name'].drop_duplicates().head(20):
        print(f"  - {name}")
    
    # HTMLにコース情報があるか確認するため、生のrace_infoを見る
    print("\n" + "=" * 80)
    print("生HTMLデータ(bin)の構造確認")
    print("=" * 80)
    
    # binファイルからサンプルを取得
    bin_dir = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\scraped\race_results\bin")
    
    if bin_dir.exists():
        bin_files = list(bin_dir.glob('*.bin'))[:5]
        print(f"\nbinファイル数: {len(list(bin_dir.glob('*.bin')))}")
        
        if len(bin_files) > 0:
            print("\nサンプルbinファイルの内容（コース情報を探す）:")
            
            for bf in bin_files:
                try:
                    with open(bf, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # コース情報を含む部分を探す
                    if '内' in content or '外' in content or 'コース' in content:
                        # 該当部分を抽出
                        lines = content.split('\n')
                        for i, line in enumerate(lines):
                            if '内' in line or '外' in line:
                                if '回り' in line or 'コース' in line:
                                    print(f"\n  {bf.name}:")
                                    print(f"    {line[:200]}...")
                                    break
                except Exception as e:
                    pass
    else:
        print(f"binディレクトリが見つかりません: {bin_dir}")
    
    print("\n" + "=" * 80)
    print("調査完了")
    print("=" * 80)


if __name__ == '__main__':
    main()

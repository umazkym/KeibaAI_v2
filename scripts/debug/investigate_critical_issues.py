"""
発見された重大な問題の詳細調査

問題1: 血統データが全馬で不完全（62件完全な馬が0頭）
問題2: 京都競馬場の割合が大幅減少（15.1% → 8.9%）
問題3: 年が飛んでいる馬が998頭
問題4: 3頭以上同着が4件
問題5: horses.parquetにない馬が2頭
"""

import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def investigate_pedigree_issue():
    """血統データ問題の詳細調査"""
    print_section("問題1: 血統データ構造の詳細調査")
    
    pedigrees = pd.read_parquet(DATA_DIR / "pedigrees" / "pedigrees.parquet")
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    
    print(f"\n  pedigrees.parquetの基本情報:")
    print(f"    総レコード数: {len(pedigrees):,}")
    print(f"    ユニーク馬数: {pedigrees['horse_id'].nunique():,}")
    print(f"    カラム: {list(pedigrees.columns)}")
    
    # サンプルデータを表示
    print(f"\n  サンプルデータ:")
    print(pedigrees.head(20).to_string())
    
    # 世代の分布
    print(f"\n  世代別レコード数:")
    gen_counts = pedigrees.groupby('generation').size()
    for gen, count in gen_counts.items():
        print(f"    世代{gen}: {count:,}件")
    
    # 本来の血統データ構造
    print(f"\n  【本来の血統データ構造】")
    print(f"    5世代の血統で、各馬には以下の先祖がいる:")
    print(f"    世代1: 父・母 = 2頭")
    print(f"    世代2: 祖父母 = 4頭")
    print(f"    世代3: 曾祖父母 = 8頭")
    print(f"    世代4: 高祖父母 = 16頭")
    print(f"    世代5: 高高祖父母 = 32頭")
    print(f"    合計: 62頭/馬")
    
    # 馬ごとのレコード数
    horse_ped_counts = pedigrees.groupby('horse_id').size()
    
    print(f"\n  馬ごとの血統レコード数分布:")
    print(f"    1件のみ: {(horse_ped_counts == 1).sum():,}頭")
    print(f"    2-10件: {((horse_ped_counts >= 2) & (horse_ped_counts <= 10)).sum():,}頭")
    print(f"    11-30件: {((horse_ped_counts > 10) & (horse_ped_counts <= 30)).sum():,}頭")
    print(f"    31-62件: {(horse_ped_counts > 30).sum():,}頭")
    
    # 1件のみの馬のサンプル
    one_record = horse_ped_counts[horse_ped_counts == 1].index[:5]
    print(f"\n  1件のみの馬のサンプル:")
    for horse_id in one_record:
        sample = pedigrees[pedigrees['horse_id'] == horse_id]
        print(f"    {horse_id}:")
        print(sample.to_string())
    
    # generetion別の詳細
    print(f"\n  【世代5のデータ確認】（最も多い）")
    gen5 = pedigrees[pedigrees['generation'] == 5]
    print(f"    レコード数: {len(gen5):,}")
    print(f"    ユニーク馬数: {gen5['horse_id'].nunique():,}")
    
    # 世代5のサンプル
    print(f"\n  世代5のサンプル:")
    print(gen5.head(10).to_string())
    
    # V15で血統データがどう使われているかを確認
    print_section("血統データのモデルへの影響分析")
    
    # sire_id（父ID）が取得できる馬の割合
    gen1 = pedigrees[pedigrees['generation'] == 1]
    horses_with_sire = gen1['horse_id'].unique()
    all_racing_horses = set(races['horse_id'].dropna().unique())
    
    horses_with_sire_set = set(horses_with_sire)
    sire_coverage = len(all_racing_horses & horses_with_sire_set) / len(all_racing_horses) * 100 if len(all_racing_horses) > 0 else 0
    
    print(f"\n  レース出走馬のうち世代1（父母）データがある馬: {sire_coverage:.1f}%")
    
    # データの形式を確認
    print(f"\n  【データ形式の確認】")
    print(f"    ancestor_idの例: {pedigrees['ancestor_id'].head(5).tolist()}")
    print(f"    ancestor_nameの例: {pedigrees['ancestor_name'].head(5).tolist()}")

def investigate_kyoto_drop():
    """京都競馬場の割合減少の調査"""
    print_section("問題2: 京都競馬場の割合減少")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    print(f"\n  年別の京都競馬場レース数:")
    kyoto = races[races['venue'] == '京都']
    kyoto_by_year = kyoto.groupby('year').size()
    total_by_year = races.groupby('year').size()
    
    print(f"  {'年':<6} {'京都レース数':>12} {'全レース数':>12} {'割合':>10}")
    print("-" * 45)
    
    for year in sorted(races['year'].unique()):
        kyoto_count = kyoto_by_year.get(year, 0)
        total_count = total_by_year.get(year, 0)
        pct = kyoto_count / total_count * 100 if total_count > 0 else 0
        print(f"  {year:<6} {kyoto_count:>12,} {total_count:>12,} {pct:>9.1f}%")
    
    print(f"\n  【考察】")
    print(f"  2020年以降の京都競馬場の減少は、京都競馬場の改修工事による")
    print(f"  長期休催が原因と考えられる（2020年11月〜2023年4月）")

def investigate_missing_horses():
    """horses.parquetにない馬の調査"""
    print_section("問題3: horses.parquetにない馬")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    horses = pd.read_parquet(DATA_DIR / "horses" / "horses.parquet")
    
    races_horse_ids = set(races['horse_id'].dropna().unique())
    horses_ids = set(horses['horse_id'].unique())
    
    missing = races_horse_ids - horses_ids
    
    print(f"\n  horses.parquetにない馬: {len(missing)}頭")
    
    for horse_id in missing:
        horse_races = races[races['horse_id'] == horse_id]
        print(f"\n  馬ID: {horse_id}")
        print(f"    レース数: {len(horse_races)}")
        print(f"    レース年: {sorted(horse_races['year'].unique())}")
        print(f"    馬名: {horse_races['horse_name'].iloc[0] if 'horse_name' in horse_races.columns else 'N/A'}")

def investigate_triple_dead_heat():
    """3頭以上同着の調査"""
    print_section("問題4: 3頭以上同着レース")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    
    # 着順ごとの頭数を集計
    finish_counts = races.groupby(['race_id', 'finish_position']).size().reset_index(name='count')
    finish_counts = finish_counts[finish_counts['finish_position'].notna()]
    
    # 3頭以上同着
    triple_plus = finish_counts[finish_counts['count'] >= 3]
    
    print(f"\n  3頭以上同着のケース: {len(triple_plus)}件")
    
    for _, row in triple_plus.iterrows():
        race_id = row['race_id']
        finish_pos = row['finish_position']
        count = row['count']
        
        race_data = races[(races['race_id'] == race_id) & (races['finish_position'] == finish_pos)]
        print(f"\n  race_id: {race_id}")
        print(f"  着順: {finish_pos}、同着馬数: {count}")
        print(f"  詳細:")
        for _, horse in race_data.iterrows():
            print(f"    #{horse['horse_number']} {horse.get('horse_name', 'N/A')} オッズ:{horse.get('win_odds', 'N/A')}")

def investigate_year_gap_horses():
    """年が飛んでいる馬の調査"""
    print_section("問題5: 年が飛んでいる馬の調査")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    horse_years = races.groupby('horse_id')['year'].agg(['min', 'max', 'nunique'])
    horse_years['span'] = horse_years['max'] - horse_years['min'] + 1
    horse_years['gap'] = horse_years['span'] - horse_years['nunique']
    
    gapped = horse_years[horse_years['gap'] > 0]
    
    print(f"\n  年が飛んでいる馬: {len(gapped):,}頭")
    
    # ギャップ別の分布
    print(f"\n  ギャップ年数の分布:")
    gap_dist = gapped['gap'].value_counts().sort_index()
    for gap, count in gap_dist.items():
        print(f"    {gap}年ギャップ: {count}頭")
    
    # モデルへの影響
    print(f"\n  【モデルへの影響】")
    print(f"  - 年がスキップされている馬は、過去統計の計算において問題を引き起こす可能性")
    print(f"  - 特に、長期休養後の復帰馬では、過去の成績が古くなりすぎている")
    
    # 主な原因
    print(f"\n  【主な原因の推測】")
    print(f"  1. 長期休養（故障、繁殖入りからの復帰等）")
    print(f"  2. 地方競馬への転厩・復帰")
    print(f"  3. 海外遠征")

def investigate_pace_data_null():
    """ペースデータNULLの調査"""
    print_section("問題6: ペースデータNULLの調査")
    
    race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    
    # ペースデータNULL
    null_pace = race_details[race_details[['first_half', 'second_half']].isnull().any(axis=1)]
    
    print(f"\n  ペースデータNULLのレース: {len(null_pace):,}件")
    
    # これらのレースの特徴
    null_race_ids = null_pace['race_id'].unique()
    null_races = races[races['race_id'].isin(null_race_ids)]
    
    # 距離別
    print(f"\n  距離別分布:")
    dist_counts = null_races.groupby('distance_m')['race_id'].nunique().sort_values(ascending=False).head()
    for dist, count in dist_counts.items():
        print(f"    {dist}m: {count}レース")
    
    # 馬場別
    print(f"\n  馬場別分布:")
    surf_counts = null_races.groupby('track_surface')['race_id'].nunique()
    for surf, count in surf_counts.items():
        print(f"    {surf}: {count}レース")
    
    # サンプル
    print(f"\n  サンプル:")
    sample = null_pace.head(5)
    print(sample[['race_id', 'first_half', 'second_half', 'lap_times_str']].to_string())

def check_v15_pedigree_usage():
    """V15で血統データがどう使われているか確認"""
    print_section("V15での血統データ使用状況")
    
    v14_path = PROJECT_ROOT / "keibaai" / "src" / "features" / "leak_free_feature_engineer_v14.py"
    v13_path = PROJECT_ROOT / "keibaai" / "src" / "features" / "leak_free_feature_engineer_v13.py"
    
    # V14を確認
    if v14_path.exists():
        with open(v14_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if 'pedigree' in content.lower() or 'sire' in content.lower() or 'dam' in content.lower():
            print(f"\n  V14で血統関連の記述あり")
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if 'sire' in line.lower() or 'dam' in line.lower() or 'pedigree' in line.lower():
                    print(f"    L{i+1}: {line.strip()[:80]}")
        else:
            print(f"\n  V14では血統データは直接使用されていない")
    
    # V13も確認
    if v13_path.exists():
        with open(v13_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if 'pedigree' in content.lower() or 'sire' in content.lower() or 'dam' in content.lower():
            print(f"\n  V13で血統関連の記述あり")
            # 使用箇所をカウント
            sire_count = content.lower().count('sire')
            dam_count = content.lower().count('dam')
            print(f"    'sire'出現回数: {sire_count}")
            print(f"    'dam'出現回数: {dam_count}")

def main():
    print("=" * 80)
    print("  重大な問題の詳細調査")
    print("=" * 80)
    
    investigate_pedigree_issue()
    investigate_kyoto_drop()
    investigate_missing_horses()
    investigate_triple_dead_heat()
    investigate_year_gap_horses()
    investigate_pace_data_null()
    check_v15_pedigree_usage()
    
    print("\n" + "=" * 80)
    print("  調査完了")
    print("=" * 80)

if __name__ == "__main__":
    main()

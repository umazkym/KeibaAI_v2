"""
データ整合性の詳細調査
1. shutuba vs racesの差異
2. 各ファイルのrace_id整合性
3. horses vs pedigreesの整合性
"""
import pandas as pd
from pathlib import Path

data_dir = Path('keibaai/data/parsed/parquet')

print("=" * 80)
print("データ整合性の詳細調査")
print("=" * 80)

# 各ファイルを読み込み
races = pd.read_parquet(data_dir / 'races/races.parquet')
shutuba = pd.read_parquet(data_dir / 'shutuba/shutuba.parquet')
returns = pd.read_parquet(data_dir / 'returns/returns.parquet')
race_details = pd.read_parquet(data_dir / 'race_details/race_details.parquet')
corners = pd.read_parquet(data_dir / 'corners/corner_positions.parquet')
horses = pd.read_parquet(data_dir / 'horses/horses.parquet')
pedigrees = pd.read_parquet(data_dir / 'pedigrees/pedigrees.parquet')

# 1. races vs shutuba の差異調査
print("\n" + "=" * 80)
print("1. races vs shutuba の差異調査")
print("=" * 80)

print(f"\nraces件数: {len(races):,}")
print(f"shutuba件数: {len(shutuba):,}")
print(f"差: {len(shutuba) - len(races):,}件")

# race_idレベルでの比較
races_race_ids = set(races['race_id'].unique())
shutuba_race_ids = set(shutuba['race_id'].unique())

print(f"\nracesのユニークrace_id数: {len(races_race_ids):,}")
print(f"shutubaのユニークrace_id数: {len(shutuba_race_ids):,}")

only_in_shutuba = shutuba_race_ids - races_race_ids
only_in_races = races_race_ids - shutuba_race_ids

print(f"\nshutubaにのみ存在するrace_id: {len(only_in_shutuba)}")
if only_in_shutuba:
    print(f"  例: {list(only_in_shutuba)[:5]}")

print(f"racesにのみ存在するrace_id: {len(only_in_races)}")
if only_in_races:
    print(f"  例: {list(only_in_races)[:5]}")

# (race_id, horse_number)レベルでの比較
races_keys = set(zip(races['race_id'], races['horse_number']))
shutuba_keys = set(zip(shutuba['race_id'], shutuba['horse_number']))

only_in_shutuba_keys = shutuba_keys - races_keys
only_in_races_keys = races_keys - shutuba_keys

print(f"\nshutubaにのみ存在する(race_id, horse_number): {len(only_in_shutuba_keys)}")
if only_in_shutuba_keys:
    sample = list(only_in_shutuba_keys)[:5]
    print(f"  例: {sample}")
    # これらの詳細を確認
    for race_id, horse_num in sample[:2]:
        row = shutuba[(shutuba['race_id'] == race_id) & (shutuba['horse_number'] == horse_num)]
        if not row.empty:
            print(f"    {race_id}/{horse_num}: scratched={row['scratched'].values[0]}, horse_name={row['horse_name'].values[0]}")

print(f"racesにのみ存在する(race_id, horse_number): {len(only_in_races_keys)}")

# 2. 各ファイルのrace_id整合性
print("\n" + "=" * 80)
print("2. 各ファイルのrace_id整合性")
print("=" * 80)

returns_race_ids = set(returns['race_id'].unique())
race_details_race_ids = set(race_details['race_id'].unique())
corners_race_ids = set(corners['race_id'].unique())

print(f"\nracesのユニークrace_id: {len(races_race_ids):,}")
print(f"returnsのユニークrace_id: {len(returns_race_ids):,}")
print(f"race_detailsのユニークrace_id: {len(race_details_race_ids):,}")
print(f"cornersのユニークrace_id: {len(corners_race_ids):,}")

# racesに対する各ファイルのカバー率
print(f"\nracesに対するカバー率:")
print(f"  returns: {len(races_race_ids & returns_race_ids) / len(races_race_ids) * 100:.1f}%")
print(f"  race_details: {len(races_race_ids & race_details_race_ids) / len(races_race_ids) * 100:.1f}%")
print(f"  corners: {len(races_race_ids & corners_race_ids) / len(races_race_ids) * 100:.1f}%")

# racesにのみ存在するrace_id（各ファイルにない）
missing_returns = races_race_ids - returns_race_ids
missing_race_details = races_race_ids - race_details_race_ids
missing_corners = races_race_ids - corners_race_ids

print(f"\nracesにはあるが不足しているrace_id数:")
print(f"  returnsに不足: {len(missing_returns):,}")
print(f"  race_detailsに不足: {len(missing_race_details):,}")
print(f"  cornersに不足: {len(missing_corners):,}")

# 3. horses vs pedigrees の整合性
print("\n" + "=" * 80)
print("3. horses vs pedigrees の整合性")
print("=" * 80)

horses_ids = set(horses['horse_id'].unique())
pedigrees_horse_ids = set(pedigrees['horse_id'].unique())

# racesに出てくるhorse_id
races_horse_ids = set(races['horse_id'].dropna().unique())

print(f"\nhorsesのユニークhorse_id: {len(horses_ids):,}")
print(f"pedigreesのユニークhorse_id: {len(pedigrees_horse_ids):,}")
print(f"racesのユニークhorse_id: {len(races_horse_ids):,}")

# カバー率
print(f"\nカバー率:")
print(f"  racesのhorse_idに対するhorsesのカバー: {len(races_horse_ids & horses_ids) / len(races_horse_ids) * 100:.1f}%")
print(f"  racesのhorse_idに対するpedigreesのカバー: {len(races_horse_ids & pedigrees_horse_ids) / len(races_horse_ids) * 100:.1f}%")
print(f"  horsesに対するpedigreesのカバー: {len(horses_ids & pedigrees_horse_ids) / len(horses_ids) * 100:.1f}%")

# 不足分
missing_horses = races_horse_ids - horses_ids
missing_pedigrees = races_horse_ids - pedigrees_horse_ids
missing_pedigrees_for_horses = horses_ids - pedigrees_horse_ids

print(f"\n不足分:")
print(f"  racesにあるがhorsesにない馬: {len(missing_horses):,}")
print(f"  racesにあるがpedigreesにない馬: {len(missing_pedigrees):,}")
print(f"  horsesにあるがpedigreesにない馬: {len(missing_pedigrees_for_horses):,}")

if missing_horses:
    print(f"  horsesにない馬の例: {list(missing_horses)[:5]}")

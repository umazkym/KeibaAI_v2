import pandas as pd
from pathlib import Path

# Check ID matching
shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')

print(f"Loading shutuba from: {shutuba_path}")
shutuba_df = pd.read_parquet(shutuba_path)

print(f"Loading races from: {races_path}")
races_df = pd.read_parquet(races_path)

target_race_id = '202406010101'
print(f"\nChecking Race ID: {target_race_id}")

s_race = shutuba_df[shutuba_df['race_id'] == target_race_id]
r_race = races_df[races_df['race_id'] == target_race_id]

print(f"Shutuba rows: {len(s_race)}")
print(f"Races rows: {len(r_race)}")

if not s_race.empty and not r_race.empty:
    s_horses = set(s_race['horse_id'].astype(str).str.strip())
    r_horses = set(r_race['horse_id'].astype(str).str.strip())
    
    print(f"Shutuba horses: {s_horses}")
    print(f"Races horses: {r_horses}")
    
    common = s_horses.intersection(r_horses)
    print(f"Common horses: {len(common)}")
    
    if len(common) == 0:
        print("NO MATCHING HORSES!")
    else:
        print("Horses match.")
        # Check finish_position for these horses in races
        print("Finish positions in races:")
        print(r_race[['horse_id', 'finish_position']])
else:
    print("Race ID not found in one of the dataframes.")

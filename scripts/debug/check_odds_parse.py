# -*- coding: utf-8 -*-
"""パース結果確認スクリプト"""
from pathlib import Path
from keibaai.src.parsers._parse_odds import parse_all_odds_for_race

RAW = Path('keibaai/data/raw/html/odds')
OUT = Path('keibaai/data/parsed/parquet/odds')
RACE_ID = '202406050810'
result = parse_all_odds_for_race(RACE_ID, RAW, OUT)

print(f'=== レースID: {RACE_ID} ===')

print('========== 単勝/複勝 (tansho_fukusho) ==========')
data = result.get('tansho_fukusho', {}).get('data', [])
print(f"件数: {len(data)}")
for item in data[:5]:
    print(f"  馬番{item['horse_number']}: 単勝={item['win_odds']}, 複勝={item['place_odds']}")

print()
print('========== 馬連 (umaren) ==========')
data = result.get('umaren', {}).get('data', [])
print(f"件数: {len(data)}")
for item in data[:5]:
    print(f"  {item['horse1']}-{item['horse2']}: {item['odds']}")

print()
print('========== ワイド (wide) ==========')
data = result.get('wide', {}).get('data', [])
print(f"件数: {len(data)}")
for item in data[:5]:
    print(f"  {item['horse1']}-{item['horse2']}: {item['odds']}")

print()
print('========== 馬単 (umatan) ==========')
data = result.get('umatan', {}).get('data', [])
print(f"件数: {len(data)}")
for item in data[:5]:
    print(f"  {item['horse1']}->{item['horse2']}: {item['odds']}")

print()
print('========== 三連複 (sanrenpuku) ==========')
data = result.get('sanrenpuku', {}).get('data', [])
print(f"件数: {len(data)}")
for item in data[:5]:
    print(f"  {item['horse1']}-{item['horse2']}-{item['horse3']}: {item['odds']}")

print()
print('========== 三連単 (sanrentan) ==========')
data = result.get('sanrentan', {}).get('data', [])
print(f"件数: {len(data)}")
print("上位5件:")
for item in data[:5]:
    print(f"  {item['horse1']}->{item['horse2']}->{item['horse3']}: {item['odds']}")
print("下位5件:")
for item in data[-5:]:
    print(f"  {item['horse1']}->{item['horse2']}->{item['horse3']}: {item['odds']}")

#!/usr/bin/env python3
"""ラップタイム構造の詳細調査"""
from pathlib import Path
from bs4 import BeautifulSoup

file_path = "keibaai/data/raw/html/race/202001010101.bin"
with open(file_path, 'rb') as f:
    html_text = f.read().decode('euc_jp', errors='replace')

soup = BeautifulSoup(html_text, 'html.parser')
lap = soup.find('table', summary='ラップタイム')

if lap:
    rows = lap.find_all('tr')
    print(f"Total rows: {len(rows)}")
    for i, row in enumerate(rows):
        ths = row.find_all('th')
        tds = row.find_all('td')
        print(f"\nRow {i}:")
        print(f"  th count: {len(ths)}")
        print(f"  td count: {len(tds)}")
        if ths:
            print(f"  th texts: {[th.get_text(strip=True) for th in ths]}")
        if tds:
            print(f"  td texts: {[td.get_text(strip=True) for td in tds[:5]]}...")
else:
    print("Lap table not found")

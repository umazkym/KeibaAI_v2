#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""馬の過去成績HTMLのパースをデバッグ"""

import requests
from bs4 import BeautifulSoup
import re

# テスト用馬ID
horse_id = "2022104939"  # ウィンターベル

url = f"https://db.netkeiba.com/horse/{horse_id}/"
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})
resp = session.get(url, timeout=15)
soup = BeautifulSoup(resp.content, 'html.parser')

result_rows = soup.select("table.db_h_race_results tr.txt_c")
print(f"Found {len(result_rows)} race history rows")

if result_rows:
    for i, row in enumerate(result_rows[:2]):
        print(f"\n=== Race {i+1} ===")
        tds = row.find_all('td')
        print(f"TD count: {len(tds)}")
        for j, td in enumerate(tds):
            text = td.get_text(strip=True)[:20]
            print(f"  [{j:2d}] {text}")

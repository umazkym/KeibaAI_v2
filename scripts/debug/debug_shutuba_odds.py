#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""出馬表HTMLのオッズ取得をデバッグ"""

import requests
from bs4 import BeautifulSoup

url = 'https://race.netkeiba.com/race/shutuba.html?race_id=202506050401'
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})
resp = session.get(url, timeout=15)
resp.encoding = 'euc-jp'
soup = BeautifulSoup(resp.content, 'html.parser')

rows = soup.select('tr.HorseList')
print(f'Found {len(rows)} horses')

for i, row in enumerate(rows[:3]):
    print(f'\n=== Horse {i+1} ===')
    
    # 馬名
    name_elem = row.select_one('.HorseName a')
    print(f'馬名: {name_elem.text.strip() if name_elem else "N/A"}')
    
    # 全てのtdを表示
    tds = row.find_all('td')
    print(f'TD count: {len(tds)}')
    
    for j, td in enumerate(tds):
        td_class = td.get('class', [])
        td_text = td.text.strip()[:30]
        print(f'  TD[{j}] class={td_class} text="{td_text}"')
        
        # spanを探す
        spans = td.find_all('span')
        for span in spans:
            span_id = span.get('id', '')
            span_text = span.text.strip()
            if span_id or span_text:
                print(f'    SPAN id="{span_id}" text="{span_text}"')

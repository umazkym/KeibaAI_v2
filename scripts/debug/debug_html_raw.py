#!/usr/bin/env python3
"""払い戻しHTML生構造の確認"""
from bs4 import BeautifulSoup
import re

file_path = "keibaai/data/raw/html/race/202001010101.bin"
with open(file_path, 'rb') as f:
    html_text = f.read().decode('euc_jp', errors='replace')

soup = BeautifulSoup(html_text, 'html.parser')
tables = soup.find_all('table', summary=re.compile(r'払戻|払い戻し'))

print("=== HTMLの生構造確認（innerHTML） ===")
for i, table in enumerate(tables):
    print(f"\n--- テーブル {i+1} ---")
    for row in table.find_all('tr'):
        th = row.find('th')
        tds = row.find_all('td')
        th_text = th.get_text(strip=True) if th else ""
        
        print(f"\n  {th_text}:")
        for j, td in enumerate(tds):
            # innerHTMLを出力
            inner = str(td)[:200]
            text = td.get_text(strip=True)
            print(f"    TD[{j}] text='{text}'")
            print(f"         html={inner[:100]}...")

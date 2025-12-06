#!/usr/bin/env python3
"""払い戻しテーブルの詳細構造確認"""
from bs4 import BeautifulSoup
import re

file_path = "keibaai/data/raw/html/race/202001010101.bin"
with open(file_path, 'rb') as f:
    html_text = f.read().decode('euc_jp', errors='replace')

soup = BeautifulSoup(html_text, 'html.parser')
tables = soup.find_all('table', summary=re.compile(r'払戻|払い戻し'))

print("=== 払い戻しテーブル詳細 ===")
for i, table in enumerate(tables):
    print(f"\n--- テーブル {i+1} ---")
    for row in table.find_all('tr'):
        th = row.find('th')
        tds = row.find_all('td')
        th_text = th.get_text(strip=True) if th else ""
        td_texts = [td.get_text(strip=True) for td in tds]
        print(f"  {th_text}: {td_texts}")

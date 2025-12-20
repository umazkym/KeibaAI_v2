"""racesにパースされていない4レースのHTMLを調査"""
from pathlib import Path
from bs4 import BeautifulSoup
import re

html_dir = Path('keibaai/data/raw/html')

missing_race_ids = ['201805010304', '201408020304', '201405010404', '201405010508']

print("=" * 80)
print("racesにパースされていない4レースのHTML調査")
print("=" * 80)

for race_id in missing_race_ids:
    race_html = html_dir / 'race' / f'{race_id}.bin'
    
    print(f"\n--- {race_id} ---")
    print(f"ファイルサイズ: {race_html.stat().st_size:,} bytes")
    
    with open(race_html, 'rb') as f:
        html_bytes = f.read()
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # タイトルを確認
    title = soup.find('title')
    print(f"title: {title.get_text()[:80] if title else 'なし'}")
    
    # race_table_01を確認
    race_table = soup.find('table', class_='race_table_01')
    if race_table:
        rows = race_table.find_all('tr')
        print(f"race_table_01: {len(rows)}行")
    else:
        print("race_table_01: なし ← これが原因!")
    
    # 他のテーブルを探す
    all_tables = soup.find_all('table')
    print(f"全テーブル数: {len(all_tables)}")
    
    # noresultなどのメッセージを探す
    noresult = soup.find('div', class_='noresult')
    if noresult:
        print(f"noresult: {noresult.get_text()[:100]}")
    
    # "中止"や"取消"を探す
    page_text = soup.get_text()
    if '中止' in page_text:
        print("⚠️ '中止' キーワードあり")
    if '取消' in page_text:
        print("⚠️ '取消' キーワードあり")
    if '不成立' in page_text:
        print("⚠️ '不成立' キーワードあり")

"""
エラーが発生したHTMLファイルの詳細調査
"""
from pathlib import Path
from bs4 import BeautifulSoup

file_path = r'C:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\race\201405010404.bin'
with open(file_path, 'rb') as f:
    html_bytes = f.read()

print(f'ファイルサイズ: {len(html_bytes)} bytes')

try:
    html_text = html_bytes.decode('euc_jp', errors='replace')
except:
    html_text = html_bytes.decode('utf-8', errors='replace')

soup = BeautifulSoup(html_text, 'html.parser')

# タイトル確認
title = soup.find('title')
print(f'タイトル: {title.get_text() if title else "なし"}')

# レース結果テーブル確認
table = soup.find('table', class_='race_table_01')
print(f'race_table_01: {"あり" if table else "なし"}')

# エラーメッセージ確認
if 'エラー' in html_text or '404' in html_text:
    print('⚠️ エラーページの可能性')

# HTMLの冒頭200文字
print(f'HTML冒頭: {html_text[:300]}')

# 全テーブル確認
all_tables = soup.find_all('table')
print(f'全テーブル数: {len(all_tables)}')
for i, t in enumerate(all_tables[:5]):
    cls = t.get('class', [])
    summary = t.get('summary', '')
    print(f'  テーブル{i}: class={cls}, summary={summary}')

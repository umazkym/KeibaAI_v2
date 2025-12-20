"""netkeibaのレスポンスを詳細調査"""
import requests
from bs4 import BeautifulSoup

url = 'https://db.netkeiba.com/race/201405010404/'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}
response = requests.get(url, headers=headers, timeout=30)
print(f'Status: {response.status_code}')
print(f'Length: {len(response.content)} bytes')

# euc-jpでデコード
html_text = response.content.decode('euc_jp', errors='replace')

soup = BeautifulSoup(html_text, 'html.parser')

# titleタグを探す
title = soup.find('title')
print(f'titleタグ: {title}')

# h1タグを探す
h1 = soup.find('h1')
print(f'h1タグ: {h1}')

# race_table_01を探す
table = soup.find('table', class_='race_table_01')
print(f'race_table_01: {"あり" if table else "なし"}')

# headタグの中身
head = soup.find('head')
if head:
    print(f'\nhead内のタグ一覧:')
    for tag in head.find_all(True)[:20]:
        print(f'  {tag.name}')

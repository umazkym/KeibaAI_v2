from bs4 import BeautifulSoup
import re

html_path = 'keibaai/data/raw/html/race/202001010101.bin'

with open(html_path, 'rb') as f:
    html_bytes = f.read()

try:
    html_text = html_bytes.decode('euc_jp', errors='replace')
except:
    html_text = html_bytes.decode('utf-8', errors='replace')

soup = BeautifulSoup(html_text, 'html.parser')

# レース結果テーブルを探す
result_table = soup.find('table', class_='race_table_01')

if not result_table:
    print("race_table_01が見つかりません")
    # 他のテーブルを探す
    tables = soup.find_all('table')
    print(f"\n全テーブル: {len(tables)}個")
    for i, table in enumerate(tables):
        print(f"\nTable {i}: class={table.get('class')}")
else:
    print("race_table_01が見つかりました\n")
    
    # ヘッダー行を確認
    thead = result_table.find('thead') or result_table
    headers = thead.find_all('th')
    print(f"ヘッダー数: {len(headers)}")
    for i, th in enumerate(headers):
        print(f"  {i}: {th.get_text(strip=True)}")
    
    # 最初のデータ行を確認（1着馬）
    tbody = result_table.find('tbody') if result_table.find('tbody') else result_table
    first_row = tbody.find('tr')
    
    if first_row:
        cells = first_row.find_all('td')
        print(f"\n最初の行のセル数: {len(cells)}")
        for i, td in enumerate(cells):
            text = td.get_text(strip=True)
            if len(text) > 50:
                text = text[:50] + "..."
            print(f"  Cell {i}: {text}")
        
        # 賞金がありそうなセルを特定（インデックス17付近）
        if len(cells) > 17:
            print(f"\nCell 15: {cells[15].get_text(strip=True)}")  # 調教師
            print(f"Cell 16: {cells[16].get_text(strip=True)}")  # 馬主？
            print(f"Cell 17: {cells[17].get_text(strip=True)}")  # 賞金？
            if len(cells) > 18:
                print(f"Cell 18: {cells[18].get_text(strip=True)}")

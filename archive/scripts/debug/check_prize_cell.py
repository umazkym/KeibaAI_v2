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
result_table = soup.find('table', class_='race_table_01')

if result_table:
    tbody = result_table.find('tbody') if result_table.find('tbody') else result_table
    rows = tbody.find_all('tr')
    
    print(f"総行数: {len(rows)}")
    
    # 最初の有効な行（tdがある行）を探す
    for i, row in enumerate(rows[:5]):
        cells = row.find_all('td')
        if cells:
            print(f"\n行 {i}: {len(cells)}個のセル")
            
            # 賞金があるか確認（インデックス20）
            if len(cells) > 20:
                prize_cell = cells[20]
                prize_text = prize_cell.get_text(strip=True)
                print(f"  賞金セル (index 20): '{prize_text}'")
                
                # 他の重要なセルも確認
                print(f"  着順 (index 0): '{cells[0].get_text(strip=True)}'")
                print(f"  馬名 (index 3): '{cells[3].get_text(strip=True)}'")
                print(f"  調教師 (index 18): '{cells[18].get_text(strip=True)}'")
                print(f"  馬主 (index 19): '{cells[19].get_text(strip=True)}'")
            else:
                print(f"  セル数が不足: {len(cells)}")
                # 各セルの内容を表示
                for j, cell in enumerate(cells):
                    text = cell.get_text(strip=True)[:30]
                    print(f"    Cell {j}: {text}")

"""
セル数を確認してデバッグ
"""
from bs4 import BeautifulSoup

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
    
    for i, row in enumerate(rows[:3]):
        cells = row.find_all('td')
        if cells:
            print(f"\n行 {i}: {len(cells)}個のセル")
            
            # 18, 19, 20の内容を確認
            if len(cells) >= 21:
                print(f"  ✓ len(cells) >= 21")
                print(f"  cells[18] (trainer): {cells[18].get_text(strip=True)[:50]}")
                print(f"  cells[19] (owner): {cells[19].get_text(strip=True)[:50]}")
                print(f"  cells[20] (prize): {cells[20].get_text(strip=True)}")
            elif len(cells) >= 18:
                print(f"  ✗ len(cells) = {len(cells)} < 21")
                print(f"  cells[15]: {cells[15].get_text(strip=True)[:50]}")
                print(f"  cells[16]: {cells[16].get_text(strip=True)[:50]}")
                print(f"  cells[17]: {cells[17].get_text(strip=True)[:50]}")
            else:
                print(f"  ✗ len(cells) = {len(cells)} < 18")

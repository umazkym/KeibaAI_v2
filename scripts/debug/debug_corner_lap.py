#!/usr/bin/env python3
"""
コーナー通過順位・ラップタイムのデバッグスクリプト
"""
from pathlib import Path
from bs4 import BeautifulSoup
import re

def debug_race_data(file_path: str):
    """HTMLからコーナー・ラップデータの構造を確認"""
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    html_text = html_bytes.decode('euc_jp', errors='replace')
    soup = BeautifulSoup(html_text, 'html.parser')
    
    race_id = Path(file_path).stem
    print("=" * 60)
    print(f"レースID: {race_id}")
    print("=" * 60)
    
    # === コーナー通過順位 ===
    print("\n【コーナー通過順位】")
    corner_table = soup.find('table', summary='コーナー通過順位')
    if corner_table:
        for row in corner_table.find_all('tr'):
            th = row.find('th')
            td = row.find('td')
            if th and td:
                corner_name = th.get_text(strip=True)
                order_text = td.get_text(strip=True)
                print(f"  {corner_name}: {order_text[:50]}...")
    else:
        print("  NOT FOUND")
    
    # === ラップタイム ===
    print("\n【ラップタイム】")
    lap_table = soup.find('table', summary='ラップタイム')
    if lap_table:
        rows = lap_table.find_all('tr')
        for i, row in enumerate(rows):
            ths = row.find_all('th')
            tds = row.find_all('td')
            if ths:
                # ヘッダー行
                headers = [th.get_text(strip=True) for th in ths]
                print(f"  距離: {headers}")
            else:
                # データ行
                data = [td.get_text(strip=True) for td in tds]
                label = "累計タイム" if i == 1 else "区間タイム"
                print(f"  {label}: {data}")
    else:
        print("  NOT FOUND")
    
    # === ペース ===
    print("\n【ペース】")
    pace_div = soup.find('div', class_='RapPace_Title')
    if pace_div:
        pace_span = pace_div.find('span')
        if pace_span:
            print(f"  ペース: {pace_span.get_text(strip=True)}")
    else:
        # 別のパターンを探す
        pace_text = soup.find(string=re.compile(r'ペース'))
        if pace_text:
            print(f"  ペーステキスト: {pace_text[:30]}")
        else:
            print("  NOT FOUND")

if __name__ == "__main__":
    import sys
    test_file = "keibaai/data/raw/html/race/202001010101.bin"
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
    
    if Path(test_file).exists():
        debug_race_data(test_file)
    else:
        print(f"ファイルが見つかりません: {test_file}")

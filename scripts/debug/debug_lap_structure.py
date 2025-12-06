#!/usr/bin/env python3
"""ラップタイムのHTML構造詳細確認"""
from pathlib import Path
from bs4 import BeautifulSoup
import re

def check_lap_structure(file_path: str):
    with open(file_path, 'rb') as f:
        html_text = f.read().decode('euc_jp', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    race_id = Path(file_path).stem
    
    print(f"\n{'='*60}")
    print(f"レース: {race_id}")
    print(f"{'='*60}")
    
    # ペース評価を探す
    print("\n【ペース評価】")
    pace_div = soup.find('div', class_='RapPace_Title')
    if pace_div:
        print(f"  div.RapPace_Title: {pace_div.get_text(strip=True)}")
        span = pace_div.find('span')
        if span:
            print(f"  ペースラベル: {span.get_text(strip=True)}")
    else:
        # 別の方法で探す
        pace_text = soup.find(string=re.compile(r'ペース'))
        if pace_text:
            print(f"  ペーステキスト: {pace_text.strip()[:50]}")
    
    # ラップテーブルを探す
    print("\n【ラップテーブル】")
    lap_table = soup.find('table', summary='ラップタイム')
    if lap_table:
        rows = lap_table.find_all('tr')
        for i, row in enumerate(rows):
            ths = row.find_all('th')
            tds = row.find_all('td')
            row_class = row.get('class', [])
            
            if ths:
                # ヘッダー行（距離）
                headers = [th.get_text(strip=True) for th in ths]
                print(f"  Row {i} (class={row_class}): HEADERS = {headers}")
            elif tds:
                # データ行
                data = [td.get_text(strip=True) for td in tds[:5]]
                print(f"  Row {i} (class={row_class}): DATA = {data}...")
    else:
        print("  ラップテーブル見つからず")


def main():
    base_dir = Path("keibaai/data/raw/html/race")
    
    print("ラップタイムHTML構造確認")
    
    # 最初の3ファイルをチェック
    for i in [1, 2, 3]:
        file_name = f"2020010101{i:02d}.bin"
        file_path = base_dir / file_name
        if file_path.exists():
            check_lap_structure(str(file_path))


if __name__ == "__main__":
    main()

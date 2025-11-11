#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
shutuba HTMLの各セルを詳細確認
"""

import sys
import re
from pathlib import Path
from bs4 import BeautifulSoup
import io

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

shutuba_file = Path(r"C:\Users\zk-ht\Keiba\test\keibaai\data\raw\html\shutuba\shutuba_202305020811_20251111T102915+0900_sha256=0bd7bfca.html")

with open(shutuba_file, 'rb') as f:
    html_bytes = f.read()

try:
    html_text = html_bytes.decode('euc_jp', errors='replace')
except:
    html_text = html_bytes.decode('utf-8', errors='replace')

soup = BeautifulSoup(html_text, 'lxml')

shutuba_table = soup.find('table', {'class': re.compile(r'.*Shutuba.*', re.I)})

if shutuba_table:
    rows = shutuba_table.find_all('tr')

    print(f"Total rows: {len(rows)}")
    print("\n" + "=" * 100)

    # 各行を詳しく表示
    for row_idx, row in enumerate(rows[:5]):
        cells = row.find_all(['th', 'td'])
        print(f"\nRow {row_idx} ({len(cells)} cells):")

        for cell_idx, cell in enumerate(cells):
            text = cell.get_text(strip=True)[:80]

            # 内部の要素を確認
            links = cell.find_all('a')
            inputs = cell.find_all('input')
            divs = cell.find_all('div')
            spans = cell.find_all('span')

            details = []
            if links:
                details.append(f"{len(links)}link")
            if inputs:
                details.append(f"{len(inputs)}input")
            if divs:
                details.append(f"{len(divs)}div")
            if spans:
                details.append(f"{len(spans)}span")

            detail_str = f" ({', '.join(details)})" if details else ""
            print(f"  [{cell_idx}] {text}{detail_str}")

            # リンク内容
            if links:
                for link in links[:2]:
                    href = link.get('href', '')
                    link_text = link.get_text(strip=True)[:40]
                    print(f"       -> href='{href[:60]}' text='{link_text}'")

            # input要素（チェックボックスなど）
            if inputs and row_idx > 1:  # ヘッダーではなくデータ行
                for inp in inputs[:1]:
                    input_type = inp.get('type', '')
                    input_name = inp.get('name', '')
                    input_value = inp.get('value', '')
                    print(f"       -> <input type='{input_type}' name='{input_name[:40]}' value='{input_value[:40]}'/>")

print("\n" + "=" * 100)
print("\n最初のデータ行（Row 2）の全セル内容を表示:")
print("=" * 100)

if len(rows) > 2:
    data_row = rows[2]
    cells = data_row.find_all(['th', 'td'])

    print(f"\n総セル数: {len(cells)}")
    print("\nセル内容詳細:")

    for cell_idx, cell in enumerate(cells):
        print(f"\n[Cell {cell_idx}]")
        print(f"  HTML: {str(cell)[:200]}")
        print(f"  Text: {cell.get_text(strip=True)[:100]}")

        # 子要素の詳細
        children = list(cell.children)
        print(f"  子要素数: {len(children)}")

        for child_idx, child in enumerate(children[:3]):
            if isinstance(child, str):
                if child.strip():
                    print(f"    Child[{child_idx}] (str): {child.strip()[:60]}")
            else:
                print(f"    Child[{child_idx}] ({child.name}): {child.get_text(strip=True)[:60]}")

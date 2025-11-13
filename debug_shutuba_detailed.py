#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
shutuba HTMLの詳細構造を調査
"""

import sys
import re
from pathlib import Path
from bs4 import BeautifulSoup
import io

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# テスト対象のHTMLファイル
shutuba_file = Path(r"C:\Users\zk-ht\Keiba\test\keibaai\data\raw\html\shutuba\shutuba_202305020811_20251111T102915+0900_sha256=0bd7bfca.html")

print("=" * 80)
print(f"shutuba HTML 詳細検査: {shutuba_file.name}")
print("=" * 80)

# HTMLを読み込み
with open(shutuba_file, 'rb') as f:
    html_bytes = f.read()

try:
    html_text = html_bytes.decode('euc_jp', errors='replace')
except:
    html_text = html_bytes.decode('utf-8', errors='replace')

soup = BeautifulSoup(html_text, 'lxml')

# すべてのテーブルを検出
tables = soup.find_all('table')
print(f"\n検出されたテーブル数: {len(tables)}")

for i, table in enumerate(tables):
    print(f"\n[Table {i}]")

    # テーブルクラスを確認
    table_class = table.get('class')
    print(f"  Class: {table_class}")

    # テーブルの行数を確認
    rows = table.find_all('tr')
    print(f"  行数: {len(rows)}")

    # 最初の3行を詳細表示
    if len(rows) > 0:
        print(f"  最初の行の構成:")
        for row_idx, row in enumerate(rows[:3]):
            cells = row.find_all(['th', 'td'])
            print(f"    Row {row_idx}: {len(cells)}個のセル")

            # セル内容を表示
            for cell_idx, cell in enumerate(cells[:5]):
                text = cell.get_text(strip=True)[:50]
                print(f"      Cell[{cell_idx}]: {text}")

# 出馬表テーブルを特定
print("\n" + "=" * 80)
print("出馬表テーブル検査")
print("=" * 80)

# 'shutuba_table'や類似のテーブルを探す
shutuba_table = soup.find('table', {'class': re.compile(r'.*shutuba.*', re.I)})

if not shutuba_table:
    # 他のパターンで探す
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) > 10:  # 出馬表なら複数の馬がいるはず
            first_row = rows[0]
            cells = first_row.find_all(['th', 'td'])
            # ヘッダーに「馬名」や「騎手」があるか確認
            header_text = ' '.join([cell.get_text() for cell in cells])
            if '馬名' in header_text or '騎手' in header_text or '枠' in header_text:
                shutuba_table = table
                break

if shutuba_table:
    print("[OK] 出馬表テーブルを検出")
    rows = shutuba_table.find_all('tr')
    print(f"  行数: {len(rows)}")

    # ヘッダー行を表示
    if len(rows) > 0:
        header_row = rows[0]
        header_cells = header_row.find_all(['th', 'td'])
        print(f"  ヘッダー({len(header_cells)}列):")
        for i, cell in enumerate(header_cells):
            print(f"    [{i}] {cell.get_text(strip=True)}")

        # データ行を表示
        print(f"\n  最初のデータ行:")
        if len(rows) > 1:
            data_row = rows[1]
            data_cells = data_row.find_all(['th', 'td'])
            print(f"  セル数: {len(data_cells)}")
            for i, cell in enumerate(data_cells):
                text = cell.get_text(strip=True)[:60]
                links = cell.find_all('a')
                link_info = f" (リンク{len(links)}個)" if links else ""
                print(f"    [{i}] {text}{link_info}")

                # 内部のaタグを詳細表示
                for link in links[:2]:
                    href = link.get('href', '')
                    link_text = link.get_text(strip=True)[:40]
                    print(f"         -> <a href='{href}'>{link_text}</a>")
else:
    print("ERROR: 出馬表テーブルが見つかりません")
    print("\n全テーブルの詳細:")
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) > 0:
            first_row = rows[0]
            cells = first_row.find_all(['th', 'td'])
            text_preview = ' '.join([cell.get_text()[:20] for cell in cells[:3]])
            print(f"  テーブル({len(rows)}行, {len(cells)}列): {text_preview}")

#!/usr/bin/env python3
"""
results_parser.py のデバッグスクリプト
実際のHTMLファイルで各セルの内容を確認
"""
import sys
from pathlib import Path
from bs4 import BeautifulSoup
import re

# プロジェクトルートを keibaai フォルダとして設定
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

# テスト対象のHTMLファイル
# (ファイルパスは keibaai フォルダからの相対パスを想定)
html_file = project_root / "data\raw\html\race\race_2023010701_20251111T102442+0900_sha256=9d291bca.html"

if not html_file.exists():
    print(f"ERROR: テスト用HTMLファイルが見つかりません: {html_file}")
    print("スクレイピングを一度実行するか、正しいパスに変更してください。")
    sys.exit(1)

with open(html_file, 'rb') as f:
    html_bytes = f.read()

try:
    html_text = html_bytes.decode('euc_jp', errors='replace')
except UnicodeDecodeError:
    try:
        html_text = html_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        print(f"HTMLのデコードに失敗しました: {e}")
        sys.exit(1)


soup = BeautifulSoup(html_text, 'lxml')

result_table = soup.find('table', class_='race_table_01')

if not result_table:
    print("ERROR: race_table_01 が見つかりません")
    sys.exit(1)

print("=" * 80)
print(f"race_table_01 の構造分析: {html_file.name}")
print("=" * 80)

tbody = result_table.find('tbody')
search_area = tbody if tbody else result_table
rows = search_area.find_all('tr')

print(f"\n総行数: {len(rows)}")

# 最初のデータ行（ヘッダーを除く）を詳細分析
# (データ行は通常 1行目から。0行目はヘッダーの場合がある)
if len(rows) < 2:
    print("データ行が見つかりません。")
    sys.exit(1)

# 1行目 (インデックス 0) がヘッダー(th)かデータ(td)か確認
if rows[0].find('th'):
    print("0行目はヘッダー行(th)と判断しました。1行目から分析します。")
    data_rows_to_analyze = rows[1:4] # 1, 2, 3行目を分析
else:
    print("0行目からデータ行(td)と判断しました。0行目から分析します。")
    data_rows_to_analyze = rows[:3] # 0, 1, 2行目を分析


for i, row in enumerate(data_rows_to_analyze):
    cells = row.find_all('td')
    print(f"\n--- Row {i} (分析対象 {i+1}行目) --- ({len(cells)} cells) ---")
    
    if len(cells) == 0:
        print("  この行には <td> がありません。")
        continue

    for j, cell in enumerate(cells):
        text = cell.get_text(strip=True)[:60] # 表示文字数を増加
        
        # リンクの有無と内容
        links = cell.find_all('a')
        link_info = ""
        if links:
            for link in links:
                href = link.get('href', '')
                if '/trainer/' in href:
                    link_info += f" [trainer: {href}]"
                elif '/jockey/' in href:
                    link_info += f" [jockey: {href}]"
                elif '/horse/' in href:
                    link_info += f" [horse: {href}]"
                elif '/race/' in href:
                     link_info += f" [race: {href}]"

        print(f"  Cell[{j:2d}]: {text}{link_info}")

print("=" * 80)
print("デバッグ完了")
print("Cell[10] (賞金), Cell[15] (調教師), Cell[16] (馬主) 付近の内容を確認してください。")
print("もしインデックスがズレている場合、(例: 賞金が Cell[11] にあるなど)、その番号を報告してください。")
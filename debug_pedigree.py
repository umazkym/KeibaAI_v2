#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pedigree_parser.pyのデバッグスクリプト
実際のHTMLファイルに対してテストを実施
"""

import sys
import re
from pathlib import Path
from bs4 import BeautifulSoup

# プロジェクトルートをsys.pathに追加
sys.path.insert(0, r"C:\Users\zk-ht\Keiba\test\keibaai\src")

from modules.parsers import pedigree_parser

# テスト対象のHTMLファイルリスト
ped_files = list(Path(r"C:\Users\zk-ht\Keiba\test\keibaai\data\raw\html\ped").glob("ped_*.html"))

print("=" * 80)
print("pedigree_parser.py デバッグ")
print("=" * 80)

for ped_file in ped_files[:3]:  # 最初の3ファイルだけテスト
    print(f"\n\n### テスト対象: {ped_file.name}")
    print("-" * 80)

    # HTMLを読み込み
    with open(ped_file, 'rb') as f:
        html_bytes = f.read()

    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_text, 'lxml')

    # blood_tableを探す
    blood_table = soup.find('table', class_='blood_table')

    if not blood_table:
        print("ERROR: blood_tableが見つかりません")
        continue

    print(f"[OK] blood_table を見つけました")

    # 馬リンクを探す
    ancestor_links = blood_table.find_all('a', href=re.compile(r'/horse/\d+'))

    print(f"[OK] 血統リンク数: {len(ancestor_links)}")

    if len(ancestor_links) == 0:
        print("WARNING: 血統リンクが見つかりません")
        # テーブルの内容を確認
        print("\n血統テーブルの構造:")
        print(blood_table.prettify()[:500])
    else:
        print(f"\n検出された祖先:")
        for i, link in enumerate(ancestor_links[:10]):
            href = link.get('href')
            match = re.search(r'/horse/(\d+)', href)
            if match:
                ancestor_id = match.group(1)
                ancestor_name = link.get_text(strip=True)
                print(f"  {i+1}. ID={ancestor_id}, Name={ancestor_name}")

    # pedigree_parser.py を実行
    print(f"\nパーサー実行結果:")
    df = pedigree_parser.parse_pedigree_html(str(ped_file))
    print(f"  解析行数: {len(df)}")
    if len(df) > 0:
        print(f"  horse_id: {df['horse_id'].iloc[0]}")
        print(f"\n詳細データ (最初の5行):")
        print(df.head().to_string())

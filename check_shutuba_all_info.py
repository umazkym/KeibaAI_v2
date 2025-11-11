#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
shutuba HTMLのすべての情報を検索
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
    html_text = f.read().decode('euc_jp', errors='replace')

soup = BeautifulSoup(html_text, 'lxml')

print("=" * 80)
print("shutuba HTML に含まれる情報検索")
print("=" * 80)

# キーワード検索
keywords = [
    ('獲得賞金', '賞金'),
    ('前日オッズ', 'オッズ'),
    ('人気', '前日人気'),
    ('騎手成績', 'career'),
    ('戦績', '成績'),
    ('馬主', 'owner'),
    ('最近5走', '5走'),
    ('調子', 'odds'),
]

for keyword, alt_keyword in keywords:
    # テキスト内検索
    if keyword in html_text or alt_keyword in html_text:
        count = html_text.count(keyword) + html_text.count(alt_keyword)
        print(f"\n[検出] {keyword}/{alt_keyword}: {count}回")

print("\n" + "=" * 80)
print("HTMLのセクション構造")
print("=" * 80)

# main, article, divなど主要セクションを探す
main_sections = soup.find_all(['main', 'article', 'section'])
print(f"メインセクション数: {len(main_sections)}")

# すべてのdivを一覧
divs_with_id = soup.find_all('div', id=True)
divs_with_class = soup.find_all('div', class_=True)

print(f"\nID付きdiv: {len(divs_with_id)}")
print("IDの例:")
for div in divs_with_id[:10]:
    div_id = div.get('id')
    classes = ' '.join(div.get('class', []))
    print(f"  id='{div_id}' class='{classes}'")

print(f"\nclass付きdiv: {len(divs_with_class)}")
print("classの例:")
for div in divs_with_class[:10]:
    classes = ' '.join(div.get('class', []))
    text_preview = div.get_text(strip=True)[:50]
    print(f"  class='{classes}' text='{text_preview}'")

print("\n" + "=" * 80)
print("JavaScript/データ埋め込み検索")
print("=" * 80)

# scriptタグを探す
scripts = soup.find_all('script')
print(f"scriptタグ数: {len(scripts)}")

for i, script in enumerate(scripts):
    text = script.get_text()
    if 'horse' in text.lower() or 'race' in text.lower() or 'data' in text.lower():
        print(f"\n[Script {i}]")
        print(f"  長さ: {len(text)}文字")
        if 'var' in text:
            print("  変数定義を含む")
        if '{' in text:
            print("  JSON/オブジェクト形式を含む")
        # 最初の200文字を表示
        preview = text[:200].replace('\n', ' ')
        print(f"  プレビュー: {preview}")

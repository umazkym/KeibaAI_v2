#!/usr/bin/env python3
"""
デバッグスクリプト05: 馬プロフィールHTMLの構造確認
血統情報が取得できない原因を特定するため、HTMLの構造を直接確認
"""

from pathlib import Path
from bs4 import BeautifulSoup
import re

def analyze_horse_profile_html(file_path: Path):
    """馬プロフィールHTMLを詳細分析"""
    print("=" * 80)
    print(f"📄 ファイル: {file_path.name}")
    print("=" * 80)

    with open(file_path, 'rb') as f:
        html_bytes = f.read()

    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_text, 'html.parser')

    # 1. 馬名の確認
    print("\n📌 1. 馬名の確認")
    horse_title = soup.find('div', class_='horse_title')
    if horse_title:
        h1 = horse_title.find('h1')
        if h1:
            print(f"   ✅ 馬名: {h1.get_text(strip=True)}")
        else:
            print(f"   ⚠️ h1タグが見つかりません")
    else:
        print(f"   ❌ horse_title が見つかりません")

    # 2. プロフィールテーブルの確認
    print("\n📌 2. プロフィールテーブルの確認")
    profile_table = soup.find('table', class_='db_prof_table')
    if profile_table:
        print(f"   ✅ db_prof_table が見つかりました")
        rows = profile_table.find_all('tr')
        print(f"   行数: {len(rows)}")

        for i, row in enumerate(rows[:10], 1):  # 最初の10行
            th = row.find('th')
            td = row.find('td')
            if th and td:
                label = th.get_text(strip=True)
                value = td.get_text(strip=True)[:50]  # 最初の50文字
                print(f"     {i:>2}. {label:15s}: {value}")
    else:
        print(f"   ❌ db_prof_table が見つかりません")

    # 3. 血統テーブルの確認（重要）
    print("\n📌 3. 血統テーブルの確認（blood_table）")
    blood_table = soup.find('table', class_='blood_table')
    if blood_table:
        print(f"   ✅ blood_table が見つかりました！")
        rows = blood_table.find_all('tr')
        print(f"   行数: {len(rows)}")

        for i, row in enumerate(rows, 1):
            print(f"\n   --- 行 {i} ---")
            td = row.find('td')
            if td:
                # リンクを探す
                links = td.find_all('a', href=re.compile(r'/horse/'))
                print(f"     リンク数: {len(links)}")
                for j, link in enumerate(links, 1):
                    horse_name = link.get_text(strip=True)
                    href = link.get('href', '')
                    horse_id_match = re.search(r'/horse/(\w+)', href)
                    horse_id = horse_id_match.group(1) if horse_id_match else 'N/A'
                    print(f"       {j}. 名前: {horse_name:20s} | ID: {horse_id}")
            else:
                print(f"     tdタグなし")
    else:
        print(f"   ❌ blood_table が見つかりません")

        # 代替パターンを探す
        print(f"\n   💡 代替パターンを探索...")

        # パターン1: 血統情報を含む他のテーブル
        all_tables = soup.find_all('table')
        print(f"   全テーブル数: {len(all_tables)}")

        for i, table in enumerate(all_tables, 1):
            table_class = table.get('class', [])
            table_id = table.get('id', '')
            print(f"     {i}. class={table_class}, id={table_id}")

            # '血統' というテキストを含むか確認
            if '血統' in table.get_text() or 'pedigree' in str(table_class).lower():
                print(f"       → 血統情報の可能性あり！")

                # このテーブルの構造を詳しく見る
                rows = table.find_all('tr')[:3]  # 最初の3行
                for j, row in enumerate(rows, 1):
                    cells = row.find_all(['th', 'td'])
                    cell_texts = [cell.get_text(strip=True)[:30] for cell in cells]
                    print(f"         行{j}: {cell_texts}")

    # 4. 血統情報を含むセクションを全探索
    print("\n📌 4. 「父」「母」を含むテキストの探索")
    text_content = soup.get_text()

    if '父' in text_content:
        print(f"   ✅ '父' という文字列が存在します")
        # 父の周辺テキストを抽出
        idx = text_content.find('父')
        surrounding = text_content[max(0, idx-20):min(len(text_content), idx+100)]
        print(f"   周辺テキスト: {surrounding.strip()[:200]}")
    else:
        print(f"   ⚠️ '父' という文字列が見つかりません")

    if '母' in text_content:
        print(f"   ✅ '母' という文字列が存在します")
        # 母の周辺テキストを抽出
        idx = text_content.find('母')
        surrounding = text_content[max(0, idx-20):min(len(text_content), idx+100)]
        print(f"   周辺テキスト: {surrounding.strip()[:200]}")
    else:
        print(f"   ⚠️ '母' という文字列が見つかりません")

    # 5. すべてのリンクから /horse/ を含むものを抽出
    print("\n📌 5. すべての /horse/ リンクを抽出")
    all_horse_links = soup.find_all('a', href=re.compile(r'/horse/'))
    print(f"   /horse/ リンク数: {len(all_horse_links)}")

    if len(all_horse_links) > 0:
        print(f"   最初の10件:")
        for i, link in enumerate(all_horse_links[:10], 1):
            horse_name = link.get_text(strip=True)
            href = link.get('href', '')
            horse_id_match = re.search(r'/horse/(\w+)', href)
            horse_id = horse_id_match.group(1) if horse_id_match else 'N/A'

            # 親要素のタグとクラスを確認
            parent = link.parent
            parent_tag = parent.name if parent else 'N/A'
            parent_class = parent.get('class', []) if parent else []

            print(f"     {i:>2}. {horse_name:20s} | ID: {horse_id:15s} | 親: <{parent_tag}> {parent_class}")

def main():
    """メイン処理"""
    print("\n")
    print("🔍 KeibaAI_v2 馬プロフィールHTML構造分析")
    print("\n")

    # サンプルファイルを3件分析
    raw_horse_dir = Path("keibaai/data/raw/html/horse")
    profile_files = sorted([f for f in raw_horse_dir.glob("*_profile.bin")])[:3]

    if not profile_files:
        print("❌ プロフィールファイルが見つかりませんでした")
        return

    for file in profile_files:
        analyze_horse_profile_html(file)
        print("\n")

    print("=" * 80)
    print("📋 分析完了")
    print("=" * 80)
    print("\n💡 次のアクション:")
    print("  1. blood_tableが見つからない場合 → 別のHTML構造を確認")
    print("  2. 代替パターンが見つかった場合 → パーサーを修正")
    print("\n")

if __name__ == "__main__":
    main()

"""
出馬表HTMLの構造を詳細に調査するデバッグスクリプト
morning_odds/morning_popularityが取得できない原因を特定する
"""

import sys
from pathlib import Path
from bs4 import BeautifulSoup

def analyze_shutuba_html(file_path):
    """
    出馬表HTMLの構造を詳細に分析
    """
    print("=" * 80)
    print(f"🔍 HTMLファイル構造分析: {Path(file_path).name}")
    print("=" * 80)
    print()

    # HTMLファイルを読み込み
    with open(file_path, 'rb') as f:
        html_bytes = f.read()

    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_text, 'html.parser')

    # --- 出馬表テーブルの存在確認 ---
    shutuba_table = soup.find('table', class_='Shutuba_Table')

    if not shutuba_table:
        print("❌ Shutuba_Table が見つかりません")
        return

    print("✅ Shutuba_Table が見つかりました")
    print()

    # --- HorseListの行を取得 ---
    horse_rows = shutuba_table.find_all('tr', class_='HorseList')
    print(f"📊 HorseList行数: {len(horse_rows)}")
    print()

    if not horse_rows:
        print("❌ HorseList行が見つかりません")
        return

    # --- 最初の1行を詳細分析 ---
    first_row = horse_rows[0]
    print("🔬 最初の1頭のHTML構造を分析:")
    print("-" * 80)

    # すべての<td>を取得
    cells = first_row.find_all('td')
    print(f"総セル数: {len(cells)}")
    print()

    # 各セルの内容とclass属性を表示
    for i, cell in enumerate(cells):
        cell_classes = cell.get('class', [])
        cell_text = cell.get_text(strip=True)[:50]  # 最初の50文字

        print(f"[セル {i}]")
        print(f"  class属性: {cell_classes}")
        print(f"  テキスト: {cell_text}")

        # spanタグの有無も確認
        spans = cell.find_all('span')
        if spans:
            print(f"  <span>の数: {len(spans)}")
            for j, span in enumerate(spans):
                span_id = span.get('id', '')
                span_text = span.get_text(strip=True)
                print(f"    span[{j}] id='{span_id}' text='{span_text}'")

        print()

    print("-" * 80)
    print()

    # --- オッズ・人気に関連しそうなセルを検索 ---
    print("🔍 オッズ・人気に関連しそうなセルを検索:")
    print()

    # "Popular"を含むclass
    popular_cells = first_row.find_all('td', class_=lambda x: x and 'Popular' in ' '.join(x) if x else False)
    print(f"class中に'Popular'を含むセル: {len(popular_cells)}個")
    for i, cell in enumerate(popular_cells):
        print(f"  [{i}] class={cell.get('class')} text='{cell.get_text(strip=True)}'")
    print()

    # "Ninki"を含むclass
    ninki_cells = first_row.find_all('td', class_=lambda x: x and 'Ninki' in ' '.join(x) if x else False)
    print(f"class中に'Ninki'を含むセル: {len(ninki_cells)}個")
    for i, cell in enumerate(ninki_cells):
        print(f"  [{i}] class={cell.get('class')} text='{cell.get_text(strip=True)}'")
    print()

    # "Txt_R"を含むclass
    txt_r_cells = first_row.find_all('td', class_=lambda x: x and 'Txt_R' in ' '.join(x) if x else False)
    print(f"class中に'Txt_R'を含むセル: {len(txt_r_cells)}個")
    for i, cell in enumerate(txt_r_cells):
        print(f"  [{i}] class={cell.get('class')} text='{cell.get_text(strip=True)}'")
    print()

    # id属性に"odds"を含むspan
    odds_spans = first_row.find_all('span', id=lambda x: x and 'odds' in x if x else False)
    print(f"id中に'odds'を含む<span>: {len(odds_spans)}個")
    for i, span in enumerate(odds_spans):
        print(f"  [{i}] id='{span.get('id')}' text='{span.get_text(strip=True)}'")
    print()

    # id属性に"ninki"を含むspan
    ninki_spans = first_row.find_all('span', id=lambda x: x and 'ninki' in x if x else False)
    print(f"id中に'ninki'を含む<span>: {len(ninki_spans)}個")
    for i, span in enumerate(ninki_spans):
        print(f"  [{i}] id='{span.get('id')}' text='{span.get_text(strip=True)}'")
    print()

    print("=" * 80)
    print("💡 結果:")
    print("=" * 80)

    if not popular_cells and not ninki_cells and not odds_spans and not ninki_spans:
        print("❌ オッズ・人気に関連する要素が見つかりませんでした")
        print()
        print("💭 考えられる原因:")
        print("   1. 2020年のHTMLにはオッズ情報が含まれていない")
        print("   2. HTMLの構造が2025年版と大きく異なる")
        print("   3. 出馬表ページではなく、結果ページの可能性")
        print()
        print("📝 推奨アクション:")
        print("   → より新しい年代（2024-2025年）のHTMLファイルでテストする")
        print("   → または、このHTMLファイルの全体構造を確認する")
    else:
        print("✅ オッズ・人気に関連する要素が見つかりました")
        print("   → 上記の詳細情報を元にパーサーを調整してください")

    print("=" * 80)

def main():
    print()
    print("🔧 出馬表HTML構造デバッグツール")
    print()

    html_dir = Path(r'keibaai\data\raw\html\shutuba')

    if not html_dir.exists():
        print(f"❌ エラー: {html_dir} が存在しません")
        return

    # 2020年のファイル1件
    bin_files_2020 = sorted([f for f in html_dir.glob('2020*.bin')])[:1]

    # 2024-2025年のファイル1件
    bin_files_recent = sorted([f for f in html_dir.glob('202[45]*.bin')])[:1]

    files_to_analyze = bin_files_2020 + bin_files_recent

    if not files_to_analyze:
        print("❌ 分析対象のファイルが見つかりません")
        return

    for file_path in files_to_analyze:
        analyze_shutuba_html(file_path)
        print()
        print()

if __name__ == '__main__':
    main()

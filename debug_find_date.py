#!/usr/bin/env python3
"""
HTMLファイルから日付情報を探索するためのデバッグスクリプト

実行方法:
1. このスクリプトをプロジェクトのルート（`keibaai/` の親ディレクトリ）に配置する。
2. `python debug_find_date.py` を実行する。
3. 出力結果をすべてコピーして私に返信してください。
"""
import sys
import re
from pathlib import Path
from bs4 import BeautifulSoup

# ANSIカラーコード
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def find_date_in_html(file_path: Path):
    """
    指定されたHTMLファイルから日付情報を探索する
    """
    
    print(f"\n{bcolors.HEADER}===== HTML日付探索: {file_path.name} ====={bcolors.ENDC}")
    
    if not file_path.exists():
        print(f"{bcolors.FAIL}エラー: HTMLファイルが見つかりません: {file_path}{bcolors.ENDC}")
        return

    try:
        with open(file_path, 'rb') as f:
            html_bytes = f.read()
        
        try:
            html_text = html_bytes.decode('euc_jp', errors='replace')
        except UnicodeDecodeError:
            html_text = html_bytes.decode('utf-8', errors='replace')
        
        soup = BeautifulSoup(html_text, 'lxml')

        # --- 1. 現在のパーサーが試行するセレクタ（失敗するはず） ---
        print(f"\n{bcolors.OKCYAN}[1. 現在のセレクタでの日付検索]{bcolors.ENDC}")
        
        # パターン1: p.RaceData01
        race_data = soup.find('p', class_='RaceData01')
        if race_data:
            print(f"{bcolors.OKGREEN}  ✓ p.RaceData01 が見つかりました: {bcolors.ENDC}{race_data.get_text(strip=True)}")
        else:
            print(f"{bcolors.WARNING}  - p.RaceData01 は見つかりません。{bcolors.ENDC}")

        # パターン2: dd.Active
        active_dd = soup.find('dd', class_='Active')
        if active_dd:
            print(f"{bcolors.OKGREEN}  ✓ dd.Active が見つかりました: {bcolors.ENDC}{active_dd.get_text(strip=True)}")
        else:
            print(f"{bcolors.WARNING}  - dd.Active は見つかりません。{bcolors.ENDC}")

        # --- 2. 代替候補の探索 (日付が含まれていそうなタグ) ---
        print(f"\n{bcolors.OKCYAN}[2. 代替候補のタグを探索します (「年」「月」「日」を含む)]{bcolors.ENDC}")
        
        date_pattern = re.compile(r'\d{4}年\d{1,2}月\d{1,2}日')
        found_tags = []

        # 汎用的なタグを探索
        for tag_name in ['p', 'div', 'span', 'dd', 'dt', 'th', 'td', 'h2', 'h3']:
            tags = soup.find_all(tag_name)
            for tag in tags:
                if tag.string and date_pattern.search(tag.string):
                    # 親やクラス情報も併せて出力
                    parent_info = f"親: <{tag.parent.name} class={' '.join(tag.parent.get('class', []))}>"
                    tag_info = f"<{tag.name} class={' '.join(tag.get('class', []))}>"
                    print(f"\n{bcolors.OKGREEN}  ✓ 日付発見:{bcolors.ENDC}")
                    print(f"    タグ: {tag_info}")
                    print(f"    親タグ: {parent_info}")
                    print(f"    テキスト: {bcolors.BOLD}{tag.get_text(strip=True)}{bcolors.ENDC}")
                    found_tags.append(tag)
        
        if not found_tags:
            print(f"{bcolors.FAIL}  - HTML内から「YYYY年MM月DD日」の形式の日付を見つけられませんでした。{bcolors.ENDC}")
            print(f"{bcolors.WARNING}  - race_id ({file_path.stem.split('_')[1]}) と実際のレース開催日が異なる可能性があります。{bcolors.ENDC}")

    except Exception as e:
        print(f"{bcolors.FAIL}ファイル {file_path} の処理中にエラーが発生しました: {e}{bcolors.ENDC}")
        import traceback
        traceback.print_exc()

def main():
    """
    メイン実行関数: ログで成功していたHTMLファイル1つを調査する
    """
    print(f"{bcolors.BOLD}Keiba AI HTML日付探索デバッグスクリプト開始...{bcolors.ENDC}")
    
    # ログ(11:34:18)で唯一パースに成功していた race_id '202305020811' の
    # HTMLファイルの一つを指定
    
    # ファイル名: race_202305020811_20251111T102646+0900_sha256=f6778e3d.html
    # (ファイル名はログから取得したもので、環境に存在するはず)
    
    base_dir = Path(__file__).resolve().parent
    html_dir = base_dir / "keibaai" / "data" / "raw" / "html"
    
    # 1. results_parser.py 用のファイル
    race_file_path = html_dir / "race" / "race_202305020811_20251111T102646+0900_sha256=f6778e3d.html"
    find_date_in_html(race_file_path)
    
    # 2. shutuba_parser.py 用のファイル
    # ファイル名: shutuba_202305020811_20251111T102915+0900_sha256=0bd7bfca.html
    shutuba_file_path = html_dir / "shutuba" / "shutuba_202305020811_20251111T102915+0900_sha256=0bd7bfca.html"
    find_date_in_html(shutuba_file_path)

    print(f"\n{bcolors.BOLD}全ての分析が完了しました。{bcolors.ENDC}")
    print("上記の結果をコピーして、私に返信してください。")
    print(f"{bcolors.WARNING}この結果に基づき、パーサー（`results_parser.py`, `shutuba_parser.py`）の日付抽出ロジックを修正します。{bcolors.ENDC}")

if __name__ == "__main__":
    main()
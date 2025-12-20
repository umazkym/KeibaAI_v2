"""
不完全なHTMLファイルの総数を調査

スクレイピングが途中で失敗したファイルを特定し、
再スクレイピングが必要なファイル数を把握する
"""
from pathlib import Path
from bs4 import BeautifulSoup
import concurrent.futures
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "keibaai" / "data" / "raw" / "html" / "race"

def check_html_file(file_path):
    """HTMLファイルの完全性をチェック"""
    try:
        with open(file_path, 'rb') as f:
            html_bytes = f.read()
        
        file_size = len(html_bytes)
        
        try:
            html_text = html_bytes.decode('euc_jp', errors='replace')
        except:
            html_text = html_bytes.decode('utf-8', errors='replace')
        
        soup = BeautifulSoup(html_text, 'html.parser')
        
        # チェック項目
        has_title = soup.find('title') is not None
        has_race_table = soup.find('table', class_='race_table_01') is not None
        has_error = 'エラー' in html_text or '見つかりません' in html_text[:1000]
        
        # 問題の分類
        if file_size < 1000:
            return ('empty', file_path.name)
        elif not has_title:
            return ('no_title', file_path.name)
        elif not has_race_table:
            return ('no_table', file_path.name)
        elif has_error:
            return ('error_page', file_path.name)
        else:
            return ('ok', None)
    except Exception as e:
        return ('read_error', file_path.name)

def main():
    print("=" * 80)
    print("  不完全なHTMLファイルの調査")
    print("=" * 80)
    
    html_files = list(RAW_DIR.glob("*.bin"))
    print(f"\n  調査対象ファイル数: {len(html_files):,}")
    
    issues = {
        'ok': 0,
        'empty': [],
        'no_title': [],
        'no_table': [],
        'error_page': [],
        'read_error': []
    }
    
    # 並列処理
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(tqdm(
            executor.map(check_html_file, html_files),
            total=len(html_files),
            desc="チェック中"
        ))
    
    for category, filename in results:
        if category == 'ok':
            issues['ok'] += 1
        else:
            issues[category].append(filename)
    
    # 結果表示
    print(f"\n  【調査結果】")
    print(f"    正常: {issues['ok']:,}件")
    print(f"    空/極小ファイル: {len(issues['empty'])}件")
    print(f"    titleなし（途中切れ）: {len(issues['no_title'])}件")
    print(f"    race_table_01なし: {len(issues['no_table'])}件")
    print(f"    エラーページ: {len(issues['error_page'])}件")
    print(f"    読み取りエラー: {len(issues['read_error'])}件")
    
    total_issues = len(issues['empty']) + len(issues['no_title']) + len(issues['no_table']) + len(issues['error_page']) + len(issues['read_error'])
    print(f"\n  問題ファイル合計: {total_issues}件 ({total_issues/len(html_files)*100:.2f}%)")
    
    # サンプル表示
    if issues['no_title']:
        print(f"\n  titleなしファイルのサンプル（最大10件）:")
        for f in issues['no_title'][:10]:
            print(f"    {f}")
    
    if issues['no_table']:
        print(f"\n  race_table_01なしファイルのサンプル（最大10件）:")
        for f in issues['no_table'][:10]:
            print(f"    {f}")
    
    # 再スクレイピング対象リスト出力
    rescrape_targets = issues['empty'] + issues['no_title'] + issues['no_table']
    if rescrape_targets:
        output_path = PROJECT_ROOT / "keibaai" / "data" / "rescrape_targets.txt"
        with open(output_path, 'w') as f:
            for filename in rescrape_targets:
                race_id = filename.replace('.bin', '')
                f.write(f"{race_id}\n")
        print(f"\n  再スクレイピング対象ファイルを出力: {output_path}")
        print(f"    対象数: {len(rescrape_targets)}件")

if __name__ == "__main__":
    main()

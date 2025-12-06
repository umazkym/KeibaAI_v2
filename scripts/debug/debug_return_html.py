#!/usr/bin/env python3
"""
払い戻しHTML構造のデバッグスクリプト
"""
from pathlib import Path
from bs4 import BeautifulSoup
import re

def debug_return_html(file_path: str):
    """HTMLの払い戻し部分の構造を確認"""
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    print("=" * 60)
    print(f"ファイル: {file_path}")
    print("=" * 60)
    
    # 1. 期待されるクラスを検索
    print("\n【期待されるクラス検索】")
    classes = ['Tansho', 'Fukusho', 'Umaren', 'Umatan', 'Wide', 'Fuku3', 'Tan3']
    for cls in classes:
        found = soup.find('tr', class_=cls)
        print(f"  tr.{cls}: {'FOUND' if found else 'NOT FOUND'}")
    
    # 2. 払い戻しテーブルを探す
    print("\n【払戻テーブル検索】")
    payout_tables = soup.find_all('table', summary=re.compile(r'払戻|払い戻し'))
    print(f"  summary='払戻*' テーブル数: {len(payout_tables)}")
    
    for i, tbl in enumerate(payout_tables[:3]):
        print(f"\n  --- テーブル {i+1} ---")
        print(f"    class: {tbl.get('class')}")
        print(f"    summary: {tbl.get('summary')}")
        
        # テーブル内のtr/thを確認
        rows = tbl.find_all('tr')[:5]
        for j, row in enumerate(rows):
            th = row.find('th')
            tds = row.find_all('td')
            th_text = th.get_text(strip=True) if th else "NO TH"
            td_texts = [td.get_text(strip=True)[:20] for td in tds[:3]]
            print(f"    Row {j}: th={th_text}, td={td_texts}")
    
    # 3. 単勝・複勝のテキストがあるthを検索
    print("\n【th内テキスト検索】")
    bet_types = ['単勝', '複勝', '馬連', '馬単', 'ワイド', '三連複', '三連単', '枠連']
    for bet_type in bet_types:
        th = soup.find('th', string=bet_type)
        if th:
            parent_tr = th.find_parent('tr')
            tr_class = parent_tr.get('class') if parent_tr else 'NO CLASS'
            print(f"  {bet_type}: FOUND (tr class={tr_class})")
        else:
            # string=が厳密すぎる可能性があるので、テキストを含むthを探す
            ths = soup.find_all('th')
            found_in_text = False
            for t in ths:
                if bet_type in t.get_text():
                    parent_tr = t.find_parent('tr')
                    tr_class = parent_tr.get('class') if parent_tr else 'NO CLASS'
                    print(f"  {bet_type}: FOUND in text (tr class={tr_class})")
                    found_in_text = True
                    break
            if not found_in_text:
                print(f"  {bet_type}: NOT FOUND")
    
    # 4. HTMLのサイズ
    print(f"\n【HTMLサイズ】{len(html_text):,} bytes")


if __name__ == "__main__":
    import sys
    
    test_file = "keibaai/data/raw/html/race/202001010101.bin"
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
    
    if Path(test_file).exists():
        debug_return_html(test_file)
    else:
        print(f"ファイルが見つかりません: {test_file}")

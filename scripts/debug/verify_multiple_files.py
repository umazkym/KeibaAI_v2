#!/usr/bin/env python3
"""
複数ファイルでのデータ構造検証スクリプト
対象: 202001010101.bin ~ 202001010112.bin
"""
from pathlib import Path
from bs4 import BeautifulSoup
import re

def verify_file(file_path: str) -> dict:
    """1ファイルのデータ構造を検証"""
    with open(file_path, 'rb') as f:
        html_text = f.read().decode('euc_jp', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    race_id = Path(file_path).stem
    
    result = {'race_id': race_id, 'issues': []}
    
    # === 1. 払い戻しテーブル ===
    payout_tables = soup.find_all('table', summary=re.compile(r'払戻|払い戻し'))
    if payout_tables:
        bet_types_found = []
        for table in payout_tables:
            for row in table.find_all('tr'):
                th = row.find('th')
                if th:
                    bet_types_found.append(th.get_text(strip=True))
        result['payout'] = bet_types_found
    else:
        result['payout'] = None
        result['issues'].append('払い戻しテーブルなし')
    
    # === 2. コーナー通過順位テーブル ===
    corner_table = soup.find('table', summary='コーナー通過順位')
    if corner_table:
        corners = {}
        for row in corner_table.find_all('tr'):
            th = row.find('th')
            td = row.find('td')
            if th and td:
                corner_name = th.get_text(strip=True)
                order_text = td.get_text(strip=True)
                corners[corner_name] = order_text[:30] if order_text else '(空)'
        result['corners'] = corners
    else:
        result['corners'] = None
        result['issues'].append('コーナーテーブルなし')
    
    # === 3. ラップタイムテーブル ===
    lap_table = soup.find('table', summary='ラップタイム')
    if lap_table:
        lap_data = {}
        for row in lap_table.find_all('tr'):
            th = row.find('th')
            td = row.find('td')
            if th and td:
                label = th.get_text(strip=True)
                value = td.get_text(strip=True)[:40]
                lap_data[label] = value
        result['lap_times'] = lap_data
    else:
        result['lap_times'] = None
        result['issues'].append('ラップテーブルなし')
    
    # === 4. PassageRate（各馬のコーナー通過順）確認 ===
    passage_cells = soup.find_all('td', class_='PassageRate')
    result['passage_rate_count'] = len(passage_cells)
    if passage_cells:
        result['passage_rate_sample'] = [c.get_text(strip=True) for c in passage_cells[:3]]
    
    return result


def main():
    base_dir = Path("keibaai/data/raw/html/race")
    
    print("=" * 70)
    print("複数ファイルでのデータ構造検証")
    print("対象: 202001010101.bin ~ 202001010112.bin")
    print("=" * 70)
    
    all_ok = True
    results = []
    
    for i in range(1, 13):
        file_name = f"2020010101{i:02d}.bin"
        file_path = base_dir / file_name
        
        if not file_path.exists():
            print(f"\n❌ {file_name}: ファイルなし")
            all_ok = False
            continue
        
        r = verify_file(str(file_path))
        results.append(r)
        
        status = "✅" if not r['issues'] else "⚠️"
        print(f"\n{status} {r['race_id']}")
        
        # 払い戻し
        if r['payout']:
            print(f"   払い戻し: {r['payout']}")
        else:
            print(f"   払い戻し: ❌ なし")
        
        # コーナー
        if r['corners']:
            print(f"   コーナー: {list(r['corners'].keys())}")
        else:
            print(f"   コーナー: ❌ なし")
        
        # ラップ
        if r['lap_times']:
            print(f"   ラップ: {list(r['lap_times'].keys())}")
        else:
            print(f"   ラップ: ❌ なし")
        
        # PassageRate
        print(f"   PassageRate: {r['passage_rate_count']}件 (例: {r.get('passage_rate_sample', [])})")
        
        if r['issues']:
            all_ok = False
            print(f"   ⚠️ 問題: {r['issues']}")
    
    print("\n" + "=" * 70)
    if all_ok:
        print("✅ 全ファイルで必要なデータ構造が確認できました")
    else:
        print("⚠️ 一部ファイルで問題があります")
    print("=" * 70)


if __name__ == "__main__":
    main()

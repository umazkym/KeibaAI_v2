#!/usr/bin/env python3
"""
コーナー通過順位の馬身差変換（正しいロジック版）
- 並走馬は同じgap_from_leader
- 同順位方式採用
"""
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd
import re


def get_gap_increment(symbol):
    """記号から馬身差を取得"""
    mapping = {
        None: 1.0,      # 記号なし（デフォルト）
        "pack": 1.0,    # 馬群直後
        ",": 1.5,       # 1-2馬身
        "-": 3.5,       # 2-5馬身
        "=": 7.0,       # 5馬身以上（中央値より少し上）
    }
    return mapping.get(symbol, 1.0)


def parse_corner_to_rows(race_id: str, corner: int, corner_str: str) -> list:
    """
    コーナー通過文字列を行データに変換
    """
    rows = []
    position = 1
    current_gap = 0.0
    i = 0
    prev_symbol = None  # 先頭は None（0.0を維持するため）
    
    while i < len(corner_str):
        char = corner_str[i]
        
        # 区切り記号
        if char in [',', '-', '=']:
            prev_symbol = char
            i += 1
            continue
        
        # 馬群（括弧内）
        if char == '(':
            # 閉じ括弧を探す
            end_idx = corner_str.find(')', i)
            if end_idx == -1:
                i += 1
                continue
            
            pack_str = corner_str[i+1:end_idx]
            horses = re.findall(r'\d+', pack_str)
            
            # 先頭馬群でなければ gap を加算
            if position > 1:
                current_gap += get_gap_increment(prev_symbol)
            
            # 馬群内の馬は全て同じ position, 同じ gap
            for horse in horses:
                rows.append({
                    "race_id": race_id,
                    "corner": corner,
                    "horse_number": int(horse),
                    "position": position,
                    "gap_from_leader": round(current_gap, 1)
                })
            
            position += len(horses)
            i = end_idx + 1
            prev_symbol = "pack"  # 馬群の後
            continue
        
        # 単独馬番
        if char.isdigit():
            horse_num = ""
            while i < len(corner_str) and corner_str[i].isdigit():
                horse_num += corner_str[i]
                i += 1
            
            # 先頭でなければ gap を加算
            if position > 1:
                current_gap += get_gap_increment(prev_symbol)
            
            rows.append({
                "race_id": race_id,
                "corner": corner,
                "horse_number": int(horse_num),
                "position": position,
                "gap_from_leader": round(current_gap, 1)
            })
            
            position += 1
            prev_symbol = None  # リセット（次の記号を待つ）
            continue
        
        # * 記号やスペースはスキップ
        i += 1
    
    return rows


def parse_corners(soup: BeautifulSoup, race_id: str) -> list:
    """コーナー通過順位をパース"""
    corner_table = soup.find('table', summary='コーナー通過順位')
    if not corner_table:
        return []
    
    results = []
    for row in corner_table.find_all('tr'):
        th = row.find('th')
        td = row.find('td')
        if th and td:
            corner_text = th.get_text(strip=True)
            match = re.search(r'(\d+)', corner_text)
            corner_num = int(match.group(1)) if match else None
            order_text = td.get_text(strip=True)
            
            if order_text and corner_num:
                rows = parse_corner_to_rows(race_id, corner_num, order_text)
                results.extend(rows)
    
    return results


def main():
    base_dir = Path("keibaai/data/raw/html/race")
    
    print("=" * 70)
    print("コーナー通過順位 - 正しいロジック版")
    print("=" * 70)
    
    print("\n【変換ルール】")
    print("  記号なし（デフォルト）: +1.0 馬身")
    print("  馬群()直後           : +1.0 馬身")
    print("  , (1-2馬身)          : +1.5 馬身")
    print("  - (2-5馬身)          : +3.5 馬身")
    print("  = (5馬身以上)        : +7.0 馬身")
    print("  ()内の馬             : 同じgap, 同じposition")
    
    all_corners = []
    
    for i in range(1, 13):
        file_name = f"2020010101{i:02d}.bin"
        file_path = base_dir / file_name
        
        if not file_path.exists():
            continue
        
        with open(file_path, 'rb') as f:
            html_text = f.read().decode('euc_jp', errors='replace')
        
        soup = BeautifulSoup(html_text, 'html.parser')
        race_id = file_path.stem
        
        corners = parse_corners(soup, race_id)
        all_corners.extend(corners)
    
    df = pd.DataFrame(all_corners)
    
    print(f"\n\n【パース結果サマリ】")
    print(f"  総レコード数: {len(df)}")
    print(f"  レース数: {df['race_id'].nunique()}")
    
    # サンプル出力
    print(f"\n\n【サンプル: 202001010101 全コーナー】")
    sample = df[df['race_id'] == '202001010101']
    for corner in sorted(sample['corner'].unique()):
        corner_data = sample[sample['corner'] == corner]
        print(f"\n--- {corner}コーナー ---")
        print(corner_data[['horse_number', 'position', 'gap_from_leader']].to_string(index=False))
    
    print(f"\n\n【馬身差分布】")
    print(df['gap_from_leader'].describe())


if __name__ == "__main__":
    main()

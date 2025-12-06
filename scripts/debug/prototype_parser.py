#!/usr/bin/env python3
"""
払い戻し・コーナー・ラップのプロトタイプパーサー（修正版）
<br/>タグで区切られたデータを正しく分解
"""
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd
import re

def parse_money(money_str: str) -> int:
    """金額文字列を整数に変換"""
    if not money_str:
        return 0
    cleaned = re.sub(r'[^\d]', '', money_str)
    return int(cleaned) if cleaned else 0


def split_by_br(td) -> list:
    """<br>タグで区切られた値を分解"""
    inner_html = str(td)
    # <br>, <br/>, <br /> を統一
    inner_html = re.sub(r'<br\s*/?>', '|||', inner_html)
    # タグを除去
    text = re.sub(r'<[^>]+>', '', inner_html)
    return [t.strip() for t in text.split('|||') if t.strip()]


def parse_payout(soup: BeautifulSoup, race_id: str) -> dict:
    """払い戻しデータをパース"""
    tables = soup.find_all('table', summary=re.compile(r'払戻|払い戻し'))
    
    results = {
        'tansho': [],      # 単勝
        'fukusho': [],     # 複勝
        'wakuren': [],     # 枠連
        'umaren': [],      # 馬連
        'wide': [],        # ワイド
        'umatan': [],      # 馬単
        'sanrenpuku': [],  # 三連複
        'sanrentan': [],   # 三連単
    }
    
    for table in tables:
        for row in table.find_all('tr'):
            th = row.find('th')
            if not th:
                continue
            bet_type = th.get_text(strip=True)
            tds = row.find_all('td')
            
            if len(tds) < 3:
                continue
            
            # <br>で分割した値を取得
            col0 = split_by_br(tds[0])
            col1 = split_by_br(tds[1])
            col2 = split_by_br(tds[2])
            
            # 単勝
            if bet_type == '単勝':
                results['tansho'].append({
                    'race_id': race_id,
                    'horse_number': int(col0[0]) if col0 and col0[0].isdigit() else None,
                    'payout': parse_money(col1[0]) if col1 else 0,
                    'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                })
            
            # 複勝（最大3頭）
            elif bet_type == '複勝':
                for i in range(min(len(col0), 3)):
                    results['fukusho'].append({
                        'race_id': race_id,
                        'horse_number': int(col0[i]) if col0[i].isdigit() else None,
                        'payout': parse_money(col1[i]) if i < len(col1) else 0,
                        'popularity': int(col2[i]) if i < len(col2) and col2[i].isdigit() else None
                    })
            
            # 枠連
            elif bet_type == '枠連':
                brackets = re.findall(r'\d+', col0[0]) if col0 else []
                if len(brackets) >= 2:
                    results['wakuren'].append({
                        'race_id': race_id,
                        'bracket_1': int(brackets[0]),
                        'bracket_2': int(brackets[1]),
                        'payout': parse_money(col1[0]) if col1 else 0,
                        'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                    })
            
            # 馬連
            elif bet_type == '馬連':
                horses = re.findall(r'\d+', col0[0]) if col0 else []
                if len(horses) >= 2:
                    results['umaren'].append({
                        'race_id': race_id,
                        'horse_1': int(horses[0]),
                        'horse_2': int(horses[1]),
                        'payout': parse_money(col1[0]) if col1 else 0,
                        'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                    })
            
            # ワイド（最大3組）
            elif bet_type == 'ワイド':
                for i in range(min(len(col0), 3)):
                    horses = re.findall(r'\d+', col0[i])
                    if len(horses) >= 2:
                        results['wide'].append({
                            'race_id': race_id,
                            'horse_1': int(horses[0]),
                            'horse_2': int(horses[1]),
                            'payout': parse_money(col1[i]) if i < len(col1) else 0,
                            'popularity': int(col2[i]) if i < len(col2) and col2[i].isdigit() else None
                        })
            
            # 馬単
            elif bet_type == '馬単':
                horses = re.findall(r'\d+', col0[0]) if col0 else []
                if len(horses) >= 2:
                    results['umatan'].append({
                        'race_id': race_id,
                        'horse_1': int(horses[0]),
                        'horse_2': int(horses[1]),
                        'payout': parse_money(col1[0]) if col1 else 0,
                        'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                    })
            
            # 三連複
            elif bet_type == '三連複':
                horses = re.findall(r'\d+', col0[0]) if col0 else []
                if len(horses) >= 3:
                    results['sanrenpuku'].append({
                        'race_id': race_id,
                        'horse_1': int(horses[0]),
                        'horse_2': int(horses[1]),
                        'horse_3': int(horses[2]),
                        'payout': parse_money(col1[0]) if col1 else 0,
                        'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                    })
            
            # 三連単
            elif bet_type == '三連単':
                horses = re.findall(r'\d+', col0[0]) if col0 else []
                if len(horses) >= 3:
                    results['sanrentan'].append({
                        'race_id': race_id,
                        'horse_1': int(horses[0]),
                        'horse_2': int(horses[1]),
                        'horse_3': int(horses[2]),
                        'payout': parse_money(col1[0]) if col1 else 0,
                        'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                    })
    
    return results


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
            
            if order_text:
                results.append({
                    'race_id': race_id,
                    'corner': corner_num,
                    'order_raw': order_text
                })
    
    return results


def parse_lap_times(soup: BeautifulSoup, race_id: str) -> dict:
    """ラップタイムをパース"""
    lap_table = soup.find('table', summary='ラップタイム')
    if not lap_table:
        return {}
    
    result = {'race_id': race_id}
    
    for row in lap_table.find_all('tr'):
        th = row.find('th')
        td = row.find('td')
        if th and td:
            label = th.get_text(strip=True)
            value = td.get_text(strip=True)
            
            if label == 'ラップ':
                laps = re.findall(r'[\d.]+', value)
                result['lap_times'] = [float(x) for x in laps]
            elif label == 'ペース':
                pace_match = re.search(r'\(([\d.]+)-([\d.]+)\)', value)
                if pace_match:
                    result['first_half'] = float(pace_match.group(1))
                    result['second_half'] = float(pace_match.group(2))
    
    return result


def main():
    base_dir = Path("keibaai/data/raw/html/race")
    
    print("=" * 70)
    print("プロトタイプパーサー - 出力データ形式確認（修正版）")
    print("対象: 202001010101.bin ~ 202001010112.bin")
    print("=" * 70)
    
    all_payouts = {k: [] for k in ['tansho', 'fukusho', 'wakuren', 'umaren', 'wide', 'umatan', 'sanrenpuku', 'sanrentan']}
    all_corners = []
    all_laps = []
    
    for i in range(1, 13):
        file_name = f"2020010101{i:02d}.bin"
        file_path = base_dir / file_name
        
        if not file_path.exists():
            continue
        
        with open(file_path, 'rb') as f:
            html_text = f.read().decode('euc_jp', errors='replace')
        
        soup = BeautifulSoup(html_text, 'html.parser')
        race_id = file_path.stem
        
        payouts = parse_payout(soup, race_id)
        corners = parse_corners(soup, race_id)
        laps = parse_lap_times(soup, race_id)
        
        for k, v in payouts.items():
            all_payouts[k].extend(v)
        all_corners.extend(corners)
        if laps:
            all_laps.append(laps)
    
    # === 払い戻し ===
    print("\n【払い戻しデータ】")
    for bet_type, data in all_payouts.items():
        if data:
            df = pd.DataFrame(data)
            print(f"\n--- {bet_type} ({len(df)}件) ---")
            print(df.head(5).to_string(index=False))
    
    # === コーナー ===
    print("\n\n【コーナー通過順位】")
    df_corners = pd.DataFrame(all_corners)
    print(f"全{len(df_corners)}件")
    print(df_corners.head(6).to_string(index=False))
    
    # === ラップタイム ===
    print("\n\n【ラップタイム】")
    for lap in all_laps[:3]:
        print(f"\n{lap['race_id']}:")
        print(f"  ラップ: {lap.get('lap_times', [])[:5]}...")
        print(f"  前半/後半: {lap.get('first_half', '-')}/{lap.get('second_half', '-')}")


if __name__ == "__main__":
    main()

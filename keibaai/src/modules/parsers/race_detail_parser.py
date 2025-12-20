#!/usr/bin/env python3
"""
レース詳細データパーサー

払い戻し、ラップタイム、コーナー通過順位をHTMLから抽出
"""
import re
import logging
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup


# 馬身差変換定数
GAP_INCREMENT = {
    None: 1.0,      # 記号なし（デフォルト）
    "pack": 1.0,    # 馬群直後
    ",": 1.5,       # 1-2馬身
    "-": 3.5,       # 2-5馬身
    "=": 7.0,       # 5馬身以上
}


def _parse_money(money_str: str) -> int:
    """金額文字列を整数に変換"""
    if not money_str:
        return 0
    cleaned = re.sub(r'[^\d]', '', money_str)
    return int(cleaned) if cleaned else 0


def _split_by_br(td) -> List[str]:
    """<br>タグで区切られた値を分解"""
    inner_html = str(td)
    inner_html = re.sub(r'<br\s*/?>', '|||', inner_html)
    text = re.sub(r'<[^>]+>', '', inner_html)
    return [t.strip() for t in text.split('|||') if t.strip()]


def parse_returns(soup: BeautifulSoup, race_id: str) -> Dict[str, pd.DataFrame]:
    """
    払い戻しデータをパース
    
    Returns:
        Dict with keys: tansho, fukusho, wakuren, umaren, wide, umatan, sanrenpuku, sanrentan
    """
    tables = soup.find_all('table', summary=re.compile(r'払戻|払い戻し'))
    
    results = {
        'tansho': [],
        'fukusho': [],
        'wakuren': [],
        'umaren': [],
        'wide': [],
        'umatan': [],
        'sanrenpuku': [],
        'sanrentan': [],
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
            
            col0 = _split_by_br(tds[0])
            col1 = _split_by_br(tds[1])
            col2 = _split_by_br(tds[2])
            
            if bet_type == '単勝':
                results['tansho'].append({
                    'race_id': race_id,
                    'horse_number': int(col0[0]) if col0 and col0[0].isdigit() else None,
                    'payout': _parse_money(col1[0]) if col1 else 0,
                    'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                })
            
            elif bet_type == '複勝':
                for i in range(min(len(col0), 3)):
                    results['fukusho'].append({
                        'race_id': race_id,
                        'horse_number': int(col0[i]) if col0[i].isdigit() else None,
                        'payout': _parse_money(col1[i]) if i < len(col1) else 0,
                        'popularity': int(col2[i]) if i < len(col2) and col2[i].isdigit() else None
                    })
            
            elif bet_type == '枠連':
                brackets = re.findall(r'\d+', col0[0]) if col0 else []
                if len(brackets) >= 2:
                    results['wakuren'].append({
                        'race_id': race_id,
                        'bracket_1': int(brackets[0]),
                        'bracket_2': int(brackets[1]),
                        'payout': _parse_money(col1[0]) if col1 else 0,
                        'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                    })
            
            elif bet_type == '馬連':
                horses = re.findall(r'\d+', col0[0]) if col0 else []
                if len(horses) >= 2:
                    results['umaren'].append({
                        'race_id': race_id,
                        'horse_1': int(horses[0]),
                        'horse_2': int(horses[1]),
                        'payout': _parse_money(col1[0]) if col1 else 0,
                        'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                    })
            
            elif bet_type == 'ワイド':
                for i in range(min(len(col0), 3)):
                    horses = re.findall(r'\d+', col0[i])
                    if len(horses) >= 2:
                        results['wide'].append({
                            'race_id': race_id,
                            'horse_1': int(horses[0]),
                            'horse_2': int(horses[1]),
                            'payout': _parse_money(col1[i]) if i < len(col1) else 0,
                            'popularity': int(col2[i]) if i < len(col2) and col2[i].isdigit() else None
                        })
            
            elif bet_type == '馬単':
                horses = re.findall(r'\d+', col0[0]) if col0 else []
                if len(horses) >= 2:
                    results['umatan'].append({
                        'race_id': race_id,
                        'horse_1': int(horses[0]),
                        'horse_2': int(horses[1]),
                        'payout': _parse_money(col1[0]) if col1 else 0,
                        'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                    })
            
            elif bet_type == '三連複':
                horses = re.findall(r'\d+', col0[0]) if col0 else []
                if len(horses) >= 3:
                    results['sanrenpuku'].append({
                        'race_id': race_id,
                        'horse_1': int(horses[0]),
                        'horse_2': int(horses[1]),
                        'horse_3': int(horses[2]),
                        'payout': _parse_money(col1[0]) if col1 else 0,
                        'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                    })
            
            elif bet_type == '三連単':
                horses = re.findall(r'\d+', col0[0]) if col0 else []
                if len(horses) >= 3:
                    results['sanrentan'].append({
                        'race_id': race_id,
                        'horse_1': int(horses[0]),
                        'horse_2': int(horses[1]),
                        'horse_3': int(horses[2]),
                        'payout': _parse_money(col1[0]) if col1 else 0,
                        'popularity': int(col2[0]) if col2 and col2[0].isdigit() else None
                    })
    
    return {k: pd.DataFrame(v) for k, v in results.items()}


def parse_lap_times(soup: BeautifulSoup, race_id: str) -> Dict:
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
                result['lap_times_str'] = value
            elif label == 'ペース':
                result['pace_str'] = value
                pace_match = re.search(r'\(([\d.]+)-([\d.]+)\)', value)
                if pace_match:
                    result['first_half'] = float(pace_match.group(1))
                    result['second_half'] = float(pace_match.group(2))
    
    return result


def parse_corners_raw(soup: BeautifulSoup, race_id: str) -> List[Dict]:
    """コーナー通過順位（生テキスト）をパース"""
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
                results.append({
                    'race_id': race_id,
                    'corner': corner_num,
                    'order_raw': order_text
                })
    
    return results


def _parse_corner_order(race_id: str, corner: int, corner_str: str) -> List[Dict]:
    """コーナー通過順位テキストを馬身差付きデータに変換
    
    対応フォーマット:
    - 通常: 1,2(3,4)-5=6
    - 空白区切り離れ馬: 1(2,7)(9,3,11)   15  （末尾に大きく離れた馬）
    - アスタリスク付き: (*9,13)(1,5,14)...
    """
    # 空白で区切られた部分を分離（大きく離れた馬の処理用）
    # 例: "1(2,7)(9,3,11)   15" -> ["1(2,7)(9,3,11)", "15"]
    parts = re.split(r'\s{2,}', corner_str.strip())  # 2つ以上の連続スペースで分割
    
    rows = []
    position = 1
    current_gap = 0.0
    
    for part_idx, part in enumerate(parts):
        if not part.strip():
            continue
            
        # 後続パートは大きく離れている（15馬身以上のギャップ）
        if part_idx > 0:
            current_gap += 15.0
        
        i = 0
        prev_symbol = None
        
        while i < len(part):
            char = part[i]
            
            # 記号処理
            if char in [',', '-', '=']:
                prev_symbol = char
                i += 1
                continue
            
            # スペース（1文字）はスキップ
            if char == ' ':
                i += 1
                continue
            
            # アスタリスク（逃げ馬表示）はスキップ
            if char == '*':
                i += 1
                continue
            
            # カッコ内の馬群処理
            if char == '(':
                end_idx = part.find(')', i)
                if end_idx == -1:
                    i += 1
                    continue
                
                pack_str = part[i+1:end_idx]
                horses = re.findall(r'\d+', pack_str)
                
                if position > 1:
                    current_gap += GAP_INCREMENT.get(prev_symbol, 1.0)
                
                for horse in horses:
                    rows.append({
                        'race_id': race_id,
                        'corner': corner,
                        'horse_number': int(horse),
                        'position': position,
                        'gap_from_leader': round(current_gap, 1)
                    })
                
                position += len(horses)
                i = end_idx + 1
                prev_symbol = "pack"
                continue
            
            # 単独馬番処理
            if char.isdigit():
                horse_num = ""
                while i < len(part) and part[i].isdigit():
                    horse_num += part[i]
                    i += 1
                
                if position > 1:
                    current_gap += GAP_INCREMENT.get(prev_symbol, 1.0)
                
                rows.append({
                    'race_id': race_id,
                    'corner': corner,
                    'horse_number': int(horse_num),
                    'position': position,
                    'gap_from_leader': round(current_gap, 1)
                })
                
                position += 1
                prev_symbol = None
                continue
            
            i += 1
    
    return rows


def parse_corner_positions(soup: BeautifulSoup, race_id: str) -> List[Dict]:
    """コーナー通過順位を馬身差付きでパース"""
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
                rows = _parse_corner_order(race_id, corner_num, order_text)
                results.extend(rows)
    
    return results


def parse_race_html(file_path: str, race_id: str = None) -> Dict:
    """
    レースHTMLファイルから全データを抽出
    
    Returns:
        {
            'returns': Dict[str, DataFrame],
            'lap_times': Dict,
            'corners_raw': List[Dict],
            'corner_positions': List[Dict]
        }
    """
    if race_id is None:
        race_id = Path(file_path).stem
    
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    return {
        'returns': parse_returns(soup, race_id),
        'lap_times': parse_lap_times(soup, race_id),
        'corners_raw': parse_corners_raw(soup, race_id),
        'corner_positions': parse_corner_positions(soup, race_id)
    }


if __name__ == "__main__":
    # テスト
    test_file = "keibaai/data/raw/html/race/202001010101.bin"
    if Path(test_file).exists():
        result = parse_race_html(test_file)
        print("=== 払い戻し ===")
        for k, df in result['returns'].items():
            if not df.empty:
                print(f"\n[{k}]")
                print(df)
        print("\n=== ラップタイム ===")
        print(result['lap_times'])
        print("\n=== コーナー生テキスト ===")
        print(result['corners_raw'][:2])
        print("\n=== コーナー馬身差 ===")
        print(pd.DataFrame(result['corner_positions']).head(10))

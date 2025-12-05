#!/usr/bin/env python3
"""
払い戻しパーサー

HTMLから払い戻し情報（単勝、複勝、馬連、馬単、ワイド、三連複、三連単）を抽出
"""

import re
import logging
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup


def parse_return_html(file_path: str, race_id: str = None) -> Dict[str, pd.DataFrame]:
    """
    レース結果HTMLから払い戻し情報をパース
    
    Args:
        file_path: HTMLファイルパス（.bin または .html）
        race_id: レースID（Noneならファイル名から抽出）
    
    Returns:
        {'tansho': DataFrame, 'fukusho': DataFrame, ...}
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
    
    return_dict = {
        'tansho': _parse_tansho(soup, race_id),
        'fukusho': _parse_fukusho(soup, race_id),
        'umaren': _parse_umaren(soup, race_id),
        'umatan': _parse_umatan(soup, race_id),
        'wide': _parse_wide(soup, race_id),
        'sanrenpuku': _parse_sanrenpuku(soup, race_id),
        'sanrentan': _parse_sanrentan(soup, race_id),
    }
    
    return return_dict


def _parse_money(money_str: str) -> int:
    """払い戻し金額をパース（例: '250円' -> 250）"""
    if not money_str:
        return 0
    cleaned = re.sub(r'[^\d]', '', money_str)
    return int(cleaned) if cleaned else 0


def _parse_tansho(soup: BeautifulSoup, race_id: str) -> pd.DataFrame:
    """単勝をパース"""
    try:
        row = soup.find('tr', class_='Tansho')
        if not row:
            return pd.DataFrame()
        
        result_cell = row.find('td', class_='Result')
        payout_cell = row.find('td', class_='Payout')
        
        if not result_cell or not payout_cell:
            return pd.DataFrame()
        
        horse_number = result_cell.find('span').text.strip() if result_cell.find('span') else ''
        payout = _parse_money(payout_cell.find('span').text if payout_cell.find('span') else '')
        
        return pd.DataFrame([{
            'race_id': race_id,
            'horse_number': int(horse_number) if horse_number.isdigit() else None,
            'payout': payout
        }])
    except Exception as e:
        logging.warning(f"単勝パースエラー: {race_id} - {e}")
        return pd.DataFrame()


def _parse_fukusho(soup: BeautifulSoup, race_id: str) -> pd.DataFrame:
    """複勝をパース（最大3頭）"""
    try:
        row = soup.find('tr', class_='Fukusho')
        if not row:
            return pd.DataFrame()
        
        result_cell = row.find('td', class_='Result')
        payout_cell = row.find('td', class_='Payout')
        
        if not result_cell or not payout_cell:
            return pd.DataFrame()
        
        # 馬番を抽出（複数のspan）
        horse_numbers = []
        for span in result_cell.find_all('span'):
            text = span.text.strip()
            if text and text.isdigit():
                horse_numbers.append(int(text))
        
        # 払い戻しを抽出（brで区切られている）
        payout_span = payout_cell.find('span')
        if not payout_span:
            return pd.DataFrame()
        
        payout_text = payout_span.decode_contents()
        payouts = [_parse_money(p) for p in payout_text.split('<br') if p.strip()]
        # <br/>タグ対応
        payouts_cleaned = []
        for p in payout_text.replace('<br/>', '<br>').replace('<br >', '<br>').split('<br>'):
            cleaned = re.sub(r'<[^>]+>', '', p).strip()
            if cleaned:
                payouts_cleaned.append(_parse_money(cleaned))
        
        # 馬番と払い戻しを対応付け
        results = []
        for i, horse_num in enumerate(horse_numbers[:3]):
            payout = payouts_cleaned[i] if i < len(payouts_cleaned) else 0
            results.append({
                'race_id': race_id,
                'horse_number': horse_num,
                'payout': payout
            })
        
        return pd.DataFrame(results)
    except Exception as e:
        logging.warning(f"複勝パースエラー: {race_id} - {e}")
        return pd.DataFrame()


def _parse_umaren(soup: BeautifulSoup, race_id: str) -> pd.DataFrame:
    """馬連をパース"""
    try:
        row = soup.find('tr', class_='Umaren')
        if not row:
            return pd.DataFrame()
        
        result_cell = row.find('td', class_='Result')
        payout_cell = row.find('td', class_='Payout')
        
        if not result_cell or not payout_cell:
            return pd.DataFrame()
        
        # 馬番を抽出
        horse_numbers = []
        for span in result_cell.find_all('span'):
            text = span.text.strip()
            if text and text.isdigit():
                horse_numbers.append(int(text))
        
        payout = _parse_money(payout_cell.find('span').text if payout_cell.find('span') else '')
        
        return pd.DataFrame([{
            'race_id': race_id,
            'horse_1': horse_numbers[0] if len(horse_numbers) > 0 else None,
            'horse_2': horse_numbers[1] if len(horse_numbers) > 1 else None,
            'payout': payout
        }])
    except Exception as e:
        logging.warning(f"馬連パースエラー: {race_id} - {e}")
        return pd.DataFrame()


def _parse_umatan(soup: BeautifulSoup, race_id: str) -> pd.DataFrame:
    """馬単をパース"""
    try:
        row = soup.find('tr', class_='Umatan')
        if not row:
            return pd.DataFrame()
        
        result_cell = row.find('td', class_='Result')
        payout_cell = row.find('td', class_='Payout')
        
        if not result_cell or not payout_cell:
            return pd.DataFrame()
        
        horse_numbers = []
        for span in result_cell.find_all('span'):
            text = span.text.strip()
            if text and text.isdigit():
                horse_numbers.append(int(text))
        
        payout = _parse_money(payout_cell.find('span').text if payout_cell.find('span') else '')
        
        return pd.DataFrame([{
            'race_id': race_id,
            'horse_1': horse_numbers[0] if len(horse_numbers) > 0 else None,
            'horse_2': horse_numbers[1] if len(horse_numbers) > 1 else None,
            'payout': payout
        }])
    except Exception as e:
        logging.warning(f"馬単パースエラー: {race_id} - {e}")
        return pd.DataFrame()


def _parse_wide(soup: BeautifulSoup, race_id: str) -> pd.DataFrame:
    """ワイドをパース（最大3組）"""
    try:
        row = soup.find('tr', class_='Wide')
        if not row:
            return pd.DataFrame()
        
        result_cell = row.find('td', class_='Result')
        payout_cell = row.find('td', class_='Payout')
        
        if not result_cell or not payout_cell:
            return pd.DataFrame()
        
        # 組み合わせを抽出（ulで区切られている）
        combinations = []
        for ul in result_cell.find_all('ul'):
            horse_numbers = []
            for span in ul.find_all('span'):
                text = span.text.strip()
                if text and text.isdigit():
                    horse_numbers.append(int(text))
            if len(horse_numbers) >= 2:
                combinations.append((horse_numbers[0], horse_numbers[1]))
        
        # 払い戻しを抽出
        payout_span = payout_cell.find('span')
        if not payout_span:
            return pd.DataFrame()
        
        payout_text = payout_span.decode_contents()
        payouts = []
        for p in payout_text.replace('<br/>', '<br>').replace('<br >', '<br>').split('<br>'):
            cleaned = re.sub(r'<[^>]+>', '', p).strip()
            if cleaned:
                payouts.append(_parse_money(cleaned))
        
        results = []
        for i, (h1, h2) in enumerate(combinations[:3]):
            payout = payouts[i] if i < len(payouts) else 0
            results.append({
                'race_id': race_id,
                'horse_1': h1,
                'horse_2': h2,
                'payout': payout
            })
        
        return pd.DataFrame(results)
    except Exception as e:
        logging.warning(f"ワイドパースエラー: {race_id} - {e}")
        return pd.DataFrame()


def _parse_sanrenpuku(soup: BeautifulSoup, race_id: str) -> pd.DataFrame:
    """三連複をパース"""
    try:
        row = soup.find('tr', class_='Fuku3')
        if not row:
            return pd.DataFrame()
        
        result_cell = row.find('td', class_='Result')
        payout_cell = row.find('td', class_='Payout')
        
        if not result_cell or not payout_cell:
            return pd.DataFrame()
        
        horse_numbers = []
        for span in result_cell.find_all('span'):
            text = span.text.strip()
            if text and text.isdigit():
                horse_numbers.append(int(text))
        
        payout = _parse_money(payout_cell.find('span').text if payout_cell.find('span') else '')
        
        return pd.DataFrame([{
            'race_id': race_id,
            'horse_1': horse_numbers[0] if len(horse_numbers) > 0 else None,
            'horse_2': horse_numbers[1] if len(horse_numbers) > 1 else None,
            'horse_3': horse_numbers[2] if len(horse_numbers) > 2 else None,
            'payout': payout
        }])
    except Exception as e:
        logging.warning(f"三連複パースエラー: {race_id} - {e}")
        return pd.DataFrame()


def _parse_sanrentan(soup: BeautifulSoup, race_id: str) -> pd.DataFrame:
    """三連単をパース"""
    try:
        row = soup.find('tr', class_='Tan3')
        if not row:
            return pd.DataFrame()
        
        result_cell = row.find('td', class_='Result')
        payout_cell = row.find('td', class_='Payout')
        
        if not result_cell or not payout_cell:
            return pd.DataFrame()
        
        horse_numbers = []
        for span in result_cell.find_all('span'):
            text = span.text.strip()
            if text and text.isdigit():
                horse_numbers.append(int(text))
        
        payout = _parse_money(payout_cell.find('span').text if payout_cell.find('span') else '')
        
        return pd.DataFrame([{
            'race_id': race_id,
            'horse_1': horse_numbers[0] if len(horse_numbers) > 0 else None,
            'horse_2': horse_numbers[1] if len(horse_numbers) > 1 else None,
            'horse_3': horse_numbers[2] if len(horse_numbers) > 2 else None,
            'payout': payout
        }])
    except Exception as e:
        logging.warning(f"三連単パースエラー: {race_id} - {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    # テスト
    import sys
    
    test_file = "keibaai/data/raw/html/race/202001010101.bin"
    if Path(test_file).exists():
        result = parse_return_html(test_file)
        print("\n=== 払い戻しパース結果 ===")
        for key, df in result.items():
            if not df.empty:
                print(f"\n[{key}]")
                print(df)
    else:
        print(f"テストファイルが見つかりません: {test_file}")

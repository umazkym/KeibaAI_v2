"""
出馬表HTMLパーサ
netkeiba.com の出馬表ページから情報を抽出
"""

import re
import logging
from typing import Dict, List, Optional
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from .common_utils import (
    parse_int_or_none,
    parse_float_or_none,
    parse_sex_age,
    parse_horse_weight,
    parse_prize_money,
    normalize_owner_name,
    parse_owner_odds, # この行は実際には使われなくなりますが、import文は残しておきます
)


def parse_shutuba_html(file_path: str, race_id: str = None) -> pd.DataFrame:
    """
    出馬表HTMLをパースしてDataFrameを返す
    
    Args:
        file_path: HTMLファイルパス
        race_id: レースID
    
    Returns:
        出馬表DataFrame
        
    Columns:
        - race_id: str
        - horse_number: int (馬番)
        - bracket_number: int (枠番)
        - horse_id: str
        - horse_name: str
        - sex_age: str (例: "牡3")
        - sex: str
        - age: int
        - basis_weight: float (斤量、kg)
        - jockey_id: str
        - jockey_name: str
        - trainer_id: str
        - trainer_name: str
        - horse_weight: int (前走馬体重)
        - owner_name: str
        - prize_total: int (獲得賞金、万円)
        - morning_odds: float (前日オッズ)
        - morning_popularity: int (前日人気)
        - career_stats: str (例: "5戦2勝")
        - career_starts: int
        - career_wins: int
        - career_places: int (2着+3着)
        - last_5_finishes: str (例: "12311")
    """
    logging.info(f"出馬表パース開始: {file_path}")
    
    if race_id is None:
        race_id = extract_race_id_from_filename(file_path)
    
    # ファイル読み込み
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'lxml')
    
    # Shutuba_Table を検索
    shutuba_table = soup.find('table', class_='Shutuba_Table')
    
    if not shutuba_table:
        logging.error(f"Shutuba_Table が見つかりません: {file_path}")
        return pd.DataFrame()
    
    rows = []
    
    # 'HorseList'クラスを持つtrのみを走査
    for tr in shutuba_table.find_all('tr', class_='HorseList'):
        try:
            row_data = parse_shutuba_row(tr, race_id)
            if row_data:
                rows.append(row_data)
        except Exception as e:
            logging.warning(f"行のパースエラー: {e}")
            continue
    
    df = pd.DataFrame(rows)
    
    logging.info(f"出馬表パース完了: {file_path} ({len(df)}行)")
    
    return df


def parse_shutuba_row(tr, race_id: str) -> Optional[Dict]:
    """
    出馬表テーブルの1行をパース
    
    Args:
        tr: BeautifulSoupのtr要素
        race_id: レースID
    
    Returns:
        行データ辞書
    """
    cells = tr.find_all('td')
    
    if len(cells) < 10:
        return None
    
    row_data = {'race_id': race_id}
    
    # 枠番
    bracket_text = cells[0].get_text(strip=True)
    row_data['bracket_number'] = parse_int_or_none(bracket_text)
    
    # 馬番
    horse_num_text = cells[1].get_text(strip=True)
    row_data['horse_number'] = parse_int_or_none(horse_num_text)
    
    # 馬名・馬ID
    horse_link = cells[3].find('a', href=re.compile(r'/horse/\d+'))
    if horse_link:
        row_data['horse_name'] = horse_link.get_text(strip=True)
        horse_id_match = re.search(r'/horse/(\d+)', horse_link['href'])
        row_data['horse_id'] = horse_id_match.group(1) if horse_id_match else None
    else:
        row_data['horse_name'] = cells[3].get_text(strip=True)
        row_data['horse_id'] = None
    
    # 性齢
    sex_age_text = cells[4].get_text(strip=True)
    row_data['sex_age'] = sex_age_text
    sex, age = parse_sex_age(sex_age_text)
    row_data['sex'] = sex
    row_data['age'] = age
    
    # 斤量
    weight_text = cells[5].get_text(strip=True)
    row_data['basis_weight'] = parse_float_or_none(weight_text)
    
    # 騎手名・騎手ID
    # 複数のパターンに対応（netkeiba.comの複数の形式に対応）
    jockey_link = cells[6].find('a', href=re.compile(r'/jockey/'))
    if jockey_link:
        row_data['jockey_name'] = jockey_link.get_text(strip=True)
        # URLパターンの優先順位: /jockey/result/recent/\d+ > /jockey/\d+
        href = jockey_link['href']
        jockey_id_match = re.search(r'/jockey/result/recent/(\d+)', href)
        if not jockey_id_match:
            jockey_id_match = re.search(r'/jockey/(\d+)', href)
        row_data['jockey_id'] = jockey_id_match.group(1) if jockey_id_match else None
    else:
        row_data['jockey_name'] = cells[6].get_text(strip=True)
        row_data['jockey_id'] = None
    
    # 調教師名・調教師ID
    trainer_link = cells[7].find('a', href=re.compile(r'/trainer/\d+'))
    if trainer_link:
        row_data['trainer_name'] = trainer_link.get_text(strip=True)
        trainer_id_match = re.search(r'/trainer/(\d+)', trainer_link['href'])
        row_data['trainer_id'] = trainer_id_match.group(1) if trainer_id_match else None
    else:
        row_data['trainer_name'] = cells[7].get_text(strip=True)
        row_data['trainer_id'] = None
    
    # 馬体重（前走）
    if len(cells) > 8:
        weight_text = cells[8].get_text(strip=True)
        horse_weight, _ = parse_horse_weight(weight_text)
        row_data['horse_weight'] = horse_weight
    
    # 馬体重（前走）の変動分を抽出
    if len(cells) > 8:
        _, horse_weight_change = parse_horse_weight(weight_text)
        row_data['horse_weight_change'] = horse_weight_change
    else:
        row_data['horse_weight_change'] = None

    # 前日オッズ（morning_odds）
    # 仕様: Cell[9]は前日オッズ（"---.-"形式で表示、数値の場合もある）
    if len(cells) > 9:
        odds_text = cells[9].get_text(strip=True)
        # (修正箇所: parse_owner_odds -> parse_float_or_none)
        row_data['morning_odds'] = parse_float_or_none(odds_text)
    else:
        row_data['morning_odds'] = None

    # 前日人気（morning_popularity）
    # 仕様: Cell[10]は前日人気（"**"、"1"等）
    if len(cells) > 10:
        popularity_text = cells[10].get_text(strip=True)
        row_data['morning_popularity'] = parse_int_or_none(popularity_text)
    else:
        row_data['morning_popularity'] = None

    # 馬主名（owner_name）
    # 仕様: 元の位置はCell[9]だったが、CELLが増えたため要確認
    # 調査結果: 出馬表では馬主情報が表示されないため Null が正常
    row_data['owner_name'] = None

    # 獲得賞金（prize_total）
    # 仕様: 出馬表ページには獲得賞金が含まれていない
    row_data['prize_total'] = None

    # 戦績情報（career_stats, career_starts, career_wins, career_places, last_5_finishes）
    # 仕様: これらは出馬表ページには含まれていない
    # Cell[13]と[14]は「編集」リンクで、戦績データではない
    row_data['career_stats'] = None
    row_data['career_starts'] = None
    row_data['career_wins'] = None
    row_data['career_places'] = None
    row_data['last_5_finishes'] = None

    return row_data


def parse_career_stats(career_text: str) -> tuple:
    """
    戦績文字列をパース
    例: "15戦3勝[3-2-4-6]" → (15, 3, 6)

    Returns:
        (出走数, 勝利数, 2着・3着回数)
    """
    if not career_text or career_text == '編集' or '編集' in career_text:
        return (None, None, None)

    # 前置/後置スペース除去
    career_text = career_text.strip()

    # パターン1: "N戦M勝[1-2-3-4]"
    match = re.match(r'(\d+)戦(\d+)勝\[(\d+)-(\d+)-(\d+)-(\d+)\]', career_text)
    if match:
        starts = int(match.group(1))
        wins = int(match.group(2))
        seconds = int(match.group(3))
        thirds = int(match.group(4))
        places = seconds + thirds
        return (starts, wins, places)

    # パターン2: "N戦M勝"
    match = re.match(r'(\d+)戦(\d+)勝', career_text)
    if match:
        starts = int(match.group(1))
        wins = int(match.group(2))
        return (starts, wins, None)

    return (None, None, None)


def parse_last_5_finishes(last_5_text: str) -> str:
    """
    直近5走の着順文字列をパース
    例: "1-2-3-1-1" → "12311"
    例: "123XX" → "123XX"（Xは出走なし）
    """
    if not last_5_text or last_5_text == '編集':
        return None

    # ハイフン除去、スペース除去、「編集」文字列除去
    cleaned = last_5_text.replace('-', '').replace(' ', '').replace('編集', '')

    # 英数字のみを抽出（数字とXのみを許可）
    cleaned = re.sub(r'[^0-9X]', '', cleaned)

    # 5文字にパディング（不足分は'X'で埋める）
    if len(cleaned) < 5:
        cleaned = cleaned.ljust(5, 'X')

    return cleaned[:5]

def extract_race_id_from_filename(file_path: str) -> str:
    """
    ファイル名からレースIDを抽出
    例: 202306010101_20251106T120000+09:00_sha256=abcd1234.bin → 202306010101
    """
    filename = Path(file_path).stem
    match = re.search(r'(\d{12})', filename)
    return match.group(1) if match else None
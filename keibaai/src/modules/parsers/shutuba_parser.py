"""
出馬表HTMLパーサ (修正版)
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
)


def parse_shutuba_html(file_path: str, race_id: str = None) -> pd.DataFrame:
    """
    出馬表HTMLをパースしてDataFrameを返す
    
    重要な注意:
    netkeiba.comの出馬表ページには基本情報のみが含まれ、
    以下のフィールドは取得できません:
    - prize_total (獲得賞金)
    - morning_odds (前日オッズ)
    - morning_popularity (前日人気)
    - career_stats (戦績)
    - last_5_finishes (直近5走)
    - owner_name (馬主名)
    
    これらは別ページ（馬詳細ページ等）から取得する必要があります。
    """
    logging.info(f"出馬表パース開始: {file_path}")
    
    if race_id is None:
        race_id = extract_race_id_from_filename(file_path)
    
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'lxml')
    
    shutuba_table = soup.find('table', class_='Shutuba_Table')
    
    if not shutuba_table:
        logging.error(f"Shutuba_Table が見つかりません: {file_path}")
        return pd.DataFrame()
    
    rows = []
    
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
    出馬表テーブルの1行をパース（修正版）
    
    修正ポイント:
    1. jockey_id: 複数URLパターン対応
    2. HTMLに存在しないフィールドはNoneに設定
    3. セル数チェックの強化
    """
    cells = tr.find_all('td')
    
    if len(cells) < 8:  # 最小限のセル数チェック
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
    
    # 騎手名・騎手ID (修正: 複数パターン対応)
    jockey_link = cells[6].find('a', href=re.compile(r'/jockey/'))
    if jockey_link:
        row_data['jockey_name'] = jockey_link.get_text(strip=True)
        href = jockey_link['href']
        # パターン1: /jockey/result/recent/数字
        jockey_id_match = re.search(r'/jockey/result/recent/(\d+)', href)
        if not jockey_id_match:
            # パターン2: /jockey/数字
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
        horse_weight, horse_weight_change = parse_horse_weight(weight_text)
        row_data['horse_weight'] = horse_weight
        row_data['horse_weight_change'] = horse_weight_change
    else:
        row_data['horse_weight'] = None
        row_data['horse_weight_change'] = None

    # 以下のフィールドは出馬表HTMLには含まれていないため、Noneに設定
    # （これらは馬詳細ページやオッズページから取得する必要がある）
    
    row_data['owner_name'] = None  # 馬主名: 出馬表には未掲載
    row_data['prize_total'] = None  # 獲得賞金: 出馬表には未掲載
    row_data['morning_odds'] = None  # 前日オッズ: 別途オッズAPIから取得
    row_data['morning_popularity'] = None  # 前日人気: 別途オッズAPIから取得
    row_data['career_stats'] = None  # 戦績: 馬詳細ページから取得
    row_data['career_starts'] = None
    row_data['career_wins'] = None
    row_data['career_places'] = None
    row_data['last_5_finishes'] = None  # 直近5走: 馬詳細ページから取得

    return row_data


def extract_race_id_from_filename(file_path: str) -> str:
    """
    ファイル名からレースIDを抽出
    """
    filename = Path(file_path).stem
    match = re.search(r'(\d{12})', filename)
    return match.group(1) if match else None
"""
レース結果HTMLパーサ (修正版)
netkeiba.com のレース結果ページから情報を抽出
"""

import re
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from .common_utils import (
    parse_int_or_none,
    parse_float_or_none,
    parse_sex_age,
    parse_time_to_seconds,
    parse_margin_to_seconds,
    parse_horse_weight,
    parse_prize_money,
)


def parse_results_html(file_path: str, race_id: str = None) -> pd.DataFrame:
    """
    レース結果HTMLをパースしてDataFrameを返す
    """
    logging.info(f"レース結果パース開始: {file_path}")
    
    if race_id is None:
        race_id = extract_race_id_from_filename(file_path)
    
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except UnicodeDecodeError:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'lxml')
    
    # diary_snap_cut タグを無力化
    for tag in soup.find_all('diary_snap_cut'):
        tag.unwrap()
    
    result_table = soup.find('table', class_='race_table_01')
    
    if not result_table:
        logging.error(f"レース結果テーブルが見つかりません: {file_path}")
        return pd.DataFrame()
    
    rows = []
    for tr in result_table.find_all('tr')[1:]:
        try:
            row_data = parse_result_row(tr, race_id)
            if row_data:
                rows.append(row_data)
        except Exception as e:
            logging.warning(f"行のパースエラー: {e}", exc_info=True)
            continue
    
    df = pd.DataFrame(rows)
    
    # margin_seconds を計算（1着との差分）
    if not df.empty and 'finish_time_seconds' in df.columns:
        first_place_time = df.loc[df['finish_position'] == 1, 'finish_time_seconds'].values
        if len(first_place_time) > 0 and pd.notna(first_place_time[0]):
            df['margin_seconds'] = df['finish_time_seconds'] - first_place_time[0]
            df.loc[df['finish_position'] == 1, 'margin_seconds'] = 0.0
            df['margin_seconds'] = df['margin_seconds'].apply(lambda x: max(x, 0) if pd.notna(x) else None)
        else:
            df['margin_seconds'] = None
    
    logging.info(f"レース結果パース完了: {file_path} ({len(df)}行)")
    
    return df


def parse_result_row(tr, race_id: str) -> Optional[Dict]:
    """
    レース結果テーブルの1行をパース（修正版）
    
    修正ポイント:
    1. jockey_id: 複数のURLパターンに対応
    2. trainer/owner/prize_money: 実際のHTMLセル位置に対応
    3. セル数の動的チェックでエラー回避
    """
    cells = tr.find_all('td')

    if len(cells) < 15:
        return None
    
    row_data = {'race_id': race_id}
    
    # 着順
    finish_text = cells[0].get_text(strip=True)
    row_data['finish_position'] = parse_int_or_none(finish_text)
    
    # 枠番
    bracket_text = cells[1].get_text(strip=True)
    row_data['bracket_number'] = parse_int_or_none(bracket_text)
    
    # 馬番
    horse_num_text = cells[2].get_text(strip=True)
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
    
    # 騎手名・騎手ID (修正: 複数URLパターン対応)
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
    
    # タイム
    time_text = cells[7].get_text(strip=True)
    row_data['finish_time_str'] = time_text
    row_data['finish_time_seconds'] = parse_time_to_seconds(time_text)
    
    # 着差（後で計算するため、ここではstrのみ保存）
    margin_text = cells[8].get_text(strip=True)
    row_data['margin_str'] = margin_text
    
    # 通過順
    passing_text = cells[10].get_text(strip=True)
    row_data['passing_order'] = passing_text
    
    # 上がり3F
    last_3f_text = cells[11].get_text(strip=True)
    row_data['last_3f_time'] = parse_float_or_none(last_3f_text)
    
    # 単勝オッズ
    odds_text = cells[12].get_text(strip=True)
    row_data['win_odds'] = parse_float_or_none(odds_text)
    
    # 人気
    popularity_text = cells[13].get_text(strip=True)
    row_data['popularity'] = parse_int_or_none(popularity_text)
    
    # 馬体重
    weight_change_text = cells[14].get_text(strip=True)
    horse_weight, weight_change = parse_horse_weight(weight_change_text)
    row_data['horse_weight'] = horse_weight
    row_data['horse_weight_change'] = weight_change
    
    # 調教師（修正: 実際のセル位置を動的に検索）
    trainer_name = None
    trainer_id = None
    
    # セル15以降を走査して調教師リンクを探す
    for cell_idx in range(15, min(len(cells), 20)):
        cell = cells[cell_idx]
        trainer_link = cell.find('a', href=re.compile(r'/trainer/'))
        if trainer_link:
            trainer_name_raw = trainer_link.get_text(strip=True)
            # "[調]" プレフィックスを除去
            trainer_name = trainer_name_raw.replace('[調]', '').strip()
            
            href = trainer_link['href']
            trainer_id_match = re.search(r'/trainer/result/recent/(\d+)', href)
            if not trainer_id_match:
                trainer_id_match = re.search(r'/trainer/(\d+)', href)
            trainer_id = trainer_id_match.group(1) if trainer_id_match else None
            break

    row_data['trainer_name'] = trainer_name
    row_data['trainer_id'] = trainer_id

    # 馬主（修正: trainerの次のセルを探す）
    owner_name = None
    
    # セル16以降を走査して馬主を探す
    for cell_idx in range(16, min(len(cells), 21)):
        cell = cells[cell_idx]
        # 馬主はリンクの場合とテキストのみの場合がある
        owner_link = cell.find('a', href=re.compile(r'/owner/'))
        if owner_link:
            owner_name = owner_link.get_text(strip=True)
            break
        else:
            owner_text = cell.get_text(strip=True)
            if owner_text and owner_text not in ['---', '---.-', '']:
                owner_name = owner_text
                break

    row_data['owner_name'] = owner_name

    # 賞金（修正: 最後のセル付近を探す）
    prize_money = None
    
    # セル17以降を走査して賞金を探す
    for cell_idx in range(17, min(len(cells), 22)):
        cell = cells[cell_idx]
        prize_text = cell.get_text(strip=True)
        parsed_prize = parse_prize_money(prize_text)
        if parsed_prize is not None:
            prize_money = parsed_prize
            break

    row_data['prize_money'] = prize_money

    return row_data


def extract_race_id_from_filename(file_path: str) -> str:
    """
    ファイル名からレースIDを抽出
    例: 202306010101_20251106T120000+09:00_sha256=abcd1234.bin → 202306010101
    """
    filename = Path(file_path).stem
    match = re.search(r'(\d{12})', filename)
    return match.group(1) if match else None
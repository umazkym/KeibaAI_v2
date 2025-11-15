"""
馬プロフィール・過去成績パーサ (修正版)
ファイル種別を判定し、プロフィールファイルのみを処理
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
    parse_time_to_seconds,
    parse_margin_to_seconds,
    parse_horse_weight,
)


def is_profile_file(file_path: str) -> bool:
    file_path_obj = Path(file_path)
    filename = file_path_obj.name.lower()
    parent_dir = file_path_obj.parent.name.lower()
    
    # pedディレクトリのファイルは血統（プロフィールではない）
    if parent_dir == 'ped':
        return False
    
    # horseディレクトリ内で_perfを含む → 成績
    if parent_dir == 'horse' and '_perf' in filename:
        return False
    
    # それ以外のhorseディレクトリ内ファイル → プロフィール
    if parent_dir == 'horse':
        return True
    
    # 新形式の判定（念のため）
    if 'profile' in filename or ('horse_' in filename and 'perf' not in filename):
        return True
    
    return False  # 不明な場合は安全側に倒す


def parse_horse_profile(file_path: str, horse_id: str = None) -> Dict:
    """
    馬プロフィールHTMLをパース
    
    【重要】このパーサーはプロフィール情報のみを扱います。
    成績ファイル (_perf.bin) は parse_horse_performance() を使用してください。
    
    Args:
        file_path: HTMLファイルパス
        horse_id: 馬ID
    
    Returns:
        プロフィール辞書
        
    Keys:
        - horse_id: str
        - horse_name: str (馬名)
        - horse_name_en: str (英語名)
        - birth_date: str (生年月日、ISO8601)
        - trainer_id: str
        - trainer_name: str
        - owner_name: str
        - breeder_name: str (生産者)
        - producing_area: str (産地)
        - sire_id: str (父馬ID)
        - sire_name: str (父馬名)
        - dam_id: str (母馬ID)
        - dam_name: str (母馬名)
        - damsire_id: str (母父ID)
        - damsire_name: str (母父名)
        - sex: str
        - coat_color: str (毛色)
    """
    # ファイル種別チェック
    if not is_profile_file(file_path):
        logging.warning(f"成績ファイルをプロフィールパーサーで処理しようとしました: {file_path}")
        # 成績ファイルの場合、horse_idのみを返す（空データとして扱う）
        if horse_id is None:
            horse_id = extract_horse_id_from_filename(file_path)
        return {'horse_id': horse_id, '_is_empty': True}
    
    logging.info(f"馬プロフィールパース開始: {file_path}")
    
    if horse_id is None:
        horse_id = extract_horse_id_from_filename(file_path)
    
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    profile = {'horse_id': horse_id}
    
    # 馬名
    horse_title = soup.find('div', class_='horse_title')
    if horse_title:
        h1 = horse_title.find('h1')
        if h1:
            profile['horse_name'] = h1.get_text(strip=True)
    
    # プロフィールテーブルが見つからない場合、空データとして扱う
    profile_table = soup.find('table', class_='db_prof_table')
    if not profile_table:
        logging.warning(f"プロフィールテーブルが見つかりません（成績ファイルの可能性）: {file_path}")
        profile['_is_empty'] = True
        return profile
    
    rows = profile_table.find_all('tr')
    
    for row in rows:
        th = row.find('th')
        td = row.find('td')
        
        if not th or not td:
            continue
        
        label = th.get_text(strip=True)
        
        # 生年月日
        if '生年月日' in label:
            birth_text = td.get_text(strip=True)
            profile['birth_date'] = parse_birth_date(birth_text)
        
        # 調教師
        if '調教師' in label:
            trainer_link = td.find('a', href=re.compile(r'/trainer/'))
            if trainer_link:
                profile['trainer_name'] = trainer_link.get_text(strip=True)
                
                # /trainer/{id} 形式のURL
                # {id} は英数字の組み合わせ
                trainer_id_match = re.search(r'/trainer/([a-zA-Z0-9]+)', trainer_link['href'])
                profile['trainer_id'] = trainer_id_match.group(1) if trainer_id_match else None
        
        # 馬主
        elif '馬主' in label:
            profile['owner_name'] = td.get_text(strip=True)
        
        # 生産者
        elif '生産者' in label:
            profile['breeder_name'] = td.get_text(strip=True)
        
        # 産地
        elif '産地' in label:
            profile['producing_area'] = td.get_text(strip=True)
        
        # 性別
        elif '性別' in label:
            profile['sex'] = td.get_text(strip=True)
        
        # 毛色
        elif '毛色' in label:
            profile['coat_color'] = td.get_text(strip=True)
    
    # 血統情報（父・母・母父）
    blood_table = soup.find('table', class_='blood_table')
    if blood_table:
        # 父馬
        sire_link = blood_table.find('a', href=re.compile(r'/horse/\d+'), text=re.compile(r'.+'))
        if sire_link:
            profile['sire_name'] = sire_link.get_text(strip=True)
            sire_id_match = re.search(r'/horse/(\d+)', sire_link['href'])
            profile['sire_id'] = sire_id_match.group(1) if sire_id_match else None
        
        # 母馬・母父（簡易版、詳細は血統ページで取得）
        all_horse_links = blood_table.find_all('a', href=re.compile(r'/horse/\d+'))
        if len(all_horse_links) >= 2:
            dam_link = all_horse_links[1]
            profile['dam_name'] = dam_link.get_text(strip=True)
            dam_id_match = re.search(r'/horse/(\d+)', dam_link['href'])
            profile['dam_id'] = dam_id_match.group(1) if dam_id_match else None
        
        if len(all_horse_links) >= 3:
            damsire_link = all_horse_links[2]
            profile['damsire_name'] = damsire_link.get_text(strip=True)
            damsire_id_match = re.search(r'/horse/(\d+)', damsire_link['href'])
            profile['damsire_id'] = damsire_id_match.group(1) if damsire_id_match else None
    
    logging.info(f"馬プロフィールパース完了: {horse_id}")
    
    return profile


def parse_horse_performance(file_path: str, horse_id: str = None) -> pd.DataFrame:
    """
    馬の過去成績HTMLをパース
    
    Args:
        file_path: HTMLファイルパス（AJAX APIレスポンスまたは _perf.bin）
        horse_id: 馬ID
    
    Returns:
        過去成績DataFrame
        
    Columns:
        - horse_id: str
        - race_date: str (ISO8601)
        - venue: str (競馬場)
        - weather: str
        - race_number: int
        - race_name: str
        - race_grade: str (G1, G2, G3, OP, 1600万等)
        - head_count: int (頭数)
        - bracket_number: int
        - horse_number: int
        - finish_position: int
        - jockey_name: str
        - basis_weight: float
        - distance_m: int
        - track_surface: str (芝/ダート)
        - track_condition: str (良/稍重/重/不良)
        - finish_time_str: str
        - finish_time_seconds: float
        - margin_str: str
        - margin_seconds: float
        - passing_order: str
        - last_3f_time: float
        - win_odds: float
        - popularity: int
        - horse_weight: int
        - horse_weight_change: int
        - race_id: str
    """
    logging.info(f"馬過去成績パース開始: {file_path}")
    
    if horse_id is None:
        horse_id = extract_horse_id_from_filename(file_path)
    
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    # --- 修正点 ---
    # euc_jp を優先に修正 (profileパーサーと統一)
    # これにより _perf.bin ファイルも正しくデコードされる
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    # --- 修正ここまで ---
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # 過去成績テーブル
    perf_table = soup.find('table', class_='db_h_race_results')
    
    if not perf_table:
        logging.warning(f"過去成績テーブルが見つかりません: {file_path}")
        return pd.DataFrame()
    
    rows = []
    
    tbody = perf_table.find('tbody')
    if not tbody:
        logging.warning(f"成績テーブルにtbodyが見つかりません: {file_path}")
        return pd.DataFrame()
    
    for tr in tbody.find_all('tr'):
        try:
            row_data = parse_horse_performance_row(tr, horse_id)
            if row_data:
                rows.append(row_data)
        except Exception as e:
            logging.warning(f"行のパースエラー: {e} in {file_path}")
            continue
    
    df = pd.DataFrame(rows)
    
    logging.info(f"馬過去成績パース完了: {horse_id} ({len(df)}レース)")
    
    return df


def parse_horse_performance_row(tr, horse_id: str) -> Optional[Dict]:
    """
    過去成績テーブルの1行をパース
    """
    cells = tr.find_all('td')
    
    if len(cells) < 15:
        return None
    
    row_data = {'horse_id': horse_id}
    
    # 日付
    date_text = cells[0].get_text(strip=True)
    row_data['race_date'] = parse_race_date(date_text)
    
    # 競馬場
    venue_text = cells[1].get_text(strip=True)
    row_data['venue'] = venue_text
    
    # 天気
    weather_text = cells[2].get_text(strip=True)
    row_data['weather'] = weather_text
    
    # レース番号
    race_num_text = cells[3].get_text(strip=True)
    row_data['race_number'] = parse_int_or_none(race_num_text.replace('R', ''))
    
    # レース名・レースID
    race_link = cells[4].find('a', href=re.compile(r'/race/\d+'))
    if race_link:
        row_data['race_name'] = race_link.get_text(strip=True)
        race_id_match = re.search(r'/race/(\d+)', race_link['href'])
        row_data['race_id'] = race_id_match.group(1) if race_id_match else None
    else:
        row_data['race_name'] = cells[4].get_text(strip=True)
        row_data['race_id'] = None
    
    # レースグレード（例: G1, OP, 1600万）
    if len(cells) > 5:
        grade_text = cells[5].get_text(strip=True)
        row_data['race_grade'] = grade_text if grade_text else None
    
    # 頭数
    head_count_text = cells[6].get_text(strip=True)
    row_data['head_count'] = parse_int_or_none(head_count_text.replace('頭', ''))
    
    # 枠番
    bracket_text = cells[7].get_text(strip=True)
    row_data['bracket_number'] = parse_int_or_none(bracket_text)
    
    # 馬番
    horse_num_text = cells[8].get_text(strip=True)
    row_data['horse_number'] = parse_int_or_none(horse_num_text)
    
    # 着順
    finish_text = cells[9].get_text(strip=True)
    row_data['finish_position'] = parse_int_or_none(finish_text)
    
    # 騎手名
    jockey_text = cells[10].get_text(strip=True)
    row_data['jockey_name'] = jockey_text
    
    # 斤量
    weight_text = cells[11].get_text(strip=True)
    row_data['basis_weight'] = parse_float_or_none(weight_text)
    
    # 距離・馬場
    distance_text = cells[12].get_text(strip=True)
    distance, surface, condition = parse_distance_surface(distance_text)
    row_data['distance_m'] = distance
    row_data['track_surface'] = surface
    row_data['track_condition'] = condition
    
    # タイム
    time_text = cells[13].get_text(strip=True)
    row_data['finish_time_str'] = time_text
    row_data['finish_time_seconds'] = parse_time_to_seconds(time_text)
    
    # 着差
    margin_text = cells[14].get_text(strip=True)
    row_data['margin_str'] = margin_text
    row_data['margin_seconds'] = parse_margin_to_seconds(margin_text)
    
    # 通過順
    if len(cells) > 15:
        passing_text = cells[15].get_text(strip=True)
        row_data['passing_order'] = passing_text
    
    # 上がり3F
    if len(cells) > 16:
        last_3f_text = cells[16].get_text(strip=True)
        row_data['last_3f_time'] = parse_float_or_none(last_3f_text)
    
    # 単勝オッズ
    if len(cells) > 17:
        odds_text = cells[17].get_text(strip=True)
        row_data['win_odds'] = parse_float_or_none(odds_text)
    
    # 人気
    if len(cells) > 18:
        popularity_text = cells[18].get_text(strip=True)
        row_data['popularity'] = parse_int_or_none(popularity_text)
    
    # 馬体重
    if len(cells) > 19:
        weight_change_text = cells[19].get_text(strip=True)
        horse_weight, weight_change = parse_horse_weight(weight_change_text)
        row_data['horse_weight'] = horse_weight
        row_data['horse_weight_change'] = weight_change
    
    return row_data


def parse_birth_date(birth_text: str) -> str:
    """
    生年月日文字列をISO8601に変換
    例: "2020年3月15日" → "2020-03-15"
    """
    if not birth_text:
        return None
    
    match = re.search(r'(\d{4})年(\d+)月(\d+)日', birth_text)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)
        day = match.group(3).zfill(2)
        return f"{year}-{month}-{day}"
    
    return None


def parse_race_date(date_text: str) -> str:
    """
    レース日付文字列をISO8601に変換
    例: "2023/06/01" → "2023-06-01"
    """
    if not date_text:
        return None
    
    # スラッシュをハイフンに置換
    date_normalized = date_text.replace('/', '-')
    
    # YYYY-MM-DD形式の検証
    match = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_normalized)
    if match:
        return date_normalized
    
    return None


def parse_distance_surface(distance_text: str) -> tuple:
    """
    距離・馬場文字列をパース
    例: "芝1600" → (1600, "芝", None)
    例: "ダ1800良" → (1800, "ダート", "良")
    
    Returns:
        (距離, 馬場種別, 馬場状態)
    """
    if not distance_text:
        return (None, None, None)
    
    # パターン1: "芝1600良"
    # "障" (障害) も考慮する場合は r'(障|芝|ダ)' のように修正
    match = re.match(r'(芝|ダ)(\d+)(良|稍重|重|不良)?', distance_text)
    if match:
        surface_abbr = match.group(1)
        distance = int(match.group(2))
        condition = match.group(3) if match.group(3) else None
        
        surface = "芝" if surface_abbr == "芝" else "ダート"
        
        return (distance, surface, condition)
    
    # パターン2: "1600" (馬場種別なし)
    match = re.match(r'(\d+)', distance_text)
    if match:
        distance = int(match.group(1))
        return (distance, None, None)
    
    return (None, None, None)


def extract_horse_id_from_filename(file_path: str) -> str:
    filename = Path(file_path).stem
    
    # 優先順位1: 新形式 (horse_/ped_プレフィックス)
    match = re.search(r'(?:horse|ped)_([a-zA-Z0-9]{10})', filename)
    if match:
        return match.group(1)
    
    # 優先順位2: 旧形式 (先頭が10桁の数字)
    match = re.match(r'^(\d{10})', filename)
    if match:
        return match.group(1)
    
    # フォールバック
    logging.warning(f"ID抽出失敗: {filename}")
    return None
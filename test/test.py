# -*- coding: utf-8 -*-
"""
競馬データ抽出スクリプト (修正版)
提供された.bin (HTML) ファイルを解析し、データを抽出します。

実行方法:
1. `beautifulsoup4`, `lxml`, `pandas` をインストールしてください。
   pip install beautifulsoup4 lxml pandas
2. .binファイル群 (202001010101.bin など) と同じディレクトリにこのスクリプトを配置します。
3. ターミナルで `python test.py` を実行します。
4. 各データ（レース結果、出馬表、馬プロフィールなど）がCSVファイルとして出力されます。
"""

import os
import re
import pandas as pd
from bs4 import BeautifulSoup
from typing import Dict, Any, Optional, List, Tuple

# --- ユーティリティ関数 ---

def parse_int_or_none(value: Optional[str]) -> Optional[int]:
    """文字列を整数に変換。失敗時はNoneを返す"""
    if value is None:
        return None
    value_cleaned = re.sub(r'[^\d]', '', value)
    return int(value_cleaned) if value_cleaned else None

def parse_float_or_none(value: Optional[str]) -> Optional[float]:
    """文字列を浮動小数点数に変換。失敗時はNoneを返す"""
    if value is None:
        return None
    value_cleaned = re.sub(r'[^\d.]', '', value)
    try:
        return float(value_cleaned)
    except ValueError:
        return None

def safe_strip(value: Optional[str]) -> Optional[str]:
    """Noneでない場合のみstripを実行"""
    if value is None:
        return None
    return value.strip()

def parse_horse_weight(weight_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """ "476(0)" のような文字列から馬体重と増減を抽出 """
    if not weight_str:
        return None, None
    
    match = re.search(r'(\d+)\(([-+]?\d+)\)', weight_str)
    if match:
        weight = parse_int_or_none(match.group(1))
        
        # --- 修正箇所 START ---
        # 元のコード:
        # change = parse_int_or_none(match.group(2))
        
        # 修正後:
        # parse_int_or_none はマイナス記号を削除してしまうため、
        # int() を直接呼び出して "-24" や "+6" を正しくパースする。
        change_str = match.group(2)
        try:
            change = int(change_str)
        except (ValueError, TypeError):
            change = None
        # --- 修正箇所 END ---

        return weight, change
    
    weight_only = parse_int_or_none(weight_str)
    return weight_only, None

def get_id_from_href(href: Optional[str], pattern: str) -> Optional[str]:
    """
    /horse/2009100502/ や /jockey/result/recent/01170/ からIDを抽出
    [修正]: 複雑なURLパス (例: /jockey/result/recent/ID/) に対応
    """
    if not href:
        return None
    # 修正: /jockey/result/recent/ID/ のような形式にも対応
    match = re.search(rf'/{pattern}/(?:[\w/]+/)?(\w+)', href)
    return match.group(1) if match else None

def parse_distance(dist_str: Optional[str]) -> Tuple[Optional[int], Optional[str]]:
    """ "ダ1200" や "障2860" から距離と馬場種別を抽出 """
    if not dist_str:
        return None, None
    
    # "障 2860" のようにスペースが入る場合も考慮
    dist_str_cleaned = re.sub(r'\s', '', dist_str)
    # [修正]: "芝右1800m" のように "右" などが含まれる場合に対応
    match = re.search(r'(芝|ダ|障)(?:右|左|)?(\d+)', dist_str_cleaned)
    
    if match:
        surface_char = match.group(1)
        distance = parse_int_or_none(match.group(2))
        
        surface_map = {
            '芝': '芝',
            'ダ': 'ダート',
            '障': '障害'
        }
        surface = surface_map.get(surface_char)
        return distance, surface
    return None, None

# --- 1. レース結果パーサー (202001010101.bin) ---

def parse_race_result(soup: BeautifulSoup, race_id: str) -> pd.DataFrame:
    """
    レース結果テーブル (`race_table_01`) を解析する
    """
    
    # --- A, B, C: レースメタ情報の抽出 ---
    metadata = extract_race_metadata(soup) # [修正] メタデータ抽出関数を先に呼ぶ
    
    result_table = soup.find('table', class_='race_table_01')
    if not result_table:
        print(f"  [!] レース結果テーブル (race_table_01) が見つかりません。 (race_id: {race_id})")
        return pd.DataFrame()

    rows_data = []
    
    rows = result_table.find_all('tr')
    if not rows:
        print(f"  [!] レース結果テーブルに tr が見つかりません。 (race_id: {race_id})")
        return pd.DataFrame()

    # ヘッダー行 (thを含む) を除外し、データ行 (tdを含む) のみを取得
    data_rows = [row for row in rows if row.find('td')]
    
    if not data_rows:
        print(f"  [!] レース結果テーブルにデータ行 (td) が見つかりません。 (race_id: {race_id})")
        return pd.DataFrame()

    # D. 頭数を取得
    metadata['head_count'] = len(data_rows)
    
    for row in data_rows:
        cols = row.find_all('td')
        if len(cols) < 18:  # データが不十分な行はスキップ
            continue
            
        row_data = metadata.copy()
        row_data['race_id'] = race_id
        
        try:
            row_data['finish_position'] = parse_int_or_none(cols[0].get_text())
            row_data['bracket_number'] = parse_int_or_none(cols[1].get_text())
            row_data['horse_number'] = parse_int_or_none(cols[2].get_text())
            
            horse_link = cols[3].find('a')
            row_data['horse_id'] = get_id_from_href(horse_link.get('href'), 'horse')
            row_data['horse_name'] = safe_strip(horse_link.get_text())
            
            row_data['sex_age'] = safe_strip(cols[4].get_text())
            row_data['basis_weight'] = parse_float_or_none(cols[5].get_text())
            
            jockey_link = cols[6].find('a')
            row_data['jockey_id'] = get_id_from_href(jockey_link.get('href'), 'jockey')
            row_data['jockey_name'] = safe_strip(jockey_link.get_text())
            
            row_data['finish_time_str'] = safe_strip(cols[7].get_text())
            row_data['margin_str'] = safe_strip(cols[8].get_text())
            
            # タイム指数、通過順、上がり (インデックスがズレる可能性があるため存在確認)
            row_data['passing_order'] = safe_strip(cols[10].get_text())
            row_data['last_3f_time'] = parse_float_or_none(cols[11].get_text())
            
            row_data['win_odds'] = parse_float_or_none(cols[12].get_text())
            row_data['popularity'] = parse_int_or_none(cols[13].get_text())
            
            weight_str = safe_strip(cols[14].get_text())
            row_data['horse_weight'], row_data['horse_weight_change'] = parse_horse_weight(weight_str)
            
            # 列のインデックスが可変 (16-18 or 19-21)
            # 調教師
            trainer_link = cols[15].find('a') or (cols[18].find('a') if len(cols) > 18 else None)
            if trainer_link:
                row_data['trainer_id'] = get_id_from_href(trainer_link.get('href'), 'trainer')
            
            # 馬主
            owner_link = cols[16].find('a') or (cols[19].find('a') if len(cols) > 19 else None)
            if owner_link:
                row_data['owner_name'] = safe_strip(owner_link.get('title'))
            
            # 賞金
            prize_col_text = safe_strip(cols[17].get_text())
            if (prize_col_text is None or not prize_col_text.strip()) and len(cols) > 20:
                prize_col_text = safe_strip(cols[20].get_text())
            
            prize_to_parse = prize_col_text.replace(',', '') if prize_col_text else None
            row_data['prize_money'] = parse_float_or_none(prize_to_parse)

            rows_data.append(row_data)

        except (AttributeError, IndexError) as e:
            print(f"  [!] レース結果行の解析エラー (race_id: {race_id}): {e} - 行をスキップします。")

    return pd.DataFrame(rows_data)

def extract_race_metadata(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    [修正] レース結果ページ上部のメタデータを抽出
    `202001010101.bin` [cite: 1131] のHTML構造に合わせてセレクタを修正
    """
    
    metadata = {
        'distance_m': None, 'track_surface': None, 'weather': None,
        'track_condition': None, 'post_time': None, 'race_name': None,
        'prize_2nd': None, 'prize_3rd': None, 'prize_4th': None, 'prize_5th': None,
        'venue': None, 'day_of_meeting': None, 'round_of_year': None
    }
    
    # A-1. レース基本情報 (data_intro > diary_snap_cut > span)
    # [修正] セレクタを 'div.RaceData01' から 'div.data_intro' 内の 'span' [cite: 1146] に変更
    race_data_span = soup.select_one('div.data_intro diary_snap_cut > span')
    
    if race_data_span:
        race_data01_str = race_data_span.get_text() # [cite: 1146]
        
        # 距離と馬場 (例: "芝右1800m") [cite: 1146]
        distance_match = re.search(r'(芝|ダ|障)(?:右|左|)?(\d+)m', race_data01_str)
        if distance_match:
            surface_char = distance_match.group(1)
            if surface_char == '芝':
                metadata['track_surface'] = '芝'
            elif surface_char == 'ダ':
                metadata['track_surface'] = 'ダート'
            elif surface_char == '障':
                metadata['track_surface'] = '障害'
            metadata['distance_m'] = parse_int_or_none(distance_match.group(2))

        # 天候 (例: "天候 : 曇") [cite: 1146]
        weather_match = re.search(r'天候\s*:\s*(\S+)', race_data01_str)
        if weather_match:
            metadata['weather'] = weather_match.group(1)

        # 馬場状態 (例: "芝 : 良") [cite: 1146]
        condition_match = re.search(r'(?:芝|ダート)\s*:\s*(\S+)', race_data01_str)
        if condition_match:
            metadata['track_condition'] = condition_match.group(1)
            
        # 発走時刻 (例: "発走 : 09:55") [cite: 1146]
        time_match = re.search(r'発走\s*:\s*(\d{1,2}:\d{2})', race_data01_str)
        if time_match:
            metadata['post_time'] = time_match.group(1)

    # A-2. レース名・賞金
    # [修正] セレクタを 'h1.RaceName' から 'dl.racedata h1' [cite: 1146] に変更 
    race_name_tag = soup.select_one('dl.racedata h1')
    if race_name_tag:
        metadata['race_name'] = safe_strip(race_name_tag.get_text())

    # [修正] 'div.RaceData02' は 202001010101.bin [cite: 1131] に存在しないため、ロジックは残すが抽出はされない
    race_data02 = soup.find('div', class_='RaceData02')
    if race_data02:
        prize_match = re.search(r'本賞金:([\d,]+)万円', race_data02.get_text())
        if prize_match:
            prizes = [parse_int_or_none(p) for p in prize_match.group(1).split(',')]
            if len(prizes) >= 2: metadata['prize_2nd'] = prizes[1]
            if len(prizes) >= 3: metadata['prize_3rd'] = prizes[2]
            if len(prizes) >= 4: metadata['prize_4th'] = prizes[3]
            if len(prizes) >= 5: metadata['prize_5th'] = prizes[4]

    # A-3. 開催情報 (smalltxt)
    smalltxt = soup.find('p', class_='smalltxt') # [cite: 1146]
    if smalltxt:
        match = re.search(r'(\d+)回(\S+?)(\d+)日目', smalltxt.get_text())
        if match:
            metadata['round_of_year'] = parse_int_or_none(match.group(1))
            metadata['venue'] = match.group(2)
            metadata['day_of_meeting'] = parse_int_or_none(match.group(3))

    return metadata


# --- 2. 出馬表パーサー (202001010102.bin) ---

def extract_shutuba_metadata(soup: BeautifulSoup) -> Dict[str, Any]:
    """[新規] 出馬表ページ上部のメタデータを抽出 (schema.md B-3)"""
    metadata = {
        'distance_m': None, 'track_surface': None, 'weather': None,
        'track_condition': None, 'post_time': None
    }
    
    # 202001010102.bin [cite: 1]
    race_data01 = soup.find('div', class_='RaceData01')
    if race_data01:
        race_data01_str = race_data01.get_text()
        
        # 距離と馬場 (例: "ダ1700m") [cite: 1]
        distance_match = re.search(r'(芝|ダ|障)(?:右|左|)?(\d+)m', race_data01_str)
        if distance_match:
            surface_char = distance_match.group(1)
            if surface_char == '芝':
                metadata['track_surface'] = '芝'
            elif surface_char == 'ダ':
                metadata['track_surface'] = 'ダート'
            elif surface_char == '障':
                metadata['track_surface'] = '障害'
            metadata['distance_m'] = parse_int_or_none(distance_match.group(2))

        # 天候 (例: "天候:曇") [cite: 1]
        weather_match = re.search(r'天候:(\S+)', race_data01_str)
        if weather_match:
            metadata['weather'] = weather_match.group(1).strip()

        # 馬場状態 (例: "馬場:良") [cite: 1]
        # 202001010102.bin [cite: 1] では "馬場:良" が span の中にある
        condition_match = re.search(r'馬場:(\S+)', race_data01_str)
        if not condition_match:
             condition_span = race_data01.find('span', class_='Item04')
             if condition_span:
                 condition_match = re.search(r'馬場:(\S+)', condition_span.get_text())
        
        if condition_match:
            metadata['track_condition'] = condition_match.group(1).strip()
            
        # 発走時刻 (例: "10:25発走") [cite: 1]
        time_match = re.search(r'(\d{1,2}:\d{2})発走', race_data01_str)
        if time_match:
            metadata['post_time'] = time_match.group(1)

    return metadata

def parse_shutuba(soup: BeautifulSoup, race_id: str) -> pd.DataFrame:
    """
    出馬表テーブル (`Shutuba_Table`) を解析する
    """
    # --- [修正] B-3: メタデータ抽出 ---
    metadata = extract_shutuba_metadata(soup)
    
    shutuba_table = soup.find('table', class_='Shutuba_Table') # [cite: 279]
    if not shutuba_table:
        print(f"  [!] 出馬表テーブル (Shutuba_Table) が見つかりません。 (race_id: {race_id})")
        return pd.DataFrame()

    rows_data = []
    rows = shutuba_table.find_all('tr', class_='HorseList') # [cite: 283]
    
    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 10:
            continue
            
        try:
            # [修正] メタデータをマージ
            row_data = metadata.copy()
            row_data['race_id'] = race_id
            
            row_data['bracket_number'] = parse_int_or_none(cols[0].get_text())
            row_data['horse_number'] = parse_int_or_none(cols[1].get_text())
            
            # cols[2] は印 (schema.md B-2) だが、202001010102.bin [cite: 283] では入力欄
            
            horse_info_cell = cols[3]
            horse_link = horse_info_cell.find('a')
            row_data['horse_id'] = get_id_from_href(horse_link.get('href'), 'horse')
            row_data['horse_name'] = safe_strip(horse_link.get_text())
            
            row_data['sex_age'] = safe_strip(cols[4].get_text())
            row_data['basis_weight'] = parse_float_or_none(cols[5].get_text())
            
            jockey_link = cols[6].find('a')
            row_data['jockey_id'] = get_id_from_href(jockey_link.get('href'), 'jockey')
            row_data['jockey_name'] = safe_strip(jockey_link.get_text())

            trainer_link = cols[7].find('a')
            row_data['trainer_id'] = get_id_from_href(trainer_link.get('href'), 'trainer')
            row_data['trainer_name'] = safe_strip(trainer_link.get('title'))

            weight_str = safe_strip(cols[8].get_text())
            row_data['horse_weight'], row_data['horse_weight_change'] = parse_horse_weight(weight_str)
            
            row_data['morning_odds'] = parse_float_or_none(cols[9].get_text())
            row_data['morning_popularity'] = parse_int_or_none(cols[10].get_text())
            
            # B-1. 馬具情報 (schema.md)
            row_data['blinkers'] = bool(horse_info_cell.find('span', class_='Blinker'))

            rows_data.append(row_data)

        except (AttributeError, IndexError) as e:
            print(f"  [!] 出馬表行の解析エラー (race_id: {race_id}): {e} - 行をスキップします。")

    return pd.DataFrame(rows_data)


# --- 3. 馬プロフィールパーサー (2009100502_profile.bin) ---

def parse_horse_profile(soup: BeautifulSoup, horse_id: str) -> Dict[str, Any]:
    """
    馬プロフィールテーブル (`db_prof_table`) [cite: 178] を解析する
    """
    # [修正] schema.md C-1, C-2 および新規項目に合わせて、抽出対象カラムをすべてNoneで初期化
    profile_data = {
        'horse_id': horse_id,
        'horse_name': None, 'sex': None, 'coat_color': None,
        'birth_date': None, 'trainer_id': None, 'trainer_name': None,
        'owner_name': None, 'breeder_name': None, 'producing_area': None,
        'sale_price': None, 'height_cm': None, 'chest_girth_cm': None,
        'cannon_bone_cm': None, 'sire_id': None, 'sire_name': None,
        'dam_id': None, 'dam_name': None, 'damsire_id': None, 'damsire_name': None,
        'prize_central': None, 'prize_regional': None, 'career_summary': None,
        'main_wins': None, 'relatives': None
    }
    
    # 馬名、性別、毛色
    horse_title_div = soup.find('div', class_='horse_title') # [cite: 184]
    if horse_title_div:
        profile_data['horse_name'] = safe_strip(horse_title_div.find('h1').get_text())
        profile_text_tag = horse_title_div.find('p', class_='txt_01')
        if profile_text_tag:
            profile_text = profile_text_tag.get_text() # [cite: 184]
            sex_match = re.search(r'(牡|牝|セ)', profile_text)
            color_match = re.search(r'(鹿毛|黒鹿毛|青鹿毛|青毛|芦毛|栗毛|栃栗毛|白毛)', profile_text)
            
            if sex_match: profile_data['sex'] = sex_match.group(1)
            if color_match: profile_data['coat_color'] = color_match.group(1)

    # メインテーブル
    prof_table = soup.find('table', class_='db_prof_table') # [cite: 220]
    if not prof_table:
        print(f"  [!] 馬プロフィールテーブル (db_prof_table) が見つかりません。 (horse_id: {horse_id})")
        return profile_data

    # [修正] 新規項目をth_mapに追加
    th_map = {
        '生年月日': 'birth_date',
        '調教師': 'trainer_name',
        '馬主': 'owner_name',
        '生産者': 'breeder_name',
        '産地': 'producing_area',
        'セリ取引価格': 'sale_price',
        '馬体高(cm)': 'height_cm',
        '胸囲(cm)': 'chest_girth_cm',
        '管囲(cm)': 'cannon_bone_cm',
        '獲得賞金(中央)': 'prize_central',
        '獲得賞金(地方)': 'prize_regional',
        '通算成績': 'career_summary',
        '主な勝鞍': 'main_wins',
        '近親馬': 'relatives',
    }

    for row in prof_table.find_all('tr'):
        th = row.find('th')
        td = row.find('td')
        if not th or not td:
            continue
            
        th_text = safe_strip(th.get_text())
        if th_text in th_map:
            key = th_map[th_text]
            # [修正] 改行や余分なスペースを整理
            value = ' '.join(td.get_text(strip=True).split())
            
            link = td.find('a')
            if key == 'trainer_name' and link:
                profile_data['trainer_id'] = get_id_from_href(link.get('href'), 'trainer')
                profile_data[key] = safe_strip(link.get_text())
            elif key == 'owner_name' and link:
                 profile_data[key] = safe_strip(link.get_text())
            # [修正] セリ取引価格が '-' の場合は文字列として保持
            elif key == 'sale_price':
                profile_data[key] = value if value == '-' else parse_int_or_none(value)
            elif key in ['prize_central', 'prize_regional']:
                profile_data[key] = parse_int_or_none(value)
            elif key in ['height_cm', 'chest_girth_cm']:
                profile_data[key] = parse_int_or_none(value)
            elif key == 'cannon_bone_cm':
                profile_data[key] = parse_float_or_none(value)
            # [修正] 近親馬の場合、複数のリンクを結合
            elif key == 'relatives':
                 profile_data[key] = ' | '.join([safe_strip(a.get_text()) for a in td.find_all('a')])
            elif link: # 生産者など
                 profile_data[key] = safe_strip(link.get_text())
            else:
                profile_data[key] = value

    # 血統テーブル (簡易)
    ped_box = soup.find('span', id='horse_pedigree_box')
    if ped_box:
        ped_table = ped_box.find('table')
        if ped_table:
            for row in ped_table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if not th or not td:
                    continue
                
                th_text = safe_strip(th.get_text())
                link = td.find('a')
                if not link:
                    continue
                    
                href = link.get('href')
                if '父' == th_text:
                    profile_data['sire_id'] = get_id_from_href(href, 'horse')
                    profile_data['sire_name'] = safe_strip(link.get_text())
                elif '母' == th_text:
                    profile_data['dam_id'] = get_id_from_href(href, 'horse')
                    profile_data['dam_name'] = safe_strip(link.get_text())
                elif '母父' == th_text:
                    profile_data['damsire_id'] = get_id_from_href(href, 'horse')
                    profile_data['damsire_name'] = safe_strip(link.get_text())

    return profile_data


# --- 4. 馬過去成績パーサー (2009100502_perf.bin) ---

def parse_horse_performance(soup: BeautifulSoup, horse_id: str) -> pd.DataFrame:
    """
    馬の過去成績テーブル (`db_h_race_results`) を解析する
    [修正]: カラムインデックスのずれを `2009100502_perf.bin` [cite: 265] に合わせて修正
    """
    perf_table = soup.find('table', class_='db_h_race_results') # [cite: 265]
    if not perf_table:
        print(f"  [!] 馬過去成績テーブル (db_h_race_results) が見つかりません。 (horse_id: {horse_id})")
        return pd.DataFrame()

    rows_data = []
    tbody = perf_table.find('tbody')
    if not tbody:
        return pd.DataFrame()
        
    rows = tbody.find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        # [修正] HTML [cite: 269] に基づき、最低カラム数を馬体重(index 24)のある 25 に設定
        if len(cols) < 25:
            continue
            
        try:
            row_data = {'horse_id': horse_id}
            
            row_data['race_date'] = safe_strip(cols[0].get_text()) # [cite: 271]
            row_data['venue'] = safe_strip(cols[1].get_text()) # [cite: 271]
            row_data['weather'] = safe_strip(cols[2].get_text()) # [cite: 271]
            row_data['race_number'] = parse_int_or_none(cols[3].get_text()) # [cite: 271]
            
            race_link = cols[4].find('a') # [cite: 272]
            if race_link:
                row_data['race_name'] = safe_strip(race_link.get_text())
                row_data['race_id'] = get_id_from_href(race_link.get('href'), 'race')
            
            # cols[5] (映像) はスキップ
            row_data['head_count'] = parse_int_or_none(cols[6].get_text()) # [cite: 273]
            row_data['bracket_number'] = parse_int_or_none(cols[7].get_text()) # [cite: 273]
            row_data['horse_number'] = parse_int_or_none(cols[8].get_text()) # [cite: 274]
            row_data['win_odds'] = parse_float_or_none(cols[9].get_text()) # [cite: 274]
            row_data['popularity'] = parse_int_or_none(cols[10].get_text()) # [cite: 274]
            row_data['finish_position'] = parse_int_or_none(cols[11].get_text()) # [cite: 274]
            
            # 騎手名 (cols[12]) 
            jockey_cell = cols[12]
            jockey_link = jockey_cell.find('a')
            row_data['jockey_name'] = safe_strip(jockey_cell.get_text())
            if jockey_link:
                 row_data['jockey_id'] = get_id_from_href(jockey_link.get('href'), 'jockey') # [修正] get_id_from_href()で対応
            
            row_data['basis_weight'] = parse_float_or_none(cols[13].get_text()) # [cite: 275]
            
            dist_str = safe_strip(cols[14].get_text()) # [cite: 276]
            row_data['distance_m'], row_data['track_surface'] = parse_distance(dist_str)
            
            # cols[15] (水分量) はスキップ
            row_data['track_condition'] = safe_strip(cols[16].get_text()) # [cite: 276] [修正] インデックス 15 -> 16
            # cols[17] (馬場指数) はスキップ
            row_data['finish_time_str'] = safe_strip(cols[18].get_text()) # [cite: 278] [修正] インデックス 17 -> 18
            row_data['margin_str'] = safe_strip(cols[19].get_text()) # [cite: 278] [修正] インデックス 18 -> 19
            
            # cols[20] (タイム指数) はスキップ
            
            # 通過順 (cols[21]) [cite: 280]
            if len(cols) > 21:
                row_data['passing_order'] = safe_strip(cols[21].get_text()) # [修正] インデックス 20 -> 21
            
            # cols[22] (ペース) はスキップ

            # 上がり3F (cols[23]) [cite: 280]
            if len(cols) > 23:
                row_data['last_3f_time'] = parse_float_or_none(cols[23].get_text()) # [修正] インデックス 22 -> 23
            
            # 馬体重 (cols[24]) 
            if len(cols) > 24:
                weight_str = safe_strip(cols[24].get_text()) # [修正] インデックス 23 -> 24
                row_data['horse_weight'], row_data['horse_weight_change'] = parse_horse_weight(weight_str)
            
            # cols[25] (厩舎コメント), cols[26] (備考) はスキップ [cite: 282]
            
            # 勝ち馬 (cols[27]) [cite: 283]
            if len(cols) > 27:
                winner_link = cols[27].find('a') # [修正] インデックス 26 -> 27
                if winner_link:
                    winner_text = winner_link.get_text()
                    if winner_text:
                        row_data['winner_name'] = safe_strip(winner_text.replace('(', '').replace(')', ''))

            rows_data.append(row_data)

        except (AttributeError, IndexError) as e:
            print(f"  [!] 馬過去成績行の解析エラー (horse_id: {horse_id}): {e} - 行をスキップします。")

    return pd.DataFrame(rows_data)


# --- 5. 血統パーサー (2009100502.bin) ---

def parse_pedigree(soup: BeautifulSoup, horse_id: str) -> pd.DataFrame:
    """
    血統テーブル (`blood_table`) を解析する
    """
    blood_table = soup.find('table', class_='blood_table') # [cite: 1175]
    if not blood_table:
        print(f"  [!] 血統テーブル (blood_table) が見つかりません。 (horse_id: {horse_id})")
        return pd.DataFrame()

    pedigree_data = []
    
    try:
        generation_1_cells = blood_table.find_all('td', attrs={'rowspan': '16'})
        generation_2_cells = blood_table.find_all('td', attrs={'rowspan': '8'})
        generation_3_cells = blood_table.find_all('td', attrs={'rowspan': '4'})
        generation_4_cells = blood_table.find_all('td', attrs={'rowspan': '2'})
        
        all_cells_gen5 = blood_table.find_all('td', attrs={'rowspan': None})
        generation_5_cells = [
            cell for cell in all_cells_gen5 
            if cell.find('a', href=re.compile(r'/horse/'))
        ]

        all_cells_by_gen = {
            1: generation_1_cells, # 2セル
            2: generation_2_cells, # 4セル
            3: generation_3_cells, # 8セル
            4: generation_4_cells, # 16セル
            5: generation_5_cells  # 32セル
        }

        for gen, cells in all_cells_by_gen.items():
            if not cells:
                 print(f"  [!] 警告: {gen}代目の血統セルが見つかりません。 (horse_id: {horse_id})")
                 continue

            for cell in cells:
                try:
                    data = {'horse_id': horse_id, 'generation': gen}
                    link = cell.find('a', href=re.compile(r'/horse/'))
                    
                    if not link:
                        continue

                    data['ancestor_id'] = get_id_from_href(link.get('href'), 'horse')
                    data['ancestor_name'] = safe_strip(link.get_text())
                    
                    # E-1. 生年と毛色 (schema.md)
                    details_text = ""
                    # [修正] 世代 1, 4 などは span がなく <br> で区切られている 
                    # cell.get_text() で子要素のテキストをすべて結合する
                    details_text = cell.get_text(separator=' ')

                    if details_text:
                        # [修正] "1970 鹿毛" や "1958 黒鹿毛" [cite: 1215] などを探す
                        match = re.search(r'(\d{4})\s+([^\s]+毛)', details_text)
                        
                        if match:
                            data['ancestor_birth_year'] = parse_int_or_none(match.group(1))
                            data['ancestor_coat_color'] = match.group(2).strip()
                    
                    if 'ancestor_id' in data:
                        pedigree_data.append(data)
                
                except (AttributeError, IndexError) as e:
                    print(f"  [!] 血統セルの内部解析エラー (horse_id: {horse_id}): {e} - セルをスキップします。")
    
    except Exception as e:
         print(f"  [!] 血統テーブル全体の解析エラー (horse_id: {horse_id}): {e}")
         return pd.DataFrame()

    # 重複を除去
    df = pd.DataFrame(pedigree_data)
    if not df.empty:
        # [修正] 生年 (ancestor_birth_year) が None の行を後回しにしてから重複除去
        # これにより、Hail to Reason (4代目) [cite: 1215] のように、片方にしか情報がない場合  でも情報を残す
        df = df.sort_values(by='ancestor_birth_year', ascending=False, na_position='last')
        df = df.drop_duplicates(subset=['horse_id', 'generation', 'ancestor_id'], keep='first')
    return df


# --- メイン実行処理 ---

def load_soup(filepath: str) -> Optional[BeautifulSoup]:
    """
    EUC-JPでエンコードされたHTMLファイルを読み込み、BeautifulSoupオブジェクトを返す
    """
    print(f"--- ファイル読み込み中: {filepath} ---")
    if not os.path.exists(filepath):
        print(f"  [!] ファイルが存在しません: {filepath}")
        return None
        
    try:
        with open(filepath, 'rb') as f:
            # EUC-JPでデコード
            html_content = f.read().decode('euc-jp', errors='replace')
        
        # HTML5パーサー (lxml) を使用
        soup = BeautifulSoup(html_content, 'lxml')
        return soup
    except Exception as e:
        print(f"  [!] ファイル読み込みまたは解析エラー (EUC-JP): {e}")
        return None

def main():
    """
    すべての.binファイルを解析し、CSVとして保存する
    """
    # [修正] 動作確認のため、ファイル名を test ディレクトリ配下 (アップロードされたパス) に変更
    base_dir = "test"
    files_to_parse = [
        {"type": "race_result", "path": os.path.join(base_dir, "202001010101.bin")},
        {"type": "shutuba", "path": os.path.join(base_dir, "202001010102.bin")},
        {"type": "horse_profile", "path": os.path.join(base_dir, "2009100502_profile.bin")},
        {"type": "horse_performance", "path": os.path.join(base_dir, "2009100502_perf.bin")},
        {"type": "pedigree", "path": os.path.join(base_dir, "2009100502.bin")},
    ]

    all_data = {
        "races": [],
        "shutuba": [],
        "horses": [],
        "horses_performance": [],
        "pedigrees": []
    }
    
    # [修正] 出力先ディレクトリを作成
    output_dir = "test/test_output"
    os.makedirs(output_dir, exist_ok=True)

    for file_info in files_to_parse:
        file_type = file_info["type"]
        filepath = file_info["path"]
        
        soup = load_soup(filepath)
        if not soup:
            continue
            
        # ファイル名からIDを抽出 (例: 202001010101.bin -> 202001010101)
        # [修正] os.path.basename を使用してファイル名部分のみを取得
        base_id = os.path.basename(filepath).split('.')[0].split('_')[0]

        if file_type == "race_result":
            df = parse_race_result(soup, base_id)
            all_data["races"].append(df)
            print(f"  [+] レース結果を解析完了 (race_id: {base_id}), {len(df)}行")
        
        elif file_type == "shutuba":
            df = parse_shutuba(soup, base_id)
            all_data["shutuba"].append(df)
            print(f"  [+] 出馬表を解析完了 (race_id: {base_id}), {len(df)}行")

        elif file_type == "horse_profile":
            profile_dict = parse_horse_profile(soup, base_id)
            all_data["horses"].append(profile_dict)
            print(f"  [+] 馬プロフィールを解析完了 (horse_id: {base_id})")

        elif file_type == "horse_performance":
            df = parse_horse_performance(soup, base_id)
            all_data["horses_performance"].append(df)
            print(f"  [+] 馬過去成績を解析完了 (horse_id: {base_id}), {len(df)}行")

        elif file_type == "pedigree":
            df = parse_pedigree(soup, base_id)
            all_data["pedigrees"].append(df)
            print(f"  [+] 血統を解析完了 (horse_id: {base_id}), {len(df)}行")
            
    # --- データの結合と保存 ---
    print(f"\n--- 全データの解析完了。CSVファイルに保存します (出力先: {output_dir}) ---")

    if all_data["races"]:
        races_df = pd.concat(all_data["races"], ignore_index=True)
        races_df = races_df.drop_duplicates(subset=['race_id', 'horse_id'])
        races_df.to_csv(os.path.join(output_dir, "races.csv"), index=False, encoding='utf-8-sig')
        print(f"  [>] races.csv (計 {len(races_df)} 行)")

    if all_data["shutuba"]:
        shutuba_df = pd.concat(all_data["shutuba"], ignore_index=True)
        shutuba_df = shutuba_df.drop_duplicates(subset=['race_id', 'horse_id'])
        shutuba_df.to_csv(os.path.join(output_dir, "shutuba.csv"), index=False, encoding='utf-8-sig')
        print(f"  [>] shutuba.csv (計 {len(shutuba_df)} 行)")

    if all_data["horses"]:
        horses_df = pd.DataFrame(all_data["horses"])
        # [修正] horses.csv に schema.md C-1 と新規カラムが必ず含まれるようにする
        all_horse_cols = [
            'horse_id', 'horse_name', 'sex', 'coat_color', 'birth_date', 
            'trainer_id', 'trainer_name', 'owner_name', 'breeder_name', 
            'producing_area', 'sale_price', 'height_cm', 'chest_girth_cm',
            'cannon_bone_cm', 'sire_id', 'sire_name', 'dam_id', 'dam_name', 
            'damsire_id', 'damsire_name', 'prize_central', 'prize_regional',
            'career_summary', 'main_wins', 'relatives'
        ]
        # 存在しないカラムを None で追加
        for col in all_horse_cols:
            if col not in horses_df.columns:
                horses_df[col] = None
        # カラムの順序を整える
        horses_df = horses_df[all_horse_cols]
        
        horses_df = horses_df.drop_duplicates(subset=['horse_id'])
        horses_df.to_csv(os.path.join(output_dir, "horses.csv"), index=False, encoding='utf-8-sig')
        print(f"  [>] horses.csv (計 {len(horses_df)} 行)")

    if all_data["horses_performance"]:
        horses_perf_df = pd.concat(all_data["horses_performance"], ignore_index=True)
        # [修正] horses_performance.csv に race_id がない行 (地方競馬など) がある場合も考慮
        horses_perf_df = horses_perf_df.drop_duplicates(subset=['horse_id', 'race_date', 'race_name'])
        horses_perf_df.to_csv(os.path.join(output_dir, "horses_performance.csv"), index=False, encoding='utf-8-sig')
        print(f"  [>] horses_performance.csv (計 {len(horses_perf_df)} 行)")

    if all_data["pedigrees"]:
        pedigrees_df = pd.concat(all_data["pedigrees"], ignore_index=True)
        # 重複除去は parse_pedigree() 内で実施済み
        pedigrees_df.to_csv(os.path.join(output_dir, "pedigrees.csv"), index=False, encoding='utf-8-sig')
        print(f"  [>] pedigrees.csv (計 {len(pedigrees_df)} 行)")

if __name__ == "__main__":
    # [修正] main() が test ディレクトリを参照できるようにする
    # このスクリプト (test.py) が test ディレクトリと同じ階層にあると仮定
    
    # サンプルデータ用のディレクトリを作成（ローカル実行用）
    # ユーザー環境ではファイルは既に存在するため、ディレクトリ作成は不要かもしれないが、
    # コードの完全性のため main() の外に移動
    
    # if not os.path.exists("test"):
    #     os.makedirs("test", exist_ok=True)
    #     print("ディレクトリ 'test' を作成しました。bin ファイルを配置してください。")

    main()
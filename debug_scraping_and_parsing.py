# -*- coding: utf-8 -*-
"""
特定日付のスクレイピング＆パース検証スクリプト (改訂版)

プロジェクトの「実際のフロー」で使われている堅牢なスクレイピング関数と
高機能なパーサーロジックを尊重し、それらを呼び出す/移植する形で再実装。
"""

import os
import re
import sys
import time
import pandas as pd
from bs4 import BeautifulSoup
from typing import Dict, Any, Optional, List, Tuple

# プロジェクトルートをパスに追加して、keibaaiモジュールをインポート可能にする
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- 「実際のフロー」からロジックをインポート/拝借 ---
# 1. 堅牢なスクレイピング関数をインポート
from keibaai.src.modules.preparing._scrape_html import fetch_html_robust_get

# 2. 高機能なパーサー関数群を移植 (common_utilsも含む)
#    元のパーサーはファイルパスを引数に取るため、HTMLコンテンツを直接受け取れるように改造

# --- 設定 ---
TARGET_DATE = '2023-10-09'
BASE_URL = 'https://db.netkeiba.com'
OUTPUT_CSV_PATH = 'debug_scraped_data.csv'

# --- 共通パーサーユーティリティ (keibaai.src.modules.parsers.common_utils より移植) ---

def parse_int_or_none(value: Optional[str]) -> Optional[int]:
    if value is None: return None
    value_cleaned = re.sub(r'[^\d]', '', str(value))
    return int(value_cleaned) if value_cleaned else None

def parse_float_or_none(value: Optional[str]) -> Optional[float]:
    if value is None: return None
    value_cleaned = re.sub(r'[^\d.]', '', str(value))
    try:
        return float(value_cleaned)
    except (ValueError, TypeError):
        return None

def safe_strip(value: Optional[str]) -> Optional[str]:
    return value.strip() if value is not None else None

def parse_horse_weight(weight_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not weight_str: return None, None
    match = re.search(r'(\d+)\((\s*[-+]?\d+)\)', weight_str)
    if match:
        weight = parse_int_or_none(match.group(1))
        try:
            change = int(match.group(2))
        except (ValueError, TypeError):
            change = None
        return weight, change
    weight_only = parse_int_or_none(weight_str)
    return weight_only, None

def parse_time_to_seconds(time_str: Optional[str]) -> Optional[float]:
    if not time_str: return None
    parts = str(time_str).split(':')
    try:
        if len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
        if len(parts) == 1: return float(parts[0])
    except (ValueError, IndexError):
        return None
    return None

def get_id_from_href(href: Optional[str], pattern: str) -> Optional[str]:
    if not href: return None
    patterns = {
        'horse': r'/horse/(\w+)',
        'jockey': r'/jockey/(?:result/recent/)?(\w+)',
        'trainer': r'/trainer/(?:result/recent/)?(\w+)',
        'race': r'/race/(\w+)'
    }
    if pattern in patterns:
        match = re.search(patterns[pattern], href)
        return match.group(1) if match else None
    return None

# --- 高機能パーサー (keibaai.src.modules.parsers.results_parser より移植・改造) ---

def parse_html_content(html_bytes: bytes, race_id: str) -> pd.DataFrame:
    """HTMLコンテンツ(bytes)を直接パースしてDataFrameを返す"""
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except Exception:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'lxml')
    
    race_metadata = extract_race_metadata_enhanced(soup)
    
    # 日付はTARGET_DATEから取得
    race_metadata['race_date'] = TARGET_DATE
    
    result_table = soup.find('table', class_='race_table_01')
    if not result_table:
        print(f"  [!] レース結果テーブルが見つかりません: {race_id}")
        return pd.DataFrame()
    
    rows = []
    tbody = result_table.find('tbody') if result_table.find('tbody') else result_table
    
    for tr in tbody.find_all('tr'):
        if not tr.find('td'): continue
        try:
            row_data = parse_result_row_enhanced(tr, race_id, race_metadata)
            if row_data:
                rows.append(row_data)
        except Exception as e:
            print(f"  [!] 行のパースエラー: {e}")
            continue
    
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = add_derived_features(df)
    return df

def extract_race_metadata_enhanced(soup: BeautifulSoup) -> Dict:
    """拡張されたレースメタデータ抽出"""
    metadata = {}
    
    # レース名とクラス
    race_name_div = soup.find('div', class_='RaceName')
    if race_name_div:
        race_name = safe_strip(race_name_div.get_text())
        metadata['race_name'] = race_name
        # クラス推定ロジック... (指示.mdより)
        if 'G1' in race_name or 'GI' in race_name: metadata['race_class'] = 'G1'
        elif 'G2' in race_name or 'GII' in race_name: metadata['race_class'] = 'G2'
        elif 'G3' in race_name or 'GIII' in race_name: metadata['race_class'] = 'G3'
        elif 'オープン' in race_name or 'OP' in race_name: metadata['race_class'] = 'OP'
        elif '1600万' in race_name: metadata['race_class'] = '1600'
        elif '1000万' in race_name: metadata['race_class'] = '1000'
        elif '500万' in race_name: metadata['race_class'] = '500'
        elif '未勝利' in race_name: metadata['race_class'] = '未勝利'
        elif '新馬' in race_name: metadata['race_class'] = '新馬'

    # レース基本情報
    race_data01 = soup.find('div', class_='RaceData01')
    if race_data01:
        text = race_data01.get_text(strip=True)
        distance_match = re.search(r'(芝|ダ|障)(?:右|左|直|外|内)?\s*(\d+)m', text)
        if distance_match:
            surface_map = {'芝': '芝', 'ダ': 'ダート', '障': '障害'}
            metadata['track_surface'] = surface_map.get(distance_match.group(1))
            metadata['distance_m'] = int(distance_match.group(2))
        
        weather_match = re.search(r'天候:(\S+)', text)
        if weather_match: metadata['weather'] = weather_match.group(1)
        
        condition_match = re.search(r'(?:芝|ダート):(\S+)', text)
        if condition_match: metadata['track_condition'] = condition_match.group(1)
        
        time_match = re.search(r'発走:(\d{1,2}:\d{2})', text)
        if time_match: metadata['post_time'] = time_match.group(1)

    # 開催情報と賞金
    race_data02 = soup.find('div', class_='RaceData02')
    if race_data02:
        text = race_data02.get_text(strip=True)
        venue_match = re.search(r'(\d+)回(\S+?)(\d+)日目', text)
        if venue_match:
            metadata['round_of_year'] = int(venue_match.group(1))
            metadata['venue'] = venue_match.group(2)
            metadata['day_of_meeting'] = int(venue_match.group(3))
        
        prize_match = re.search(r'本賞金:([\d,]+)万円', text)
        if prize_match:
            prizes = [int(p.replace(',', '')) for p in prize_match.group(1).split(',')]
            if len(prizes) >= 1: metadata['prize_1st'] = prizes[0]
            # ... (以下同様に2-5着も設定可能)
    
    return metadata

def parse_result_row_enhanced(tr: BeautifulSoup, race_id: str, race_metadata: Dict) -> Optional[Dict]:
    """拡張されたレース結果行のパース"""
    cells = tr.find_all('td')
    if len(cells) < 15: return None
    
    row_data = {'race_id': race_id}
    row_data.update(race_metadata)
    
    row_data['finish_position'] = parse_int_or_none(cells[0].get_text())
    row_data['bracket_number'] = parse_int_or_none(cells[1].get_text())
    row_data['horse_number'] = parse_int_or_none(cells[2].get_text())
    
    horse_link = cells[3].find('a')
    if horse_link:
        row_data['horse_id'] = get_id_from_href(horse_link.get('href'), 'horse')
        row_data['horse_name'] = safe_strip(horse_link.get_text())
    
    row_data['sex_age'] = safe_strip(cells[4].get_text())
    row_data['basis_weight'] = parse_float_or_none(cells[5].get_text())
    
    jockey_link = cells[6].find('a')
    if jockey_link:
        row_data['jockey_id'] = get_id_from_href(jockey_link.get('href'), 'jockey')
        row_data['jockey_name'] = safe_strip(jockey_link.get_text())
    
    time_str = safe_strip(cells[7].get_text())
    row_data['finish_time_str'] = time_str
    row_data['finish_time_sec'] = parse_time_to_seconds(time_str)
    
    row_data['margin_str'] = safe_strip(cells[8].get_text())
    
    passing_str = safe_strip(cells[10].get_text())
    corners = passing_str.split('-') if passing_str else []
    for i in range(4):
        row_data[f'passing_order_{i+1}'] = parse_int_or_none(corners[i]) if i < len(corners) else None
    
    row_data['last_3f_time'] = parse_float_or_none(cells[11].get_text())
    row_data['win_odds'] = parse_float_or_none(cells[12].get_text())
    row_data['popularity'] = parse_int_or_none(cells[13].get_text())
    
    row_data['horse_weight'], row_data['horse_weight_change'] = parse_horse_weight(cells[14].get_text())
    
    # 調教師・馬主は末尾から取得
    trainer_link = cells[-3].find('a')
    if trainer_link:
        row_data['trainer_id'] = get_id_from_href(trainer_link.get('href'), 'trainer')
        row_data['trainer_name'] = safe_strip(trainer_link.get_text())

    owner_link = cells[-2].find('a')
    if owner_link:
        row_data['owner_name'] = safe_strip(owner_link.get('title'))
        
    row_data['prize_money'] = parse_float_or_none(cells[-1].get_text().replace(',', ''))

    return row_data

def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """モデル精度向上のための派生特徴量を追加"""
    if 'finish_time_sec' in df.columns and 'last_3f_time' in df.columns:
        df['time_before_last_3f'] = round(df['finish_time_sec'] - df['last_3f_time'], 1)
    
    if 'popularity' in df.columns and 'finish_position' in df.columns:
        df['popularity_finish_diff'] = df['finish_position'] - df['popularity']
        
    return df

# --- スクレイピング実行部 ---

def get_race_ids_for_date(date_str: str) -> List[str]:
    """指定された日付のレースIDリストを取得する"""
    date_formatted = date_str.replace('-', '')
    race_list_url = f"https://db.netkeiba.com/race/list/{date_formatted}/"
    
    print(f"--- {date_str} のレース一覧を取得中 ---")
    html_content = fetch_html_robust_get(race_list_url)
    
    if not html_content:
        print("  [!] レース一覧ページの取得に失敗しました。")
        return []
        
    soup = BeautifulSoup(html_content.decode('euc-jp', 'replace'), 'lxml')
    race_ids = []
    # セレクタを 'RaceList_Box' から 'race_list' に修正
    race_list_div = soup.find('div', class_='race_list')
    if not race_list_div:
        print("  [!] レース一覧のコンテナが見つかりません。")
        return []
        
    links = race_list_div.find_all('a', href=re.compile(r'^/race/\d+'))
    for link in links:
        race_id = get_id_from_href(link.get('href'), 'race')
        if race_id and race_id not in race_ids:
            race_ids.append(race_id)
            
    print(f"  [+] {len(race_ids)} 件のレースIDが見つかりました。")
    return race_ids

def main():
    """メイン実行処理"""
    print(f"--- {TARGET_DATE} のスクレイピングとパース処理を開始します ---")
    
    race_ids = get_race_ids_for_date(TARGET_DATE)
    if not race_ids:
        print("処理を終了します。")
        return

    all_races_data = []
    for i, race_id in enumerate(race_ids):
        print(f"\n--- レース {i+1}/{len(race_ids)} (ID: {race_id}) の処理中 ---")
        race_url = f"{BASE_URL}/race/{race_id}/"
        
        html_content = fetch_html_robust_get(race_url)
        if not html_content:
            print(f"  [!] HTML取得に失敗。スキップします。")
            continue
        
        df = parse_html_content(html_content, race_id)
        if not df.empty:
            print(f"  [+] パース成功: {len(df)}頭のデータを取得")
            all_races_data.append(df)
        else:
            print(f"  [!] パース失敗またはデータなし")

    if all_races_data:
        final_df = pd.concat(all_races_data, ignore_index=True)
        final_df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8-sig')
        print(f"\n--- 全ての処理が完了しました ---")
        print(f"  [>] {len(final_df)} 行のデータを {OUTPUT_CSV_PATH} に保存しました。")
    else:
        print("\n--- データを取得できなかったため、ファイルは出力されませんでした ---")

if __name__ == "__main__":
    main()

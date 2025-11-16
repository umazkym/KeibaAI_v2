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
    if value is None or value == '': return None
    value_cleaned = re.sub(r'[^\d]', '', str(value))
    if not value_cleaned: return None
    try:
        return int(value_cleaned)
    except (ValueError, TypeError):
        return None

def parse_sex_age(sex_age_str: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    """性齢文字列を性別と年齢に分割"""
    if not sex_age_str: return None, None
    # "牡3" → sex="牡", age=3
    match = re.match(r'([牡牝セ])(\d+)', sex_age_str.strip())
    if match:
        return match.group(1), int(match.group(2))
    return None, None

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

def parse_margin_to_seconds(margin_str: Optional[str]) -> Optional[float]:
    """着差文字列を秒数に変換（推定値）

    1馬身 = 0.2秒として換算

    Args:
        margin_str: 着差文字列（例: "1.1/2", "3/4", "クビ", "大差"）

    Returns:
        着差秒数
    """
    if not margin_str or margin_str.strip() in ['---', '']:
        # 1着の場合は着差がないのでNoneを返す
        return None

    # 特殊な着差のマッピング
    special_margins = {
        '同着': 0.0,
        'ハナ': 0.02,
        'アタマ': 0.04,
        'クビ': 0.05,
        '大差': 5.0,
        '大': 5.0,
    }

    if margin_str in special_margins:
        return special_margins[margin_str]


    # 分数表記の処理
    # 1馬身 = 0.2秒として換算
    SECONDS_PER_LENGTH = 0.2
    total_length = 0.0

    try:
        # "1.1/2" のような形式（1と1/2馬身）
        if '.' in margin_str and '/' in margin_str:
            integer_part_str, fraction_str = margin_str.split('.')
            total_length += float(integer_part_str)

            numerator, denominator = fraction_str.split('/')
            total_length += float(numerator) / float(denominator)


        # "3/4" のような形式（3/4馬身）
        elif '/' in margin_str:
            numerator, denominator = margin_str.split('/')
            total_length += float(numerator) / float(denominator)

        # "5" のような整数形式（5馬身）
        else:
            total_length = float(margin_str)

        return round(total_length * SECONDS_PER_LENGTH, 3)

    except (ValueError, ZeroDivisionError):
        return None

# --- 高機能パーサー (keibaai.src.modules.parsers.results_parser より移植・改造) ---

def parse_html_content(html_bytes: bytes, race_id: str) -> pd.DataFrame:
    """HTMLコンテンツ(bytes)を直接パースしてDataFrameを返す"""
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except Exception:
        html_text = html_bytes.decode('utf-8', errors='replace')

    # CLAUDE.mdの推奨に従い、html.parserを使用（lxmlよりも互換性が高い）
    soup = BeautifulSoup(html_text, 'html.parser')
    
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
    """拡張されたレースメタデータ抽出 (改善版 - 4段階フォールバック対応)

    HTMLの複数フォーマットに対応:
    1. data_intro (通常のレース)
    2. diary_snap_cut (一部のレース)
    3. racedata dl > dd (障害レースや古いページ)
    4. RaceData01 (出馬表や古いレース結果ページ)
    """

    metadata = {
        'distance_m': None, 'track_surface': None, 'weather': None,
        'track_condition': None, 'post_time': None, 'race_name': None,
        'prize_1st': None, 'prize_2nd': None, 'prize_3rd': None,
        'prize_4th': None, 'prize_5th': None,
        'venue': None, 'day_of_meeting': None, 'round_of_year': None,
        'race_class': None, 'head_count': None,
        'metadata_source': None  # 診断用: どのフォールバックレベルが使われたか
    }


    # レース基本情報の抽出（複数フォールバック対応）
    # 方法1: data_intro を探す（通常のレース）
    race_data_intro = soup.find('div', class_='data_intro')
    if race_data_intro:
        metadata['metadata_source'] = 'data_intro'

    # 方法2: diary_snap_cut を探す
    if not race_data_intro:
        race_data_intro = soup.find('div', class_='diary_snap_cut')
        if race_data_intro:
            metadata['metadata_source'] = 'diary_snap_cut'

    # 方法3: racedata > dd を探す（障害レースや古いページ）
    if not race_data_intro:
        race_data_dl = soup.find('dl', class_='racedata')
        if race_data_dl:
            race_data_intro = race_data_dl.find('dd')
            if race_data_intro:
                metadata['metadata_source'] = 'racedata_dl_dd'

    # 方法4: RaceData01 を探す（出馬表や古いレース結果ページ）
    if not race_data_intro:
        race_data_intro = soup.find('div', class_='RaceData01')
        if race_data_intro:
            metadata['metadata_source'] = 'RaceData01'

    if race_data_intro:
        # テキスト全体を取得
        text = race_data_intro.get_text(strip=True)


        # 距離と馬場（改善版 - より柔軟な正規表現）
        # 通常: "芝1800m", "ダ1200m"
        # 障害: "障3000m", "障3110m"
        # 複雑: "芝右 外1800m", "障芝 ダート3000m"

        # パターン1: 障害レース（「障」があれば障害レース扱い）
        # 「障」の後、任意の文字（芝、ダート、スペースなど）を経て数字にマッチ
        # 非貪欲マッチ *? で最初の数字を優先的にマッチ
        if '障' in text:
            distance_match = re.search(r'障[^0-9]*?(\d+)\s*m?', text)
            if distance_match:
                metadata['track_surface'] = '障害'
                metadata['distance_m'] = int(distance_match.group(1))

        # パターン2: 通常レース
        # 「芝」「ダート」の後、数字以外の文字（方向、記号、スペースなど）を非貪欲でマッチ
        if metadata['distance_m'] is None:
            distance_match = re.search(r'(芝|ダート?)[^0-9]*?(\d+)\s*m?', text, re.IGNORECASE)
            if distance_match:
                surface_map = {'芝': '芝', 'ダ': 'ダート', 'ダート': 'ダート'}
                metadata['track_surface'] = surface_map.get(distance_match.group(1))
                metadata['distance_m'] = int(distance_match.group(2))

        # 距離が見つからない場合、spanタグ内を個別に探す
        if metadata['distance_m'] is None:
            spans = race_data_intro.find_all('span')
            for span in spans:
                span_text = span.get_text(strip=True)

                # パターン1: 障害レース（「障」があれば障害レース扱い）
                if '障' in span_text:
                    distance_match = re.search(r'障[^0-9]*?(\d+)', span_text)
                    if distance_match:
                        metadata['track_surface'] = '障害'
                        metadata['distance_m'] = int(distance_match.group(1))
                        break

                # パターン2: 通常レース
                distance_match = re.search(r'(芝|ダート?)[^0-9]*?(\d+)', span_text)
                if distance_match:
                    surface_map = {'芝': '芝', 'ダ': 'ダート', 'ダート': 'ダート'}
                    metadata['track_surface'] = surface_map.get(distance_match.group(1))
                    metadata['distance_m'] = int(distance_match.group(2))
                    break

        # 天候
        weather_match = re.search(r'天候\s*:\s*(\S+)', text)
        if weather_match:
            metadata['weather'] = weather_match.group(1)

        # 馬場状態（障害レースにも対応）
        condition_match = re.search(r'(?:芝|ダート|馬場)\s*:\s*(\S+)', text)

        if condition_match:
            metadata['track_condition'] = condition_match.group(1)

        # 発走時刻
        time_match = re.search(r'発走\s*:\s*(\d{1,2}:\d{2})', text)
        if time_match:
            metadata['post_time'] = time_match.group(1)

    # 障害レースで距離が取得できない場合の補完ロジック
    # 注: 4段階のフォールバック（data_intro, diary_snap_cut, racedata dl > dd, RaceData01）で
    #     ほとんどの障害レース距離は取得できるはず
    #     それでも取得できない場合は、HTMLに距離情報がないと判断し、track_surfaceのみ設定
    if metadata['distance_m'] is None and metadata['race_name']:
        race_name = metadata['race_name']
        if '障害' in race_name:
            # HTMLに距離情報がない場合、track_surfaceのみ設定（距離は補完しない）
            metadata['track_surface'] = '障害'

    # レース名とクラス（修正: セレクタを変更）
    race_name_tag = soup.find('dl', class_='racedata')
    if race_name_tag:
        h1_tag = race_name_tag.find('h1')
        if h1_tag:
            race_name = h1_tag.get_text(strip=True)
            metadata['race_name'] = race_name
 
            # レースクラスの推定（改善版 - JRAとJpnを区別）
            # JRAのG1/G2/G3
            if 'GI' in race_name or 'G1' in race_name:
                # JpnIではない純粋なG1
                if 'Jpn' not in race_name:
                    metadata['race_class'] = 'G1'
                else:
                    metadata['race_class'] = 'JpnI'
            elif 'GII' in race_name or 'G2' in race_name:
                if 'Jpn' not in race_name:
                    metadata['race_class'] = 'G2'
                else:
                    metadata['race_class'] = 'JpnII'
            elif 'GIII' in race_name or 'G3' in race_name:
                if 'Jpn' not in race_name:
                    metadata['race_class'] = 'G3'
                else:
                    metadata['race_class'] = 'JpnIII'
            # 地方のJpnグレード（明示的にJpnIなど）
            elif 'JpnI' in race_name:
                metadata['race_class'] = 'JpnI'
            elif 'JpnII' in race_name:
                metadata['race_class'] = 'JpnII'
            elif 'JpnIII' in race_name:
                metadata['race_class'] = 'JpnIII'
            # その他のクラス
            elif 'オープン' in race_name or 'OP' in race_name:
                metadata['race_class'] = 'OP'
            elif 'L' in race_name or 'リステッド' in race_name:
                metadata['race_class'] = 'L'
            elif '1600万' in race_name or '3勝' in race_name:
                metadata['race_class'] = '1600'
            elif '1000万' in race_name or '2勝' in race_name:
                metadata['race_class'] = '1000'
            elif '500万' in race_name or '1勝' in race_name:
                metadata['race_class'] = '500'
            elif '未勝利' in race_name:
                metadata['race_class'] = '未勝利'
            elif '新馬' in race_name:
                metadata['race_class'] = '新馬'
            else:
                # 障害レースや地方競馬など
                metadata['race_class'] = 'その他'

    # 賞金情報（レース全体の賞金設定）
    # 注: レース結果HTMLには RaceData02 が存在しないため、prize_1st~5th は取得できません
    # 賞金設定は出馬表HTMLに記載されています（本賞金:510,200,130,77,51万円）
    # 各馬が獲得した賞金（prize_money）は parse_result_row_enhanced() で取得されます
    prize_info = soup.find('div', class_='RaceData02')
    if prize_info:
        prize_text = prize_info.get_text()
        prize_match = re.search(r'本賞金:([\d,]+)万円', prize_text)
        if prize_match:
            prizes = [int(p.replace(',', '')) for p in prize_match.group(1).split(',')]
            if len(prizes) >= 1: metadata['prize_1st'] = prizes[0]
            if len(prizes) >= 2: metadata['prize_2nd'] = prizes[1]
            if len(prizes) >= 3: metadata['prize_3rd'] = prizes[2]
            if len(prizes) >= 4: metadata['prize_4th'] = prizes[3]
            if len(prizes) >= 5: metadata['prize_5th'] = prizes[4]

    # 開催情報
    smalltxt = soup.find('p', class_='smalltxt')
    if smalltxt:
        text = smalltxt.get_text()
        match = re.search(r'(\d+)回(\S+?)(\d+)日目', text)

        if match:
            metadata['round_of_year'] = int(match.group(1))
            metadata['venue'] = match.group(2)
            metadata['day_of_meeting'] = int(match.group(3))

    # 出走頭数の取得
    result_table = soup.find('table', class_='race_table_01')
    if result_table:
        tbody = result_table.find('tbody') if result_table.find('tbody') else result_table
        rows = tbody.find_all('tr')
        data_rows = [row for row in rows if row.find('td')]
        metadata['head_count'] = len(data_rows)

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
    
    # 性齢（分割版）
    sex_age_text = safe_strip(cells[4].get_text())
    row_data['sex_age'] = sex_age_text
    sex, age = parse_sex_age(sex_age_text)
    row_data['sex'] = sex
    row_data['age'] = age

    row_data['basis_weight'] = parse_float_or_none(cells[5].get_text())

    # 騎手情報（記号除去版）
    jockey_link = cells[6].find('a')
    if jockey_link:
        row_data['jockey_id'] = get_id_from_href(jockey_link.get('href'), 'jockey')
        # 記号を除去
        jockey_name = safe_strip(jockey_link.get_text())
        if jockey_name:
            jockey_name = re.sub(r'[◎○▲△☆★]', '', jockey_name).strip()
        row_data['jockey_name'] = jockey_name
    else:
        row_data['jockey_id'] = None
        row_data['jockey_name'] = None
    
    time_str = safe_strip(cells[7].get_text())
    row_data['finish_time_str'] = time_str
    row_data['finish_time_sec'] = parse_time_to_seconds(time_str)
    
    margin_str = safe_strip(cells[8].get_text())
    row_data['margin_str'] = margin_str
    row_data['margin_seconds'] = parse_margin_to_seconds(margin_str)

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

    # 馬主（titleまたはテキストから取得）
    owner_cell = cells[-2]
    owner_link = owner_cell.find('a')
    if owner_link:
        owner_name = owner_link.get('title') or owner_link.get_text(strip=True)
        row_data['owner_name'] = safe_strip(owner_name)
    else:
        row_data['owner_name'] = safe_strip(owner_cell.get_text())

    # 賞金
    prize_text = safe_strip(cells[-1].get_text())
    if prize_text and prize_text not in ['---', '']:
        row_data['prize_money'] = parse_float_or_none(prize_text.replace(',', ''))
    else:
        row_data['prize_money'] = None

    return row_data

def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """モデル精度向上のための派生特徴量を追加

    機械学習モデル用の重要な特徴量を生成
    """
    if df.empty:
        return df

    # 1. 前半タイム（上がり3Fを除く）
    if 'finish_time_sec' in df.columns and 'last_3f_time' in df.columns:
        df['time_before_last_3f'] = df['finish_time_sec'] - df['last_3f_time']
        df['time_before_last_3f'] = df['time_before_last_3f'].round(1)

    # 2. 人気と着順の差（穴馬検出用）
    if 'popularity' in df.columns and 'finish_position' in df.columns:
        df['popularity_finish_diff'] = df['finish_position'] - df['popularity']

    # 3. オッズと人気の不整合（期待値検出用）
    if 'win_odds' in df.columns and 'popularity' in df.columns:
        # オッズから逆算した理論人気との差
        df['odds_rank'] = df.groupby('race_id')['win_odds'].rank(method='min')
        df['odds_popularity_diff'] = df['odds_rank'] - df['popularity']

    # 4. 着差の累積（レース全体のペース判定用）
    if 'margin_seconds' in df.columns:
        df['cumulative_margin'] = df.groupby('race_id')['margin_seconds'].cumsum().fillna(0)

    # 5. レース全体の平均タイム（標準化用）
    if 'finish_time_sec' in df.columns:
        df['race_avg_time'] = df.groupby('race_id')['finish_time_sec'].transform('mean')
        df['time_deviation'] = df['finish_time_sec'] - df['race_avg_time']

    # 6. 通過順位の変動（末脚の強さ指標）
    if 'passing_order_1' in df.columns and 'finish_position' in df.columns:
        # 最初のコーナーと最終着順の差
        df['position_change'] = df['passing_order_1'] - df['finish_position']

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

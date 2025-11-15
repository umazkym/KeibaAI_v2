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

# common_utils のインポートパスを修正 (プロジェクトルートからの相対パスを想定)
try:
    from .common_utils import (
        parse_int_or_none,
        parse_float_or_none,
        parse_sex_age,
        parse_horse_weight,
    )
except ImportError:
    # スクリプトとして直接実行された場合などのフォールバック
    logging.warning("common_utils の相対インポートに失敗。絶対インポートを試みます。")
    try:
        from modules.parsers.common_utils import (
            parse_int_or_none,
            parse_float_or_none,
            parse_sex_age,
            parse_horse_weight,
        )
    except ImportError:
        logging.error("common_utils が見つかりません。")
        # 簡易的なフォールバック（実際には環境設定が必要）
        parse_int_or_none = lambda x: int(x) if x and x.isdigit() else None
        parse_float_or_none = lambda x: float(x) if x and x.replace('.', '', 1).isdigit() else None
        parse_sex_age = lambda x: (x[0], int(x[1:])) if x and len(x) > 1 else (None, None)
        parse_horse_weight = lambda x: (None, None)


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
    
    # --- レース日付を HTML から抽出 ---
    race_date = extract_race_date_from_html(soup, race_id)
    
    shutuba_table = soup.find('table', class_='Shutuba_Table')
    
    if not shutuba_table:
        logging.error(f"Shutuba_Table が見つかりません: {file_path}")
        return pd.DataFrame()
    
    rows = []
    
    # class 'HorseList' を持つ <tr> のみを取得
    for tr in shutuba_table.find_all('tr', class_='HorseList'):
        try:
            row_data = parse_shutuba_row(tr, race_id)
            if row_data:
                # レース日付を追加
                row_data['race_date'] = race_date
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
    2. trainer_id: 複数URLパターン対応 (BugFix)
    3. HTMLに存在しないフィールドはNoneに設定
    4. セル数チェックの強化
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
    
    # ▼▼▼ 修正箇所（バグ修正） ▼▼▼
    # 調教師名・調教師ID (修正: 複数パターン対応)
    trainer_link = cells[7].find('a', href=re.compile(r'/trainer/'))
    if trainer_link:
        row_data['trainer_name'] = trainer_link.get_text(strip=True)
        href = trainer_link['href']
        # パターン1: /trainer/result/recent/数字
        trainer_id_match = re.search(r'/trainer/result/recent/(\d+)', href)
        if not trainer_id_match:
            # パターン2: /trainer/数字
            trainer_id_match = re.search(r'/trainer/(\d+)', href)
        row_data['trainer_id'] = trainer_id_match.group(1) if trainer_id_match else None
    else:
        row_data['trainer_name'] = cells[7].get_text(strip=True)
        row_data['trainer_id'] = None
    # ▲▲▲ 修正箇所 ▲▲▲
    
    # 馬体重（前走）
    if len(cells) > 8:
        weight_text = cells[8].get_text(strip=True)
        horse_weight, horse_weight_change = parse_horse_weight(weight_text)
        row_data['horse_weight'] = horse_weight
        row_data['horse_weight_change'] = horse_weight_change
    else:
        row_data['horse_weight'] = None
        row_data['horse_weight_change'] = None

    # --- ▼▼▼ 修正箇所 (エラー内容.txt に基づく) ▼▼▼

    # 前日オッズ (morning_odds) - cells[9]
    if len(cells) > 9:
        odds_text = cells[9].get_text(strip=True)
        row_data['morning_odds'] = parse_float_or_none(odds_text)
    else:
        row_data['morning_odds'] = None

    # 前日人気 (morning_popularity) - cells[10]
    if len(cells) > 10:
        pop_text = cells[10].get_text(strip=True)
        row_data['morning_popularity'] = parse_int_or_none(pop_text)
    else:
        row_data['morning_popularity'] = None

    # 以下のフィールドは出馬表HTMLには含まれていないため、Noneに設定
    # （これらは馬詳細ページやオッズページから取得する必要がある）
    row_data['owner_name'] = None  # 馬主名: 出馬表には未掲載
    row_data['prize_total'] = None  # 獲得賞金: 出馬表には未掲載
    
    # career_stats, last_5_finishes なども同様にHTMLに存在しない
    row_data['career_stats'] = None
    row_data['career_starts'] = None
    row_data['career_wins'] = None
    row_data['career_places'] = None
    row_data['last_5_finishes'] = None
    # --- ▲▲▲ 修正箇所 ▲▲▲ ---

    return row_data


def extract_race_id_from_filename(file_path: str) -> str:
    """
    ファイル名からレースIDを抽出
    """
    filename = Path(file_path).stem
    match = re.search(r'(\d{12})', filename)
    return match.group(1) if match else None


def extract_race_date_from_html(soup: BeautifulSoup, race_id: str) -> Optional[str]:
    """
    出馬表HTMLからレース日付を抽出 (修正版)
    
    Args:
        soup: BeautifulSoup オブジェクト
        race_id: レースID (フォールバック用)
    
    Returns:
        race_date (ISO8601形式: YYYY-MM-DD) または None
    
    抽出パターン (優先順):
        1. <p class="smalltxt"> (レース結果ページと同じタグ。debug_find_date.py で発見)
        2. <p class="RaceData01">
        3. <dd class="Active"> (ただし年が欠落している可能性あり)
    """
    try:
        # パターン1: <p class="smalltxt"> (debug_find_date.py で発見)
        # (注: shutuba.html にはこのクラスが存在しない場合があるが、
        #  results_parser.py とロジックを共通化するため残す)
        smalltxt = soup.find('p', class_='smalltxt')
        if smalltxt:
            date_text = smalltxt.get_text(strip=True)
            # "2023年05月14日 2回東京8日目..."
            match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_text)
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)
                day = match.group(3).zfill(2)
                logging.info(f"日付抽出成功 (smalltxt): {year}-{month}-{day}")
                return f"{year}-{month}-{day}"

        # パターン2: p.RaceData01 (レース情報)
        race_data = soup.find('p', class_='RaceData01')
        if race_data:
            date_text = race_data.get_text(strip=True)
            match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_text)
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)
                day = match.group(3).zfill(2)
                logging.info(f"日付抽出成功 (RaceData01): {year}-{month}-{day}")
                return f"{year}-{month}-{day}"
        
        # パターン3: dd.Active (開催日表示)
        active_dd = soup.find('dd', class_='Active')
        if active_dd:
            date_text = active_dd.get_text(strip=True)
            # "2023年5月28日" or "5月28日"
            match_full = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_text)
            if match_full:
                year = match_full.group(1)
                month = match_full.group(2).zfill(2)
                day = match_full.group(3).zfill(2)
                logging.info(f"日付抽出成功 (Active Full): {year}-{month}-{day}")
                return f"{year}-{month}-{day}"
            
            # 年が欠落している場合 ("5月14日(日)")
            # この場合、他のタグから年（YYYY）を探す
            match_partial = re.search(r'(\d{1,2})月(\d{1,2})日', date_text)
            if match_partial and '年' not in date_text:
                logging.warning(f"日付抽出 (Active Partial): 年が欠落しています '{date_text}'。年を別途探索します。")
                # 年の探索: <li class="Active">YYYY</li> または <title>YYYY</title>
                year_str = None
                
                # <li class="Active"> の親の前の <li> タグ
                active_li = active_dd.find_parent('li')
                if active_li:
                    prev_li = active_li.find_previous_sibling('li', class_='Active')
                    if prev_li:
                        year_match = re.search(r'(\d{4})', prev_li.get_text(strip=True))
                        if year_match:
                            year_str = year_match.group(1)

                # title タグからも試行
                if not year_str:
                    title_tag = soup.find('title')
                    if title_tag:
                         year_match = re.search(r'(\d{4})', title_tag.get_text(strip=True))
                         if year_match:
                            year_str = year_match.group(1)

                if year_str:
                    month = match_partial.group(1).zfill(2)
                    day = match_partial.group(2).zfill(2)
                    logging.info(f"日付抽出成功 (Active Partial + Year): {year_str}-{month}-{day}")
                    return f"{year_str}-{month}-{day}"
                else:
                    logging.warning(f"年（YYYY）の特定に失敗しました。")

        
        # HTMLから日付を見つけられなかった場合
        logging.warning(f"HTMLから日付を抽出できませんでした (race_id: {race_id})。Noneを返します。")
        return None
        
    except Exception as e:
        logging.error(f"レース日付の抽出に失敗: {e}")
        return None


def extract_race_date_from_race_id(race_id_series: pd.Series) -> pd.Series:
    """
    (この関数は現在、shutuba_parser.pyからは呼び出されていません)
    race_id (YYYYMMDD形式を含む12桁) から race_date を生成
    
    ★警告★: このロジックはユーザーの指摘により「NG」です。
    """
    try:
        # race_id の先頭8桁を抽出 (YYYYMMDD)
        date_str_series = race_id_series.astype(str).str[:8]
        
        # datetime に変換
        race_date_series = pd.to_datetime(date_str_series, format='%Y%m%d', errors='coerce')
        
        return race_date_series
        
    except Exception as e:
        logging.error(f"race_id から race_date の生成に失敗: {e}")
        return pd.Series([None] * len(race_id_series))
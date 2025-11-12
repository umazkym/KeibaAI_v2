"""
レース結果HTMLパーサ (修正版 v1.G.5)
netkeiba.com のレース結果ページから情報を抽出

v1.G.5 での修正点:
1. tbody検索のロバスト化: 
   - <table> 直下に <tbody> が存在しないケースに対応。
   - <tbody> が見つからない場合は、<table> 自体から <tr> を検索するよう修正。
v1.0.4 での修正点:
1. テーブル検索: class_='RaceTable01' から class_='race_table_01' に変更
v1.0.3 での修正点:
1. 日付抽出: shutuba_parser.py (v1.0.1) の extract_race_date_from_html を移植
2. 依存関係: common_utils から parse_time_to_seconds, parse_margin_to_seconds 等をインポート
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
        parse_time_to_seconds,
        parse_margin_to_seconds,
        parse_prize_money,
        normalize_owner_name,
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
            parse_time_to_seconds,
            parse_margin_to_seconds,
            parse_prize_money,
            normalize_owner_name,
        )
    except ImportError:
        logging.error("common_utils が見つかりません。")
        # 簡易的なフォールバック（実際には環境設定が必要）
        parse_int_or_none = lambda x: int(x) if x and x.isdigit() else None
        parse_float_or_none = lambda x: float(x) if x and x.replace('.', '', 1).isdigit() else None
        parse_sex_age = lambda x: (x[0], int(x[1:])) if x and len(x) > 1 else (None, None)
        parse_horse_weight = lambda x: (None, None)
        parse_time_to_seconds = lambda x: None
        parse_margin_to_seconds = lambda x: None # 引数修正
        parse_prize_money = lambda x: None
        normalize_owner_name = lambda x: x


def parse_results_html(file_path: str, race_id: str = None) -> pd.DataFrame:
    """
    レース結果HTMLをパースしてDataFrameを返す (v1.G.5)
    """
    logging.info(f"レース結果パース開始: {file_path}")
    
    # レースID抽出
    if race_id is None:
        race_id = extract_race_id_from_filename(file_path)
    
    # ファイル読み込み
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    # EUC-JPでデコード
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'lxml')

    # --- (v1.0.3) 日付抽出 ---
    race_date = extract_race_date_from_html(soup, race_id)
    
    # --- (v1.0.4) テーブル検索 ---
    result_table = soup.find('table', class_='race_table_01')
    
    if not result_table:
        logging.error(f"レース結果テーブル (class='race_table_01') が見つかりません: {file_path}")
        return pd.DataFrame()
    
    rows = []
    
    # --- ▼▼▼ 修正箇所 (v1.G.5) ▼▼▼ ---
    # tbody内のtrを走査 (tbody がなくても <table> から直接 tr を探す)
    
    tbody = result_table.find('tbody')
    
    # tbody が見つかれば tbody を、見つからなければ table 全体を検索対象にする
    search_area = tbody if tbody else result_table
    
    rows_found = search_area.find_all('tr')
    
    if not rows_found:
        logging.error(f"テーブル (class='race_table_01') 内に行 (tr) が見つかりません: {file_path}")
        return pd.DataFrame()
        
    for tr in rows_found:
    # --- ▲▲▲ 修正箇所 ▲▲▲ ---
        try:
            row_data = parse_result_row(tr, race_id, race_date)
            if row_data:
                rows.append(row_data)
        except Exception as e:
            logging.warning(f"行のパースエラー: {e}")
            continue
    
    df = pd.DataFrame(rows)
    
    logging.info(f"レース結果パース完了: {file_path} ({len(df)}行)")
    
    return df


def parse_result_row(tr, race_id: str, race_date: Optional[str]) -> Optional[Dict]:
    """
    レース結果テーブルの1行をパース (v1.G.5)
    """
    cells = tr.find_all('td')
    
    if len(cells) < 15: # 最小限のカラム数 (ヘッダ行などはここで弾かれる)
        return None
    
    row_data = {'race_id': race_id, 'race_date': race_date}
    
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
    
    # 性齢（例: "牡3"）
    sex_age_text = cells[4].get_text(strip=True)
    row_data['sex_age'] = sex_age_text
    sex, age = parse_sex_age(sex_age_text)
    row_data['sex'] = sex
    row_data['age'] = age
    
    # 斤量
    weight_text = cells[5].get_text(strip=True)
    row_data['basis_weight'] = parse_float_or_none(weight_text)
    
    # 騎手名・騎手ID
    jockey_link = cells[6].find('a', href=re.compile(r'/jockey/'))
    if jockey_link:
        row_data['jockey_name'] = jockey_link.get_text(strip=True)
        href = jockey_link['href']
        # 複数パターン対応
        jockey_id_match = re.search(r'/jockey/result/recent/(\d+)', href)
        if not jockey_id_match:
            jockey_id_match = re.search(r'/jockey/(\d+)', href)
        row_data['jockey_id'] = jockey_id_match.group(1) if jockey_id_match else None
    else:
        row_data['jockey_name'] = cells[6].get_text(strip=True)
        row_data['jockey_id'] = None
    
    # タイム
    time_text = cells[7].get_text(strip=True)
    row_data['finish_time_str'] = time_text
    row_data['finish_time_seconds'] = parse_time_to_seconds(time_text)
    
    # 着差
    margin_text = cells[8].get_text(strip=True)
    row_data['margin_str'] = margin_text
    # 1着の着差は '---' や '' になるため、0.0 に正規化
    if row_data['finish_position'] == 1 and margin_text in ['---', '']:
        row_data['margin_seconds'] = 0.0
    else:
        # --- ▼▼▼ 修正箇所 (v1.0.4) ▼▼▼ ---
        row_data['margin_seconds'] = parse_margin_to_seconds(margin_text)
        # --- ▲▲▲ 修正箇所 ▲▲▲ ---

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
    
    # --- 適応的セルインデックス検索 (v1.0.1) ---
    # 調教師 (cells[15] or [18])
    row_data['trainer_name'] = None
    row_data['trainer_id'] = None
    # 典型的な位置 (15) または (18)
    for cell_idx in [15, 18]: 
        if len(cells) > cell_idx:
            cell = cells[cell_idx]
            trainer_link = cell.find('a', href=re.compile(r'/trainer/'))
            if trainer_link:
                row_data['trainer_name'] = trainer_link.get_text(strip=True)
                href = trainer_link['href']
                trainer_id_match = re.search(r'/trainer/result/recent/(\d+)', href)
                if not trainer_id_match:
                    trainer_id_match = re.search(r'/trainer/(\d+)', href)
                row_data['trainer_id'] = trainer_id_match.group(1) if trainer_id_match else None
                break # 見つかったらループ終了

    # 馬主 (cells[16] or [19])
    row_data['owner_name'] = None
    for cell_idx in [16, 19]:
        if len(cells) > cell_idx:
            cell = cells[cell_idx]
            owner_text = cell.get_text(strip=True)
            # common_utils v1.0.1 の呼び出し
            normalized_owner = normalize_owner_name(owner_text)
            if normalized_owner:
                row_data['owner_name'] = normalized_owner
                break

    # 賞金 (cells[17] or [20])
    row_data['prize_money'] = None
    for cell_idx in [17, 20]:
        if len(cells) > cell_idx:
            cell = cells[cell_idx]
            prize_text = cell.get_text(strip=True)
            # common_utils v1.0.1 の呼び出し
            prize_money = parse_prize_money(prize_text)
            if prize_money is not None:
                row_data['prize_money'] = prize_money
                break

    return row_data


# ========================================
# ユーティリティ関数
# ========================================

def extract_race_id_from_filename(file_path: str) -> str:
    """
    ファイル名からレースIDを抽出
    """
    filename = Path(file_path).stem
    match = re.search(r'(\d{12})', filename)
    return match.group(1) if match else None

# --- (v1.0.3) 日付抽出関数 ---
def extract_race_date_from_html(soup: BeautifulSoup, race_id: str) -> Optional[str]:
    """
    HTMLからレース日付を抽出 (shutuba_parser.py v1.0.1 と同一ロジック)
    """
    try:
        # パターン1: <p class="smalltxt"> (レース結果ページ)
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

        # パターン2: p.RaceData01 (出馬表ページ)
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
        
        # パターン3: dd.Active (カレンダーページなど)
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
            match_partial = re.search(r'(\d{1,2})月(\d{1,2})日', date_text)
            if match_partial and '年' not in date_text:
                logging.warning(f"日付抽出 (Active Partial): 年が欠落しています '{date_text}'。年を別途探索します。")
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
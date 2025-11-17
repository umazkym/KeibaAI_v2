# src/modules/parsers/results_parser.py の修正版

import re
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup

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

def extract_race_id_from_filename(file_path: str) -> str:
    """
    ファイル名からレースIDを抽出
    """
    filename = Path(file_path).stem
    # race_202305020811... から 202305020811 を抽出
    match = re.search(r'(\d{12})', filename)
    if match:
        return match.group(1)

    # _202305020811... のような別プレフィックスにも対応
    match = re.search(r'_(\d{12})', filename)
    if match:
        return match.group(1)

    logging.warning(f"ファイル名 {filename} から race_id (12桁) を抽出できませんでした。")
    return None


def extract_race_date_from_html(soup: BeautifulSoup, race_id: str) -> Optional[str]:
    """
    レース結果HTMLからレース日付を抽出

    Args:
        soup: BeautifulSoup オブジェクト
        race_id: レースID (フォールバック用)

    Returns:
        race_date (ISO8601形式: YYYY-MM-DD) または None
    """
    # 方法1: data_introのspanタグから日付を抽出
    data_intro = soup.find('div', class_='data_intro')
    if data_intro:
        date_text = data_intro.get_text()
        # "2020年7月25日" のような形式を探す
        match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_text)
        if match:
            year, month, day = match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d}"

    # 方法2: smalltxtから抽出（フォールバック）
    smalltxt = soup.find('p', class_='smalltxt')
    if smalltxt:
        date_text = smalltxt.get_text()
        match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_text)
        if match:
            year, month, day = match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d}"

    # 方法3: race_idから年を抽出（最終フォールバック）
    if race_id and len(race_id) >= 4:
        year = race_id[:4]
        logging.warning(f"HTML から日付を抽出できませんでした。race_id から年のみ抽出: {year}")
        return f"{year}-01-01"  # デフォルト日付

    logging.error(f"日付の抽出に完全に失敗しました")
    return None


def parse_results_html(file_path: str, race_id: str = None) -> pd.DataFrame:
    """レース結果HTMLをパースしてDataFrameを返す (拡張版)"""
    logging.info(f"レース結果パース開始: {file_path}")
    
    if race_id is None:
        race_id = extract_race_id_from_filename(file_path)
    
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # レースメタデータを抽出
    race_metadata = extract_race_metadata_enhanced(soup)
    race_date = extract_race_date_from_html(soup, race_id)
    
    result_table = soup.find('table', class_='race_table_01')
    if not result_table:
        logging.error(f"レース結果テーブルが見つかりません: {file_path}")
        return pd.DataFrame()
    
    rows = []
    tbody = result_table.find('tbody') if result_table.find('tbody') else result_table
    
    for tr in tbody.find_all('tr'):
        try:
            row_data = parse_result_row_enhanced(tr, race_id, race_date, race_metadata)
            if row_data:
                rows.append(row_data)
        except Exception as e:
            logging.warning(f"行のパースエラー: {e}")
            continue
    
    df = pd.DataFrame(rows)
    
    # 派生特徴量の生成
    if not df.empty:
        df = add_derived_features(df)
    
    logging.info(f"レース結果パース完了: {file_path} ({len(df)}行)")
    
    return df

def extract_race_metadata_enhanced(soup: BeautifulSoup) -> Dict:
    """拡張されたレースメタデータ抽出"""
    metadata = {
        'distance_m': None, 'track_surface': None, 'weather': None,
        'track_condition': None, 'post_time': None, 'race_name': None,
        'prize_1st': None, 'prize_2nd': None, 'prize_3rd': None, 
        'prize_4th': None, 'prize_5th': None,
        'venue': None, 'day_of_meeting': None, 'round_of_year': None,
        'race_class': None, 'age_restriction': None
    }
    
    # レース基本情報の抽出を強化
    race_data_intro = soup.find('div', class_='data_intro')
    if race_data_intro:
        span_text = race_data_intro.find('diary_snap_cut')
        if span_text:
            span_content = span_text.find('span')
            if span_content:
                text = span_content.get_text()
                
                # 距離と馬場（改善版）
                distance_match = re.search(r'(芝|ダ|障)\s*(?:右|左|直|外|内)?\s*(\d+)m', text)
                if distance_match:
                    surface_map = {'芝': '芝', 'ダ': 'ダート', '障': '障害'}
                    metadata['track_surface'] = surface_map.get(distance_match.group(1))
                    metadata['distance_m'] = int(distance_match.group(2))
                
                # 天候
                weather_match = re.search(r'天候\s*:\s*(\S+)', text)
                if weather_match:
                    metadata['weather'] = weather_match.group(1)
                
                # 馬場状態（改善版）
                condition_match = re.search(r'(?:芝|ダート)\s*:\s*(\S+)', text)
                if condition_match:
                    metadata['track_condition'] = condition_match.group(1)
                
                # 発走時刻
                time_match = re.search(r'発走\s*:\s*(\d{1,2}:\d{2})', text)
                if time_match:
                    metadata['post_time'] = time_match.group(1)
    
    # レース名とクラス
    race_name_tag = soup.find('dl', class_='racedata')
    if race_name_tag:
        h1_tag = race_name_tag.find('h1')
        if h1_tag:
            race_name = h1_tag.get_text(strip=True)
            metadata['race_name'] = race_name
            
            # レースクラスの推定
            if 'G1' in race_name or 'GI' in race_name:
                metadata['race_class'] = 'G1'
            elif 'G2' in race_name or 'GII' in race_name:
                metadata['race_class'] = 'G2'
            elif 'G3' in race_name or 'GIII' in race_name:
                metadata['race_class'] = 'G3'
            elif 'オープン' in race_name or 'OP' in race_name:
                metadata['race_class'] = 'OP'
            elif '1600万' in race_name:
                metadata['race_class'] = '1600'
            elif '1000万' in race_name:
                metadata['race_class'] = '1000'
            elif '500万' in race_name:
                metadata['race_class'] = '500'
            elif '未勝利' in race_name:
                metadata['race_class'] = '未勝利'
            elif '新馬' in race_name:
                metadata['race_class'] = '新馬'
            
            # 年齢制限
            if '2歳' in race_name:
                metadata['age_restriction'] = '2歳'
            elif '3歳' in race_name:
                metadata['age_restriction'] = '3歳'
            elif '3歳以上' in race_name:
                metadata['age_restriction'] = '3歳以上'
            elif '4歳以上' in race_name:
                metadata['age_restriction'] = '4歳以上'
    
    # 賞金情報
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
    
    return metadata

def parse_result_row_enhanced(tr, race_id: str, race_date: str, race_metadata: Dict) -> Optional[Dict]:
    """拡張されたレース結果行のパース"""
    cells = tr.find_all('td')
    
    if len(cells) < 15:
        return None
    
    row_data = {'race_id': race_id, 'race_date': race_date}
    row_data.update(race_metadata)
    
    # 既存のパース処理（改善版）
    row_data['finish_position'] = parse_int_or_none(cells[0].get_text())
    row_data['bracket_number'] = parse_int_or_none(cells[1].get_text())
    row_data['horse_number'] = parse_int_or_none(cells[2].get_text())
    
    # 馬情報
    horse_link = cells[3].find('a')
    if horse_link:
        row_data['horse_id'] = get_id_from_href(horse_link.get('href'), 'horse')
        row_data['horse_name'] = horse_link.get_text(strip=True)
    
    sex_age_text = cells[4].get_text(strip=True)
    row_data['sex_age'] = sex_age_text
    sex, age = parse_sex_age(sex_age_text)
    row_data['sex'] = sex
    row_data['age'] = age
    
    row_data['basis_weight'] = parse_float_or_none(cells[5].get_text())
    
    # 騎手情報（改善版）
    jockey_link = cells[6].find('a')
    if jockey_link:
        row_data['jockey_id'] = get_id_from_href(jockey_link.get('href'), 'jockey')
        row_data['jockey_name'] = jockey_link.get_text(strip=True)
    
    # タイム情報（拡張版）
    time_str = cells[7].get_text(strip=True)
    row_data['finish_time_str'] = time_str
    time_seconds = parse_time_to_seconds(time_str)
    row_data['finish_time_seconds'] = time_seconds
    
    # 着差（拡張版）
    margin_str = cells[8].get_text(strip=True)
    row_data['margin_str'] = margin_str
    row_data['margin_seconds'] = parse_margin_to_seconds(margin_str)
    
    # 通過順位（分割版）
    passing_str = cells[10].get_text(strip=True) if len(cells) > 10 else None
    if passing_str:
        corners = passing_str.split('-')
        for i in range(4):
            col_name = f'passing_order_{i+1}'
            if i < len(corners):
                row_data[col_name] = parse_int_or_none(corners[i])
            else:
                row_data[col_name] = None
    
    # 上がり3F
    last_3f = parse_float_or_none(cells[11].get_text()) if len(cells) > 11 else None
    row_data['last_3f_time'] = last_3f
    
    # 上がり3Fを除いたタイム
    if time_seconds and last_3f:
        row_data['time_except_last3f'] = round(time_seconds - last_3f, 1)
    
    row_data['win_odds'] = parse_float_or_none(cells[12].get_text()) if len(cells) > 12 else None
    row_data['popularity'] = parse_int_or_none(cells[13].get_text()) if len(cells) > 13 else None
    
    # 馬体重
    if len(cells) > 14:
        weight_str = cells[14].get_text(strip=True)
        row_data['horse_weight'], row_data['horse_weight_change'] = parse_horse_weight(weight_str)
    
    # 調教師・馬主（安定化版）
    row_data['trainer_id'] = None
    row_data['trainer_name'] = None
    row_data['owner_name'] = None
    row_data['prize_money'] = None
    
    # 列数に応じた柔軟な処理
    if len(cells) >= 18:
        # 通常のフォーマット（調教師、馬主、賞金の順）
        trainer_idx = 15
        owner_idx = 16
        prize_idx = 17
        
        # 賞金があるかチェック（1着の場合）
        if row_data['finish_position'] == 1 and len(cells) > prize_idx:
            prize_text = cells[prize_idx].get_text(strip=True)
            if prize_text and prize_text.replace(',', '').replace('.', '').isdigit():
                row_data['prize_money'] = parse_prize_money(prize_text)
            else:
                # 賞金がない場合、インデックスをシフト
                trainer_idx = 15
                owner_idx = 16
        
        # 調教師
        if len(cells) > trainer_idx:
            trainer_cell = cells[trainer_idx]
            trainer_link = trainer_cell.find('a')
            if trainer_link:
                row_data['trainer_id'] = get_id_from_href(trainer_link.get('href'), 'trainer')
                row_data['trainer_name'] = trainer_link.get_text(strip=True)
        
        # 馬主
        if len(cells) > owner_idx:
            owner_cell = cells[owner_idx]
            owner_text = owner_cell.get_text(strip=True)
            if owner_text and owner_text not in ['---', '']:
                row_data['owner_name'] = normalize_owner_name(owner_text)
    
    return row_data

def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """モデル精度向上のための派生特徴量を追加"""
    
    # 1. ペース関連の特徴量
    if 'time_except_last3f' in df.columns and 'last_3f_time' in df.columns:
        # ペースインデックス（前半/後半の比率）
        df['pace_index'] = df['time_except_last3f'] / (df['last_3f_time'] + 0.1)  # ゼロ除算防止
        
        # レース内での上がり3F順位
        df['last3f_rank'] = df.groupby('race_id')['last_3f_time'].rank(method='min')
    
    # 2. 順位変動の特徴量
    for i in range(1, 4):
        curr_col = f'passing_order_{i}'
        next_col = f'passing_order_{i+1}'
        if curr_col in df.columns and next_col in df.columns:
            # NoneType比較エラーを防ぐため、Noneを含む計算を安全に処理
            df[f'position_change_{i}_{i+1}'] = (
                df[next_col].astype('float64') - df[curr_col].astype('float64')
            )

    # 最終コーナーから着順への変化
    if 'passing_order_4' in df.columns:
        df['final_corner_to_finish'] = (
            df['finish_position'].astype('float64') - df['passing_order_4'].astype('float64')
        )
    elif 'passing_order_3' in df.columns:
        df['final_corner_to_finish'] = (
            df['finish_position'].astype('float64') - df['passing_order_3'].astype('float64')
        )
    
    # 3. 相対的な指標
    # レース内での馬体重の偏差値
    df['horse_weight_deviation'] = df.groupby('race_id')['horse_weight'].transform(
        lambda x: 50 + 10 * (x - x.mean()) / (x.std() + 0.1)
    )
    
    # 人気と着順の乖離
    if 'popularity' in df.columns:
        df['popularity_finish_diff'] = (
            df['finish_position'].astype('float64') - df['popularity'].astype('float64')
        )
    
    # 4. オッズ関連
    if 'win_odds' in df.columns:
        # 確率への変換
        df['win_probability'] = 1 / (df['win_odds'] + 1)
        
        # レース内での相対的なオッズ
        df['relative_odds'] = df.groupby('race_id')['win_odds'].transform(
            lambda x: x / x.mean()
        )
    
    # 5. 馬場・距離適性の準備（実際の計算は過去成績と結合後）
    df['distance_category'] = pd.cut(
        df['distance_m'],
        bins=[0, 1400, 1800, 2200, 3000, 4000],
        labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
    )
    
    return df

def get_id_from_href(href: Optional[str], pattern: str) -> Optional[str]:
    """改善版: 複雑なURLパターンに対応"""
    if not href:
        return None
    
    # パターンごとの正規表現
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
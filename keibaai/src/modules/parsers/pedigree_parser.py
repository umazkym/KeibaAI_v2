"""
血統情報パーサ (修正版 - ancestor_id英字対応)
"""

import re
import logging
from typing import Dict, List, Optional
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup


def parse_pedigree_html(file_path: str, horse_id: str = None) -> pd.DataFrame:
    """
    血統HTMLをパースしてDataFrameを返す
    """
    logging.info(f"血統情報パース開始: {file_path}")

    if horse_id is None:
        horse_id = extract_horse_id_from_filename(file_path)

    with open(file_path, 'rb') as f:
        html_bytes = f.read()

    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_text, 'lxml')

    blood_table = soup.find('table', class_='blood_table')

    if not blood_table:
        logging.error(f"血統テーブルが見つかりません: {file_path}")
        return pd.DataFrame()

    rows = []

    # 血統テーブル内のすべての馬リンクを取得
    ancestor_links = blood_table.find_all('a', href=re.compile(r'/horse/'))

    for link in ancestor_links:
        href = link.get('href')
        match = re.search(r'/horse/([^/]+)', href)
        if match:
            raw_id = match.group(1)
            ancestor_id = normalize_ancestor_id(raw_id)
            ancestor_name = normalize_ancestor_name(link.get_text(strip=True))

            # バリデーション: 有効なIDと名前のみ追加
            if ancestor_id and ancestor_name:
                # 重複チェック
                if not any(d.get('ancestor_id') == ancestor_id for d in rows):
                    rows.append({
                        'horse_id': horse_id,
                        'ancestor_id': ancestor_id,
                        'ancestor_name': ancestor_name,
                    })

    df = pd.DataFrame(rows)
    logging.info(f"血統情報パース完了: {file_path} ({len(df)}行)")

    return df


def normalize_ancestor_id(ancestor_id: str) -> Optional[str]:
    """
    祖先馬IDを正規化（修正版 - 英字対応）
    
    修正ポイント:
    1. 英数字を許可（数字のみの制限を解除）
    2. 外国馬ID「000」を除外
    3. JRA馬IDの桁数チェック（4-10桁）
    
    Args:
        ancestor_id: 生のID文字列
    
    Returns:
        正規化されたID、または無効な場合はNone
    """
    if not ancestor_id or ancestor_id.strip() == '':
        return None

    # ▼▼▼ 修正箇所: 英数字を抽出（数字のみではない） ▼▼▼
    # 英数字のみを抽出（記号やスペースは削除）
    cleaned = re.sub(r'[^a-zA-Z0-9]', '', ancestor_id).strip()
    # ▲▲▲ 修正箇所 ▲▲▲

    if not cleaned:
        return None

    # 外国馬（netkeiba ID = "000"）を除外
    # これはnetkeiba上で詳細ページが存在しない祖先を示す
    if cleaned == '000':
        return None

    # 全ゼロパターンを除外（000以外の桁数でも）
    # ただし、英字が含まれる場合は除外しない（例: 000a00fe2a は有効）
    if cleaned.isdigit() and cleaned == '0' * len(cleaned):
        return None

    # ▼▼▼ 修正箇所: 桁数チェックを英数字混在IDにも対応 ▼▼▼
    # JRA馬IDおよび英数字混在IDの桁数チェック（4-10桁）
    if len(cleaned) < 4 or len(cleaned) > 10:
        return None
    # ▲▲▲ 修正箇所 ▲▲▲

    return cleaned


def normalize_ancestor_name(name: str) -> Optional[str]:
    """
    祖先馬名を正規化（修正版）
    
    修正ポイント:
    1. 複数行テキストから最初の行のみ取得
    2. 前後のスペース除去
    3. 空文字列チェック
    
    Note:
    英名とカタカナの混在は元データの仕様なので、
    そのまま保持します（正規化しません）
    """
    if not name:
        return None

    # 前後のスペース除去
    name = name.strip()

    if not name:
        return None

    # 複数行テキストから最初の行のみを取得
    name = name.split('\n')[0].strip()

    if not name:
        return None

    return name


def extract_horse_id_from_filename(file_path: str) -> str:
    """
    ファイル名から馬IDを抽出
    """
    filename = Path(file_path).stem
    match = re.search(r'(\d{10})', filename)
    return match.group(1) if match else None
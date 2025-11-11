"""
血統情報パーサ
netkeiba.com の血統ページから情報を抽出
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

    Args:
        file_path: HTMLファイルパス
        horse_id: 馬ID

    Returns:
        5代血統のDataFrame
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
    ancestor_links = blood_table.find_all('a', href=re.compile(r'/horse/\d+'))

    for link in ancestor_links:
        href = link.get('href')
        match = re.search(r'/horse/(\d+)', href)
        if match:
            ancestor_id = normalize_ancestor_id(match.group(1))
            ancestor_name = normalize_ancestor_name(link.get_text(strip=True))

            # バリデーション: 無効なIDを除外
            if ancestor_id and ancestor_name:
                # 重複を避ける
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
    祖先馬IDを正規化
    - 外国馬（netkeiba ID = "000"）を除外
    - JRA馬ID（4-10桁の数字）のみ有効
    - 有効なJRA ID例: 2004110007, 1991190001, 2010104155

    Args:
        ancestor_id: 生のID文字列

    Returns:
        正規化されたID、または無効な場合はNone
    """
    if not ancestor_id or ancestor_id.strip() == '':
        return None

    # 数字のみを抽出
    cleaned = re.sub(r'[^\d]', '', ancestor_id).strip()

    # 空の場合
    if not cleaned:
        return None

    # 外国馬（"000"や同等の無効パターン）を除外
    # これらはnetkeiba上で個別情報ページがない祖先（外国生まれ等）
    if cleaned in ['0', '00', '000', '0000', '00000', '000000', '0000000', '00000000', '000000000', '0000000000']:
        return None

    # JRA馬ID長チェック（通常4-10桁）
    if len(cleaned) < 4 or len(cleaned) > 10:
        return None

    return cleaned


def normalize_ancestor_name(name: str) -> Optional[str]:
    """
    祖先馬名を正規化
    - スペース/タブを統一
    - 英名とカタカナの混在を正規化（カタカナ優先）

    Args:
        name: 生の名前文字列

    Returns:
        正規化された名前、または無効な場合はNone
    """
    if not name:
        return None

    # スペース/タブを統一（先頭後置のスペース除去）
    name = name.strip()

    # 空の場合
    if not name:
        return None

    # 複数行テキストから最初の行のみを取得
    name = name.split('\n')[0].strip()

    return name


def extract_horse_id_from_filename(file_path: str) -> str:
    """
    ファイル名から馬IDを抽出
    """
    filename = Path(file_path).stem
    match = re.search(r'(\d{10})', filename)
    return match.group(1) if match else None

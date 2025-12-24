"""
血統情報パーサ (修正版 - 世代情報(generation)カラム追加および構造的パース)
"""

import re
import logging
from typing import Dict, List, Optional
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup, element

# 世代特定用のマップ
# (rowspan, height) -> generation
# 1代目(親) ～ 5代目(高祖父母の親)
GENERATION_MAP = {
    "16": 1, # rowspan="16"
    "8": 2,  # rowspan="8"
    "4": 3,  # rowspan="4"
    "2": 4,  # rowspan="2"
    "20": 5, # height="20"
}


def parse_pedigree_html(file_path: str, horse_id: str = None) -> pd.DataFrame:
    """
    血統HTMLをパースしてDataFrameを返す (構造解析対応版)
    """
    logging.info(f"血統情報パース開始: {file_path}")

    if horse_id is None:
        horse_id = extract_horse_id_from_filename(file_path)

    with open(file_path, 'rb') as f:
        html_bytes = f.read()

    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except Exception:
        html_text = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_text, 'html.parser')

    blood_table = soup.find('table', class_='blood_table')

    if not blood_table:
        logging.error(f"血統テーブルが見つかりません: {file_path}")
        return pd.DataFrame()

    rows = []
    
    # ▼▼▼ 修正: 構造的パース ▼▼▼
    # 既存の find_all('a') ではなく、tbody > tr > td を解析
    
    # 収集済みのIDを管理するセット（重複防止用）
    collected_ids = set()

    # blood_table 内のすべての <td> タグを取得
    all_tds = blood_table.find_all('td')

    for td in all_tds:
        generation = None
        
        # 1. rowspan 属性で世代を特定
        if td.has_attr('rowspan'):
            rowspan_val = td.get('rowspan')
            if rowspan_val in GENERATION_MAP:
                generation = GENERATION_MAP[rowspan_val]
        
        # 2. rowspan がない場合、height 属性で世代を特定 (5代目)
        elif td.has_attr('height'):
            height_val = td.get('height')
            if height_val in GENERATION_MAP:
                generation = GENERATION_MAP[height_val]

        # 世代が特定できた <td> のみ処理
        if generation:
            # <td> タグ内の馬リンク <a> を探す
            link = td.find('a', href=re.compile(r'/horse/'))
            
            if link:
                href = link.get('href')
                match = re.search(r'/horse/([^/]+)', href)
                
                if match:
                    raw_id = match.group(1)
                    
                    # IDと名前を正規化
                    ancestor_id = normalize_ancestor_id(raw_id)
                    
                    # ▼▼▼ 修正: 名前の取得ロジック ▼▼▼
                    # <a> タグ内のテキストだけでなく、<br> で区切られる前の
                    # <td> 全体のテキストから馬名（カタカナ）を抽出する
                    # (例: サンデーサイレンス <br> Sunday Silence(米))
                    ancestor_name = normalize_ancestor_name(td, link)
                    # ▲▲▲ 修正 ▲▲▲

                    # バリデーション: 有効なIDと名前のみ追加
                    if ancestor_id and ancestor_name:
                        # 重複チェック
                        if ancestor_id not in collected_ids:
                            rows.append({
                                'horse_id': horse_id,
                                'ancestor_id': ancestor_id,
                                'ancestor_name': ancestor_name,
                                'generation': generation, # 世代カラムを追加
                            })
                            collected_ids.add(ancestor_id)
    # ▲▲▲ 修正 ▲▲▲

    df = pd.DataFrame(rows)
    logging.info(f"血統情報パース完了: {file_path} ({len(df)}行)")

    return df


def normalize_ancestor_id(ancestor_id: str) -> Optional[str]:
    """
    祖先馬IDを正規化（修正版 - 英字対応）
    
    修正ポイント:
    1. 英数字を許可（数字のみの制限を解除）
    2. 外国馬ID「000」を除外
    3. IDの桁数チェック（4-10桁）
    
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
    # (netkeibaの外国馬IDは '000a00033a' のように10桁)
    if len(cleaned) < 4 or len(cleaned) > 10:
        # ログレベルをDEBUGに変更（ファイル名由来のhorse_idは10桁を超える場合があるため）
        logging.debug(f"無効な桁数のIDをスキップ: {cleaned} (元: {ancestor_id})")
        return None
    # ▲▲▲ 修正箇所 ▲▲▲

    return cleaned


def normalize_ancestor_name(td_tag: element.Tag, link_tag: element.Tag) -> Optional[str]:
    """
    祖先馬名を正規化（修正版 - 構造的解析）
    
    修正ポイント:
    1. <a> タグの直接のテキスト (link_tag.get_text()) を優先する
       (例: <a ...>スズカマンボ</a>)
    2. <a> タグのテキストが空の場合、<td> の先頭のテキストノードを取得する
       (例: <td> <a ...></a><br>Sunday Silence(米) ... の場合に Sunday Silence を取得)
       ※ただし、提供されたHTMLでは <a> タグ内に名前が必ず入っている
    
    Args:
        td_tag (element.Tag): 解析対象の <td>
        link_tag (element.Tag): <td> 内の <a>
    
    Returns:
        正規化された馬名、またはNone
    """
    
    # <a> タグ直下のテキスト (<span> タグを含む場合も考慮)
    name = link_tag.get_text(strip=True)

    if not name:
        # フォールバック: <td> の最初のテキストノード (<a> の前にある場合など)
        # ただし、通常は <a> の中に名前があるはず
        td_texts = td_tag.find_all(string=True, recursive=False)
        if td_texts:
            name = td_texts[0].strip()
        else:
            name = td_tag.get_text(strip=True) # 最終手段

    if not name:
        return None

    # 前後のスペース除去
    name = name.strip()

    if not name:
        return None

    # 複数行テキスト (例: "サンデーサイレンス \n Sunday Silence(米)")
    # や、<br> タグによって分割されている場合を考慮し、
    # 最初の意味のある行を取得する
    
    # <a> タグの .contents を見て、最初のテキストノードを取得するのが最も確実
    primary_name = None
    if link_tag.contents:
        for content in link_tag.contents:
            if isinstance(content, element.NavigableString):
                primary_name = content.strip()
                if primary_name:
                    break
            # <span> タグ (例: <span class="red">Mr. Prospector</span>) の中も見る
            elif isinstance(content, element.Tag) and content.name == 'span':
                primary_name = content.get_text(strip=True)
                if primary_name:
                    break
    
    if primary_name:
        name = primary_name

    # それでも見つからない場合は、get_text() の結果をフォールバックとして使用
    if not name:
        return None

    # name に <br> が含まれている場合 (get_text() が <br> を \n にしない場合がある)
    # または \n が含まれている場合
    name = name.split('\n')[0].split('<br>')[0].strip()

    if not name:
        return None

    return name


def extract_horse_id_from_filename(file_path: str) -> str:
    """
    ファイル名から馬IDを抽出 (ped_XXXXXXXXXX.html)
    """
    filename = Path(file_path).stem
    # ped_2001103890... から 2001103890 を抽出
    match = re.search(r'(\d{10})', filename)
    if match:
        return match.group(1)

    # フォールバック (英字IDの場合)
    # ped_000a00033a...
    match = re.search(r'ped_([a-zA-Z0-9]{10})', filename)
    if match:
        return match.group(1)

    logging.warning(f"ファイル名 {filename} から horse_id (10桁) を抽出できませんでした。")
    return None
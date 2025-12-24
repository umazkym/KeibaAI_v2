"""
HTML完全性検証ユーティリティ

スクレイピング後にHTMLが完全に取得できたかを検証する共通モジュール。
他のスクレイピングスクリプトからインポートして使用可能。
"""
from pathlib import Path
from typing import Tuple, Optional
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)


class HtmlValidationResult:
    """HTML検証結果"""
    def __init__(self, is_valid: bool, error_type: Optional[str] = None, message: str = "OK"):
        self.is_valid = is_valid
        self.error_type = error_type
        self.message = message
    
    def __bool__(self):
        return self.is_valid
    
    def __repr__(self):
        if self.is_valid:
            return f"HtmlValidationResult(valid=True, message='{self.message}')"
        return f"HtmlValidationResult(valid=False, error_type='{self.error_type}', message='{self.message}')"


def validate_race_html(html_bytes: bytes) -> HtmlValidationResult:
    """
    レースHTMLの完全性を検証
    
    チェック項目:
    1. ファイルサイズが十分か
    2. titleタグが存在するか
    3. race_table_01が存在するか（レース中止等を除く）
    
    Args:
        html_bytes: HTMLのバイトデータ
    
    Returns:
        HtmlValidationResult
    """
    # サイズチェック
    if len(html_bytes) < 1000:
        return HtmlValidationResult(
            is_valid=False,
            error_type='empty_or_small',
            message=f'ファイルサイズが小さすぎます ({len(html_bytes)} bytes)'
        )
    
    # HTMLデコード
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # タイトルチェック（HTMLが途中で切れていないか）
    title_tag = soup.find('title')
    if not title_tag:
        return HtmlValidationResult(
            is_valid=False,
            error_type='no_title',
            message='titleタグがありません（HTMLが途中で切れている可能性）'
        )
    
    title_text = title_tag.get_text()
    
    # エラーページチェック
    if 'エラー' in title_text or '見つかりません' in title_text or '404' in title_text:
        return HtmlValidationResult(
            is_valid=True,
            error_type='error_page',
            message='エラーページ（レース中止・取消等）'
        )
    
    # レース結果テーブルチェック
    race_table = soup.find('table', class_='race_table_01')
    if not race_table:
        return HtmlValidationResult(
            is_valid=False,
            error_type='no_race_table',
            message='race_table_01がありません'
        )
    
    # 結果テーブルに行があるか
    rows = race_table.find_all('tr')
    if len(rows) < 2:  # ヘッダー + 最低1行
        return HtmlValidationResult(
            is_valid=False,
            error_type='empty_race_table',
            message='race_table_01に結果行がありません'
        )
    
    return HtmlValidationResult(
        is_valid=True,
        message=f'OK ({len(rows) - 1}頭)'
    )


def validate_horse_html(html_bytes: bytes) -> HtmlValidationResult:
    """馬プロフィールHTMLの完全性を検証"""
    if len(html_bytes) < 500:
        return HtmlValidationResult(
            is_valid=False,
            error_type='empty_or_small',
            message=f'ファイルサイズが小さすぎます ({len(html_bytes)} bytes)'
        )
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # タイトルチェック
    if not soup.find('title'):
        return HtmlValidationResult(
            is_valid=False,
            error_type='no_title',
            message='titleタグがありません'
        )
    
    # 馬名チェック
    horse_title = soup.find('div', class_='horse_title')
    if not horse_title:
        return HtmlValidationResult(
            is_valid=False,
            error_type='no_horse_title',
            message='horse_titleがありません'
        )
    
    return HtmlValidationResult(is_valid=True, message='OK')


def validate_pedigree_html(html_bytes: bytes) -> HtmlValidationResult:
    """血統HTMLの完全性を検証"""
    if len(html_bytes) < 1000:
        return HtmlValidationResult(
            is_valid=False,
            error_type='empty_or_small',
            message=f'ファイルサイズが小さすぎます ({len(html_bytes)} bytes)'
        )
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # 血統テーブルチェック
    pedigree_table = soup.find('table', class_='blood_table')
    if not pedigree_table:
        return HtmlValidationResult(
            is_valid=False,
            error_type='no_blood_table',
            message='blood_tableがありません'
        )
    
    return HtmlValidationResult(is_valid=True, message='OK')


def validate_html_file(file_path: Path, html_type: str = 'race') -> HtmlValidationResult:
    """
    ファイルパスからHTMLを読み込んで検証
    
    Args:
        file_path: HTMLファイルのパス
        html_type: 'race', 'horse', 'pedigree'のいずれか
    
    Returns:
        HtmlValidationResult
    """
    if not file_path.exists():
        return HtmlValidationResult(
            is_valid=False,
            error_type='file_not_found',
            message=f'ファイルが存在しません: {file_path}'
        )
    
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    validators = {
        'race': validate_race_html,
        'horse': validate_horse_html,
        'pedigree': validate_pedigree_html,
    }
    
    validator = validators.get(html_type, validate_race_html)
    return validator(html_bytes)


# テスト用
if __name__ == "__main__":
    from pathlib import Path
    
    # テスト
    test_files = [
        Path(r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\race\201405010404.bin"),
        Path(r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\race\201401010101.bin"),
    ]
    
    for f in test_files:
        if f.exists():
            result = validate_html_file(f, 'race')
            print(f"{f.name}: {result}")

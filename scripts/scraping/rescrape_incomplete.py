"""
不完全なHTMLファイルの再スクレイピングと検証

機能:
1. 指定されたrace_idのHTMLを再取得
2. 取得後に完全性を検証
3. 検証失敗時はリトライ
"""
import time
import random
import logging
from pathlib import Path
from typing import List, Tuple
import requests
from bs4 import BeautifulSoup

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "keibaai" / "data" / "raw" / "html" / "race"

# リトライ設定
MAX_RETRIES = 3
RETRY_DELAY = 5  # 秒
REQUEST_DELAY = 2  # リクエスト間隔

def validate_html_content(html_bytes: bytes) -> Tuple[bool, str]:
    """
    HTMLコンテンツの完全性を検証
    
    Returns:
        (is_valid, error_message)
    """
    if len(html_bytes) < 1000:
        return False, "ファイルサイズが小さすぎます"
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # タイトルチェック
    if not soup.find('title'):
        return False, "titleタグがありません"
    
    # レース結果テーブルチェック
    if not soup.find('table', class_='race_table_01'):
        # エラーページかどうか確認
        title = soup.find('title')
        if title and ('エラー' in title.get_text() or '見つかりません' in title.get_text()):
            return True, "エラーページ（レース中止等）"
        return False, "race_table_01がありません"
    
    return True, "OK"

def scrape_race_html(race_id: str) -> Tuple[bool, bytes, str]:
    """
    レースHTMLをスクレイピング
    
    Returns:
        (success, html_bytes, message)
    """
    url = f"https://db.netkeiba.com/race/{race_id}/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            html_bytes = response.content
            
            # 完全性検証
            is_valid, message = validate_html_content(html_bytes)
            
            if is_valid:
                return True, html_bytes, message
            else:
                logger.warning(f"検証失敗 (attempt {attempt + 1}/{MAX_RETRIES}): {message}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                return False, html_bytes, message
                
        except requests.RequestException as e:
            logger.error(f"リクエストエラー (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return False, b'', str(e)
    
    return False, b'', "最大リトライ回数に達しました"

def rescrape_files(race_ids: List[str], dry_run: bool = False) -> dict:
    """
    指定されたrace_idのファイルを再スクレイピング
    
    Args:
        race_ids: 再スクレイピング対象のrace_idリスト
        dry_run: Trueの場合、実際の保存は行わない
    
    Returns:
        結果の辞書
    """
    results = {
        'success': [],
        'failed': [],
        'skipped': []
    }
    
    logger.info(f"再スクレイピング開始: {len(race_ids)}件")
    
    for i, race_id in enumerate(race_ids, 1):
        logger.info(f"[{i}/{len(race_ids)}] race_id: {race_id}")
        
        success, html_bytes, message = scrape_race_html(race_id)
        
        if success:
            if not dry_run:
                output_path = RAW_DIR / f"{race_id}.bin"
                with open(output_path, 'wb') as f:
                    f.write(html_bytes)
                logger.info(f"  ✓ 保存完了: {output_path.name} ({len(html_bytes)} bytes)")
            else:
                logger.info(f"  ✓ [DRY RUN] 取得成功 ({len(html_bytes)} bytes)")
            results['success'].append(race_id)
        else:
            logger.error(f"  ✗ 失敗: {message}")
            results['failed'].append((race_id, message))
        
        # リクエスト間隔
        if i < len(race_ids):
            jitter = random.uniform(0.5, 1.5)
            time.sleep(REQUEST_DELAY * jitter)
    
    return results

def main():
    print("=" * 80)
    print("  不完全HTMLファイルの再スクレイピング")
    print("=" * 80)
    
    # 再スクレイピング対象を読み込み
    targets_file = PROJECT_ROOT / "keibaai" / "data" / "rescrape_targets.txt"
    
    if not targets_file.exists():
        print(f"\n  対象ファイルが見つかりません: {targets_file}")
        return
    
    with open(targets_file, 'r') as f:
        race_ids = [line.strip() for line in f if line.strip()]
    
    print(f"\n  対象ファイル数: {len(race_ids)}")
    for rid in race_ids:
        print(f"    - {rid}")
    
    print(f"\n  再スクレイピングを開始します...")
    results = rescrape_files(race_ids, dry_run=False)
    
    print(f"\n  【結果】")
    print(f"    成功: {len(results['success'])}件")
    print(f"    失敗: {len(results['failed'])}件")
    
    if results['failed']:
        print(f"\n  失敗したファイル:")
        for race_id, message in results['failed']:
            print(f"    - {race_id}: {message}")
    
    print("\n" + "=" * 80)
    print("  完了")
    print("=" * 80)

if __name__ == "__main__":
    main()

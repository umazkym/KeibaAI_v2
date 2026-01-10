# -*- coding: utf-8 -*-
"""
エラーレースの再スクレイピングスクリプト

error_races.log から軸切替エラーが発生したレースを読み込み、
該当の券種のみを再スクレイピングします。

使用方法:
    python scripts/scraping/rescrape_errors.py
"""
import sys
import logging
from pathlib import Path
from datetime import datetime

# プロジェクトルートをパスに追加
PROJECT_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from keibaai.src.preparing._scrape_odds_html import scrape_odds_html_selenium
from keibaai.src.preparing._scrape_html import prepare_chrome_driver

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

RAW_ODDS_DIR = PROJECT_ROOT / 'keibaai' / 'data' / 'raw' / 'html' / 'odds'
ERROR_LOG_PATH = RAW_ODDS_DIR / 'error_races.log'


def load_error_races():
    """エラーログからレースIDと券種を読み込む"""
    if not ERROR_LOG_PATH.exists():
        logger.info("エラーログが存在しません")
        return []
    
    errors = []
    with open(ERROR_LOG_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 2:
                race_id = parts[0]
                bet_type = parts[1]
                errors.append((race_id, bet_type))
    
    # 重複を除去
    return list(set(errors))


def main():
    errors = load_error_races()
    
    if not errors:
        logger.info("再スクレイピング対象がありません")
        return
    
    logger.info(f"再スクレイピング対象: {len(errors)}件")
    
    driver = prepare_chrome_driver(headless=True)
    success_count = 0
    
    try:
        for race_id, bet_type in errors:
            logger.info(f"再スクレイピング: {race_id} ({bet_type})")
            output_dir = RAW_ODDS_DIR / bet_type
            
            # 強制再取得（skip_existing=False）
            result = scrape_odds_html_selenium(
                driver, race_id, bet_type, output_dir, skip_existing=False
            )
            
            if result:
                success_count += 1
                logger.info(f"成功: {race_id} ({bet_type})")
            else:
                logger.warning(f"失敗: {race_id} ({bet_type})")
    
    finally:
        driver.quit()
    
    logger.info(f"完了: {success_count}/{len(errors)}件成功")
    
    # 成功したらエラーログをクリア
    if success_count == len(errors):
        ERROR_LOG_PATH.unlink(missing_ok=True)
        logger.info("エラーログをクリアしました")


if __name__ == "__main__":
    main()

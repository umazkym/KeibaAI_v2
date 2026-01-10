# -*- coding: utf-8 -*-
"""
不完全データの再スクレイピングスクリプト

incomplete_races.log から不完全なデータのレースを読み込み、
該当の券種のみを強制再取得します。

使用方法:
    python scripts/scraping/rescrape_incomplete.py
"""
import sys
import logging
import time
from pathlib import Path
from tqdm import tqdm

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
INCOMPLETE_LOG_PATH = RAW_ODDS_DIR / 'incomplete_races.log'


def load_incomplete_races():
    """不完全レースログからレースIDと券種を読み込む"""
    if not INCOMPLETE_LOG_PATH.exists():
        logger.info("不完全レースログが存在しません")
        return []
    
    races = []
    with open(INCOMPLETE_LOG_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 2:
                race_id = parts[0]
                bet_type = parts[1]
                races.append((race_id, bet_type))
    
    # 重複を除去
    return list(set(races))


def main():
    races = load_incomplete_races()
    
    if not races:
        logger.info("再スクレイピング対象がありません")
        return
    
    logger.info(f"再スクレイピング対象: {len(races)}件")
    logger.info("これには数時間かかる可能性があります。Ctrl+Cで中断可能、再実行で続行可能。")
    
    driver = prepare_chrome_driver(headless=True)
    success_count = 0
    failed_races = []
    
    try:
        for race_id, bet_type in tqdm(races, desc="再スクレイピング"):
            output_dir = RAW_ODDS_DIR / bet_type
            
            try:
                # 強制再取得（skip_existing=False）
                result = scrape_odds_html_selenium(
                    driver, race_id, bet_type, output_dir, skip_existing=False
                )
                
                if result:
                    success_count += 1
                else:
                    failed_races.append((race_id, bet_type))
                    
            except Exception as e:
                logger.warning(f"エラー: {race_id} ({bet_type}): {e}")
                failed_races.append((race_id, bet_type))
                # ドライバー再起動
                try:
                    driver.quit()
                except:
                    pass
                time.sleep(5)
                driver = prepare_chrome_driver(headless=True)
    
    except KeyboardInterrupt:
        logger.warning("ユーザーによる中断")
    finally:
        driver.quit()
    
    logger.info(f"完了: {success_count}/{len(races)}件成功")
    
    # 失敗分を新しいログに保存
    if failed_races:
        failed_log = RAW_ODDS_DIR / 'still_incomplete.log'
        with open(failed_log, 'w', encoding='utf-8') as f:
            for race_id, bet_type in failed_races:
                f.write(f"{race_id},{bet_type}\n")
        logger.info(f"失敗分を保存: {failed_log}")
    else:
        # 全成功ならログをクリア
        INCOMPLETE_LOG_PATH.unlink(missing_ok=True)
        logger.info("全て成功。不完全レースログをクリアしました。")


if __name__ == "__main__":
    main()

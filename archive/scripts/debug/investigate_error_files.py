#!/usr/bin/env python3
"""
エラーが発生した特定のファイルを詳細調査
"""

import sys
import logging
from pathlib import Path

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('investigate_errors.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / 'keibaai'))

from bs4 import BeautifulSoup
from keibaai.src.modules.parsers.results_parser import parse_results_html


def investigate_file(race_id: str):
    """特定のレースファイルを詳細調査"""
    logger.info("=" * 80)
    logger.info(f"レースID: {race_id} の詳細調査")
    logger.info("=" * 80)

    file_path = Path(f"keibaai/data/raw/html/race/{race_id}.bin")

    if not file_path.exists():
        logger.error(f"❌ ファイルが存在しません: {file_path}")
        return

    logger.info(f"ファイルパス: {file_path}")
    logger.info(f"ファイルサイズ: {file_path.stat().st_size} bytes")

    # ファイルを読み込み
    try:
        with open(file_path, 'rb') as f:
            html_bytes = f.read()

        # エンコーディングを試行
        html_text = None
        for encoding in ['euc_jp', 'utf-8', 'shift_jis']:
            try:
                html_text = html_bytes.decode(encoding, errors='replace')
                logger.info(f"✅ エンコーディング成功: {encoding}")
                break
            except Exception:
                continue

        if not html_text:
            logger.error("❌ エンコーディングに失敗しました")
            return

        # HTML解析
        soup = BeautifulSoup(html_text, "html.parser")
        title = soup.find('title')
        logger.info(f"タイトル: {title.text if title else 'N/A'}")

        # HTML構造の確認
        logger.info("\nHTML構造:")

        race_table = soup.find('table', class_='race_table_01')
        logger.info(f"  race_table_01: {'✅ あり' if race_table else '❌ なし'}")

        if race_table:
            rows = race_table.find_all('tr')
            logger.info(f"  テーブル行数: {len(rows)}")

        data_intro = soup.find('div', class_='data_intro')
        logger.info(f"  data_intro: {'✅ あり' if data_intro else '❌ なし'}")

        racedata = soup.find('dl', class_='racedata')
        logger.info(f"  racedata: {'✅ あり' if racedata else '❌ なし'}")

        # エラーメッセージの確認
        logger.info("\nエラーメッセージの確認:")
        error_messages = [
            "レース情報が見つかりません",
            "該当するデータがありません",
            "開催されていません",
            "中止",
            "取消",
            "404 Not Found"
        ]

        for msg in error_messages:
            if msg in html_text:
                logger.warning(f"  ⚠️ 検出: {msg}")

        # HTMLの一部を表示（デバッグ用）
        logger.info("\nHTML冒頭（200文字）:")
        logger.info(html_text[:200])

        # パース試行
        logger.info("\nパース試行:")
        try:
            df = parse_results_html(str(file_path), race_id=race_id)
            logger.info(f"✅ パース成功: {len(df)}行 × {len(df.columns)}列")

            # 最初の数行を表示
            logger.info("\nパース結果（最初の3行）:")
            logger.info(df.head(3).to_string())

        except Exception as e:
            logger.error(f"❌ パースエラー: {str(e)}")
            logger.error("詳細:", exc_info=True)

    except Exception as e:
        logger.error(f"❌ ファイル読み込みエラー: {str(e)}", exc_info=True)

    logger.info("\n")


def main():
    """エラーが発生したファイルを調査"""
    logger.info("エラーファイル詳細調査")
    logger.info("=" * 80)

    # デバッグログから特定されたエラーファイル
    error_files = [
        "202109060502",  # 2021年9月 阪神06 5日目 2R
        "202304010208",  # 2023年4月 中山01 2日目 8R
    ]

    for race_id in error_files:
        investigate_file(race_id)

    logger.info("=" * 80)
    logger.info("調査完了")
    logger.info("詳細ログ: investigate_errors.log")


if __name__ == "__main__":
    main()

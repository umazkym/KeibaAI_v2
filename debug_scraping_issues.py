#!/usr/bin/env python3
"""
スクレイピング問題の段階的デバッグスクリプト
各問題を個別に調査して原因を特定します
"""

import sys
import logging
from pathlib import Path
import requests
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime

# ロギング設定
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('debug_scraping.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / 'keibaai'))

from keibaai.src.modules.constants import UrlPaths

# テスト用の設定
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

def test_1_url_constants():
    """テスト1: URL定数の確認"""
    logger.info("=" * 80)
    logger.info("テスト1: URL定数の確認")
    logger.info("=" * 80)

    try:
        logger.info(f"CALENDAR_URL: {UrlPaths.CALENDAR_URL}")
        logger.info(f"RACE_LIST_URL: {UrlPaths.RACE_LIST_URL}")
        return True
    except Exception as e:
        logger.error(f"URL定数の取得に失敗: {e}")
        return False


def test_2_simple_calendar_request():
    """テスト2: カレンダーURLへのシンプルなリクエスト"""
    logger.info("=" * 80)
    logger.info("テスト2: カレンダーURLへのシンプルなリクエスト")
    logger.info("=" * 80)

    # 2020年1月のカレンダーをテスト
    year, month = 2020, 1
    url = f'{UrlPaths.CALENDAR_URL}?year={year}&month={month}'
    logger.info(f"テストURL: {url}")

    try:
        # 最小限のヘッダーでリクエスト
        headers = {
            "User-Agent": random.choice(USER_AGENTS)
        }

        logger.info(f"リクエスト送信中... (ヘッダー: {headers})")
        response = requests.get(url, headers=headers, timeout=30)

        logger.info(f"ステータスコード: {response.status_code}")
        logger.info(f"レスポンスヘッダー: {dict(response.headers)}")
        logger.info(f"コンテンツ長: {len(response.content)} bytes")

        if response.status_code == 200:
            logger.info("✅ リクエスト成功")

            # HTMLの一部を表示
            try:
                soup = BeautifulSoup(response.content, "html.parser")
                title = soup.find('title')
                logger.info(f"ページタイトル: {title.text if title else 'N/A'}")

                # カレンダーテーブルの存在確認
                calendar_table = soup.find('table', class_='Calendar_Table')
                if calendar_table:
                    logger.info("✅ カレンダーテーブルが見つかりました")
                else:
                    logger.warning("⚠️ カレンダーテーブルが見つかりません")
                    # 代替パターンを探す
                    all_tables = soup.find_all('table')
                    logger.info(f"ページ内のテーブル数: {len(all_tables)}")
                    for i, table in enumerate(all_tables[:3]):
                        logger.info(f"テーブル{i+1}のクラス: {table.get('class', [])}")

            except Exception as e:
                logger.error(f"HTML解析エラー: {e}")

            return True
        elif response.status_code == 400:
            logger.error("❌ HTTP 400 エラー - リクエストが不正です")
            logger.error(f"レスポンス内容: {response.text[:500]}")
            return False
        else:
            logger.warning(f"⚠️ 予期しないステータスコード: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ リクエストエラー: {e}")
        return False


def test_3_calendar_with_delay():
    """テスト3: 待機時間を設けた複数リクエスト"""
    logger.info("=" * 80)
    logger.info("テスト3: 待機時間を設けた複数リクエスト")
    logger.info("=" * 80)

    test_months = [
        (2020, 1),
        (2023, 10),
        (2024, 1),
    ]

    for year, month in test_months:
        url = f'{UrlPaths.CALENDAR_URL}?year={year}&month={month}'
        logger.info(f"\nテスト: {year}年{month}月")
        logger.info(f"URL: {url}")

        # ランダム待機
        sleep_time = random.uniform(3.0, 6.0)
        logger.info(f"待機時間: {sleep_time:.2f}秒")
        time.sleep(sleep_time)

        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            response = requests.get(url, headers=headers, timeout=30)

            logger.info(f"ステータスコード: {response.status_code}")

            if response.status_code == 200:
                logger.info("✅ 成功")
            elif response.status_code == 400:
                logger.error("❌ HTTP 400 エラー")
                logger.error(f"レスポンス: {response.text[:300]}")
                return False
            else:
                logger.warning(f"⚠️ ステータス: {response.status_code}")

        except Exception as e:
            logger.error(f"❌ エラー: {e}")
            return False

    return True


def test_4_parse_error_analysis():
    """テスト4: パースエラーの分析"""
    logger.info("=" * 80)
    logger.info("テスト4: パースエラーの分析")
    logger.info("=" * 80)

    # パースエラーが発生したファイルを調査
    race_dir = Path("keibaai/data/raw/html/race")

    if not race_dir.exists():
        logger.warning(f"⚠️ レースディレクトリが存在しません: {race_dir}")
        return False

    # 2025年のレース結果ファイルを探す
    race_files_2025 = sorted(race_dir.glob("2025*.bin"))

    if not race_files_2025:
        logger.warning("⚠️ 2025年のレースファイルが見つかりません")
        return False

    logger.info(f"2025年のレースファイル数: {len(race_files_2025)}")

    # 最初の3ファイルを調査
    for i, file_path in enumerate(race_files_2025[:3]):
        logger.info(f"\n--- ファイル {i+1}: {file_path.name} ---")
        logger.info(f"ファイルサイズ: {file_path.stat().st_size} bytes")

        try:
            # ファイルを読み込み
            with open(file_path, 'rb') as f:
                html_bytes = f.read()

            # エンコーディングを試行
            for encoding in ['euc_jp', 'utf-8', 'shift_jis']:
                try:
                    html_text = html_bytes.decode(encoding)
                    logger.info(f"✅ エンコーディング成功: {encoding}")

                    # HTMLの基本情報を取得
                    soup = BeautifulSoup(html_text, "html.parser")
                    title = soup.find('title')
                    logger.info(f"タイトル: {title.text if title else 'N/A'}")

                    # レース結果テーブルの存在確認
                    result_table = soup.find('table', class_='race_table_01')
                    if result_table:
                        logger.info("✅ レース結果テーブルが見つかりました")
                    else:
                        logger.warning("⚠️ レース結果テーブルが見つかりません")
                        # 他のパターンを探す
                        if "レース情報が見つかりません" in html_text:
                            logger.warning("⚠️ レースが実施されていない可能性があります")
                        elif "404" in html_text or "Not Found" in html_text:
                            logger.warning("⚠️ ページが存在しません（404エラー）")

                    break
                except UnicodeDecodeError:
                    logger.debug(f"エンコーディング失敗: {encoding}")

        except Exception as e:
            logger.error(f"❌ ファイル読み込みエラー: {e}")

    return True


def test_5_log_encoding_check():
    """テスト5: ログファイルのエンコーディング確認"""
    logger.info("=" * 80)
    logger.info("テスト5: ログファイルのエンコーディング確認")
    logger.info("=" * 80)

    log_file = Path("keibaai/data/logs/2025/11/18/scraping.log")

    if not log_file.exists():
        logger.warning(f"⚠️ ログファイルが存在しません: {log_file}")
        # 代わりにデバッグログを作成
        logger.info("✅ 新しいデバッグログを作成しました: debug_scraping.log")
        logger.info("日本語テスト: スクレイピングパイプラインを開始します")
        return True

    logger.info(f"ログファイル: {log_file}")
    logger.info(f"ファイルサイズ: {log_file.stat().st_size} bytes")

    # 各エンコーディングで読み込みを試行
    for encoding in ['utf-8', 'shift_jis', 'euc_jp', 'cp932']:
        try:
            with open(log_file, 'r', encoding=encoding) as f:
                first_lines = [f.readline() for _ in range(5)]

            logger.info(f"✅ エンコーディング成功: {encoding}")
            logger.info("最初の5行:")
            for line in first_lines:
                logger.info(f"  {line.strip()}")
            break
        except UnicodeDecodeError:
            logger.debug(f"エンコーディング失敗: {encoding}")

    return True


def main():
    """全テストを実行"""
    logger.info("=" * 80)
    logger.info("KeibaAI_v2 スクレイピング問題デバッグ")
    logger.info(f"実行日時: {datetime.now()}")
    logger.info("=" * 80)

    tests = [
        ("URL定数の確認", test_1_url_constants),
        ("シンプルなカレンダーリクエスト", test_2_simple_calendar_request),
        ("待機時間を設けた複数リクエスト", test_3_calendar_with_delay),
        ("パースエラーの分析", test_4_parse_error_analysis),
        ("ログエンコーディング確認", test_5_log_encoding_check),
    ]

    results = {}

    for test_name, test_func in tests:
        logger.info("\n")
        try:
            result = test_func()
            results[test_name] = "✅ PASS" if result else "❌ FAIL"
        except Exception as e:
            logger.error(f"テスト実行エラー: {e}", exc_info=True)
            results[test_name] = "❌ ERROR"

        # テスト間の待機
        time.sleep(2)

    # 結果サマリー
    logger.info("\n" + "=" * 80)
    logger.info("テスト結果サマリー")
    logger.info("=" * 80)
    for test_name, result in results.items():
        logger.info(f"{result} {test_name}")

    logger.info("\n" + "=" * 80)
    logger.info("デバッグ完了")
    logger.info("詳細ログ: debug_scraping.log")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()

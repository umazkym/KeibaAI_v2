#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
バッチ処理対応版スクレイピングパイプライン

Seleniumタイムアウトを防ぐため、処理を小さなバッチに分割して実行します。

使用方法:
    # 2020-2024年を100日ごとに分割して処理
    python keibaai/scripts/run_scraping_batch.py --from 2020-01-01 --to 2024-12-31 --batch-size 100

    # カスタムバッチサイズ（200日ごと）
    python keibaai/scripts/run_scraping_batch.py --from 2020-01-01 --to 2024-12-31 --batch-size 200

メリット:
    1. Seleniumタイムアウトのリスク低減
    2. エラー時の影響範囲が限定される
    3. 進捗が可視化される
    4. 途中から再開可能
"""

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys
import subprocess
import time

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

def setup_logging():
    """ロギング設定"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                project_root / 'keibaai' / 'data' / 'logs' / 'batch_scraping.log',
                encoding='utf-8'
            )
        ]
    )

def split_date_range(start_date: str, end_date: str, batch_size_days: int):
    """
    日付範囲をバッチに分割する

    Args:
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        batch_size_days: バッチサイズ（日数）

    Returns:
        List[(start, end)]: バッチごとの開始日・終了日のリスト
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    batches = []
    current_start = start

    while current_start <= end:
        current_end = min(current_start + timedelta(days=batch_size_days - 1), end)
        batches.append((
            current_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        ))
        current_start = current_end + timedelta(days=1)

    return batches

def run_scraping_batch(from_date: str, to_date: str, skip: bool):
    """
    1バッチ分のスクレイピングを実行

    Args:
        from_date: 開始日
        to_date: 終了日
        skip: 既存ファイルをスキップするか

    Returns:
        bool: 成功したらTrue
    """
    script_path = project_root / 'keibaai' / 'src' / 'run_scraping_pipeline_with_args.py'

    cmd = [
        sys.executable,
        str(script_path),
        '--from', from_date,
        '--to', to_date
    ]

    if skip:
        cmd.append('--skip')
    else:
        cmd.append('--no-skip')

    logging.info(f"バッチ実行: {from_date} 〜 {to_date}")
    logging.info(f"コマンド: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            timeout=7200  # 2時間タイムアウト
        )

        if result.returncode == 0:
            logging.info(f"✓ バッチ成功: {from_date} 〜 {to_date}")
            return True
        else:
            logging.error(f"✗ バッチ失敗: {from_date} 〜 {to_date}")
            logging.error(f"エラー出力:\n{result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logging.error(f"✗ バッチタイムアウト: {from_date} 〜 {to_date}")
        return False
    except Exception as e:
        logging.error(f"✗ バッチ実行エラー: {e}")
        return False

def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='バッチ処理対応版スクレイピングパイプライン',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 2020-2024年を100日ごとに分割
  python %(prog)s --from 2020-01-01 --to 2024-12-31 --batch-size 100

  # カスタムバッチサイズ（50日ごと）
  python %(prog)s --from 2024-01-01 --to 2024-12-31 --batch-size 50 --no-skip
        """
    )

    parser.add_argument('--from', dest='from_date', required=True,
                        help='開始日（YYYY-MM-DD形式）')
    parser.add_argument('--to', dest='to_date', required=True,
                        help='終了日（YYYY-MM-DD形式）')
    parser.add_argument('--batch-size', dest='batch_size', type=int, default=100,
                        help='バッチサイズ（日数）デフォルト: 100')
    parser.add_argument('--skip', dest='skip', action='store_true', default=True,
                        help='既存ファイルをスキップ（デフォルト）')
    parser.add_argument('--no-skip', dest='skip', action='store_false',
                        help='既存ファイルを上書き')
    parser.add_argument('--retry-failed', action='store_true',
                        help='失敗したバッチを再試行')
    parser.add_argument('--wait-between-batches', type=int, default=30,
                        help='バッチ間の待機時間（秒）デフォルト: 30')

    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info("バッチ処理スクレイピングパイプライン開始")
    logger.info("=" * 80)
    logger.info(f"期間: {args.from_date} 〜 {args.to_date}")
    logger.info(f"バッチサイズ: {args.batch_size}日")
    logger.info(f"既存ファイルスキップ: {'有効' if args.skip else '無効'}")
    logger.info("=" * 80)

    # バッチに分割
    batches = split_date_range(args.from_date, args.to_date, args.batch_size)
    logger.info(f"\n全{len(batches)}個のバッチに分割されました:\n")

    for i, (start, end) in enumerate(batches, 1):
        logger.info(f"  バッチ {i}/{len(batches)}: {start} 〜 {end}")

    logger.info("\n" + "=" * 80)

    # バッチ実行
    success_count = 0
    failed_batches = []

    for i, (start, end) in enumerate(batches, 1):
        logger.info(f"\n[バッチ {i}/{len(batches)}] 実行開始")
        logger.info(f"対象期間: {start} 〜 {end}")

        success = run_scraping_batch(start, end, args.skip)

        if success:
            success_count += 1
        else:
            failed_batches.append((i, start, end))

        # バッチ間で待機（サーバー負荷軽減）
        if i < len(batches):
            wait_time = args.wait_between_batches
            logger.info(f"次のバッチまで {wait_time} 秒待機...")
            time.sleep(wait_time)

    # 結果サマリー
    logger.info("\n" + "=" * 80)
    logger.info("バッチ処理完了")
    logger.info("=" * 80)
    logger.info(f"成功: {success_count}/{len(batches)} バッチ")
    logger.info(f"失敗: {len(failed_batches)}/{len(batches)} バッチ")

    if failed_batches:
        logger.warning("\n失敗したバッチ:")
        for batch_num, start, end in failed_batches:
            logger.warning(f"  バッチ {batch_num}: {start} 〜 {end}")

        if args.retry_failed:
            logger.info("\n失敗したバッチを再試行します...")
            retry_success = 0

            for batch_num, start, end in failed_batches:
                logger.info(f"\n[再試行] バッチ {batch_num}: {start} 〜 {end}")
                if run_scraping_batch(start, end, args.skip):
                    retry_success += 1
                time.sleep(args.wait_between_batches)

            logger.info(f"\n再試行結果: {retry_success}/{len(failed_batches)} バッチが成功")
    else:
        logger.info("\n✓ すべてのバッチが正常終了しました")

    logger.info("\n" + "=" * 80)

if __name__ == "__main__":
    main()

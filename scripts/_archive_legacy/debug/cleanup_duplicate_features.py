#!/usr/bin/env python3
"""
重複Feature Parquetファイルのクリーンアップスクリプト

目的:
    各パーティション（year=YYYY/month=MM）内に複数存在するParquetファイルのうち、
    最新の1ファイルのみを残し、古いファイルを削除する。

背景:
    generate_features.py が複数回実行された結果、同じパーティション内に
    重複したファイルが存在し、pd.read_parquet() で重複データが読み込まれる問題が発生。

処理内容:
    1. 各パーティションディレクトリを走査
    2. *.parquet ファイルを更新日時でソート
    3. 最新の1ファイルを除く、すべてのファイルを削除
    4. 削除ログを出力
"""

import sys
from pathlib import Path
from datetime import datetime
import logging

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def cleanup_partition_duplicates(features_base_dir: Path, dry_run: bool = True):
    """
    パーティションディレクトリ内の重複Parquetファイルをクリーンアップ

    Args:
        features_base_dir: features/parquet のパス
        dry_run: Trueの場合、削除せずにログ出力のみ（デフォルト: True）
    """
    if not features_base_dir.exists():
        logging.error(f"ディレクトリが存在しません: {features_base_dir}")
        return

    logging.info("="*80)
    logging.info(f"Feature Parquetファイル クリーンアップ開始")
    logging.info(f"対象ディレクトリ: {features_base_dir}")
    logging.info(f"モード: {'DRY RUN（削除しない）' if dry_run else '実削除'}")
    logging.info("="*80)

    total_files_found = 0
    total_files_to_delete = 0
    total_space_to_free = 0

    # year=YYYY/month=MM パターンのディレクトリを走査
    partition_dirs = sorted(features_base_dir.glob("year=*/month=*"))

    if not partition_dirs:
        logging.warning("パーティションディレクトリが見つかりませんでした")
        return

    logging.info(f"\n検出されたパーティション数: {len(partition_dirs)}")

    for partition_dir in partition_dirs:
        # このパーティション内のparquetファイルを取得
        parquet_files = list(partition_dir.glob("*.parquet"))

        if not parquet_files:
            continue

        total_files_found += len(parquet_files)

        # ファイルを更新日時でソート（新しい順）
        parquet_files_sorted = sorted(
            parquet_files,
            key=lambda f: f.stat().st_mtime,
            reverse=True
        )

        # 最新ファイル
        latest_file = parquet_files_sorted[0]
        latest_mtime = datetime.fromtimestamp(latest_file.stat().st_mtime)

        # 削除対象（最新以外）
        files_to_delete = parquet_files_sorted[1:]

        if files_to_delete:
            partition_name = f"{partition_dir.parent.name}/{partition_dir.name}"
            logging.info(f"\n[{partition_name}]")
            logging.info(f"  総ファイル数: {len(parquet_files)}")
            logging.info(f"  保持: {latest_file.name} (更新: {latest_mtime.strftime('%Y-%m-%d %H:%M:%S')})")

            for old_file in files_to_delete:
                old_mtime = datetime.fromtimestamp(old_file.stat().st_mtime)
                file_size = old_file.stat().st_size
                total_files_to_delete += 1
                total_space_to_free += file_size

                logging.info(
                    f"  削除: {old_file.name} "
                    f"(更新: {old_mtime.strftime('%Y-%m-%d %H:%M:%S')}, "
                    f"サイズ: {file_size/1024:.1f} KB)"
                )

                if not dry_run:
                    try:
                        old_file.unlink()
                        logging.info(f"    ✓ 削除完了")
                    except Exception as e:
                        logging.error(f"    ✗ 削除失敗: {e}")

    # サマリー
    logging.info("\n" + "="*80)
    logging.info("クリーンアップサマリー")
    logging.info("="*80)
    logging.info(f"総ファイル数: {total_files_found}")
    logging.info(f"削除対象: {total_files_to_delete}ファイル")
    logging.info(f"解放予定容量: {total_space_to_free / (1024*1024):.2f} MB")

    if dry_run:
        logging.info("\n⚠️ これはDRY RUNです。実際には削除されていません。")
        logging.info("実際に削除するには --execute オプションを付けて再実行してください。")
    else:
        logging.info("\n✅ 削除が完了しました。")

    logging.info("="*80)

def main():
    """メイン処理"""
    import argparse

    parser = argparse.ArgumentParser(
        description='重複Feature Parquetファイルのクリーンアップ',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # DRY RUN（削除せず確認のみ）
  python cleanup_duplicate_features.py

  # 実際に削除を実行
  python cleanup_duplicate_features.py --execute
        """
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='実際に削除を実行する（デフォルトはDRY RUN）'
    )

    args = parser.parse_args()

    # features/parquet のパスを構築
    features_base_dir = project_root / "keibaai" / "data" / "features" / "parquet"

    # クリーンアップ実行
    cleanup_partition_duplicates(
        features_base_dir=features_base_dir,
        dry_run=not args.execute
    )

if __name__ == "__main__":
    main()

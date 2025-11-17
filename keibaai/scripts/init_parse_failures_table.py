#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
parse_failuresテーブルを作成するスクリプト

使用方法:
    python keibaai/scripts/init_parse_failures_table.py
"""

import sqlite3
import logging
from pathlib import Path
import sys

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

def create_parse_failures_table(db_path: str):
    """
    parse_failuresテーブルを作成する

    Args:
        db_path: データベースファイルのパス
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger(__name__)

    # データベース接続
    logger.info(f"データベースに接続: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # テーブルが既に存在するか確認
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='parse_failures'
        """)

        if cursor.fetchone():
            logger.info("parse_failuresテーブルは既に存在します")

            # テーブル構造を表示
            cursor.execute("PRAGMA table_info(parse_failures)")
            columns = cursor.fetchall()
            logger.info("現在のテーブル構造:")
            for col in columns:
                logger.info(f"  {col[1]} ({col[2]})")
        else:
            # テーブル作成
            logger.info("parse_failuresテーブルを作成中...")
            cursor.execute("""
                CREATE TABLE parse_failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parser_name TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    error_type TEXT,
                    error_message TEXT,
                    stack_trace TEXT,
                    failed_ts TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # インデックス作成
            cursor.execute("""
                CREATE INDEX idx_parse_failures_parser
                ON parse_failures(parser_name)
            """)

            cursor.execute("""
                CREATE INDEX idx_parse_failures_source
                ON parse_failures(source_file)
            """)

            cursor.execute("""
                CREATE INDEX idx_parse_failures_ts
                ON parse_failures(failed_ts)
            """)

            conn.commit()
            logger.info("✓ parse_failuresテーブルを作成しました")
            logger.info("✓ インデックスを作成しました")

        # レコード数を確認
        cursor.execute("SELECT COUNT(*) FROM parse_failures")
        count = cursor.fetchone()[0]
        logger.info(f"現在のエラーレコード数: {count}")

    except sqlite3.Error as e:
        logger.error(f"データベースエラー: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()
        logger.info("データベース接続をクローズしました")

def main():
    """メイン処理"""
    # デフォルトのデータベースパス
    default_db_path = Path(__file__).resolve().parent.parent / "data" / "metadata" / "db.sqlite3"

    # データベースディレクトリが存在しない場合は作成
    default_db_path.parent.mkdir(parents=True, exist_ok=True)

    create_parse_failures_table(str(default_db_path))

    print("\n" + "="*70)
    print("✓ セットアップ完了")
    print("="*70)
    print(f"データベース: {default_db_path}")
    print("\nこれで parse_with_error_handling() でエラーを記録できます。")

if __name__ == "__main__":
    main()

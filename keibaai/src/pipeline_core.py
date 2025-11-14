#!/usr/bin/env python3
# src/pipeline_core.py
"""
パイプラインコアユーティリティ
- atomic_write: ファイルの安全な書き込み
- parse_with_error_handling: エラーハンドリング付きパーサ実行
- setup_logging: ロギング設定
- load_config: YAML設定ファイルの読み込み
- get_db_connection: SQLiteデータベース接続の取得
"""

import os
import tempfile
import traceback
import logging
import json
import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

import yaml  # YAMLを扱うために追加


def load_config(config_path: str) -> Dict[str, Any]:
    """
    YAML設定ファイルをロードする
    """
    logging.info(f"設定ファイルをロード中: {config_path}")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        logging.error(f"設定ファイルが見つかりません: {config_path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"設定ファイルのパースに失敗: {e}")
        raise
    except Exception as e:
        logging.error(f"設定ファイルの読み込み中に予期せぬエラーが発生: {e}")
        raise


def setup_logging(
    level: str = 'INFO',
    logging_config: Dict[str, Any] = None
):
    """
    ロギングを設定する
    
    Args:
        level: ログレベル
        logging_config: ロギング設定辞書 (log_file テンプレートを含む)
    """
    try:
        # ログファイルテンプレートを取得
        if logging_config and 'log_file' in logging_config:
            log_file_template = logging_config['log_file']
        else:
            log_file_template = 'data/logs/{YYYY}/{MM}/{DD}/default.log'
        
        # ログパスのプレースホルダを置換
        now = datetime.now(timezone(timedelta(hours=9)))
        log_path = log_file_template.format(
            YYYY=now.year,
            MM=f"{now.month:02}",
            DD=f"{now.day:02}"
        )
        
        log_dir = Path(log_path).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # ロガー設定
        logging.basicConfig(
            level=level.upper(),
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            handlers=[
                logging.FileHandler(log_path, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ],
            force=True  # 既存の設定を上書き
        )
        logging.info("ロギングが正常に設定されました")
        
    except Exception as e:
        # フォールバック (簡易ロギング)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        )
        logging.error(f"ロギングの初期化に失敗しました: {e}")
        logging.info("簡易フォールバックロギングを使用します")


def get_db_connection(db_path: str) -> sqlite3.Connection:
    """
    SQLiteデータベース接続を取得する
    """
    logging.info(f"データベースに接続中: {db_path}")
    try:
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        conn.execute('PRAGMA journal_mode=WAL;')  # Write-Ahead Loggingを有効化
        conn.execute('PRAGMA busy_timeout = 5000;') # タイムアウト設定
        logging.info("データベース接続成功")
        return conn
    except sqlite3.Error as e:
        logging.error(f"データベース接続に失敗: {e}")
        raise


def atomic_write(path: str, data: bytes):
    """
    一時ファイルに書き込み、完了後にリネームすることで
    書き込み中のファイル破損を防ぐ
    """
    dir_path = os.path.dirname(path)
    os.makedirs(dir_path, exist_ok=True)
    
    # 一時ファイル作成
    fd, tmp_path = tempfile.mkstemp(
        dir=dir_path,
        prefix='.tmp_',
        suffix=os.path.basename(path)
    )
    
    try:
        # データ書き込み
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        
        # アトミックリネーム
        os.replace(tmp_path, path)
        
    except Exception as e:
        # エラー時は一時ファイルを削除
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise e


def parse_with_error_handling(
    file_path: str,
    parser_name: str,
    parse_func,
    db_conn
):
    """
    エラーハンドリング付きパーサ実行
    
    Args:
        file_path: 対象ファイルパス
        parser_name: パーサ名
        parse_func: パース関数
        db_conn: SQLite接続
    
    Returns:
        パース結果（成功時）またはNone（失敗時）
    """
    
    try:
        result = parse_func(file_path)
        return result
    
    except Exception as e:
        # エラーログ記録
        error_message = str(e)
        stack_trace = traceback.format_exc()
        
        logging.error(f"パースエラー ({parser_name}): {file_path} - {error_message}")
        
        # データベースに記録
        try:
            cursor = db_conn.cursor()
            cursor.execute('''
                INSERT INTO parse_failures (
                    parser_name, source_file, error_type, 
                    error_message, stack_trace, failed_ts
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                parser_name,
                file_path,
                type(e).__name__,
                error_message,
                stack_trace,
                datetime.now(timezone.utc).isoformat()
            ))
            db_conn.commit()
        except Exception as db_e:
            logging.error(f"パースエラーのDB記録に失敗: {db_e}")

        
        # エラー詳細をJSONに保存
        try:
            error_dir = Path(f'data/errors/parse_failures/{parser_name}')
            error_dir.mkdir(parents=True, exist_ok=True)
            
            error_file = error_dir / f"{Path(file_path).stem}_error.json"
            
            error_data = {
                'file_path': file_path,
                'parser_name': parser_name,
                'error_type': type(e).__name__,
                'error_message': error_message,
                'stack_trace': stack_trace,
                'failed_ts': datetime.now(timezone.utc).isoformat()
            }
            
            with open(error_file, 'w', encoding='utf-8') as f:
                json.dump(error_data, f, ensure_ascii=False, indent=2)
        except Exception as json_e:
            logging.error(f"パースエラーのJSON保存に失敗: {json_e}")

        return None
import os
import tempfile
import traceback
import logging
import json
from pathlib import Path
from datetime import datetime, timezone

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
        
        # エラー詳細をJSONに保存
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
        
        return None

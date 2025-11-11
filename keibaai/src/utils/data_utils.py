import hashlib
from datetime import datetime, timezone, timedelta

def save_fetch_metadata(
db_conn,
url: str,
file_path: str,
data: bytes,
http_status: int,
fetch_method: str,
error_message: str = None
):
    """
    データ取得のメタデータをSQLiteに保存
    """
    sha256 = hashlib.sha256(data).hexdigest()

    # [修正] UTC -> Asia/Tokyo (+09:00)
    jst = timezone(timedelta(hours=9))
    fetched_ts = datetime.now(jst).isoformat()

    file_size = len(data)

    cursor = db_conn.cursor()
    cursor.execute('''
INSERT OR REPLACE INTO fetch_log (
url, file_path, fetched_ts, sha256,
file_size, fetch_method, http_status, error_message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
''', (
url, file_path, fetched_ts, sha256,
file_size, fetch_method, http_status, error_message
))
    db_conn.commit()

def generate_data_version(data: bytes) -> str:
    """
    データバージョン文字列を生成
    形式: YYYYMMDDTHHMMSS+0900_sha256={hash[:8]}
    """
    timestamp = datetime.now(timezone.utc).astimezone(
        timezone(timedelta(hours=9))
    ).strftime('%Y%m%dT%H%M%S%z')
    
    sha256_short = hashlib.sha256(data).hexdigest()[:8]
    
    return f"{timestamp}_sha256={sha256_short}"

def construct_filename(
    base_name: str,
    identifier: str,
    data: bytes,
    extension: str = 'bin'
) -> str:
    """
    バージョン付きファイル名を構築
    例: race_202306010101_20251106T120000+09:00_sha256=abcd1234.bin
    """
    data_version = generate_data_version(data)
    return f"{base_name}_{identifier}_{data_version}.{extension}"

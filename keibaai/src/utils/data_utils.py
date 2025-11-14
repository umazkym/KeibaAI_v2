#!/usr/bin/env python3
# src/utils/data_utils.py

import hashlib
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.dataset as ds
import pyarrow as pa


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
    """
    data_version = generate_data_version(data)
    return f"{base_name}_{identifier}_{data_version}.{extension}"


def load_parquet_data_by_date(
    base_dir: Path,
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
    date_col: str = 'race_date'
) -> pd.DataFrame:
    """
    指定された日付範囲に基づいてパーティション化されたParquetデータをロードする。
    rglobを使用して安定性を重視。
    """
    if not base_dir.exists():
        logging.warning(f"ディレクトリが見つかりません: {base_dir}")
        return pd.DataFrame()

    try:
        all_dfs = []
        target_files = list(base_dir.rglob("*.parquet"))
        
        if not target_files:
            logging.warning(f"Parquetファイルが見つかりません: {base_dir}")
            return pd.DataFrame()
            
        for parquet_file in target_files:
            try:
                df = pd.read_parquet(parquet_file)
                all_dfs.append(df)
            except Exception as read_e:
                logging.warning(f"Parquetファイルの読み込み失敗 ({parquet_file}): {read_e}")

        if not all_dfs:
            return pd.DataFrame()

        combined_df = pd.concat(all_dfs, ignore_index=True)
        logging.info(f"読み込み成功: {len(combined_df)}行 from {len(target_files)} files")

        # 日付フィルタリング
        if start_dt is None and end_dt is None:
            return combined_df

        if date_col not in combined_df.columns:
            logging.warning(f"日付カラム '{date_col}' がDataFrameに存在しません。フィルタリングをスキップします。")
            return combined_df

        # タイムゾーン情報を除去して比較
        combined_df[date_col] = pd.to_datetime(combined_df[date_col]).dt.tz_localize(None)

        mask = True
        if start_dt:
            mask &= (combined_df[date_col] >= start_dt)
        if end_dt:
            mask &= (combined_df[date_col] <= end_dt)
        
        filtered_df = combined_df[mask].copy()
        
        if filtered_df.empty:
            logging.warning(f"指定期間のデータが見つかりませんでした: {start_dt} - {end_dt}")

        return filtered_df

    except Exception as e:
        logging.error(f"Parquetデータのロード中に予期せぬエラーが発生しました: {e}", exc_info=True)
        return pd.DataFrame()

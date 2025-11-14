#!/usr/bin/env python3
# src/utils/data_utils.py

import hashlib
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd


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

# --- ▼▼▼ 追記する関数 ▼▼▼ ---

def load_parquet_data_by_date(
    base_dir: Path,
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
    date_col: str = 'race_date'
) -> pd.DataFrame:
    """
    指定された日付範囲に基づいてParquetデータをロードする
    (パーティション化されたParquetも対応)

    Args:
        base_dir: Parquetファイル群が格納されているディレクトリ
        start_dt: 開始日 (Noneの場合は指定なし)
        end_dt: 終了日 (Noneの場合は指定なし)
        date_col: 日付フィルタリングに使用するカラム名

    Returns:
        結合されたDataFrame
    """
    
    if not base_dir.exists():
        logging.warning(f"ディレクトリが見つかりません: {base_dir}")
        return pd.DataFrame()

    # まずパーティション化されたParquetを読み込む試み（pyarrow）
    try:
        # パーティション構造（year=YYYY/month=M/*.parquet）を自動認識
        df = pd.read_parquet(base_dir, engine='pyarrow')
        logging.info(f"パーティション化されたParquetを読み込みました: {len(df)}行")
        
        # 日付フィルタリング
        if start_dt or end_dt:
            if date_col not in df.columns:
                logging.warning(f"日付カラム '{date_col}' が存在しません。race_id から生成を試みます。")
                if 'race_id' in df.columns:
                    try:
                        df['race_date_str'] = df['race_id'].astype(str).str[:8]
                        df[date_col] = pd.to_datetime(df['race_date_str'], format='%Y%m%d', errors='coerce')
                        logging.info(f"'{date_col}' カラムを 'race_id' から生成しました。")
                    except Exception:
                        return df
                else:
                    return df
            
            # タイムゾーンを意識しない比較に統一
            df[date_col] = pd.to_datetime(df[date_col]).dt.tz_localize(None)
            
            if start_dt and end_dt:
                mask = (df[date_col] >= start_dt) & (df[date_col] <= end_dt)
                return df[mask].copy()
            elif start_dt:
                mask = (df[date_col] >= start_dt)
                return df[mask].copy()
            elif end_dt:
                mask = (df[date_col] <= end_dt)
                return df[mask].copy()
        
        return df
        
    except Exception as e:
        # パーティション読み込み失敗時は、単一ファイル検索にフォールバック
        logging.debug(f"パーティション読み込み失敗（フォールバックします）: {e}")
    
    # フォールバック: 単一ファイル検索
    all_dfs = []
    target_files = list(base_dir.glob("*.parquet"))
    
    if not target_files:
        logging.warning(f"Parquetファイルが見つかりません: {base_dir}")
        return pd.DataFrame()
        
    for parquet_file in target_files:
        try:
            df = pd.read_parquet(parquet_file)
            all_dfs.append(df)
        except Exception as e:
            logging.warning(f"Parquetファイルの読み込み失敗 ({parquet_file}): {e}")

    if not all_dfs:
        return pd.DataFrame()

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # 日付フィルタリング
    if start_dt is None and end_dt is None:
        return combined_df

    if date_col not in combined_df.columns:
        logging.warning(f"日付カラム '{date_col}' がDataFrameに存在しません。race_id から生成を試みます。")
        if 'race_id' in combined_df.columns:
            try:
                combined_df['race_date_str'] = combined_df['race_id'].astype(str).str[:8]
                combined_df[date_col] = pd.to_datetime(combined_df['race_date_str'], format='%Y%m%d', errors='coerce')
                logging.info(f"'{date_col}' カラムを 'race_id' から生成しました。")
            except Exception:
                return combined_df
        else:
             return combined_df

    # タイムゾーンを意識しない比較に統一
    combined_df[date_col] = pd.to_datetime(combined_df[date_col]).dt.tz_localize(None)

    if start_dt and end_dt:
        mask = (combined_df[date_col] >= start_dt) & (combined_df[date_col] <= end_dt)
        return combined_df[mask].copy()
    elif start_dt:
        mask = (combined_df[date_col] >= start_dt)
        return combined_df[mask].copy()
    elif end_dt:
        mask = (combined_df[date_col] <= end_dt)
        return combined_df[mask].copy()
        
    return combined_df
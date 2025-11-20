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
    if data:
        sha256 = hashlib.sha256(data).hexdigest()
        file_size = len(data)
    else:
        sha256 = None
        file_size = 0

    jst = timezone(timedelta(hours=9))
    fetched_ts = datetime.now(jst).isoformat()

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
    データバージョン文字列を生成（互換性のため残す）
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
    ファイル名を構築（.bin形式用に簡略化）
    
    新しい形式:
    - race: {race_id}.bin
    - shutuba: {race_id}.bin
    - horse: {horse_id}_profile.bin, {horse_id}_perf.bin
    - ped: {horse_id}.bin
    
    互換性のため、既存のコードからの呼び出しに対応
    """
    # base_nameに応じてシンプルなファイル名を返す
    if base_name == 'race':
        return f"{identifier}.{extension}"
    elif base_name == 'shutuba':
        return f"{identifier}.{extension}"
    elif base_name == 'horse':
        # デフォルトでプロフィール用
        return f"{identifier}_profile.{extension}"
    elif base_name == 'horse_perf':
        return f"{identifier}_perf.{extension}"
    elif base_name == 'ped':
        return f"{identifier}.{extension}"
    else:
        # フォールバック（旧形式）
        data_version = generate_data_version(data)
        return f"{base_name}_{identifier}_{data_version}.{extension}"


def construct_bin_filename(
    data_type: str,
    identifier: str,
    subtype: str = None
) -> str:
    """
    .bin形式のファイル名を構築（新規追加）
    
    Args:
        data_type: データ種別（race, shutuba, horse, ped）
        identifier: ID（race_id または horse_id）
        subtype: サブタイプ（horseの場合のみ: profile, perf）
        
    Returns:
        ファイル名
    """
    if data_type in ['race', 'shutuba', 'ped']:
        return f"{identifier}.bin"
    elif data_type == 'horse':
        if subtype == 'perf':
            return f"{identifier}_perf.bin"
        else:
            return f"{identifier}_profile.bin"
    else:
        raise ValueError(f"Unknown data_type: {data_type}")


def load_parquet_data_by_date(
    base_dir: Path,
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
    date_col: str = 'race_date'
) -> pd.DataFrame:
    """
    指定された日付範囲に基づいてパーティション化されたParquetデータをロードする。
    PyArrow Dataset APIを使用してメモリ効率よくフィルタリングを行う。
    """
    if not base_dir.exists():
        logging.warning(f"ディレクトリが見つかりません: {base_dir}")
        return pd.DataFrame()

    try:
        # 1. 対象のParquetファイルを検索
        target_files = list(base_dir.rglob("*.parquet"))
        if not target_files:
            logging.warning(f"Parquetファイルが見つかりません: {base_dir}")
            return pd.DataFrame()
            
        # 2. PyArrow Datasetとして読み込む (ファイルリストを渡す)
        # partitioning="hive" を指定することで、パス内の key=value をパーティションとして認識させる
        dataset = ds.dataset(target_files, format="parquet", partitioning="hive")
        
        # フィルタ式の構築
        filter_expr = None
        
        if start_dt or end_dt:
            # 日付カラムの型を確認するためにスキーマを取得
            schema = dataset.schema
            if date_col not in schema.names:
                # 日付カラムがない場合はフィルタリングせずに警告
                logging.warning(f"日付カラム '{date_col}' がスキーマに存在しません。フィルタリングなしでロードします。")
            else:
                field_type = schema.field(date_col).type
                
                # Timestamp型の場合のみPyArrowでフィルタリング (高速・省メモリ)
                if pa.types.is_timestamp(field_type) or pa.types.is_date(field_type):
                    field = ds.field(date_col)
                    
                    if start_dt:
                        # タイムゾーンなしに変換
                        s_dt = start_dt.replace(tzinfo=None)
                        filter_expr = (field >= s_dt)
                    
                    if end_dt:
                        e_dt = end_dt.replace(tzinfo=None)
                        if filter_expr is not None:
                            filter_expr &= (field <= e_dt)
                        else:
                            filter_expr = (field <= e_dt)
                else:
                    # 文字列などの場合はPyArrowでの比較が難しいため、Pandasロード後にフィルタリングする
                    # (features.parquetはTimestamp型なので、メモリ問題は回避できるはず)
                    logging.info(f"カラム '{date_col}' はTimestamp型ではありません ({field_type})。Pandasロード後にフィルタリングします。")

        # データをロード (フィルタ適用)
        if filter_expr is not None:
            table = dataset.to_table(filter=filter_expr)
        else:
            table = dataset.to_table()
            
        df = table.to_pandas()
        
        # PyArrowでフィルタできなかった場合 (文字列型など) のために、Pandasで再度フィルタリング
        # (PyArrowでフィルタ済みの場合も、念のため実行してもコストは低い)
        if not df.empty and (start_dt or end_dt) and date_col in df.columns:
             # 型変換してフィルタ
             # 既にTimestampなら高速、文字列なら変換される
             temp_dates = pd.to_datetime(df[date_col], errors='coerce').dt.tz_localize(None)
             
             mask = True
             if start_dt:
                 mask &= (temp_dates >= start_dt.replace(tzinfo=None))
             if end_dt:
                 mask &= (temp_dates <= end_dt.replace(tzinfo=None))
                 
             df = df[mask].copy()
        
        logging.info(f"読み込み成功: {len(df)}行")
        
        if df.empty and (start_dt or end_dt):
             logging.warning(f"指定期間のデータが見つかりませんでした: {start_dt} - {end_dt}")

        return df

    except Exception as e:
        logging.error(f"Parquetデータのロード中に予期せぬエラーが発生しました: {e}", exc_info=True)
        # フォールバック: 従来の方法 (ただしメモリ不足のリスクあり)
        # ここではエラーを返して終了する方が安全
        return pd.DataFrame()
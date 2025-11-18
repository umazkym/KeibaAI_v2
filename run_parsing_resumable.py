#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
再開可能なパースパイプライン
途中で停止しても、既にパース済みのファイルをスキップして続行可能
"""

import logging
import sqlite3
from pathlib import Path
import yaml
from datetime import datetime
import sys
import pandas as pd
import os
from tqdm import tqdm
import argparse

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent / 'keibaai'
sys.path.append(str(project_root))

from src import pipeline_core
from src.modules.parsers import results_parser, shutuba_parser, horse_info_parser, pedigree_parser


def load_config():
    """設定ファイルをロードする"""
    config_path = Path(__file__).resolve().parent / "keibaai" / "configs"
    with open(config_path / "default.yaml", "r", encoding="utf-8") as f:
        default_cfg = yaml.safe_load(f)

    data_root = Path(__file__).resolve().parent / "keibaai" / default_cfg['data_path']
    default_cfg['raw_data_path'] = str(data_root / 'raw')
    default_cfg['parsed_data_path'] = str(data_root / 'parsed')
    default_cfg['database']['path'] = str(data_root / 'metadata' / 'db.sqlite3')
    default_cfg['logging']['log_file'] = str(data_root / 'logs' / '{YYYY}' / '{MM}' / '{DD}' / 'parsing_resumable.log')

    config = {"default": default_cfg}
    return config


def setup_logging(log_path_template: str):
    """ログ設定を行う"""
    now = datetime.now()
    log_path = log_path_template.format(YYYY=now.year, MM=f"{now.month:02}", DD=f"{now.day:02}")
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def get_already_parsed_ids(parquet_path: Path, id_column: str) -> set:
    """
    既存のParquetファイルから既にパース済みのIDを取得

    Args:
        parquet_path: Parquetファイルのパス
        id_column: IDカラム名 (例: 'race_id', 'horse_id')

    Returns:
        既にパース済みのIDのセット
    """
    if not parquet_path.exists():
        return set()

    try:
        df = pd.read_parquet(parquet_path, columns=[id_column])
        parsed_ids = set(df[id_column].unique())
        return parsed_ids
    except Exception as e:
        logging.warning(f"既存のParquetファイル読み込みエラー ({parquet_path}): {e}")
        return set()


def get_error_files(conn, parser_name: str) -> set:
    """
    データベースからエラーが発生したファイルのパスを取得

    Args:
        conn: SQLite接続
        parser_name: パーサ名 (例: 'results_parser', 'shutuba_parser')

    Returns:
        エラーファイルのパスのセット
    """
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT DISTINCT source_file
            FROM parse_failures
            WHERE parser_name = ?
            ORDER BY failed_ts DESC
        ''', (parser_name,))

        error_files = {row[0] for row in cursor.fetchall()}
        return error_files
    except Exception as e:
        logging.warning(f"エラーファイル取得失敗 ({parser_name}): {e}")
        return set()


def clear_error_record(conn, file_path: str, parser_name: str):
    """
    パース成功時にエラーレコードを削除

    Args:
        conn: SQLite接続
        file_path: ファイルパス
        parser_name: パーサ名
    """
    try:
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM parse_failures
            WHERE source_file = ? AND parser_name = ?
        ''', (file_path, parser_name))
        conn.commit()

        deleted_count = cursor.rowcount
        if deleted_count > 0:
            logging.debug(f"エラーレコード削除: {file_path} ({deleted_count}件)")
    except Exception as e:
        logging.warning(f"エラーレコード削除失敗 ({file_path}): {e}")


def extract_race_id_from_filename(file_path: str) -> str:
    """ファイル名からrace_idを抽出 (例: 202305040301.bin → 202305040301)"""
    return Path(file_path).stem.replace('_perf', '').replace('_profile', '')


def parse_phase_races(cfg, conn, skip_existing: bool = False, retry_errors: bool = False):
    """フェーズ1: レース結果HTMLのパース"""
    log = logging.getLogger(__name__)
    log.info("\n" + "=" * 80)
    log.info("【フェーズ1/5】レース結果HTMLのパース処理を開始")
    log.info("=" * 80)

    raw_race_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "race"
    parsed_race_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "races"
    parsed_race_parquet_dir.mkdir(parents=True, exist_ok=True)
    output_path = parsed_race_parquet_dir / "races.parquet"

    # 全HTMLファイルを収集
    race_html_files = []
    for root, _, files in os.walk(raw_race_html_dir):
        for file in files:
            if file.endswith((".html", ".bin")):
                race_html_files.append(os.path.join(root, file))

    log.info(f"  → {len(race_html_files):,}件のレース結果HTMLファイルが見つかりました")

    # エラーファイルを取得
    error_files = get_error_files(conn, 'results_parser')
    if error_files:
        log.info(f"  → エラー履歴: {len(error_files):,}件 (自動的に再処理対象に含めます)")

    # スキップ処理
    if skip_existing and not retry_errors:
        already_parsed = get_already_parsed_ids(output_path, 'race_id')
        log.info(f"  → 既にパース済み: {len(already_parsed):,}件")

        # 新規ファイル + エラーファイル
        race_html_files_to_process = [
            f for f in race_html_files
            if extract_race_id_from_filename(f) not in already_parsed or f in error_files
        ]

        new_files = len([f for f in race_html_files if extract_race_id_from_filename(f) not in already_parsed])
        error_retry = len([f for f in race_html_files_to_process if f in error_files])
        log.info(f"  → 処理対象: {len(race_html_files_to_process):,}件 (新規: {new_files:,}件, エラー再処理: {error_retry:,}件)")
    elif retry_errors:
        # エラーファイルのみ
        race_html_files_to_process = [f for f in race_html_files if f in error_files]
        log.info(f"  → 処理対象: {len(race_html_files_to_process):,}件 (エラー再処理のみ)")
    else:
        race_html_files_to_process = race_html_files
        log.info(f"  → 処理対象: {len(race_html_files_to_process):,}件 (全ファイル)")

    # パース実行
    all_results_df = []
    success_count = 0
    error_count = 0
    error_cleared_count = 0

    for html_file in tqdm(race_html_files_to_process, desc="レース結果パース", unit="件"):
        df = pipeline_core.parse_with_error_handling(
            str(html_file), "results_parser", results_parser.parse_results_html, conn
        )
        if df is not None and not df.empty:
            all_results_df.append(df)
            success_count += 1
            # パース成功したファイルを記録（保存成功後にエラーレコードをクリアするため）
            if html_file in error_files:
                # 一旦、成功したエラーファイルのリストに追加（後でクリア）
                pass  # 後で処理
        else:
            error_count += 1

    # 保存 (既存データと結合) - エラーハンドリング強化
    if all_results_df:
        try:
            new_results_df = pd.concat(all_results_df, ignore_index=True)

            if skip_existing and output_path.exists():
                # 既存データを読み込んで結合
                existing_df = pd.read_parquet(output_path)
                final_results_df = pd.concat([existing_df, new_results_df], ignore_index=True)
                # 重複排除 (race_id + horse_numberで一意)
                final_results_df = final_results_df.drop_duplicates(
                    subset=['race_id', 'horse_number'], keep='last'
                )
                log.info(f"  → 既存 {len(existing_df):,}件 + 新規 {len(new_results_df):,}件 = 合計 {len(final_results_df):,}件")
            else:
                final_results_df = new_results_df

            # 保存実行
            final_results_df.to_parquet(output_path, index=False)
            log.info(f"  ✓ 保存完了: {output_path} ({len(final_results_df):,}レコード)")

            # 保存成功後、パース成功したエラーファイルのレコードをクリア
            for html_file in race_html_files_to_process:
                if html_file in error_files:
                    # このファイルのデータがfinal_results_dfに含まれているか確認
                    race_id = extract_race_id_from_filename(html_file)
                    if race_id in final_results_df['race_id'].values:
                        clear_error_record(conn, html_file, 'results_parser')
                        error_cleared_count += 1

            # 処理統計を表示
            log.info(f"  📊 処理統計: 成功 {success_count:,}件 / エラー {error_count:,}件 / エラー解消 {error_cleared_count:,}件")

        except Exception as e:
            log.error(f"  ❌ Parquet保存エラー: {e}", exc_info=True)
            log.warning(f"  ⚠️ {len(all_results_df):,}件のパース結果が保存できませんでした")
            log.warning(f"  💡 次回実行時に再処理されます")
    else:
        log.warning("処理できるレース結果データがありませんでした。")
        if error_count > 0:
            log.info(f"  📊 処理統計: エラー {error_count:,}件")


def parse_phase_shutuba(cfg, conn, skip_existing: bool = False, retry_errors: bool = False):
    """フェーズ2: 出馬表HTMLのパース"""
    log = logging.getLogger(__name__)
    log.info("\n" + "=" * 80)
    log.info("【フェーズ2/5】出馬表HTMLのパース処理を開始")
    log.info("=" * 80)

    raw_shutuba_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "shutuba"
    parsed_shutuba_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "shutuba"
    parsed_shutuba_parquet_dir.mkdir(parents=True, exist_ok=True)
    output_path = parsed_shutuba_parquet_dir / "shutuba.parquet"

    shutuba_html_files = []
    for root, _, files in os.walk(raw_shutuba_html_dir):
        for file in files:
            if file.endswith((".html", ".bin")):
                shutuba_html_files.append(os.path.join(root, file))

    log.info(f"  → {len(shutuba_html_files):,}件の出馬表HTMLファイルが見つかりました")

    # エラーファイルを取得
    error_files = get_error_files(conn, 'shutuba_parser')
    if error_files:
        log.info(f"  → エラー履歴: {len(error_files):,}件 (自動的に再処理対象に含めます)")

    if skip_existing and not retry_errors:
        already_parsed = get_already_parsed_ids(output_path, 'race_id')
        log.info(f"  → 既にパース済み: {len(already_parsed):,}件")

        shutuba_html_files_to_process = [
            f for f in shutuba_html_files
            if extract_race_id_from_filename(f) not in already_parsed or f in error_files
        ]

        new_files = len([f for f in shutuba_html_files if extract_race_id_from_filename(f) not in already_parsed])
        error_retry = len([f for f in shutuba_html_files_to_process if f in error_files])
        log.info(f"  → 処理対象: {len(shutuba_html_files_to_process):,}件 (新規: {new_files:,}件, エラー再処理: {error_retry:,}件)")
    elif retry_errors:
        shutuba_html_files_to_process = [f for f in shutuba_html_files if f in error_files]
        log.info(f"  → 処理対象: {len(shutuba_html_files_to_process):,}件 (エラー再処理のみ)")
    else:
        shutuba_html_files_to_process = shutuba_html_files

    all_shutuba_df = []
    success_count = 0
    error_count = 0
    error_cleared_count = 0

    for html_file in tqdm(shutuba_html_files_to_process, desc="出馬表パース", unit="件"):
        df = pipeline_core.parse_with_error_handling(
            str(html_file), "shutuba_parser", shutuba_parser.parse_shutuba_html, conn
        )
        if df is not None and not df.empty:
            all_shutuba_df.append(df)
            success_count += 1
        else:
            error_count += 1

    if all_shutuba_df:
        try:
            new_shutuba_df = pd.concat(all_shutuba_df, ignore_index=True)

            if skip_existing and output_path.exists():
                existing_df = pd.read_parquet(output_path)
                final_shutuba_df = pd.concat([existing_df, new_shutuba_df], ignore_index=True)
                final_shutuba_df = final_shutuba_df.drop_duplicates(
                    subset=['race_id', 'horse_number'], keep='last'
                )
                log.info(f"  → 既存 {len(existing_df):,}件 + 新規 {len(new_shutuba_df):,}件 = 合計 {len(final_shutuba_df):,}件")
            else:
                final_shutuba_df = new_shutuba_df

            final_shutuba_df.to_parquet(output_path, index=False)
            log.info(f"  ✓ 保存完了: {output_path} ({len(final_shutuba_df):,}レコード)")

            # 保存成功後、エラーレコードをクリア
            for html_file in shutuba_html_files_to_process:
                if html_file in error_files:
                    race_id = extract_race_id_from_filename(html_file)
                    if race_id in final_shutuba_df['race_id'].values:
                        clear_error_record(conn, html_file, 'shutuba_parser')
                        error_cleared_count += 1

            log.info(f"  📊 処理統計: 成功 {success_count:,}件 / エラー {error_count:,}件 / エラー解消 {error_cleared_count:,}件")

        except Exception as e:
            log.error(f"  ❌ Parquet保存エラー: {e}", exc_info=True)
            log.warning(f"  ⚠️ {len(all_shutuba_df):,}件のパース結果が保存できませんでした")
            log.warning(f"  💡 次回実行時に再処理されます")
    else:
        log.warning("処理できる出馬表データがありませんでした。")
        if error_count > 0:
            log.info(f"  📊 処理統計: エラー {error_count:,}件")


def parse_phase_horses(cfg, conn, skip_existing: bool = False, retry_errors: bool = False):
    """フェーズ3: 馬プロフィールHTMLのパース"""
    log = logging.getLogger(__name__)
    log.info("\n" + "=" * 80)
    log.info("【フェーズ3/5】馬プロフィールHTMLのパース処理を開始")
    log.info("=" * 80)

    raw_horse_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "horse"
    parsed_horse_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "horses"
    parsed_horse_parquet_dir.mkdir(parents=True, exist_ok=True)
    output_path = parsed_horse_parquet_dir / "horses.parquet"

    horse_html_files = []
    for root, _, files in os.walk(raw_horse_html_dir):
        for file in files:
            if file.endswith((".html", ".bin")):
                full_path = os.path.join(root, file)
                if horse_info_parser.is_profile_file(full_path):
                    horse_html_files.append(full_path)

    log.info(f"  → {len(horse_html_files):,}件の馬プロフィールHTMLファイルが見つかりました")

    # エラーファイルを取得
    error_files = get_error_files(conn, 'horse_info_parser')
    if error_files:
        log.info(f"  → エラー履歴: {len(error_files):,}件 (自動的に再処理対象に含めます)")

    if skip_existing and not retry_errors:
        already_parsed = get_already_parsed_ids(output_path, 'horse_id')
        log.info(f"  → 既にパース済み: {len(already_parsed):,}頭")

        horse_html_files_to_process = [
            f for f in horse_html_files
            if extract_race_id_from_filename(f) not in already_parsed or f in error_files
        ]

        new_files = len([f for f in horse_html_files if extract_race_id_from_filename(f) not in already_parsed])
        error_retry = len([f for f in horse_html_files_to_process if f in error_files])
        log.info(f"  → 処理対象: {len(horse_html_files_to_process):,}頭 (新規: {new_files:,}頭, エラー再処理: {error_retry:,}頭)")
    elif retry_errors:
        horse_html_files_to_process = [f for f in horse_html_files if f in error_files]
        log.info(f"  → 処理対象: {len(horse_html_files_to_process):,}頭 (エラー再処理のみ)")
    else:
        horse_html_files_to_process = horse_html_files

    all_horses_data = []
    success_count = 0
    error_count = 0
    error_cleared_count = 0

    for html_file in tqdm(horse_html_files_to_process, desc="馬プロフィールパース", unit="頭"):
        data = pipeline_core.parse_with_error_handling(
            str(html_file), "horse_info_parser", horse_info_parser.parse_horse_profile, conn
        )
        if data and 'horse_id' in data and data['horse_id']:
            if not data.get('_is_empty', False):
                all_horses_data.append(data)
                success_count += 1
        else:
            error_count += 1

    if all_horses_data:
        try:
            new_horses_df = pd.DataFrame(all_horses_data)

            if skip_existing and output_path.exists():
                existing_df = pd.read_parquet(output_path)
                final_horses_df = pd.concat([existing_df, new_horses_df], ignore_index=True)
                final_horses_df = final_horses_df.drop_duplicates(subset='horse_id', keep='last')
                log.info(f"  → 既存 {len(existing_df):,}頭 + 新規 {len(new_horses_df):,}頭 = 合計 {len(final_horses_df):,}頭")
            else:
                final_horses_df = new_horses_df

            final_horses_df.to_parquet(output_path, index=False)
            log.info(f"  ✓ 保存完了: {output_path} ({len(final_horses_df):,}レコード)")

            # 保存成功後、エラーレコードをクリア
            for html_file in horse_html_files_to_process:
                if html_file in error_files:
                    horse_id = extract_race_id_from_filename(html_file)
                    if horse_id in final_horses_df['horse_id'].values:
                        clear_error_record(conn, html_file, 'horse_info_parser')
                        error_cleared_count += 1

            log.info(f"  📊 処理統計: 成功 {success_count:,}頭 / エラー {error_count:,}頭 / エラー解消 {error_cleared_count:,}頭")

        except Exception as e:
            log.error(f"  ❌ Parquet保存エラー: {e}", exc_info=True)
            log.warning(f"  ⚠️ {len(all_horses_data):,}頭のパース結果が保存できませんでした")
            log.warning(f"  💡 次回実行時に再処理されます")
    else:
        log.warning("処理できる馬プロフィールデータがありませんでした。")
        if error_count > 0:
            log.info(f"  📊 処理統計: エラー {error_count:,}頭")


def parse_phase_pedigrees(cfg, conn, skip_existing: bool = False, retry_errors: bool = False):
    """フェーズ4: 血統HTMLのパース"""
    log = logging.getLogger(__name__)
    log.info("\n" + "=" * 80)
    log.info("【フェーズ4/5】血統HTMLのパース処理を開始")
    log.info("=" * 80)

    raw_ped_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "ped"
    parsed_ped_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "pedigrees"
    parsed_ped_parquet_dir.mkdir(parents=True, exist_ok=True)
    output_path = parsed_ped_parquet_dir / "pedigrees.parquet"

    ped_html_files = []
    for root, _, files in os.walk(raw_ped_html_dir):
        for file in files:
            if file.endswith((".html", ".bin")):
                ped_html_files.append(os.path.join(root, file))

    log.info(f"  → {len(ped_html_files):,}件の血統HTMLファイルが見つかりました")

    # エラーファイルを取得
    error_files = get_error_files(conn, 'pedigree_parser')
    if error_files:
        log.info(f"  → エラー履歴: {len(error_files):,}件 (自動的に再処理対象に含めます)")

    if skip_existing and not retry_errors:
        already_parsed = get_already_parsed_ids(output_path, 'horse_id')
        log.info(f"  → 既にパース済み: {len(already_parsed):,}頭")

        ped_html_files_to_process = [
            f for f in ped_html_files
            if extract_race_id_from_filename(f) not in already_parsed or f in error_files
        ]

        new_files = len([f for f in ped_html_files if extract_race_id_from_filename(f) not in already_parsed])
        error_retry = len([f for f in ped_html_files_to_process if f in error_files])
        log.info(f"  → 処理対象: {len(ped_html_files_to_process):,}頭 (新規: {new_files:,}頭, エラー再処理: {error_retry:,}頭)")
    elif retry_errors:
        ped_html_files_to_process = [f for f in ped_html_files if f in error_files]
        log.info(f"  → 処理対象: {len(ped_html_files_to_process):,}頭 (エラー再処理のみ)")
    else:
        ped_html_files_to_process = ped_html_files

    all_pedigrees_df = []
    success_count = 0
    error_count = 0
    error_cleared_count = 0

    for html_file in tqdm(ped_html_files_to_process, desc="血統パース", unit="頭"):
        df = pipeline_core.parse_with_error_handling(
            str(html_file), "pedigree_parser", pedigree_parser.parse_pedigree_html, conn
        )
        if df is not None and not df.empty:
            all_pedigrees_df.append(df)
            success_count += 1
        else:
            error_count += 1

    if all_pedigrees_df:
        try:
            new_pedigrees_df = pd.concat(all_pedigrees_df, ignore_index=True)

            if skip_existing and output_path.exists():
                existing_df = pd.read_parquet(output_path)
                final_pedigrees_df = pd.concat([existing_df, new_pedigrees_df], ignore_index=True)
                # 血統データは1頭に対して複数レコード（5世代分）があるので、horse_id + generation + position で重複判定
                if 'generation' in final_pedigrees_df.columns and 'position' in final_pedigrees_df.columns:
                    final_pedigrees_df = final_pedigrees_df.drop_duplicates(
                        subset=['horse_id', 'generation', 'position'], keep='last'
                    )
                else:
                    final_pedigrees_df = final_pedigrees_df.drop_duplicates(subset='horse_id', keep='last')
                log.info(f"  → 既存 {len(existing_df):,}件 + 新規 {len(new_pedigrees_df):,}件 = 合計 {len(final_pedigrees_df):,}件")
            else:
                final_pedigrees_df = new_pedigrees_df

            final_pedigrees_df.to_parquet(output_path, index=False)
            log.info(f"  ✓ 保存完了: {output_path} ({len(final_pedigrees_df):,}レコード)")

            # 保存成功後、エラーレコードをクリア
            for html_file in ped_html_files_to_process:
                if html_file in error_files:
                    horse_id = extract_race_id_from_filename(html_file)
                    if horse_id in final_pedigrees_df['horse_id'].values:
                        clear_error_record(conn, html_file, 'pedigree_parser')
                        error_cleared_count += 1

            log.info(f"  📊 処理統計: 成功 {success_count:,}頭 / エラー {error_count:,}頭 / エラー解消 {error_cleared_count:,}頭")

        except Exception as e:
            log.error(f"  ❌ Parquet保存エラー: {e}", exc_info=True)
            log.warning(f"  ⚠️ {len(all_pedigrees_df):,}頭のパース結果が保存できませんでした")
            log.warning(f"  💡 次回実行時に再処理されます")
    else:
        log.warning("処理できる血統データがありませんでした。")
        if error_count > 0:
            log.info(f"  📊 処理統計: エラー {error_count:,}頭")


def parse_phase_performance(cfg, conn, skip_existing: bool = False, retry_errors: bool = False):
    """フェーズ5: 馬過去成績のパース"""
    log = logging.getLogger(__name__)
    log.info("\n" + "=" * 80)
    log.info("【フェーズ5/5】馬過去成績HTMLのパース処理を開始")
    log.info("=" * 80)

    raw_horse_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "horse"
    parsed_perf_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "horses_performance"
    parsed_perf_parquet_dir.mkdir(parents=True, exist_ok=True)
    output_path = parsed_perf_parquet_dir / "horses_performance.parquet"

    horse_perf_files = []
    for root, _, files in os.walk(raw_horse_html_dir):
        for file in files:
            if file.endswith(("_perf.html", "_perf.bin")):
                horse_perf_files.append(os.path.join(root, file))

    log.info(f"  → {len(horse_perf_files):,}件の馬過去成績ファイルが見つかりました")

    # エラーファイルを取得
    error_files = get_error_files(conn, 'horse_performance_parser')
    if error_files:
        log.info(f"  → エラー履歴: {len(error_files):,}件 (自動的に再処理対象に含めます)")

    if skip_existing and not retry_errors:
        already_parsed = get_already_parsed_ids(output_path, 'horse_id')
        log.info(f"  → 既にパース済み: {len(already_parsed):,}頭")

        horse_perf_files_to_process = [
            f for f in horse_perf_files
            if extract_race_id_from_filename(f) not in already_parsed or f in error_files
        ]

        new_files = len([f for f in horse_perf_files if extract_race_id_from_filename(f) not in already_parsed])
        error_retry = len([f for f in horse_perf_files_to_process if f in error_files])
        log.info(f"  → 処理対象: {len(horse_perf_files_to_process):,}頭 (新規: {new_files:,}頭, エラー再処理: {error_retry:,}頭)")
    elif retry_errors:
        horse_perf_files_to_process = [f for f in horse_perf_files if f in error_files]
        log.info(f"  → 処理対象: {len(horse_perf_files_to_process):,}頭 (エラー再処理のみ)")
    else:
        horse_perf_files_to_process = horse_perf_files

    all_perf_df = []
    success_count = 0
    error_count = 0
    error_cleared_count = 0

    for html_file in tqdm(horse_perf_files_to_process, desc="馬過去成績パース", unit="頭"):
        df = pipeline_core.parse_with_error_handling(
            str(html_file), "horse_performance_parser", horse_info_parser.parse_horse_performance, conn
        )
        if df is not None and not df.empty:
            all_perf_df.append(df)
            success_count += 1
        else:
            error_count += 1

    if all_perf_df:
        try:
            new_perf_df = pd.concat(all_perf_df, ignore_index=True)

            # データ型の最適化
            int_columns = ['race_number', 'head_count', 'bracket_number', 'horse_number',
                          'finish_position', 'popularity', 'horse_weight', 'horse_weight_change']
            for col in int_columns:
                if col in new_perf_df.columns:
                    new_perf_df[col] = new_perf_df[col].astype('Int64')

            if skip_existing and output_path.exists():
                existing_df = pd.read_parquet(output_path)
                final_perf_df = pd.concat([existing_df, new_perf_df], ignore_index=True)
                # 過去成績は1頭×複数レースなので、horse_id + race_date + race_name で重複判定
                if 'race_date' in final_perf_df.columns and 'race_name' in final_perf_df.columns:
                    final_perf_df = final_perf_df.drop_duplicates(
                        subset=['horse_id', 'race_date', 'race_name'], keep='last'
                    )
                else:
                    final_perf_df = final_perf_df.drop_duplicates(subset='horse_id', keep='last')
                log.info(f"  → 既存 {len(existing_df):,}件 + 新規 {len(new_perf_df):,}件 = 合計 {len(final_perf_df):,}件")
            else:
                final_perf_df = new_perf_df

            final_perf_df.to_parquet(output_path, index=False)
            log.info(f"  ✓ 保存完了: {output_path} ({len(final_perf_df):,}レコード)")

            # 保存成功後、エラーレコードをクリア
            for html_file in horse_perf_files_to_process:
                if html_file in error_files:
                    horse_id = extract_race_id_from_filename(html_file)
                    if horse_id in final_perf_df['horse_id'].values:
                        clear_error_record(conn, html_file, 'horse_performance_parser')
                        error_cleared_count += 1

            log.info(f"  📊 処理統計: 成功 {success_count:,}頭 / エラー {error_count:,}頭 / エラー解消 {error_cleared_count:,}頭")

        except Exception as e:
            log.error(f"  ❌ Parquet保存エラー: {e}", exc_info=True)
            log.warning(f"  ⚠️ {len(all_perf_df):,}頭のパース結果が保存できませんでした")
            log.warning(f"  💡 次回実行時に再処理されます")
    else:
        log.warning("処理できる馬過去成績データがありませんでした。")
        if error_count > 0:
            log.info(f"  📊 処理統計: エラー {error_count:,}頭")


def main():
    """再開可能なパースパイプライン"""
    parser = argparse.ArgumentParser(description='再開可能なパースパイプライン')
    parser.add_argument('--force-reparse', action='store_true',
                        help='既存ファイルを含めて全件再パース（デフォルトは新規ファイルのみ処理）')
    parser.add_argument('--retry-errors', action='store_true',
                        help='エラーファイルのみ再処理（他のファイルはスキップ）')
    parser.add_argument('--phase', type=str, default='all',
                        choices=['1', '2', '3', '4', '5', 'all'],
                        help='実行するフェーズ (1=レース, 2=出馬表, 3=馬, 4=血統, 5=成績, all=全て)')
    args = parser.parse_args()

    # デフォルトはスキップモード（既存ファイルを処理しない）
    skip_existing = not args.force_reparse

    cfg = load_config()
    setup_logging(cfg["default"]["logging"]["log_file"])

    log = logging.getLogger(__name__)
    log.info("=" * 80)
    log.info("再開可能なデータ整形パイプラインを開始します...")
    if args.retry_errors:
        log.info(f"モード: エラーファイルのみ再処理")
    elif args.force_reparse:
        log.info(f"モード: 全件再パース")
    else:
        log.info(f"モード: 新規ファイルのみ処理 (デフォルト)")
    log.info(f"フェーズ: {args.phase}")
    log.info("=" * 80)

    db_path = Path(cfg["default"]["database"]["path"])
    conn = sqlite3.connect(db_path)

    try:
        if args.phase in ['1', 'all']:
            parse_phase_races(cfg, conn, skip_existing=skip_existing, retry_errors=args.retry_errors)

        if args.phase in ['2', 'all']:
            parse_phase_shutuba(cfg, conn, skip_existing=skip_existing, retry_errors=args.retry_errors)

        if args.phase in ['3', 'all']:
            parse_phase_horses(cfg, conn, skip_existing=skip_existing, retry_errors=args.retry_errors)

        if args.phase in ['4', 'all']:
            parse_phase_pedigrees(cfg, conn, skip_existing=skip_existing, retry_errors=args.retry_errors)

        if args.phase in ['5', 'all']:
            parse_phase_performance(cfg, conn, skip_existing=skip_existing, retry_errors=args.retry_errors)

    except KeyboardInterrupt:
        log.warning("\n" + "=" * 80)
        log.warning("⚠️ ユーザーによる中断を検出しました")
        log.warning("次回実行時にそのまま再実行すると、ここから再開できます:")
        log.warning(f"  python run_parsing_resumable.py --phase {args.phase}")
        log.warning("=" * 80)
    except Exception as e:
        log.error(f"パイプラインの実行中にエラーが発生しました: {e}", exc_info=True)
    finally:
        conn.close()
        log.info("データベース接続をクローズしました。")

    log.info("=" * 80)
    log.info("データ整形パイプラインが終了しました。")
    log.info("=" * 80)


if __name__ == "__main__":
    main()

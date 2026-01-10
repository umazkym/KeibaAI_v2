#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
再開可能なスクレイピングパイプライン
途中で停止しても、既にスクレイピング済みのファイルをスキップして続行可能
エラーファイルの自動検出と再スクレイピングに対応
"""

import logging
import sqlite3
from pathlib import Path
import yaml
from datetime import datetime
import sys
import argparse
import os
from tqdm import tqdm

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from keibaai.src.preparing import _scrape_html
from keibaai.src.constants import LocalPaths


def load_config():
    """設定ファイルをロードする"""
    config_path = project_root / "keibaai" / "configs"
    with open(config_path / "default.yaml", "r", encoding="utf-8") as f:
        default_cfg = yaml.safe_load(f)
    with open(config_path / "scraping.yaml", "r", encoding="utf-8") as f:
        scraping_cfg_raw = yaml.safe_load(f)

    data_root = project_root / default_cfg['data_path']
    default_cfg['raw_data_path'] = str(data_root / 'raw')
    default_cfg['parsed_data_path'] = str(data_root / 'parsed')
    default_cfg['database']['path'] = str(data_root / 'metadata' / 'db.sqlite3')
    default_cfg['logging']['log_file'] = str(data_root / 'logs' / '{YYYY}' / '{MM}' / '{DD}' / 'scraping_resumable.log')

    config = {
        "default": default_cfg,
        "scraping": scraping_cfg_raw['scraping']
    }
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
            logging.StreamHandler(sys.stdout)
        ]
    )


def get_error_files(conn, error_type: str) -> set:
    """
    データベースからスクレイピングエラーが発生したIDを取得

    Args:
        conn: SQLite接続
        error_type: エラータイプ ('race', 'shutuba', 'horse', 'pedigree')

    Returns:
        エラーIDのセット
    """
    try:
        cursor = conn.cursor()
        # スクレイピングエラーも同じparse_failuresテーブルに記録されている想定
        # もし専用テーブルがあればそれを使う
        cursor.execute('''
            SELECT DISTINCT source_file
            FROM parse_failures
            WHERE parser_name LIKE ?
            ORDER BY failed_ts DESC
        ''', (f'%{error_type}%',))

        error_ids = set()
        for row in cursor.fetchall():
            source_file = row[0]
            # ファイル名からIDを抽出
            file_id = Path(source_file).stem.replace('_perf', '').replace('_profile', '')
            error_ids.add(file_id)

        return error_ids
    except Exception as e:
        logging.warning(f"エラーID取得失敗 ({error_type}): {e}")
        return set()


def clear_error_record(conn, file_path: str, parser_name: str):
    """
    スクレイピング成功時にエラーレコードを削除

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


def get_existing_files(directory: Path, extension: str = '.bin') -> set:
    """
    既存のファイルからIDを取得

    Args:
        directory: ディレクトリパス
        extension: ファイル拡張子

    Returns:
        既存IDのセット
    """
    if not directory.exists():
        return set()

    existing_ids = set()
    for file in directory.glob(f'*{extension}'):
        file_id = file.stem.replace('_perf', '').replace('_profile', '')
        existing_ids.add(file_id)

    return existing_ids


def scrape_phase_races(conn, race_ids: list, skip_existing: bool = True, retry_errors: bool = False):
    """フェーズ0b-1: レース結果HTMLのスクレイピング"""
    log = logging.getLogger(__name__)
    log.info("\n" + "=" * 80)
    log.info("【フェーズ0b-1】レース結果HTMLのスクレイピングを開始")
    log.info("=" * 80)

    race_dir = Path(LocalPaths.HTML_RACE_DIR)
    race_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"  → {len(race_ids):,}件のレースIDが対象です")

    # エラーファイルを取得
    error_ids = get_error_files(conn, 'race')
    if error_ids:
        log.info(f"  → エラー履歴: {len(error_ids):,}件 (自動的に再スクレイピング対象に含めます)")

    # 既存ファイルを確認
    if skip_existing and not retry_errors:
        existing_ids = get_existing_files(race_dir)
        log.info(f"  → 既にスクレイピング済み: {len(existing_ids):,}件")

        # 新規ID + エラーID
        race_ids_to_process = [
            rid for rid in race_ids
            if rid not in existing_ids or rid in error_ids
        ]

        new_count = len([rid for rid in race_ids if rid not in existing_ids])
        error_retry = len([rid for rid in race_ids_to_process if rid in error_ids])
        log.info(f"  → 処理対象: {len(race_ids_to_process):,}件 (新規: {new_count:,}件, エラー再処理: {error_retry:,}件)")
    elif retry_errors:
        # エラーIDのみ
        race_ids_to_process = [rid for rid in race_ids if rid in error_ids]
        log.info(f"  → 処理対象: {len(race_ids_to_process):,}件 (エラー再処理のみ)")
    else:
        race_ids_to_process = race_ids
        log.info(f"  → 処理対象: {len(race_ids_to_process):,}件 (全ID)")

    if not race_ids_to_process:
        log.info("  → 処理対象がありません。スキップします。")
        return

    # スクレイピング実行
    log.info("  → スクレイピングを開始します...")
    saved_paths = _scrape_html.scrape_html_race(race_ids_to_process, skip=False)

    # 結果確認と統計
    success_count = len(saved_paths)
    error_count = len(race_ids_to_process) - success_count

    # 成功したファイルのエラーレコードをクリア
    error_cleared_count = 0
    for race_id in race_ids_to_process:
        file_path = race_dir / f"{race_id}.bin"
        if file_path.exists() and race_id in error_ids:
            clear_error_record(conn, str(file_path), 'results_parser')
            error_cleared_count += 1

    log.info(f"  📊 処理統計: 成功 {success_count:,}件 / エラー {error_count:,}件 / エラー解消 {error_cleared_count:,}件")


def scrape_phase_shutuba(conn, race_ids: list, skip_existing: bool = True, retry_errors: bool = False):
    """フェーズ0b-2: 出馬表HTMLのスクレイピング"""
    log = logging.getLogger(__name__)
    log.info("\n" + "=" * 80)
    log.info("【フェーズ0b-2】出馬表HTMLのスクレイピングを開始")
    log.info("=" * 80)

    shutuba_dir = Path(LocalPaths.HTML_SHUTUBA_DIR)
    shutuba_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"  → {len(race_ids):,}件のレースIDが対象です")

    # エラーファイルを取得
    error_ids = get_error_files(conn, 'shutuba')
    if error_ids:
        log.info(f"  → エラー履歴: {len(error_ids):,}件 (自動的に再スクレイピング対象に含めます)")

    # 既存ファイルを確認
    if skip_existing and not retry_errors:
        existing_ids = get_existing_files(shutuba_dir)
        log.info(f"  → 既にスクレイピング済み: {len(existing_ids):,}件")

        shutuba_ids_to_process = [
            rid for rid in race_ids
            if rid not in existing_ids or rid in error_ids
        ]

        new_count = len([rid for rid in race_ids if rid not in existing_ids])
        error_retry = len([rid for rid in shutuba_ids_to_process if rid in error_ids])
        log.info(f"  → 処理対象: {len(shutuba_ids_to_process):,}件 (新規: {new_count:,}件, エラー再処理: {error_retry:,}件)")
    elif retry_errors:
        shutuba_ids_to_process = [rid for rid in race_ids if rid in error_ids]
        log.info(f"  → 処理対象: {len(shutuba_ids_to_process):,}件 (エラー再処理のみ)")
    else:
        shutuba_ids_to_process = race_ids
        log.info(f"  → 処理対象: {len(shutuba_ids_to_process):,}件 (全ID)")

    if not shutuba_ids_to_process:
        log.info("  → 処理対象がありません。スキップします。")
        return

    log.info("  → スクレイピングを開始します...")
    saved_paths = _scrape_html.scrape_html_shutuba(shutuba_ids_to_process, skip=False)

    success_count = len(saved_paths)
    error_count = len(shutuba_ids_to_process) - success_count

    error_cleared_count = 0
    for race_id in shutuba_ids_to_process:
        file_path = shutuba_dir / f"{race_id}.bin"
        if file_path.exists() and race_id in error_ids:
            clear_error_record(conn, str(file_path), 'shutuba_parser')
            error_cleared_count += 1

    log.info(f"  📊 処理統計: 成功 {success_count:,}件 / エラー {error_count:,}件 / エラー解消 {error_cleared_count:,}件")


def scrape_phase_horses(conn, horse_ids: list, skip_existing: bool = True, retry_errors: bool = False):
    """フェーズ2-1: 馬プロフィール・成績HTMLのスクレイピング"""
    log = logging.getLogger(__name__)
    log.info("\n" + "=" * 80)
    log.info("【フェーズ2-1】馬プロフィール・成績HTMLのスクレイピングを開始")
    log.info("=" * 80)

    horse_dir = Path(LocalPaths.HTML_HORSE_DIR)
    horse_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"  → {len(horse_ids):,}頭の馬IDが対象です")

    # エラーファイルを取得
    error_ids = get_error_files(conn, 'horse')
    if error_ids:
        log.info(f"  → エラー履歴: {len(error_ids):,}件 (自動的に再スクレイピング対象に含めます)")

    # 既存ファイルを確認（プロフィールファイルで判定）
    if skip_existing and not retry_errors:
        existing_ids = set()
        for file in horse_dir.glob('*_profile.bin'):
            horse_id = file.stem.replace('_profile', '')
            existing_ids.add(horse_id)

        log.info(f"  → 既にプロフィール取得済み: {len(existing_ids):,}頭")

        # 修正: 過去成績(perf)の更新チェックが必要なため、既存ファイルがあっても除外せずに全件渡す
        # _scrape_html.scrape_html_horse 内部で skip=True であっても TTL期限切れなら perf を更新するロジックがある
        horse_ids_to_process = horse_ids
        
        log.info(f"  → 処理対象: {len(horse_ids_to_process):,}頭 (更新チェック含む)")
    elif retry_errors:
        horse_ids_to_process = [hid for hid in horse_ids if hid in error_ids]
        log.info(f"  → 処理対象: {len(horse_ids_to_process):,}頭 (エラー再処理のみ)")
    else:
        horse_ids_to_process = horse_ids
        log.info(f"  → 処理対象: {len(horse_ids_to_process):,}頭 (全ID)")

    if not horse_ids_to_process:
        log.info("  → 処理対象がありません。スキップします。")
        return

    log.info("  → スクレイピングを開始します...")
    saved_paths = _scrape_html.scrape_html_horse(horse_ids_to_process, skip=False)

    success_count = len(saved_paths) // 2  # profile + perf で2ファイル
    error_count = len(horse_ids_to_process) - success_count

    error_cleared_count = 0
    for horse_id in horse_ids_to_process:
        profile_path = horse_dir / f"{horse_id}_profile.bin"
        if profile_path.exists() and horse_id in error_ids:
            clear_error_record(conn, str(profile_path), 'horse_info_parser')
            error_cleared_count += 1

    log.info(f"  📊 処理統計: 成功 {success_count:,}頭 / エラー {error_count:,}頭 / エラー解消 {error_cleared_count:,}頭")


def scrape_phase_pedigrees(conn, horse_ids: list, skip_existing: bool = True, retry_errors: bool = False):
    """フェーズ2-2: 血統HTMLのスクレイピング"""
    log = logging.getLogger(__name__)
    log.info("\n" + "=" * 80)
    log.info("【フェーズ2-2】血統HTMLのスクレイピングを開始")
    log.info("=" * 80)

    ped_dir = Path(LocalPaths.HTML_PED_DIR)
    ped_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"  → {len(horse_ids):,}頭の馬IDが対象です")

    # エラーファイルを取得
    error_ids = get_error_files(conn, 'pedigree')
    if error_ids:
        log.info(f"  → エラー履歴: {len(error_ids):,}件 (自動的に再スクレイピング対象に含めます)")

    # 既存ファイルを確認
    if skip_existing and not retry_errors:
        existing_ids = get_existing_files(ped_dir)
        log.info(f"  → 既にスクレイピング済み: {len(existing_ids):,}頭")

        ped_ids_to_process = [
            hid for hid in horse_ids
            if hid not in existing_ids or hid in error_ids
        ]

        new_count = len([hid for hid in horse_ids if hid not in existing_ids])
        error_retry = len([hid for hid in ped_ids_to_process if hid in error_ids])
        log.info(f"  → 処理対象: {len(ped_ids_to_process):,}頭 (新規: {new_count:,}頭, エラー再処理: {error_retry:,}頭)")
    elif retry_errors:
        ped_ids_to_process = [hid for hid in horse_ids if hid in error_ids]
        log.info(f"  → 処理対象: {len(ped_ids_to_process):,}頭 (エラー再処理のみ)")
    else:
        ped_ids_to_process = horse_ids
        log.info(f"  → 処理対象: {len(ped_ids_to_process):,}頭 (全ID)")

    if not ped_ids_to_process:
        log.info("  → 処理対象がありません。スキップします。")
        return

    log.info("  → スクレイピングを開始します（Selenium使用、時間がかかります）...")
    saved_paths = _scrape_html.scrape_html_ped(ped_ids_to_process, skip=False)

    success_count = len(saved_paths)
    error_count = len(ped_ids_to_process) - success_count

    error_cleared_count = 0
    for horse_id in ped_ids_to_process:
        file_path = ped_dir / f"{horse_id}.bin"
        if file_path.exists() and horse_id in error_ids:
            clear_error_record(conn, str(file_path), 'pedigree_parser')
            error_cleared_count += 1

    log.info(f"  📊 処理統計: 成功 {success_count:,}頭 / エラー {error_count:,}頭 / エラー解消 {error_cleared_count:,}頭")


def main():
    """再開可能なスクレイピングパイプライン"""
    parser = argparse.ArgumentParser(description='再開可能なスクレイピングパイプライン')
    parser.add_argument('--from-date', type=str, required=True,
                        help='開始日 (YYYY-MM-DD形式)')
    parser.add_argument('--to-date', type=str, required=True,
                        help='終了日 (YYYY-MM-DD形式)')
    parser.add_argument('--force-rescrape', action='store_true',
                        help='既存ファイルを含めて全件再スクレイピング（デフォルトは新規ファイルのみ処理）')
    parser.add_argument('--retry-errors', action='store_true',
                        help='エラーIDのみ再スクレイピング（他のIDはスキップ）')
    parser.add_argument('--phase', type=str, default='all',
                        choices=['0a', '0b', '1', '2', 'all'],
                        help='実行するフェーズ (0a: 開催日/レースID, 0b: レース/出馬表, 1: 馬ID, 2: 馬データ/血統, all: 全て)')
    args = parser.parse_args()

    # デフォルトはスキップモード（既存ファイルを処理しない）
    skip_existing = not args.force_rescrape

    cfg = load_config()
    setup_logging(cfg["default"]["logging"]["log_file"])

    log = logging.getLogger(__name__)
    log.info("=" * 80)
    log.info("再開可能なスクレイピングパイプラインを開始します...")
    log.info(f"期間: {args.from_date} ～ {args.to_date}")
    if args.retry_errors:
        log.info(f"モード: エラーIDのみ再スクレイピング")
    elif args.force_rescrape:
        log.info(f"モード: 全件再スクレイピング")
    else:
        log.info(f"モード: 新規IDのみ処理 (デフォルト)")
    log.info(f"フェーズ: {args.phase}")
    log.info("=" * 80)

    db_path = Path(cfg["default"]["database"]["path"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    try:
        # --- フェーズ0a: 開催日とレースIDの取得 ---
        if args.phase in ['0a', 'all']:
            log.info("\n" + "=" * 80)
            log.info("【フェーズ0a】開催日とレースIDの取得を開始")
            log.info("=" * 80)

            kaisai_dates = _scrape_html.scrape_kaisai_date(args.from_date, args.to_date)
            log.info(f"  ✅ {len(kaisai_dates)}個の開催日を取得しました")

            if not kaisai_dates:
                log.warning("  ⚠️ 開催日が取得できませんでした。処理を中断します。")
                return

            race_ids = _scrape_html.scrape_race_id_list(kaisai_dates)
            log.info(f"  ✅ {len(race_ids)}個のレースIDを取得しました")

            if not race_ids:
                log.warning("  ⚠️ レースIDが取得できませんでした。処理を中断します。")
                return
        else:
            # 既存のレースIDリストを読み込む
            import pandas as pd
            race_id_file = Path("race_id_list.csv")
            if race_id_file.exists():
                race_ids = pd.read_csv(race_id_file)['race_id'].astype(str).tolist()
                log.info(f"  → 既存のレースIDリストを読み込みました: {len(race_ids)}件")
            else:
                if args.phase in ['0b']:
                    log.error("  ❌ race_id_list.csv が見つかりません。フェーズ0aを先に実行してください。")
                    return
                else:
                    log.info("  ℹ️ race_id_list.csv が見つかりませんが、現在のフェーズでは不要なため続行します。")
                    race_ids = []

        # --- フェーズ0b: レース結果と出馬表の取得 ---
        if args.phase in ['0b', 'all']:
            scrape_phase_races(conn, race_ids, skip_existing=skip_existing, retry_errors=args.retry_errors)
            scrape_phase_shutuba(conn, race_ids, skip_existing=skip_existing, retry_errors=args.retry_errors)

        # --- フェーズ1: 馬IDの収集 ---
        if args.phase in ['1', 'all']:
            log.info("\n" + "=" * 80)
            log.info("【フェーズ1】馬IDの収集を開始")
            log.info("=" * 80)

            raw_race_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "race"
            if not raw_race_dir.exists():
                log.error(f"  ❌ レースディレクトリが存在しません: {raw_race_dir}")
                return

            # 指定期間のレースIDに対応するHTMLファイルのみから馬IDを抽出
            if race_ids:
                log.info(f"  → 指定期間のレースID {len(race_ids):,}件から馬IDを抽出します")
                horse_ids = _scrape_html.extract_horse_ids_from_race_ids(race_ids, str(raw_race_dir))
            else:
                log.warning("  ⚠️ レースIDがありません。全ディレクトリから抽出します（非推奨）。")
                horse_ids = _scrape_html.extract_horse_ids_from_html(str(raw_race_dir))
            log.info(f"  ✅ {len(horse_ids)}頭のユニークな馬IDを取得しました")

            if not horse_ids:
                log.warning("  ⚠️ 馬IDが取得できませんでした。")
                return

            horse_ids = list(horse_ids)
        else:
            # 既存の馬IDリストを読み込む
            horse_id_file = Path("horse_id_list.txt")
            if horse_id_file.exists():
                with open(horse_id_file, 'r') as f:
                    horse_ids = [line.strip() for line in f if line.strip()]
                log.info(f"  → 既存の馬IDリストを読み込みました: {len(horse_ids)}頭")
            else:
                log.warning("  ⚠️ horse_id_list.txt が見つかりません。")
                horse_ids = []

        # --- フェーズ2: 馬データと血統の取得 ---
        if args.phase in ['2', 'all'] and horse_ids:
            scrape_phase_horses(conn, horse_ids, skip_existing=skip_existing, retry_errors=args.retry_errors)
            scrape_phase_pedigrees(conn, horse_ids, skip_existing=skip_existing, retry_errors=args.retry_errors)

        log.info("\n" + "=" * 80)
        log.info("✅ スクレイピングパイプラインが正常に終了しました")
        log.info("=" * 80)

    except KeyboardInterrupt:
        log.warning("\n" + "=" * 80)
        log.warning("⚠️ ユーザーによる中断を検出しました")
        log.warning("次回実行時にそのまま再実行すると、ここから再開できます:")
        log.warning(f"  python run_scraping_resumable.py --from-date {args.from_date} --to-date {args.to_date} --phase {args.phase}")
        log.warning("=" * 80)
    except Exception as e:
        log.error(f"❌ パイプラインの実行中にエラーが発生しました: {e}", exc_info=True)
    finally:
        conn.close()
        log.info("データベース接続をクローズしました")


if __name__ == "__main__":
    main()

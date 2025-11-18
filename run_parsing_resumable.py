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


def extract_race_id_from_filename(file_path: str) -> str:
    """ファイル名からrace_idを抽出 (例: 202305040301.bin → 202305040301)"""
    return Path(file_path).stem.replace('_perf', '').replace('_profile', '')


def parse_phase_races(cfg, conn, skip_existing: bool = False):
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

    # スキップ処理
    if skip_existing:
        already_parsed = get_already_parsed_ids(output_path, 'race_id')
        log.info(f"  → 既にパース済み: {len(already_parsed):,}件")

        race_html_files_to_process = [
            f for f in race_html_files
            if extract_race_id_from_filename(f) not in already_parsed
        ]
        log.info(f"  → 処理対象: {len(race_html_files_to_process):,}件 (新規)")
    else:
        race_html_files_to_process = race_html_files
        log.info(f"  → 処理対象: {len(race_html_files_to_process):,}件 (全ファイル)")

    # パース実行
    all_results_df = []
    for html_file in tqdm(race_html_files_to_process, desc="レース結果パース", unit="件"):
        df = pipeline_core.parse_with_error_handling(
            str(html_file), "results_parser", results_parser.parse_results_html, conn
        )
        if df is not None and not df.empty:
            all_results_df.append(df)

    # 保存 (既存データと結合)
    if all_results_df:
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

        final_results_df.to_parquet(output_path, index=False)
        log.info(f"  ✓ 保存完了: {output_path} ({len(final_results_df):,}レコード)")
    else:
        log.warning("処理できるレース結果データがありませんでした。")


def parse_phase_shutuba(cfg, conn, skip_existing: bool = False):
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

    if skip_existing:
        already_parsed = get_already_parsed_ids(output_path, 'race_id')
        log.info(f"  → 既にパース済み: {len(already_parsed):,}件")

        shutuba_html_files_to_process = [
            f for f in shutuba_html_files
            if extract_race_id_from_filename(f) not in already_parsed
        ]
        log.info(f"  → 処理対象: {len(shutuba_html_files_to_process):,}件 (新規)")
    else:
        shutuba_html_files_to_process = shutuba_html_files

    all_shutuba_df = []
    for html_file in tqdm(shutuba_html_files_to_process, desc="出馬表パース", unit="件"):
        df = pipeline_core.parse_with_error_handling(
            str(html_file), "shutuba_parser", shutuba_parser.parse_shutuba_html, conn
        )
        if df is not None and not df.empty:
            all_shutuba_df.append(df)

    if all_shutuba_df:
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
    else:
        log.warning("処理できる出馬表データがありませんでした。")


def parse_phase_horses(cfg, conn, skip_existing: bool = False):
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

    if skip_existing:
        already_parsed = get_already_parsed_ids(output_path, 'horse_id')
        log.info(f"  → 既にパース済み: {len(already_parsed):,}頭")

        horse_html_files_to_process = [
            f for f in horse_html_files
            if extract_race_id_from_filename(f) not in already_parsed
        ]
        log.info(f"  → 処理対象: {len(horse_html_files_to_process):,}頭 (新規)")
    else:
        horse_html_files_to_process = horse_html_files

    all_horses_data = []
    for html_file in tqdm(horse_html_files_to_process, desc="馬プロフィールパース", unit="頭"):
        data = pipeline_core.parse_with_error_handling(
            str(html_file), "horse_info_parser", horse_info_parser.parse_horse_profile, conn
        )
        if data and 'horse_id' in data and data['horse_id']:
            if not data.get('_is_empty', False):
                all_horses_data.append(data)

    if all_horses_data:
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
    else:
        log.warning("処理できる馬プロフィールデータがありませんでした。")


def parse_phase_pedigrees(cfg, conn, skip_existing: bool = False):
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

    if skip_existing:
        already_parsed = get_already_parsed_ids(output_path, 'horse_id')
        log.info(f"  → 既にパース済み: {len(already_parsed):,}頭")

        ped_html_files_to_process = [
            f for f in ped_html_files
            if extract_race_id_from_filename(f) not in already_parsed
        ]
        log.info(f"  → 処理対象: {len(ped_html_files_to_process):,}頭 (新規)")
    else:
        ped_html_files_to_process = ped_html_files

    all_pedigrees_df = []
    for html_file in tqdm(ped_html_files_to_process, desc="血統パース", unit="頭"):
        df = pipeline_core.parse_with_error_handling(
            str(html_file), "pedigree_parser", pedigree_parser.parse_pedigree_html, conn
        )
        if df is not None and not df.empty:
            all_pedigrees_df.append(df)

    if all_pedigrees_df:
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
    else:
        log.warning("処理できる血統データがありませんでした。")


def parse_phase_performance(cfg, conn, skip_existing: bool = False):
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

    if skip_existing:
        already_parsed = get_already_parsed_ids(output_path, 'horse_id')
        log.info(f"  → 既にパース済み: {len(already_parsed):,}頭")

        horse_perf_files_to_process = [
            f for f in horse_perf_files
            if extract_race_id_from_filename(f) not in already_parsed
        ]
        log.info(f"  → 処理対象: {len(horse_perf_files_to_process):,}頭 (新規)")
    else:
        horse_perf_files_to_process = horse_perf_files

    all_perf_df = []
    for html_file in tqdm(horse_perf_files_to_process, desc="馬過去成績パース", unit="頭"):
        df = pipeline_core.parse_with_error_handling(
            str(html_file), "horse_performance_parser", horse_info_parser.parse_horse_performance, conn
        )
        if df is not None and not df.empty:
            all_perf_df.append(df)

    if all_perf_df:
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
    else:
        log.warning("処理できる馬過去成績データがありませんでした。")


def main():
    """再開可能なパースパイプライン"""
    parser = argparse.ArgumentParser(description='再開可能なパースパイプライン')
    parser.add_argument('--skip-existing', action='store_true',
                        help='既にパース済みのファイルをスキップ（再開モード）')
    parser.add_argument('--phase', type=str, default='all',
                        choices=['1', '2', '3', '4', '5', 'all'],
                        help='実行するフェーズ (1=レース, 2=出馬表, 3=馬, 4=血統, 5=成績, all=全て)')
    args = parser.parse_args()

    cfg = load_config()
    setup_logging(cfg["default"]["logging"]["log_file"])

    log = logging.getLogger(__name__)
    log.info("=" * 80)
    log.info("再開可能なデータ整形パイプラインを開始します...")
    log.info(f"モード: {'スキップモード (再開)' if args.skip_existing else '全件処理モード'}")
    log.info(f"フェーズ: {args.phase}")
    log.info("=" * 80)

    db_path = Path(cfg["default"]["database"]["path"])
    conn = sqlite3.connect(db_path)

    try:
        if args.phase in ['1', 'all']:
            parse_phase_races(cfg, conn, skip_existing=args.skip_existing)

        if args.phase in ['2', 'all']:
            parse_phase_shutuba(cfg, conn, skip_existing=args.skip_existing)

        if args.phase in ['3', 'all']:
            parse_phase_horses(cfg, conn, skip_existing=args.skip_existing)

        if args.phase in ['4', 'all']:
            parse_phase_pedigrees(cfg, conn, skip_existing=args.skip_existing)

        if args.phase in ['5', 'all']:
            parse_phase_performance(cfg, conn, skip_existing=args.skip_existing)

    except KeyboardInterrupt:
        log.warning("\n" + "=" * 80)
        log.warning("⚠️ ユーザーによる中断を検出しました")
        log.warning("次回実行時に --skip-existing を指定すると、ここから再開できます:")
        log.warning(f"  python run_parsing_resumable.py --skip-existing --phase {args.phase}")
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

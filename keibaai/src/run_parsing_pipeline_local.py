# keibaai/src/run_parsing_pipeline_local.py

import logging
import sqlite3
from pathlib import Path
import yaml
from datetime import datetime
import sys
import pandas as pd
import os

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src import pipeline_core
from src.modules.parsers import results_parser, shutuba_parser, horse_info_parser, pedigree_parser

def load_config():
    """設定ファイルをロードする"""
    config_path = Path(__file__).resolve().parent.parent / "configs"
    with open(config_path / "default.yaml", "r", encoding="utf-8") as f:
        default_cfg = yaml.safe_load(f)
    
    data_root = Path(__file__).resolve().parent.parent / default_cfg['data_path']
    default_cfg['raw_data_path'] = str(data_root / 'raw')
    default_cfg['parsed_data_path'] = str(data_root / 'parsed')
    default_cfg['database']['path'] = str(data_root / 'metadata' / 'db.sqlite3')
    default_cfg['logging']['log_file'] = str(data_root / 'logs' / '{YYYY}' / '{MM}' / '{DD}' / 'parsing.log')

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

def main():
    """
    生のHTMLデータをパースし、Parquet形式で保存するパイプライン
    """
    cfg = load_config()
    setup_logging(cfg["default"]["logging"]["log_file"])
    
    log = logging.getLogger(__name__)
    log.info("データ整形パイプラインを開始します...")

    db_path = Path(cfg["default"]["database"]["path"])
    conn = sqlite3.connect(db_path)

    try:
        # --- 1. レース結果HTMLのパース ---
        log.info("レース結果HTMLのパース処理を開始します。")
        raw_race_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "race"
        parsed_race_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "races"
        parsed_race_parquet_dir.mkdir(parents=True, exist_ok=True)
        race_html_files = []
        for root, _, files in os.walk(raw_race_html_dir):
            for file in files:
                if file.endswith((".html", ".bin")):
                    race_html_files.append(os.path.join(root, file))
        log.info(f"{len(race_html_files)}件のレース結果HTMLファイルが見つかりました。")
        all_results_df = []
        for html_file in race_html_files:
            df = pipeline_core.parse_with_error_handling(str(html_file), "results_parser", results_parser.parse_results_html, conn)
            if df is not None and not df.empty:
                all_results_df.append(df)
        if all_results_df:
            final_results_df = pd.concat(all_results_df, ignore_index=True)
            output_path = parsed_race_parquet_dir / "races.parquet"
            final_results_df.to_parquet(output_path, index=False)
            log.info(f"レース結果のパース結果をParquetファイルとして保存しました: {output_path} ({len(final_results_df)}レコード)")
        else:
            log.warning("処理できるレース結果データがありませんでした。")

        # --- 2. 出馬表HTMLのパース ---
        log.info("出馬表HTMLのパース処理を開始します。")
        raw_shutuba_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "shutuba"
        parsed_shutuba_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "shutuba"
        parsed_shutuba_parquet_dir.mkdir(parents=True, exist_ok=True)
        shutuba_html_files = []
        for root, _, files in os.walk(raw_shutuba_html_dir):
            for file in files:
                if file.endswith((".html", ".bin")):
                    shutuba_html_files.append(os.path.join(root, file))
        log.info(f"{len(shutuba_html_files)}件の出馬表HTMLファイルが見つかりました。")
        all_shutuba_df = []
        for html_file in shutuba_html_files:
            df = pipeline_core.parse_with_error_handling(str(html_file), "shutuba_parser", shutuba_parser.parse_shutuba_html, conn)
            if df is not None and not df.empty:
                all_shutuba_df.append(df)
        if all_shutuba_df:
            final_shutuba_df = pd.concat(all_shutuba_df, ignore_index=True)
            output_path = parsed_shutuba_parquet_dir / "shutuba.parquet"
            final_shutuba_df.to_parquet(output_path, index=False)
            log.info(f"出馬表のパース結果をParquetファイルとして保存しました: {output_path} ({len(final_shutuba_df)}レコード)")
        else:
            log.warning("処理できる出馬表データがありませんでした。")

        # --- 3. 馬プロフィールHTMLのパース (修正版) ---
        log.info("馬プロフィールHTMLのパース処理を開始します。")
        raw_horse_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "horse"
        parsed_horse_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "horses"
        parsed_horse_parquet_dir.mkdir(parents=True, exist_ok=True)
        
        # ▼▼▼ 修正箇所1: プロフィールファイルのみをフィルタリング ▼▼▼
        horse_html_files = []
        for root, _, files in os.walk(raw_horse_html_dir):
            for file in files:
                if file.endswith((".html", ".bin")):
                    full_path = os.path.join(root, file)
                    # プロフィールファイルのみを対象とする
                    if horse_info_parser.is_profile_file(full_path):
                        horse_html_files.append(full_path)
        # ▲▲▲ 修正箇所1 ▲▲▲
        
        log.info(f"{len(horse_html_files)}件の馬プロフィールHTMLファイルが見つかりました。")
        
        # ▼▼▼ 修正箇所2: 辞書ではなくリストで管理 + 空データの除外 ▼▼▼
        all_horses_data = []
        
        for html_file in horse_html_files:
            data = pipeline_core.parse_with_error_handling(
                str(html_file), 
                "horse_info_parser", 
                horse_info_parser.parse_horse_profile, 
                conn
            )
            
            # パース成功 かつ 空データではない場合のみ追加
            if data and 'horse_id' in data and data['horse_id']:
                # '_is_empty'フラグがある場合はスキップ（成績ファイルを誤処理した場合）
                if not data.get('_is_empty', False):
                    all_horses_data.append(data)
                else:
                    log.debug(f"空データをスキップしました: {html_file}")
        # ▲▲▲ 修正箇所2 ▲▲▲
        
        if all_horses_data:
            final_horses_df = pd.DataFrame(all_horses_data)
            
            # ▼▼▼ 修正箇所3: 重複排除（最新データを優先） ▼▼▼
            # horse_idでソートして、重複がある場合は最後（最新）を残す
            final_horses_df = final_horses_df.sort_values(by='horse_id')
            final_horses_df = final_horses_df.drop_duplicates(subset='horse_id', keep='last')
            # ▲▲▲ 修正箇所3 ▲▲▲
            
            output_path = parsed_horse_parquet_dir / "horses.parquet"
            final_horses_df.to_parquet(output_path, index=False)
            log.info(f"馬プロフィールのパース結果をParquetファイルとして保存しました: {output_path} ({len(final_horses_df)}レコード)")
        else:
            log.warning("処理できる馬プロフィールデータがありませんでした。")

        # --- 4. 血統HTMLのパース ---
        log.info("血統HTMLのパース処理を開始します。")
        raw_ped_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "ped"
        parsed_ped_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "pedigrees"
        parsed_ped_parquet_dir.mkdir(parents=True, exist_ok=True)
        ped_html_files = []
        for root, _, files in os.walk(raw_ped_html_dir):
            for file in files:
                if file.endswith((".html", ".bin")):
                    ped_html_files.append(os.path.join(root, file))
        log.info(f"{len(ped_html_files)}件の血統HTMLファイルが見つかりました。")
        all_pedigrees_df = []
        for html_file in ped_html_files:
            df = pipeline_core.parse_with_error_handling(str(html_file), "pedigree_parser", pedigree_parser.parse_pedigree_html, conn)
            if df is not None and not df.empty:
                all_pedigrees_df.append(df)
        if all_pedigrees_df:
            final_pedigrees_df = pd.concat(all_pedigrees_df, ignore_index=True)
            output_path = parsed_ped_parquet_dir / "pedigrees.parquet"
            final_pedigrees_df.to_parquet(output_path, index=False)
            log.info(f"血統のパース結果をParquetファイルとして保存しました: {output_path} ({len(final_pedigrees_df)}レコード)")
        else:
            log.warning("処理できる血統データがありませんでした。")

    except Exception as e:
        log.error(f"パイプラインの実行中にエラーが発生しました: {e}", exc_info=True)
    finally:
        conn.close()
        log.info("データベース接続をクローズしました。")

    log.info("データ整形パイプラインが終了しました。")

if __name__ == "__main__":
    main()
# keibaai/src/run_scraping_pipeline_local.py

import logging
import sqlite3
from pathlib import Path
import yaml
from datetime import datetime
import sys
import pandas as pd

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.modules.preparing import _requests_utils, _scrape_jra_odds
from src import pipeline_core
from src.utils import data_utils
from src.modules.parsers import shutuba_parser

def load_config():
    """設定ファイルをロードする"""
    config_path = Path(__file__).resolve().parent.parent / "configs"
    with open(config_path / "default.yaml", "r", encoding="utf-8") as f:
        default_cfg = yaml.safe_load(f)
    with open(config_path / "scraping.yaml", "r", encoding="utf-8") as f:
        scraping_cfg_raw = yaml.safe_load(f)
    
    data_root = Path(__file__).resolve().parent.parent / default_cfg['data_path']
    default_cfg['raw_data_path'] = str(data_root / 'raw')
    default_cfg['parsed_data_path'] = str(data_root / 'parsed')
    default_cfg['database']['path'] = str(data_root / 'metadata' / 'db.sqlite3')
    default_cfg['logging']['log_file'] = str(data_root / 'logs' / '{YYYY}' / '{MM}' / '{DD}' / 'pipeline.log')

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
            logging.StreamHandler()
        ]
    )

def scrape_and_save_html(identifier: str, data_type: str, cfg: dict, conn: sqlite3.Connection):
    """指定されたデータ種別のHTMLを取得して保存する"""
    log = logging.getLogger(__name__)
    
    url_map = {
        "race": (f"https://db.netkeiba.com/race/{identifier}", Path(cfg['default']['raw_data_path']) / "html" / "race"),
        "shutuba": (f"https://race.netkeiba.com/race/shutuba.html?race_id={identifier}", Path(cfg['default']['raw_data_path']) / "html" / "shutuba"),
        "horse": (f"https://db.netkeiba.com/horse/{identifier}", Path(cfg['default']['raw_data_path']) / "html" / "horse"),
        "ped": (f"https://db.netkeiba.com/horse/ped/{identifier}", Path(cfg['default']['raw_data_path']) / "html" / "ped"),
    }

    if data_type not in url_map:
        log.warning(f"未対応のデータ種別です: {data_type}")
        return None

    url, output_dir = url_map[data_type]
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        response = _requests_utils.fetch_html(url, cfg)
        file_name = data_utils.construct_filename(
            base_name=data_type,
            identifier=identifier,
            data=response.content,
            extension="html"
        )
        file_path = output_dir / file_name
        
        pipeline_core.atomic_write(str(file_path), response.content)
        log.info(f"{data_type} HTMLを保存しました: {file_path}")
        
        data_utils.save_fetch_metadata(
            db_conn=conn, url=url, file_path=str(file_path), data=response.content,
            http_status=response.status_code, fetch_method='requests', error_message=None
        )
        log.info(f"メタデータをデータベースに保存しました: {url}")
        return file_path
    except Exception as e:
        log.error(f"ID {identifier} ({data_type}) の処理中にエラーが発生しました: {e}", exc_info=True)
        return None

def main():
    """設定に基づいてスクレイピングパイプラインを実行する"""
    cfg = load_config()
    setup_logging(cfg["default"]["logging"]["log_file"])
    log = logging.getLogger(__name__)
    log.info("スクレイピングパイプラインを開始します...")
    
    db_path = Path(cfg["default"]["database"]["path"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    try:
        # --- 1. レース情報のスクレイピング ---
        log.info("レース情報のスクレイピングを開始...")
        kaisai_dates = _requests_utils.scrape_kaisai_dates(cfg)
        race_ids = _requests_utils.scrape_race_id_list(kaisai_dates, cfg)
        
        shutuba_html_paths = []
        for race_id in race_ids:
            scrape_and_save_html(race_id, "race", cfg, conn)
            shutuba_path = scrape_and_save_html(race_id, "shutuba", cfg, conn)
            if shutuba_path:
                shutuba_html_paths.append(shutuba_path)
        
        # --- 2. 出馬表から馬IDを取得 ---
        log.info("出馬表から馬IDを取得...")
        horse_ids = set()
        for html_file in shutuba_html_paths:
            df = shutuba_parser.parse_shutuba_html(str(html_file))
            if df is not None and not df.empty and 'horse_id' in df.columns:
                horse_ids.update(df['horse_id'].dropna().unique())
        
        log.info(f"{len(horse_ids)}頭のユニークな馬IDを取得しました。")

        # --- 3. 馬関連情報のスクレイピング ---
        log.info("馬関連情報のスクレイピングを開始...")
        for horse_id in list(horse_ids):
            scrape_and_save_html(horse_id, "horse", cfg, conn)
            scrape_and_save_html(horse_id, "ped", cfg, conn)

        # --- 4. 当日オッズ情報の取得 ---
        if True: # 常に実行
            log.info("当日バッチ処理（オッズ取得）を開始...")
            for race_id in race_ids:
                _scrape_jra_odds.scrape_and_save_jra_odds(race_id, cfg, conn)
            log.info("当日バッチ処理が完了。")

    except Exception as e:
        log.error(f"パイプラインの実行中にエラーが発生しました: {e}", exc_info=True)
    finally:
        conn.close()
        log.info("データベース接続をクローズしました。")

    log.info("スクレイピングパイプラインが終了しました。")

if __name__ == "__main__":
    main()
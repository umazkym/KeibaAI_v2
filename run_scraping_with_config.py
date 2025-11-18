#!/usr/bin/env python3
"""
設定可能な日付範囲でスクレイピングを実行するスクリプト
より堅牢なエラーハンドリングと詳細なログを含む
"""

import logging
import sqlite3
from pathlib import Path
import yaml
from datetime import datetime
import sys
import argparse

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / 'keibaai'))

from keibaai.src.modules.preparing import _scrape_html
from keibaai.src.utils import data_utils


def load_config():
    """設定ファイルをロードする"""
    config_path = Path(__file__).resolve().parent / "keibaai" / "configs"
    with open(config_path / "default.yaml", "r", encoding="utf-8") as f:
        default_cfg = yaml.safe_load(f)
    with open(config_path / "scraping.yaml", "r", encoding="utf-8") as f:
        scraping_cfg_raw = yaml.safe_load(f)

    data_root = Path(__file__).resolve().parent / "keibaai" / default_cfg['data_path']
    default_cfg['raw_data_path'] = str(data_root / 'raw')
    default_cfg['parsed_data_path'] = str(data_root / 'parsed')
    default_cfg['database']['path'] = str(data_root / 'metadata' / 'db.sqlite3')
    default_cfg['logging']['log_file'] = str(data_root / 'logs' / '{YYYY}' / '{MM}' / '{DD}' / 'scraping.log')

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


def main():
    """設定可能な日付範囲でスクレイピングを実行"""
    parser = argparse.ArgumentParser(description='KeibaAI_v2 スクレイピングパイプライン')
    parser.add_argument('--from-date', type=str, required=True,
                        help='開始日 (YYYY-MM-DD形式)')
    parser.add_argument('--to-date', type=str, required=True,
                        help='終了日 (YYYY-MM-DD形式)')
    parser.add_argument('--skip-existing', action='store_true',
                        help='既存ファイルをスキップする')
    parser.add_argument('--phase', type=str, choices=['0a', '0b', '1', '2', 'all'], default='all',
                        help='実行するフェーズ (0a: 開催日/ID取得, 0b: レース/出馬表, 1: 馬ID収集, 2: 馬データ取得)')

    args = parser.parse_args()

    cfg = load_config()
    setup_logging(cfg["default"]["logging"]["log_file"])
    log = logging.getLogger(__name__)

    log.info("=" * 70)
    log.info("スクレイピングパイプライン（.bin形式）を開始します")
    log.info("=" * 70)
    log.info(f"期間: {args.from_date} ～ {args.to_date}")
    log.info(f"既存ファイルスキップ: {'有効' if args.skip_existing else '無効'}")
    log.info("=" * 70)

    db_path = Path(cfg["default"]["database"]["path"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    try:
        # --- フェーズ0a: 開催日とレースIDの取得 ---
        if args.phase in ['0a', 'all']:
            log.info("")
            log.info("【フェーズ0a】開催日とレースIDの取得を開始")
            log.info("=" * 70)

            kaisai_dates = _scrape_html.scrape_kaisai_date(args.from_date, args.to_date)
            log.info(f"✅ {len(kaisai_dates)}個の開催日を取得しました")

            if not kaisai_dates:
                log.warning("⚠️ 開催日が取得できませんでした。処理を中断します。")
                return

            race_ids = _scrape_html.scrape_race_id_list(kaisai_dates)
            log.info(f"✅ {len(race_ids)}個のレースIDを取得しました")

            if not race_ids:
                log.warning("⚠️ レースIDが取得できませんでした。処理を中断します。")
                return
        else:
            # 既存のレースIDリストを読み込む
            race_id_file = Path("race_id_list.csv")
            if race_id_file.exists():
                import pandas as pd
                race_ids = pd.read_csv(race_id_file)['race_id'].astype(str).tolist()
                log.info(f"既存のレースIDリストを読み込みました: {len(race_ids)}件")
            else:
                log.error("❌ race_id_list.csv が見つかりません。フェーズ0aを先に実行してください。")
                return

        # --- フェーズ0b: レース結果と出馬表の取得 ---
        if args.phase in ['0b', 'all']:
            log.info("")
            log.info("【フェーズ0b】レースデータの取得を開始")
            log.info("=" * 70)

            # レース結果を取得
            race_paths = _scrape_html.scrape_html_race(race_ids)
            log.info(f"✅ {len(race_paths)}個のレース結果を取得しました")

            # 出馬表を取得
            shutuba_paths = _scrape_html.scrape_html_shutuba(race_ids)
            log.info(f"✅ {len(shutuba_paths)}個の出馬表を取得しました")

        # --- フェーズ1: 馬IDの収集 ---
        if args.phase in ['1', 'all']:
            log.info("")
            log.info("【フェーズ1】馬IDの収集を開始")
            log.info("=" * 70)

            raw_race_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "race"
            if not raw_race_dir.exists():
                log.error(f"❌ レースディレクトリが存在しません: {raw_race_dir}")
                return

            horse_ids = _scrape_html.extract_horse_ids_from_html(str(raw_race_dir))
            log.info(f"✅ {len(horse_ids)}頭のユニークな馬IDを取得しました")

            if not horse_ids:
                log.warning("⚠️ 馬IDが取得できませんでした。")
                return

        else:
            # 既存の馬IDリストを読み込む
            horse_id_file = Path("horse_id_list.txt")
            if horse_id_file.exists():
                with open(horse_id_file, 'r') as f:
                    horse_ids = set([line.strip() for line in f if line.strip()])
                log.info(f"既存の馬IDリストを読み込みました: {len(horse_ids)}頭")
            else:
                log.warning("⚠️ horse_id_list.txt が見つかりません。")
                horse_ids = set()

        # --- フェーズ2: 馬データの取得 ---
        if args.phase in ['2', 'all'] and horse_ids:
            log.info("")
            log.info("【フェーズ2】馬データの取得を開始")
            log.info("=" * 70)

            horse_id_list = list(horse_ids)

            # 馬プロフィールと成績を取得
            horse_paths = _scrape_html.scrape_html_horse(horse_id_list)
            log.info(f"✅ {len(horse_paths)}個の馬情報を取得しました")

            # 血統情報を取得（Selenium使用）
            ped_paths = _scrape_html.scrape_html_ped(horse_id_list)
            log.info(f"✅ {len(ped_paths)}個の血統情報を取得しました")

        log.info("")
        log.info("=" * 70)
        log.info("✅ スクレイピングパイプラインが正常に終了しました")
        log.info("=" * 70)

    except KeyboardInterrupt:
        log.warning("\n⚠️ ユーザーによって中断されました")
    except Exception as e:
        log.error(f"❌ パイプラインの実行中にエラーが発生しました: {e}", exc_info=True)
    finally:
        conn.close()
        log.info("データベース接続をクローズしました")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
コマンドライン引数対応版スクレイピングパイプライン

使用方法:
    # 特定期間をスクレイピング
    python keibaai/src/run_scraping_pipeline_with_args.py \
        --from 2020-01-01 --to 2020-12-31

    # 既存データをスキップ（デフォルト）
    python keibaai/src/run_scraping_pipeline_with_args.py \
        --from 2024-01-01 --to 2024-12-31 --skip

    # 強制再取得（既存データを上書き）
    python keibaai/src/run_scraping_pipeline_with_args.py \
        --from 2024-01-01 --to 2024-01-31 --no-skip

    # 5年分を一括スクレイピング（既存データはスキップ）
    python keibaai/src/run_scraping_pipeline_with_args.py \
        --from 2020-01-01 --to 2024-12-31
"""

import logging
import sqlite3
import argparse
from pathlib import Path
import yaml
from datetime import datetime
import sys

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.modules.preparing import _scrape_html, _scrape_jra_odds
from src import pipeline_core
from src.utils import data_utils

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
            logging.StreamHandler()
        ]
    )

def main():
    """設定に基づいてスクレイピングパイプラインを実行する"""
    # コマンドライン引数のパース
    parser = argparse.ArgumentParser(
        description='スクレイピングパイプライン（日付範囲指定対応版）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 2024年のデータをスクレイピング（既存データはスキップ）
  python %(prog)s --from 2024-01-01 --to 2024-12-31

  # 2020-2024年の5年分を一括スクレイピング
  python %(prog)s --from 2020-01-01 --to 2024-12-31

  # 既存データを強制上書き
  python %(prog)s --from 2024-01-01 --to 2024-01-31 --no-skip
        """
    )
    parser.add_argument('--from', dest='from_date', required=True,
                        help='開始日（YYYY-MM-DD形式）')
    parser.add_argument('--to', dest='to_date', required=True,
                        help='終了日（YYYY-MM-DD形式）')
    parser.add_argument('--skip', dest='skip', action='store_true', default=True,
                        help='既存ファイルをスキップ（デフォルト）')
    parser.add_argument('--no-skip', dest='skip', action='store_false',
                        help='既存ファイルを上書き（強制再取得）')

    args = parser.parse_args()

    cfg = load_config()
    setup_logging(cfg["default"]["logging"]["log_file"])
    log = logging.getLogger(__name__)

    log.info("=" * 70)
    log.info("スクレイピングパイプライン（.bin形式）を開始")
    log.info("=" * 70)
    log.info(f"期間: {args.from_date} 〜 {args.to_date}")
    log.info(f"既存ファイルスキップ: {'有効' if args.skip else '無効（強制上書き）'}")
    log.info("=" * 70)

    db_path = Path(cfg["default"]["database"]["path"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    try:
        # --- フェーズ0a: スクレイピング対象のIDを特定 ---
        log.info("\n【フェーズ0a】開催日とレースIDの取得を開始...")

        kaisai_dates = _scrape_html.scrape_kaisai_date(args.from_date, args.to_date)
        log.info(f"  ✓ {len(kaisai_dates)}個の開催日を取得しました")

        if not kaisai_dates:
            log.warning("対象の開催日が見つかりませんでした。")
            return

        race_ids = _scrape_html.scrape_race_id_list(kaisai_dates)
        log.info(f"  ✓ {len(race_ids)}個のレースIDを取得しました")

        if not race_ids:
            log.warning("対象のレースIDが見つかりませんでした。")
            return

        # --- フェーズ0b: レース結果と出馬表の取得 ---
        log.info("\n【フェーズ0b】レースデータの取得を開始...")

        # レース結果を取得
        race_paths = _scrape_html.scrape_html_race(race_ids, skip=args.skip)
        log.info(f"  ✓ {len(race_paths)}個のレース結果を{'スクレイピング' if not args.skip else '取得（既存スキップ）'}しました")

        # 出馬表を取得
        shutuba_paths = _scrape_html.scrape_html_shutuba(race_ids, skip=args.skip)
        log.info(f"  ✓ {len(shutuba_paths)}個の出馬表を{'スクレイピング' if not args.skip else '取得（既存スキップ）'}しました")

        # --- フェーズ1: 馬IDの収集 ---
        log.info("\n【フェーズ1】馬IDの収集を開始...")

        # レース結果HTMLから馬IDを抽出
        raw_race_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "race"
        horse_ids = _scrape_html.extract_horse_ids_from_html(str(raw_race_dir))
        log.info(f"  ✓ {len(horse_ids)}頭のユニークな馬IDを取得しました")

        # --- フェーズ2: 馬データの取得 ---
        log.info("\n【フェーズ2】馬データの取得を開始...")

        horse_id_list = list(horse_ids)

        # 馬プロフィールと成績を取得
        horse_paths = _scrape_html.scrape_html_horse(horse_id_list, skip=args.skip)
        log.info(f"  ✓ {len(horse_paths)}個の馬情報を{'スクレイピング' if not args.skip else '取得（既存スキップ）'}しました")

        # 血統情報を取得（Selenium使用）
        ped_paths = _scrape_html.scrape_html_ped(horse_id_list, skip=args.skip)
        log.info(f"  ✓ {len(ped_paths)}個の血統情報を{'スクレイピング' if not args.skip else '取得（既存スキップ）'}しました")

        # メタデータの保存
        log.info("\n【フェーズ3】メタデータの保存...")
        saved_count = 0
        for race_id in race_ids:
            url = f"https://db.netkeiba.com/race/{race_id}"
            file_path = str(raw_race_dir / f"{race_id}.bin")
            if Path(file_path).exists():
                with open(file_path, 'rb') as f:
                    data = f.read()
                data_utils.save_fetch_metadata(
                    db_conn=conn, url=url, file_path=file_path, data=data,
                    http_status=200, fetch_method='requests'
                )
                saved_count += 1
        log.info(f"  ✓ {saved_count}件のメタデータを保存しました")

    except Exception as e:
        log.error(f"パイプラインの実行中にエラーが発生しました: {e}", exc_info=True)
    finally:
        conn.close()
        log.info("データベース接続をクローズしました")

    log.info("\n" + "=" * 70)
    log.info("スクレイピングパイプライン（.bin形式）が正常終了しました")
    log.info("=" * 70)

if __name__ == "__main__":
    main()

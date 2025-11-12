#!/usr/bin/env python3
# src/run_scraping_pipeline_local.py

import logging
import sqlite3
from pathlib import Path
import yaml
from datetime import datetime
import sys
import pandas as pd
import argparse # 修正: argparse をインポート

# --- ▼▼▼ 修正: パス解決ロジックを他のスクリプトと統一 ---
# スクリプト(keibaai/src/run_scraping_pipeline_local.py) の2階層上が keibaai (プロジェクトルート)
project_root = Path(__file__).resolve().parent.parent
# keibaai (プロジェクトルート) を sys.path に追加
sys.path.append(str(project_root))
# --- ▲▲▲ 修正ここまで ---

try:
  # --- ▼▼▼ 修正: 'src.modules' からの絶対パスでインポート ---
  from src.modules.preparing import _requests_utils, _scrape_jra_odds
  from src import pipeline_core
  from src.utils import data_utils
  from src.modules.parsers import shutuba_parser
  # --- ▲▲▲ 修正ここまで ---
except ImportError as e:
  print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
  print("プロジェクトルート（Keiba_AI_v2）から実行しているか、")
  print("keibaai/src/ 配下に 'modules' フォルダ (preparing, parsers を含む) が存在するか確認してください。")
  print(f"sys.path: {sys.path}")
  sys.exit(1)

def load_config():
  """設定ファイルをロードする"""
  # --- ▼▼▼ 修正: パス解決に project_root (keibaai/) を使用 ---
  config_path = project_root / "configs" # 修正
  with open(config_path / "default.yaml", "r", encoding="utf-8") as f:
    default_cfg = yaml.safe_load(f)
  with open(config_path / "scraping.yaml", "r", encoding="utf-8") as f:
    scraping_cfg_raw = yaml.safe_load(f)
 
  # default.yaml の data_path (例: data) を project_root 基準で解決
  # (注: default.yaml の data_path は "data" であり、"keibaai/data" ではないと仮定)
  data_path_val = default_cfg.get('data_path', 'data')
  data_root = project_root / data_path_val
  # --- ▲▲▲ 修正ここまで ---
 
  default_cfg['raw_data_path'] = str(data_root / 'raw')
  default_cfg['parsed_data_path'] = str(data_root / 'parsed')
  default_cfg['database']['path'] = str(data_root / 'metadata' / 'db.sqlite3')

  # --- 修正: ログパスの解決を project_root 基準に変更 ---
  # (他のスクリプトのログ解決ロジックと統一)
 
  # default.yaml の 'paths' セクションを取得 (存在しない場合は data_path から構築)
  # (注: train_mu_model.py のロジックと合わせる)
  paths_config = default_cfg.get('paths', {})
  data_path_val_from_paths = paths_config.get('data_path', 'data')
  
  logs_path_base = paths_config.get('logs_path', 'data/logs')
  # ${data_path} を置換
  logs_path_base = logs_path_base.replace('${data_path}', data_path_val_from_paths)
   
  log_path_template = default_cfg.get('logging', {}).get('log_file', 'data/logs/{YYYY}/{MM}/{DD}/pipeline.log')
  # ${logs_path} を置換
  log_path_template = log_path_template.replace('${logs_path}', logs_path_base)
 
  default_cfg['logging']['log_file_template'] = log_path_template # テンプレートを保持
  # --- 修正ここまで ---

  config = {
    "default": default_cfg,
    "scraping": scraping_cfg_raw['scraping']
  }
  return config

def setup_logging(log_path_template: str):
  """ログ設定を行う"""
  now = datetime.now()
  log_path = log_path_template.format(YYYY=now.year, MM=f"{now.month:02}", DD=f"{now.day:02}")
 
  # --- 修正: project_root (keibaai/) からの絶対パスとして解決 ---
  log_path_abs = project_root / log_path
  log_path_abs.parent.mkdir(parents=True, exist_ok=True)
  # --- 修正ここまで ---
 
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
      logging.FileHandler(log_path_abs, encoding='utf-8'), # 修正: 絶対パスを使用
      logging.StreamHandler(sys.stdout) # 修正: stdout に変更
    ],
    force=True # 修正: 既存のハンドラを上書き
  )

def scrape_and_save_html(identifier: str, data_type: str, cfg: dict, conn: sqlite3.Connection):
  """指定されたデータ種別のHTMLを取得して保存する"""
  log = logging.getLogger(__name__)
 
  # cfg['default']['raw_data_path'] は load_config で絶対パス解決済み
  raw_data_path = Path(cfg['default']['raw_data_path'])
 
  url_map = {
    "race": (f"https://db.netkeiba.com/race/{identifier}", raw_data_path / "html" / "race"),
    "shutuba": (f"https://race.netkeiba.com/race/shutuba.html?race_id={identifier}", raw_data_path / "html" / "shutuba"),
    "horse": (f"https://db.netkeiba.com/horse/{identifier}", raw_data_path / "html" / "horse"),
    "ped": (f"https://db.netkeiba.com/horse/ped/{identifier}", raw_data_path / "html" / "ped"),
  }

  if data_type not in url_map:
    log.warning(f"未対応のデータ種別です: {data_type}")
    return None

  url, output_dir = url_map[data_type]
  output_dir.mkdir(parents=True, exist_ok=True)

  try:
    # --- 修正: scraping_cfg を渡す ---
    response = _requests_utils.fetch_html(url, cfg['scraping'])
   
    if response is None:
      log.info(f"ID {identifier} ({data_type}) のHTML取得をスキップ (キャッシュ/設定/エラー)")
      return None
     
    # 修正: レスポンスがNoneでないことを確認してから .content にアクセス
    if response.content is None:
      log.warning(f"ID {identifier} ({data_type}) のレスポンスボディが空です。")
      return None
   
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
    log.error(f"ID {identifier} ({data_type}) の処理中にエラーが発生しました: {e}", exc_info=False)
    try:
      status_code = response.status_code if 'response' in locals() and hasattr(response, 'status_code') else 500
      data_utils.save_fetch_metadata(
        db_conn=conn, url=url, file_path=str(output_dir / f"{identifier}_error.html"), data=b"",
        http_status=status_code,
        fetch_method='requests', error_message=str(e)
      )
    except Exception as db_e:
      log.error(f"エラーメタデータの保存に失敗: {db_e}")
    return None

# --- ▼▼▼ 修正: argparse を使った main 関数 ---
def main():
  """設定に基づいてスクレイピングパイプラインを実行する"""
 
  # 1. 引数パーサーを追加
  parser = argparse.ArgumentParser(description='Keiba AI スクレイピングパイプライン')
  parser.add_argument(
    '--from_date',
    type=str,
    required=True,
    help='スクレイピング開始日 (YYYY-MM-DD)'
  )
  parser.add_argument(
    '--to_date',
    type=str,
    required=True,
    help='スクレイピング終了日 (YYYY-MM-DD)'
  )
  # (仕様書[Source 420] に合わせるため skip_race_html も追加)
  parser.add_argument(
    '--skip_race_html',
    action='store_true',
    help='レース結果(race)HTMLの取得をスキップする'
  )
  args = parser.parse_args()

  # 2. 設定とロギング
  cfg = load_config()
  setup_logging(cfg["default"]["logging"]["log_file_template"]) # 修正: テンプレートパスを渡す
  log = logging.getLogger(__name__)
  log.info("スクレイピングパイプラインを開始します...")
  log.info(f"対象期間: {args.from_date} - {args.to_date}")
 
  # --- 修正: db_path を project_root 基準で解決 ---
  db_path = Path(cfg["default"]["database"]["path"])
  db_path.parent.mkdir(parents=True, exist_ok=True)
  conn = None # 修正: finally のために先に定義
  try:
    conn = sqlite3.connect(db_path)

    # --- 3. レース情報のスクレイピング ---
    log.info("レース情報のスクレイピングを開始...")
   
    # --- 修正: cfg['scraping'] を渡す ---
    all_kaisai_dates = _requests_utils.scrape_kaisai_dates(cfg['scraping'])
   
    try:
      start_dt = datetime.strptime(args.from_date, '%Y-%m-%d').date()
      end_dt = datetime.strptime(args.to_date, '%Y-%m-%d').date()
     
      target_kaisai_dates = [
        d for d in all_kaisai_dates
        if start_dt <= datetime.strptime(d, '%Y-%m-%d').date() <= end_dt
      ]
      log.info(f"対象開催日数: {len(target_kaisai_dates)}日 (全{len(all_kaisai_dates)}日中)")
    except ValueError as e:
      log.error(f"日付フォーマットエラー: {e}")
      sys.exit(1)
     
    if not target_kaisai_dates:
      log.warning("対象期間の開催日が見つかりません。")
      if conn:
        conn.close()
      return # 修正: finally の外なのでここで return

    # --- 修正: cfg['scraping'] を渡す ---
    race_ids = _requests_utils.scrape_race_id_list(target_kaisai_dates, cfg['scraping'])
    log.info(f"対象レースID数: {len(race_ids)}件")
   
    shutuba_html_paths = []
    for i, race_id in enumerate(race_ids, 1):
      log.info(f"--- レース {i}/{len(race_ids)} ({race_id}) ---")
      if not args.skip_race_html:
        scrape_and_save_html(race_id, "race", cfg, conn)
     
      shutuba_path = scrape_and_save_html(race_id, "shutuba", cfg, conn)
      if shutuba_path:
        shutuba_html_paths.append(shutuba_path)
   
    # --- 4. 出馬表から馬IDを取得 ---
    log.info("出馬表から馬IDを取得...")
    horse_ids = set()
    for html_file in shutuba_html_paths:
      try:
        # 修正: race_id を渡す
        # (ファイル名が {base_name}_{identifier}_{data_version}.{extension} と想定)
        race_id_from_file = Path(html_file).name.split('_')[1]
        df = shutuba_parser.parse_shutuba_html(str(html_file), race_id=race_id_from_file)
        if df is not None and not df.empty and 'horse_id' in df.columns:
          horse_ids.update(df['horse_id'].dropna().unique())
      except Exception as e:
        log.error(f"出馬表パースエラー ({html_file}): {e}", exc_info=True)
   
    log.info(f"{len(horse_ids)}頭のユニークな馬IDを取得しました。")

    # --- 5. 馬関連情報のスクレイピング ---
    log.info("馬関連情報のスクレイピングを開始...")
    for i, horse_id in enumerate(list(horse_ids), 1):
      if horse_id: # 修正: None や空文字を除外
        log.info(f"--- 馬 {i}/{len(horse_ids)} ({horse_id}) ---")
        scrape_and_save_html(horse_id, "horse", cfg, conn)
        scrape_and_save_html(horse_id, "ped", cfg, conn)

    # --- 6. 当日オッズ情報の取得 (仕様書 13.1 では別スクリプト) ---
    fetch_odds_config = cfg.get("scraping", {}).get("fetch_jra_odds_in_pipeline", False)
    if fetch_odds_config:
      log.info("当日バッチ処理（オッズ取得）を開始...")
      for i, race_id in enumerate(race_ids, 1):
        log.info(f"--- オッズ {i}/{len(race_ids)} ({race_id}) ---")
        # --- 修正: cfg (全体) と cfg['default']['raw_data_path'] を渡す ---
        _scrape_jra_odds.scrape_and_save_jra_odds(
          race_id, cfg, conn, cfg['default']['raw_data_path']
        )
      log.info("当日バッチ処理が完了。")
    else:
      log.info("JRAオッズ取得はスキップされました（設定: fetch_jra_odds_in_pipeline=False）。")

  except Exception as e:
    log.error(f"パイプラインの実行中にエラーが発生しました: {e}", exc_info=True)
  finally:
    if conn:
      conn.close()
      log.info("データベース接続をクローズしました。")

  log.info("スクレイピングパイプラインが終了しました。")

if __name__ == "__main__":
  main()
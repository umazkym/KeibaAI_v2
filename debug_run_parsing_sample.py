# keibaai/src/run_parsing_pipeline_local.py をベースにしたデバッグ用スクリプト (v3)
# v3 変更点:
# 1. (v2) インポートパスを修正
# 2. (v3) Parquetの保存を to_parquet() から write_to_dataset() に変更し、
#    data_utils.py が期待する Hive パーティション (year=.../month=...) 形式で書き込む

import logging
import sqlite3
from pathlib import Path
import yaml
from datetime import datetime
import sys
import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq

# プロジェクトルートをsys.pathに追加
# (このスクリプトがプロジェクトルートにある前提)
project_root = Path(__file__).resolve().parent
src_root = project_root / "keibaai" / "src"
sys.path.append(str(src_root))

try:
    import pipeline_core
    from modules.parsers import results_parser, shutuba_parser, horse_info_parser, pedigree_parser
except ImportError as e:
    print(f"エラー: モジュールのインポートに失敗しました: {e}")
    print(f"TRACE: {e.args}")
    print("`keibaai/src` が正しく sys.path に追加されているか確認してください。")
    print("`keibaai/src/modules/parsers/__init__.py` 等のファイルが存在するか確認してください。")
    print("このスクリプトはプロジェクトルート (Keiba_AI_v2) で実行する必要があります。")
    sys.exit(1)

def load_config():
    """設定ファイルをロードする"""
    config_path = project_root / "keibaai" / "configs"
    
    with open(config_path / "default.yaml", "r", encoding="utf-8") as f:
        default_cfg = yaml.safe_load(f)
    
    data_root = project_root / "keibaai" / default_cfg['data_path']
    default_cfg['raw_data_path'] = str(data_root / 'raw')
    default_cfg['parsed_data_path'] = str(data_root / 'parsed')
    default_cfg['database']['path'] = str(data_root / 'metadata' / 'db.sqlite3')
    default_cfg['logging']['log_file'] = str(data_root / 'logs' / '{YYYY}' / '{MM}' / '{DD}' / 'parsing.log')

    config = {"default": default_cfg}
    return config

def setup_logging(log_path_template: str):
    """ログ設定を行う"""
    now = datetime.now()
    log_path = log_path_template.format(
        YYYY=now.strftime('%Y'),
        MM=now.strftime('%m'),
        DD=now.strftime('%d')
    )
    
    pipeline_core.setup_logging(
        log_level="INFO",
        log_file=log_path,
        log_format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s"
    )

def save_dataframe_with_hive_partition(df: pd.DataFrame, base_dir: Path, date_col: str):
    """
    (v3 追加)
    DataFrameを日付カラムに基づいて Hive 形式 (year/month) で保存する
    """
    if df.empty:
        logging.warning(f"DataFrameが空のため、 {base_dir} への書き込みをスキップします。")
        return
        
    if date_col not in df.columns:
        logging.error(f"パーティションキー '{date_col}' が見つかりません。単一ファイルにフォールバックします。")
        df.to_parquet(base_dir / f"{base_dir.name}.parquet", index=False)
        return

    try:
        # pd.to_datetime で date オブジェクトを datetime に変換 (pyarrow互換性のため)
        df['dt_col_for_partition'] = pd.to_datetime(df[date_col])
        df['year'] = df['dt_col_for_partition'].dt.year
        df['month'] = df['dt_col_for_partition'].dt.month
        
        # 不要な一時カラムを削除
        df = df.drop(columns=['dt_col_for_partition'])
        
        table = pa.Table.from_pandas(df)
        
        # Hive形式でパーティション分割して書き込み
        pq.write_to_dataset(
            table,
            root_path=base_dir,
            partition_cols=['year', 'month']
        )
        logging.info(f"Hive形式でParquetを保存しました: {base_dir}")

    except Exception as e:
        logging.error(f"Hive形式でのParquet保存中にエラーが発生: {e}", exc_info=True)


def main():
    log = logging.getLogger(__name__)
    log.info("===== デバッグ パイプライン実行 (v3 - Hive書き込み) 開始 =====")
    
    cfg = load_config()
    setup_logging(cfg["default"]["logging"]["log_file"])

    db_path = cfg["default"]["database"]["path"]
    conn = pipeline_core.get_db_connection(db_path) #

    try:
        # --- 1. レース結果 (results_parser) ---
        log.info("--- 1. レース結果 (results) のパース開始 ---")
        raw_race_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "race"
        parsed_race_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "races" #
        parsed_race_parquet_dir.mkdir(parents=True, exist_ok=True)
        race_html_files = []
        for root, _, files in os.walk(raw_race_html_dir):
            for file in files:
                if file.endswith((".html", ".bin")):
                    race_html_files.append(os.path.join(root, file))
        
        race_html_files = race_html_files[:1000] # サンプル制限
        log.info(f"[デバッグ] {len(race_html_files)}件のレース結果HTMLファイルを処理します。")

        all_races_df = []
        for html_file in race_html_files:
            df = pipeline_core.parse_with_error_handling(str(html_file), "results_parser", results_parser.parse_results_html, conn) #
            if df is not None and not df.empty:
                all_races_df.append(df)
        if all_races_df:
            final_races_df = pd.concat(all_races_df, ignore_index=True)
            # === ▼▼▼ 修正箇所 (v3) ▼▼▼ ===
            save_dataframe_with_hive_partition(final_races_df, parsed_race_parquet_dir, "race_date")
            # === ▲▲▲ 修正箇所 ▲▲▲ ===

        # --- 2. 出馬表 (shutuba_parser) ---
        log.info("--- 2. 出馬表 (shutuba) のパース開始 ---")
        raw_shutuba_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "shutuba"
        parsed_shutuba_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "shutuba" #
        parsed_shutuba_parquet_dir.mkdir(parents=True, exist_ok=True)
        shutuba_html_files = []
        for root, _, files in os.walk(raw_shutuba_html_dir):
            for file in files:
                if file.endswith((".html", ".bin")):
                    shutuba_html_files.append(os.path.join(root, file))

        shutuba_html_files = shutuba_html_files[:1000] # サンプル制限
        log.info(f"[デバッグ] {len(shutuba_html_files)}件の出馬表HTMLファイルを処理します。")

        all_shutuba_df = []
        for html_file in shutuba_html_files:
            df = pipeline_core.parse_with_error_handling(str(html_file), "shutuba_parser", shutuba_parser.parse_shutuba_html, conn) #
            if df is not None and not df.empty:
                all_shutuba_df.append(df)
        if all_shutuba_df:
            final_shutuba_df = pd.concat(all_shutuba_df, ignore_index=True)
            # === ▼▼▼ 修正箇所 (v3) ▼▼▼ ===
            save_dataframe_with_hive_partition(final_shutuba_df, parsed_shutuba_parquet_dir, "race_date")
            # === ▲▲▲ 修正箇所 ▲▲▲ ===

        # --- 3. 馬プロフィール (horse_info_parser) ---
        log.info("--- 3. 馬プロフィール (horse_info) のパース開始 ---")
        raw_horse_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "horse"
        parsed_horse_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "horses" #
        parsed_horse_parquet_dir.mkdir(parents=True, exist_ok=True)
        horse_html_files = []
        for root, _, files in os.walk(raw_horse_html_dir):
            for file in files:
                if file.endswith((".html", ".bin")):
                    horse_html_files.append(os.path.join(root, file))
        
        horse_html_files = horse_html_files[:1000] # サンプル制限
        log.info(f"[デバッグ] {len(horse_html_files)}件の馬プロフィールHTMLファイルを処理します。")

        all_horses_profiles = []
        for html_file in horse_html_files:
            profile_dict = pipeline_core.parse_with_error_handling(str(html_file), "horse_info_parser", horse_info_parser.parse_horse_profile, conn) #
            if profile_dict:
                all_horses_profiles.append(profile_dict)
        if all_horses_profiles:
            final_horses_df = pd.DataFrame(all_horses_profiles)
            # === ▼▼▼ 修正箇所 (v3) ▼▼▼ ===
            # horses.parquet は birth_date でパーティション分割
            save_dataframe_with_hive_partition(final_horses_df, parsed_horse_parquet_dir, "birth_date")
            # === ▲▲▲ 修正箇所 ▲▲▲ ===

        # --- 4. 血統 (pedigree_parser) ---
        log.info("--- 4. 血統 (pedigree) のパース開始 ---")
        raw_ped_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "ped"
        parsed_ped_parquet_dir = Path(cfg["default"]["parsed_data_path"]) / "parquet" / "pedigrees" #
        parsed_ped_parquet_dir.mkdir(parents=True, exist_ok=True)
        ped_html_files = []
        for root, _, files in os.walk(raw_ped_html_dir):
            for file in files:
                if file.endswith((".html", ".bin")):
                    ped_html_files.append(os.path.join(root, file))

        ped_html_files = ped_html_files[:1000] # サンプル制限
        log.info(f"[デバッグ] {len(ped_html_files)}件の血統HTMLファイルを処理します。")

        all_pedigrees_df = []
        for html_file in ped_html_files:
            df = pipeline_core.parse_with_error_handling(str(html_file), "pedigree_parser", pedigree_parser.parse_pedigree_html, conn) #
            if df is not None and not df.empty:
                all_pedigrees_df.append(df)
        if all_pedigrees_df:
            final_pedigrees_df = pd.concat(all_pedigrees_df, ignore_index=True)
            # === ▼▼▼ 修正箇所 (v3) ▼▼▼ ===
            # (pedigree には日付キーがないため、従来通り単一ファイルに保存)
            output_path = parsed_ped_parquet_dir / "pedigrees.parquet"
            final_pedigrees_df.to_parquet(output_path, index=False)
            log.info(f"血統のパース結果をParquetファイルとして保存しました: {output_path}")
            # === ▲▲▲ 修正箇所 ▲▲▲ ===

    except Exception as e:
        log.error(f"パイプラインの実行中にエラーが発生しました: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            log.info("データベース接続をクローズしました。")
    
    log.info("===== デバッグ パイプライン実行 (v3 - Hive書き込み) 正常終了 =====")

if __name__ == "__main__":
    main()
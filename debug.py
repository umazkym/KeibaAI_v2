#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修正版パーサー・データローダー 動作確認用デバッグスクリプト (v3)

(v3 変更なし)
- data_utils.py の v3 修正をテストするためのスクリプト
"""

import os
import shutil
import logging
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
import sqlite3
import traceback

# --- ロギング設定 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s',
    stream=sys.stdout
)

# --- (v3) ダミー 'modules' ディレクトリを強制クリーンアップ ---
PROJECT_ROOT = Path.cwd()
DUMMY_MODULES_DIR = PROJECT_ROOT / 'modules'

def pre_cleanup():
    """
    インポートの前に、CWD (プロジェクトルート) に存在する可能性のある
    v1 が作成したダミー 'modules' ディレクトリを削除する。
    """
    try:
        if DUMMY_MODULES_DIR.is_dir():
            logging.warning(f"競合の可能性があるダミーディレクトリを削除します: {DUMMY_MODULES_DIR}")
            shutil.rmtree(DUMMY_MODULES_DIR)
            logging.info("ダミーディレクトリの削除完了。")
    except Exception as e:
        logging.error(f"ダミーディレクトリのクリーンアップに失敗: {e}")
        logging.error("テストを続行しますが、インポートが失敗する可能性があります。")

# --- インポート実行 ---
# 1. (v3) 競合する可能性のあるダミーを先に削除
pre_cleanup()

# 2. (v2) 'keibaai/src' を sys.path に追加
SRC_DIR = PROJECT_ROOT / 'keibaai' / 'src'
if not SRC_DIR.exists():
    logging.error(f"エラー: `keibaai/src` ディレクトリが見つかりません。")
    logging.error(f"想定パス: {SRC_DIR}")
    logging.error("このスクリプトはプロジェクトルート (Keiba_AI_v2) で実行してください。")
    sys.exit(1)

sys.path.insert(0, str(SRC_DIR))
logging.info(f"`keibaai/src` を sys.path に追加しました: {SRC_DIR}")

# 3. モジュールをインポート
try:
    logging.info("修正版モジュールをインポート中...")
    
    # keibaai/src/modules/parsers/horse_info_parser.py
    from modules.parsers.horse_info_parser import parse_horse_profile, parse_horse_performance
    
    # keibaai/src/modules/parsers/results_parser.py
    from modules.parsers.results_parser import parse_results_html
    
    # keibaai/src/utils/data_utils.py
    from utils.data_utils import load_parquet_data_by_date
    
    # keibaai/src/pipeline_core.py
    from pipeline_core import parse_with_error_handling, get_db_connection

    # keibaai/src/modules/parsers/common_utils.py (実ファイル)
    import modules.parsers.common_utils

    logging.info("モジュールのインポート成功。")

except ImportError as e:
    logging.error(f"モジュールのインポートに失敗しました: {e}")
    logging.error(traceback.format_exc())
    logging.error("`keibaai/src` 以下のファイル構成が正しいか、")
    logging.error("`__init__.py` ファイルが各ディレクトリ (modules, modules/parsers, utils) に存在するか確認してください。")
    sys.exit(1)
except Exception as e:
    logging.error(f"インポート中に予期せぬエラーが発生しました: {e}")
    logging.error(traceback.format_exc())
    sys.exit(1)


# --- テスト用セットアップ ---
# (プロジェクトルート直下に debug_workspace を作成)
WORKSPACE_DIR = PROJECT_ROOT / "debug_workspace"
HTML_DIR = WORKSPACE_DIR / "html"
PARQUET_DIR = WORKSPACE_DIR / "parquet"
DB_PATH = WORKSPACE_DIR / "debug_db.sqlite3"

# --- ダミーHTMLコンテンツ ---
# 1. 馬プロフィール (成功ケース)
horse_ok_html = """
<html><body>
    <div class="horse_title"><h1>テストホース</h1></div>
    <table class="db_prof_table">
        <tr><th>生年月日</th><td>2020年3月15日</td></tr>
        <tr><th>性別</th><td>牡</td></tr>
    </table>
    <table class="blood_table">
        </table>
</body></html>
"""

# 2. 馬プロフィール (失敗ケース: db_prof_table がない)
horse_ng_html = """
<html><body>
    <div class="horse_title"><h1>データ欠損ホース</h1></div>
    </body></html>
"""

# 3. レース結果 (成功ケース)
results_ok_html = """
<html><body>
    <p class="smalltxt">2023年05月14日 2回東京8日目 11R</p>
    <table class="race_table_01">
        <tbody>
            <tr>
                <td>1</td><td>1</td><td>1</td>
                <td><a href="/horse/2020100001">イクイノックス</a></td>
                <td>牡3</td><td>57.0</td>
                <td><a href="/jockey/00001">ルメール</a></td>
                <td>1:58.5</td><td>-</td><td></td>
                <td>2-2-2</td><td>34.5</td>
                <td>1.5</td><td>1</td><td>480(0)</td>
                <td><a href="/trainer/0001">木村</a></td>
                <td>シルクＲ</td><td>10000.0</td>
            </tr>
            <tr>
                <td>2</td><td>2</td><td>2</td>
                <td><a href="/horse/2020100002">ドウデュース</a></td>
                <td>牡3</td><td>57.0</td>
                <td><a href="/jockey/00002">武豊</a></td>
                <td>1:58.6</td><td>1/2</td><td></td>
                <td>3-3-3</td><td>34.6</td>
                <td>3.0</td><td>2</td><td>500(+2)</td>
                <td><a href="/trainer/0002">友道</a></td>
                <td>キーファーズ</td><td>4000.0</td>
            </tr>
        </tbody>
    </table>
</body></html>
"""

def setup_workspace():
    """テスト用のディレクトリとダミーファイルを作成"""
    logging.info(f"ワークスペースを作成中: {WORKSPACE_DIR}")
    if WORKSPACE_DIR.exists():
        shutil.rmtree(WORKSPACE_DIR)
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # ダミーHTMLファイルを作成 (euc_jp でエンコード)
    try:
        (HTML_DIR / "horse_ok.html").write_bytes(horse_ok_html.encode('euc_jp'))
        (HTML_DIR / "horse_ng.html").write_bytes(horse_ng_html.encode('euc_jp'))
        (HTML_DIR / "results_ok.html").write_bytes(results_ok_html.encode('euc_jp'))
    except Exception as e:
        logging.error(f"ダミーHTMLのエンコード/書き込み失敗: {e}")
        logging.warning("テストを続行しますが、パースが失敗する可能性があります。")
        (HTML_DIR / "horse_ok.html").write_text(horse_ok_html, encoding='utf-8')
        (HTML_DIR / "horse_ng.html").write_text(horse_ng_html, encoding='utf-8')
        (HTML_DIR / "results_ok.html").write_text(results_ok_html, encoding='utf-8')


    # ダミーParquetファイルを作成 (data_utils.py テスト用)
    logging.info(f"ダミーParquetを作成中: {PARQUET_DIR}")
    df_parquet = pd.DataFrame({
        'race_id': [f'20230101{i:02d}' for i in range(1, 6)],
        'race_date': [
            date(2023, 10, 1),
            date(2023, 10, 1),
            date(2023, 11, 5),
            date(2023, 12, 20),
            date(2023, 12, 25),
        ],
        'value': [100, 200, 300, 400, 500]
    })
    
    # Hiveパーティショニングのために 'year' と 'month' カラムを追加
    df_parquet['year'] = pd.to_datetime(df_parquet['race_date']).dt.year
    df_parquet['month'] = pd.to_datetime(df_parquet['race_date']).dt.month

    table = pa.Table.from_pandas(df_parquet)
    
    # Hive形式でパーティション分割して書き込み
    pq.write_to_dataset(
        table,
        root_path=PARQUET_DIR,
        partition_cols=['year', 'month']
    )
    
    logging.info("ワークスペースの準備完了。")

def cleanup_workspace():
    """テスト用のディレクトリとファイルを削除"""
    logging.info(f"ワークスペースをクリーンアップ中: {WORKSPACE_DIR}")
    if WORKSPACE_DIR.exists():
        shutil.rmtree(WORKSPACE_DIR)
    
    # (v3) v1 が CWD に作成したダミー 'modules' も（残っていれば）削除
    if DUMMY_MODULES_DIR.exists():
        logging.warning(f"CWDのダミーディレクトリをクリーンアップ: {DUMMY_MODULES_DIR}")
        shutil.rmtree(DUMMY_MODULES_DIR)

    logging.info("クリーンアップ完了。")

# === テスト実行 ===

def test_horse_info_parser():
    """ 1. horse_info_parser.py のテスト """
    logging.info("--- 1. test_horse_info_parser 開始 ---")
    test_passed = True
    
    # (A) 成功ケース
    try:
        ok_path = str(HTML_DIR / "horse_ok.html")
        profile = parse_horse_profile(ok_path, "2020100001")
        birth_date_val = profile.get('birth_date')
        
        if isinstance(birth_date_val, date):
            logging.info(f" [成功] (A) 正常なHTMLのパースに成功。birth_date の型: {type(birth_date_val)}")
            if birth_date_val != date(2020, 3, 15):
                logging.error(f" [失敗] (A) 日付の値が不正です。期待値: {date(2020, 3, 15)}, 実際: {birth_date_val}")
                test_passed = False
        else:
            logging.error(f" [失敗] (A) birth_date が date オブジェクトではありません。型: {type(birth_date_val)}")
            test_passed = False
            
    except Exception as e:
        logging.error(f" [失敗] (A) 正常なHTMLのパースで予期せぬ例外が発生: {e}\n{traceback.format_exc()}")
        test_passed = False

    # (B) 失敗ケース (サイレント障害の防止)
    conn = None
    try:
        conn = get_db_connection(str(DB_PATH))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS parse_failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parser_name TEXT,
                source_file TEXT,
                error_type TEXT,
                error_message TEXT,
                stack_trace TEXT,
                failed_ts TEXT
            )
        """)
        
        ng_path = str(HTML_DIR / "horse_ng.html")
        
        result = parse_with_error_handling(
            file_path=ng_path,
            parser_name="test_horse_profile_ng",
            parse_func=lambda p: parse_horse_profile(p, "2020100002"),
            db_conn=conn
        )
        
        if result is not None:
            logging.error(f" [失敗] (B) サイレント障害テスト失敗。パーサーが None を返しませんでした。 result: {result}")
            test_passed = False
        else:
            logging.info(" [成功] (B) サイレント障害テスト: パーサーが None を返しました。")
            
        cursor = conn.cursor()
        cursor.execute("SELECT error_type, error_message FROM parse_failures WHERE source_file = ?", (ng_path,))
        failure = cursor.fetchone()
        
        if failure and failure[0] == 'ValueError':
            logging.info(f" [成功] (B) DBに 'ValueError' が正しく記録されました。")
            logging.info(f"     エラー内容: {failure[1][:80]}...")
        elif failure:
            logging.error(f" [失敗] (B) DBに記録されたエラー型が不正です。期待値: 'ValueError', 実際: '{failure[0]}'")
            test_passed = False
        else:
            logging.error(f" [失敗] (B) DBにエラーが記録されていません。")
            test_passed = False

    except Exception as e:
        logging.error(f" [失敗] (B) 失敗ケースのテスト中に予期せぬ例外が発生: {e}\n{traceback.format_exc()}")
        test_passed = False
    finally:
        if conn:
            conn.close()
            
    logging.info(f"--- 1. test_horse_info_parser 終了 ({'成功' if test_passed else '失敗'}) ---")
    return test_passed


def test_results_parser():
    """ 2. results_parser.py のテスト """
    logging.info("--- 2. test_results_parser 開始 ---")
    test_passed = True
    
    try:
        ok_path = str(HTML_DIR / "results_ok.html")
        df = parse_results_html(ok_path, "202305020811")
        
        if df.empty or 'race_date' not in df.columns:
            logging.error(f" [失敗] パース結果が空か、'race_date' カラムが存在しません。")
            test_passed = False
            return False

        if df['race_date'].dtype != 'object':
            logging.warning(f" [警告] race_date カラムの dtype が 'object' ではありません (実際: {df['race_date'].dtype})。")

        first_date_val = df['race_date'].iloc[0]
        if isinstance(first_date_val, date):
            logging.info(f" [成功] 'race_date' カラムの要素は date オブジェクトです。型: {type(first_date_val)}")
            if first_date_val != date(2023, 5, 14):
                logging.error(f" [失敗] 日付の値が不正です。期待値: {date(2023, 5, 14)}, 実際: {first_date_val}")
                test_passed = False
        else:
            logging.error(f" [失敗] 'race_date' カラムの要素が date オブジェクトではありません。型: {type(first_date_val)}")
            test_passed = False
            
    except Exception as e:
        logging.error(f" [失敗] results_parser のテスト中に予期せぬ例外が発生: {e}\n{traceback.format_exc()}")
        test_passed = False

    logging.info(f"--- 2. test_results_parser 終了 ({'成功' if test_passed else '失敗'}) ---")
    return test_passed


def test_data_utils_loader():
    """ 3. data_utils.py のテスト """
    logging.info("--- 3. test_data_utils_loader 開始 ---")
    test_passed = True
    
    test_cases = [
        # (テスト名, start_dt, end_dt, 期待される行数)
        ("全期間 (None)", None, None, 5),
        ("2023-11-01 以降", datetime(2023, 11, 1), None, 3),
        ("2023-11-30 以前", None, datetime(2023, 11, 30), 3),
        ("2023-10-01 のみ", datetime(2023, 10, 1), datetime(2023, 10, 1), 2),
        ("2023-11-01 ～ 2023-12-20", datetime(2023, 11, 1), datetime(2023, 12, 20), 2),
        ("範囲外 (未来)", datetime(2024, 1, 1), None, 0),
        ("範囲外 (過去)", None, datetime(2023, 1, 1), 0),
    ]

    for name, start_dt, end_dt, expected_rows in test_cases:
        try:
            df = load_parquet_data_by_date(
                base_dir=PARQUET_DIR,
                start_dt=start_dt,
                end_dt=end_dt,
                date_col='race_date'
            )
            
            if len(df) == expected_rows:
                logging.info(f" [成功] ({name}) 期待通りの行数を取得: {len(df)}")
            else:
                logging.error(f" [失敗] ({name}) 行数が異なります。期待値: {expected_rows}, 実際: {len(df)}")
                test_passed = False
        except Exception as e:
            logging.error(f" [失敗] ({name}) data_utils のテスト中に予期せぬ例外が発生: {e}\n{traceback.format_exc()}")
            test_passed = False

    logging.info(f"--- 3. test_data_utils_loader 終了 ({'成功' if test_passed else '失敗'}) ---")
    return test_passed


def main():
    try:
        setup_workspace()
        
        results = []
        results.append(test_horse_info_parser())
        results.append(test_results_parser())
        results.append(test_data_utils_loader())
        
        logging.info("="*50)
        if all(results):
            logging.info("🎉 全てのデバッグテストが成功しました。")
        else:
            logging.error(f"🔥 一部のデバッグテストが失敗しました。 (成功: {sum(results)}/{len(results)})")
            
    except Exception as e:
        logging.error(f"デバッグスクリプトの実行中に致命的なエラーが発生: {e}")
        logging.error(traceback.format_exc())
        
    finally:
        cleanup_workspace()

if __name__ == "__main__":
    main()
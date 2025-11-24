#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
再生成された Parquet ファイルの品質検証スクリプト (validate_new_parquet.py)

v3 修正点:
1. (v2) `datetime` をインポート
2. (v2) 'sex' カラム存在チェックを追加
3. (v3) Test 4 の成功判定を修正 (エラーで None が返された場合を「失敗」とする)
"""

import sys
import logging
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# --- ロギング設定 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s',
    stream=sys.stdout
)

# --- パス設定 ---
PROJECT_ROOT = Path.cwd()
SRC_DIR = PROJECT_ROOT / 'keibaai' / 'src'
PARQUET_DIR = PROJECT_ROOT / 'keibaai' / 'data' / 'parsed' / 'parquet'

# --- (1) sys.path に 'keibaai/src' を追加 ---
if not SRC_DIR.exists():
    logging.error(f"エラー: `keibaai/src` ディレクトリが見つかりません: {SRC_DIR}")
    sys.exit(1)
sys.path.insert(0, str(SRC_DIR))
logging.info(f"`keibaai/src` を sys.path に追加しました: {SRC_DIR}")

# --- (2) data_utils をインポート ---
try:
    from utils.data_utils import load_parquet_data_by_date
    logging.info("utils.data_utils のインポート成功。")
except ImportError as e:
    logging.error(f"モジュールのインポートに失敗しました: {e}")
    logging.error("`keibaai/src/utils/__init__.py` が存在するか確認してください。")
    sys.exit(1)

def validate_parquet_quality():
    """
    再生成されたParquetファイルの品質を検証する
    """
    logging.info("--- Parquet 品質検証 開始 ---")
    all_tests_passed = True
    
    # === 1. races.parquet の検証 ===
    races_dir = PARQUET_DIR / "races" #
    if not races_dir.exists() or not any(races_dir.iterdir()):
        logging.warning(f"[警告] races パーティションディレクトリが見つからないか空です: {races_dir}")
        logging.warning("races の検証をスキップします。")
    else:
        try:
            logging.info(f"スキーマ読み込み中: {races_dir}")
            # (v3) Hive形式のディレクトリからスキーマを読み取る
            races_schema = pq.read_schema(next(races_dir.rglob("*.parquet")))
            race_date_type = races_schema.field('race_date').type
            
            if race_date_type == pa.date32():
                logging.info(f" [成功] (Test 1) `races` の 'race_date' は期待通りの `date32` 型です。")
            else:
                logging.error(f" [失敗] (Test 1) `races` の 'race_date' の型が不正です。")
                all_tests_passed = False
                
        except Exception as e:
            logging.error(f" [失敗] (Test 1) `races` の読み込み/検証中にエラー: {e}", exc_info=True)
            all_tests_passed = False

    # === 2. horses.parquet の検証 ===
    horses_dir = PARQUET_DIR / "horses" #
    if not horses_dir.exists() or not any(horses_dir.iterdir()):
        logging.warning(f"[警告] horses パーティションディレクトリが見つからないか空です: {horses_dir}")
        logging.warning("horses の検証をスキップします。")
    else:
        try:
            logging.info(f"読み込み中: {horses_dir}")
            # (v3) Hive形式のディレクトリからDFを読み込む
            df_horses = pd.read_parquet(horses_dir)
            total_rows = len(df_horses)
            logging.info(f"`horses` を読み込み完了 (総行数: {total_rows})")

            # (Test 2) 日付の型を検証
            horses_schema = pq.read_schema(next(horses_dir.rglob("*.parquet")))
            birth_date_pa_type = horses_schema.field('birth_date').type

            if birth_date_pa_type == pa.date32():
                logging.info(f" [成功] (Test 2) `horses` の 'birth_date' は期待通りの `date32` 型です。")
            else:
                logging.error(f" [失敗] (Test 2) `horses` の 'birth_date' の型が不正です。")
                all_tests_passed = False

            # (Test 3) サイレント障害（NULL行）の検証
            if 'sex' not in df_horses.columns:
                logging.warning(f" [警告] (Test 3) `horses` に 'sex' カラムが生成されていません。")
            else:
                null_sex_count = df_horses['sex'].isnull().sum()
                if null_sex_count > 0:
                    logging.warning(f" [警告] (Test 3) `horses` に 'sex' がNULLの行が {null_sex_count} 件見つかりました。")
                else:
                    logging.info(f" [成功] (Test 3) `horses` に 'sex' がNULLの行はありませんでした。")

        except KeyError as e:
            logging.error(f" [失敗] (Test 2/3) `horses` の読み込み/検証中にキーエラー: {e}")
            all_tests_passed = False
        except Exception as e:
            logging.error(f" [失敗] (Test 2/3) `horses` の読み込み/検証中に予期せぬエラー: {e}", exc_info=True)
            all_tests_passed = False
            
    # === 4. data_utils.py の日付フィルタ検証 ===
    if races_dir.exists():
        try:
            logging.info("--- (Test 4) data_utils.py の日付フィルタ検証 開始 ---")
            
            df_filtered = load_parquet_data_by_date(
                base_dir=races_dir, #
                start_dt=datetime(2023, 1, 1),
                end_dt=datetime(2023, 12, 31),
                date_col='race_date'
            )
            
            # === ▼▼▼ 修正箇所 (v3) ▼▼▼ ===
            # (エラー時に None が返ることを正しくチェックする)
            if df_filtered is not None:
                logging.info(f" [成功] (Test 4) `load_parquet_data_by_date` が正常に実行されました (フィルタ後: {len(df_filtered)}行)。")
            else:
                # data_utils.py がエラーで None を返した場合
                logging.error(f" [失敗] (Test 4) `load_parquet_data_by_date` が None を返しました。(エラーが発生しました)")
                all_tests_passed = False
            # === ▲▲▲ 修正箇所 ▲▲▲ ===
                
        except Exception as e:
            logging.error(f" [失敗] (Test 4) `load_parquet_data_by_date` の実行中にエラー: {e}", exc_info=True)
            all_tests_passed = False
            
    # === 結果まとめ ===
    logging.info("="*50)
    if all_tests_passed:
        logging.info("🎉 全ての品質検証テストが成功しました。")
    else:
        logging.error("🔥 一部の品質検証テストが失敗しました。ログを確認してください。")

if __name__ == "__main__":
    validate_parquet_quality()
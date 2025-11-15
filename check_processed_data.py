import pandas as pd
from pathlib import Path
import logging
import sys

# --- 準備 ---
try:
    import pyarrow
except ImportError:
    print("=" * 70, file=sys.stderr)
    print(" エラー: 'pyarrow' がインストールされていません。", file=sys.stderr)
    print(" ターミナルで pip install pyarrow を実行してください。", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    sys.exit(1)

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 設定 ---
BASE_DIR = Path.cwd() 
PARSED_DIR = BASE_DIR / "keibaai" / "data" / "parsed" / "parquet"

# ★★★ 修正点 ★★★
# pipeline_core.py の仕様に基づき、単一ファイルパスとディレクトリパスをマッピング
DATASETS_TO_CHECK_MAP = {
    # 'key' : 'path_relative_to_parsed_dir'
    "race": Path("races/races.parquet"),
    "horse_info": Path("horses/horses.parquet"),
    "pedigree": Path("pedigrees/pedigrees.parquet"),
    "shutuba": Path("shutuba") # shutuba のみパーティション分割ディレクトリ
}
# ★★★ 修正ここまで ★★★


logger.info(f"プロジェクトベースディレクトリ: {BASE_DIR}")
logger.info(f"確認対象ディレクトリ: {PARSED_DIR}")
print("-" * 70)

all_success = True
found_any_data = False

# --- デバッグ実行 ---
for dataset_name, relative_path in DATASETS_TO_CHECK_MAP.items():
    logger.info(f"【{dataset_name}】の確認を開始...")
    
    full_path = PARSED_DIR / relative_path
    
    # 1. パス(ファイルまたはディレクトリ)の存在確認
    if not full_path.exists():
        logger.warning(f"  [結果] ❌ FAILED: パスが見つかりません: {full_path}")
        all_success = False
        print("-" * 70)
        continue

    # 3. データの読み込みと内容確認
    try:
        df = pd.DataFrame()
        if full_path.is_dir():
            # 'shutuba' のようなパーティション分割ディレクトリの場合
            parquet_files = list(full_path.glob("**/*.parquet"))
            if not parquet_files:
                 logger.warning(f"  [結果] ❌ FAILED: ディレクトリ内に .parquet ファイルが見つかりません: {full_path}")
                 all_success = False
                 print("-" * 70)
                 continue
            
            logger.info(f"  [情報] {len(parquet_files)} 個の Parquet ファイル（パーティション）を発見しました。")
            df = pd.read_parquet(full_path)
        else:
            # 'race', 'horse_info', 'pedigree' のような単一ファイルの場合
            logger.info(f"  [情報] 単一の Parquet ファイルを発見しました: {full_path}")
            df = pd.read_parquet(full_path)
        
        # --- 読み込み後の共通チェック ---
        if df.empty:
            logger.warning(f"  [結果] ⚠️ WARNING: データは読み込めましたが、中身が空です (0行)。")
            logger.warning("          (注: 1000件のデバッグ実行では、対象データが含まれない場合もあります)")
        else:
            logger.info(f"  [結果] ✅ SUCCESS: 正常に読み込み完了。")
            logger.info(f"  [情報] 総行数: {len(df)}")
            logger.info(f"  [情報] カラム数: {len(df.columns)}")
            print("--- 先頭5行 (head) ---")
            pd.set_option('display.max_columns', None) # 全てのカラムを表示
            print(df.head())
            print("." * 20)
            found_any_data = True

    except Exception as e:
        logger.error(f"  [結果] ❌ FAILED: Parquet の読み込み中にエラーが発生しました。")
        logger.error(f"  パス: {full_path}")
        logger.error(f"  エラー内容: {e}")
        all_success = False

    print("-" * 70)

# --- 総合結果 ---
if all_success and found_any_data:
    logger.info("🎉 総合結果: すべてのデータセットを正常に読み込め、データが存在することを確認しました。")
elif all_success and not found_any_data:
    logger.warning("🤔 総合結果: スクリプトは正常に動作しましたが、全データセットが空でした。")
    logger.warning("          debug_run_parsing_pipeline_local_1000.py の対象ファイル(1000件)に処理対象が含まれていなかった可能性があります。")
else:
    logger.error("🔥 総合結果: いくつかのデータセットの確認に失敗しました。上記ログを確認してください。")
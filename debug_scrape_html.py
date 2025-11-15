#!/usr/bin/env python3
# debug_scrape_html_v2.py (修正版)

"""
_scrape_html.py の各関数を個別にデバッグ（動作検証）するためのスクリプト。
最小限のデータ（1日分、1レース、1頭）を取得し、各ステップが機能するかを確認する。

【実行方法】
1. このスクリプトをプロジェクトルート（'keibaai'フォルダと同じ階層）に置く。
2. プロジェクトルートから実行する:
   python debug_scrape_html.py
"""

import logging
import sys
from pathlib import Path
import shutil

# --- プロジェクトルートをsys.pathに追加 ---
# このスクリプトはプロジェクトルート (keibaai/ の親ディレクトリ) に置くことを想定
try:
    project_root = Path(__file__).resolve().parent
    keibaai_root = project_root / "keibaai"
    keibaai_src_root = keibaai_root / "src"

    if not keibaai_src_root.exists():
        raise FileNotFoundError(f"keibaai/src ディレクトリが見つかりません: {keibaai_src_root}")

    # 'keibaai' を追加 (例: from src.modules... のため)
    sys.path.append(str(keibaai_root))

    # インポートのテスト
    # run_scraping_pipeline_local.py に倣う
    from src.modules.preparing import _scrape_html
    from src.utils import data_utils
    
    # ★★★ 修正点 ★★★
    # _scrape_html がインポートしているものと同じ LocalPaths を直接インポートする
    # (sys.path に keibaai が追加されているため、 'src.modules' からインポートできる)
    from src.modules.constants import LocalPaths

except ImportError as e:
    print(f"モジュールのインポートに失敗しました: {e}")
    print("プロジェクトのインポート構造（PYTHONPATH）が正しいか確認してください。")
    print("または、lxml, selenium, webdriver-manager などのライブラリが不足している可能性があります。")
    print(f"現在の sys.path: {sys.path}")
    sys.exit(1)
except FileNotFoundError as e:
    print(e)
    print("このスクリプトはプロジェクトルート（'keibaai'フォルダと同じ階層）に置いて実行してください。")
    sys.exit(1)


def setup_debug_logging():
    """デバッグ用の簡易ロギング設定"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )
    # Seleniumのログを抑制
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('webdriver_manager').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

def clear_debug_cache(test_race_id, test_horse_id):
    """検証用のキャッシュを削除する"""
    log = logging.getLogger(__name__)
    
    # LocalPaths を直接使用 (インポート済み)
    paths_to_clear = [
        Path(LocalPaths.HTML_RACE_DIR) / f"{test_race_id}.bin",
        Path(LocalPaths.HTML_SHUTUBA_DIR) / f"{test_race_id}.bin",
        Path(LocalPaths.HTML_HORSE_DIR) / f"{test_horse_id}_profile.bin",
        Path(LocalPaths.HTML_HORSE_DIR) / f"{test_horse_id}_perf.bin",
        Path(LocalPaths.HTML_PED_DIR) / f"{test_horse_id}.bin",
    ]
    
    log.info("--- 検証用キャッシュの削除開始 ---")
    for path in paths_to_clear:
        # スクリプト実行（プロジェクトルート）からの相対パスとして扱う
        if path.exists():
            try:
                path.unlink()
                log.info(f"削除しました: {path}")
            except OSError as e:
                log.warning(f"削除失敗: {path} - {e}")
        else:
            log.info(f"存在しません (スキップ): {path}")
            
        # ディレクトリが存在しない場合のエラーチェック
        if not path.parent.exists():
            log.warning(f"キャッシュディレクトリが存在しません: {path.parent}")
            # _scrape_html.py が実行時に作成するので、ここでは警告のみ
            
    log.info("--- 検証用キャッシュの削除完了 ---")


def main():
    """各スクレイピング関数を順に実行して検証する"""
    setup_debug_logging()
    log = logging.getLogger(__name__)

    # --- 検証用パラメータ ---
    TEST_FROM_DATE = "2023-01-05"
    TEST_TO_DATE = "2023-01-05"
    # サンプル馬ID (ディープインパクト)
    TEST_HORSE_ID = "2002100816" 
    
    test_race_id = None
    
    try:
        # === ステップ1: 開催日取得 ===
        log.info("--- 【ステップ1】開催日取得 (scrape_kaisai_date) 検証 ---")
        kaisai_dates = _scrape_html.scrape_kaisai_date(TEST_FROM_DATE, TEST_TO_DATE)
        if not kaisai_dates:
            log.error("ステップ1: 開催日の取得に失敗しました。")
            return
        log.info(f"ステップ1: 取得した開催日: {kaisai_dates}")

        # === ステップ2: レースID取得 ===
        log.info("--- 【ステップ2】レースID取得 (scrape_race_id_list) 検証 ---")
        race_ids = _scrape_html.scrape_race_id_list(kaisai_dates)
        if not race_ids:
            log.error("ステップ2: レースIDの取得に失敗しました。")
            return
        
        # 2023-01-05 の最初のレースIDが使われるはず
        test_race_id = race_ids[0] 
        log.info(f"ステップ2: 取得した全レースID数: {len(race_ids)}")
        log.info(f"ステップ2: 検証に使用するレースID: {test_race_id}")

        # --- 検証前にキャッシュをクリア ---
        clear_debug_cache(test_race_id, TEST_HORSE_ID)

        # === ステップ3: レース結果HTML取得 ===
        log.info("--- 【ステップ3】レース結果HTML取得 (scrape_html_race) 検証 ---")
        # skip=False で強制的に取得
        race_paths = _scrape_html.scrape_html_race([test_race_id], skip=False) 
        if not race_paths:
            log.error("ステップ3: レース結果HTMLの取得に失敗しました。")
            return
        log.info(f"ステップ3: 保存したファイルパス: {race_paths}")

        # === ステップ4: 出馬表HTML取得 ===
        log.info("--- 【ステップ4】出馬表HTML取得 (scrape_html_shutuba) 検証 ---")
        # skip=False で強制的に取得
        shutuba_paths = _scrape_html.scrape_html_shutuba([test_race_id], skip=False)
        if not shutuba_paths:
            log.error("ステップ4: 出馬表HTMLの取得に失敗しました。")
            return
        log.info(f"ステップ4: 保存したファイルパス: {shutuba_paths}")

        # === ステップ5: 馬ID抽出 ===
        log.info("--- 【ステップ5】馬ID抽出 (extract_horse_ids_from_html) 検証 ---")
        
        # LocalPaths を直接使用 (インポート済み)
        # 実行場所（プロジェクトルート）からの相対パス
        raw_race_dir_path = Path(LocalPaths.HTML_RACE_DIR)

        if not raw_race_dir_path.exists():
            log.error(f"ステップ5: 馬ID抽出対象のディレクトリが見つかりません: {raw_race_dir_path}")
            log.error("ステップ3が正常に完了し、ファイルが保存されているか確認してください。")
            return

        horse_ids = _scrape_html.extract_horse_ids_from_html(str(raw_race_dir_path))
        if not horse_ids:
            log.warning(f"ステップ5: 馬IDの抽出ができませんでした（{raw_race_dir_path}）")
            # ステップ3で取得したレースIDに関連する馬がいない場合もあるため、処理は続行
        else:
            log.info(f"ステップ5: {len(horse_ids)} 頭の馬IDを抽出しました。")

        # === ステップ6: 馬情報HTML取得 (プロフィール & 成績AJAX) ===
        log.info("--- 【ステップ6】馬情報HTML取得 (scrape_html_horse) 検証 ---")
        log.info(f"（テスト用ID: {TEST_HORSE_ID} を使用します）")
        # skip=False で強制的に取得
        horse_paths = _scrape_html.scrape_html_horse([TEST_HORSE_ID], skip=False)
        if not horse_paths or len(horse_paths) < 2:
            log.error(f"ステップ6: 馬情報（プロフィール・成績）の取得に失敗しました。{horse_paths}")
            return
        log.info(f"ステップ6: 保存したファイルパス: {horse_paths}")
        log.info("（_profile.bin と _perf.bin の2ファイルが保存されていれば成功です）")

        # === ステップ7: 血統HTML取得 (Selenium) ===
        log.info("--- 【ステップ7】血統HTML取得 (scrape_html_ped) 検証 ---")
        log.info(f"（テスト用ID: {TEST_HORSE_ID} を使用します）")
        # skip=False で強制的に取得
        ped_paths = _scrape_html.scrape_html_ped([TEST_HORSE_ID], skip=False)
        if not ped_paths:
            log.error("ステップ7: 血統HTMLの取得に失敗しました。")
            return
        log.info(f"ステップ7: 保存したファイルパス: {ped_paths}")

        log.info("--- 全てのデバッグステップが完了しました ---")

    except Exception as e:
        log.error(f"デバッグスクリプトの実行中に予期せぬエラーが発生しました: {e}", exc_info=True)
    finally:
        log.info("デバッグスクリプト終了")

if __name__ == "__main__":
    main()
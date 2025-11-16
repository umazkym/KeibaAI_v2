import os
import glob
import pandas as pd
from datetime import datetime

# --- 定数定義 ---
# このスクリプトはプロジェクトのルートディレクトリ(例: Keiba_AI_v2)から
# `python keibaai/tool/cache_manager.py` のように実行されることを想定しています。

# 生HTMLデータ(.bin)が保存されているディレクトリ
RAW_HTML_DIR = os.path.join('keibaai', 'data', 'raw', 'html')
# 競走馬データのキャッシュログ（どの馬のデータをいつ取得したか）
HORSE_CACHE_LOG_PATH = os.path.join('keibaai', 'data', 'master', 'horse_cache_log.csv')

# --- 機能1: .binファイルのタイムスタンプを更新 ---
def update_bin_timestamps():
    """
    keibaai/data/raw/html/ 以下のすべての.binファイル（キャッシュファイル）の
    最終アクセス日時と最終更新日時を現在時刻に更新します。
    """
    print("--- 1. .binファイルのタイムスタンプ更新 ---")
    target_dir = RAW_HTML_DIR
    
    if not os.path.isdir(target_dir):
        print(f"エラー: ディレクトリが見つかりません: '{target_dir}'")
        print("--- 処理中断 ---")
        return

    print(f"{target_dir} 以下の.binファイルを再帰的に検索中...")
    # recursive=True でサブディレクトリもすべて検索します
    bin_files = glob.glob(os.path.join(target_dir, '**', '*.bin'), recursive=True)

    if not bin_files:
        print(".binファイルが見つかりませんでした。")
        print("--- 完了 ---")
        return

    count = 0
    for file_path in bin_files:
        try:
            # os.utime(path, None) でタイムスタンプを現在時刻に設定します
            os.utime(file_path, None)
            count += 1
        except Exception as e:
            print(f"エラー: {file_path} のタイムスタンプ更新に失敗しました: {e}")

    print(f"正常に {count}個のファイルのタイムスタンプを更新しました。")
    print("--- 完了 ---")


# --- 機能2: horse_cache_log.csv の全タイムスタンプを更新 ---
def update_all_log_timestamps():
    """
    horse_cache_log.csv を読み込み、すべてのレコードの 'last_updated' 列
    （最終更新日時）を現在時刻に更新して上書き保存します。
    """
    print("--- 2. horse_cache_log.csv の全タイムスタンプを更新 ---")
    file_path = HORSE_CACHE_LOG_PATH

    if not os.path.exists(file_path):
        print(f"エラー: ファイルが見つかりません: {file_path}")
        print("--- 処理中断 ---")
        return

    try:
        try:
            # まずUTF-8での読み込みを試みます
            df = pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            # UTF-8で失敗した場合、Shift_JIS (古いWindows環境など) で読み込みます
            print("UTF-8での読み込みに失敗。Shift_JISで再試行します。")
            df = pd.read_csv(file_path, encoding='shift_jis')
        except pd.errors.EmptyDataError:
            print(f"ファイルは空です: {file_path}")
            print("--- 完了 ---")
            return

        print(f"{file_path} から {len(df)} 件のレコードを読み込みました。")

        # 現在時刻を取得し、指定のフォーマット（マイクロ秒まで）の文字列に変換
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # 'last_updated' 列のすべての値を現在の時刻文字列で上書き
        df['last_updated'] = now_str

        # UTF-8でCSVファイルに上書き保存（インデックスは保存しない）
        df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"'last_updated'列の全レコードを {now_str} に正常に更新しました。")

    except ImportError:
        print("エラー: 'pandas' ライブラリが見つかりません。")
        print("'pip install pandas' コマンドでインストールしてください。")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
    
    print("--- 完了 ---")


# --- 機能3: horse_cache_log.csv を再生成 ---
def regenerate_horse_cache_log_from_bins():
    """
    /horse ディレクトリにある.binファイルの一覧をスキャンし、
    ファイル名から horse_id を抽出して、
    horse_cache_log.csv を最初から作り直します。
    （既存のログは上書きされます）
    """
    print("--- 3. .binファイルから horse_cache_log.csv を再生成 ---")
    
    # 競走馬データ（.bin）が保存されているディレクトリ
    horse_dir = os.path.join(RAW_HTML_DIR, 'horse')
    output_csv = HORSE_CACHE_LOG_PATH

    if not os.path.isdir(horse_dir):
        print(f"エラー: 'horse' ディレクトリが見つかりません: '{horse_dir}'")
        print("--- 処理中断 ---")
        return

    print(f"検索対象ディレクトリ: {horse_dir}")
    # horse_dir 直下の .bin ファイルのみを検索
    bin_files = glob.glob(os.path.join(horse_dir, '*.bin'))

    if not bin_files:
        print(".binファイルが見つかりませんでした。")
        # .binファイルが存在しない場合でも、空のCSVファイルを作成する
        df = pd.DataFrame(columns=['horse_id', 'last_updated'])
        df.to_csv(output_csv, index=False, encoding='utf-8')
        print(f"空のキャッシュログファイルを作成しました: {output_csv}")
        print("--- 完了 ---")
        return

    # ファイルパスからファイル名（拡張子なし）を取得
    # 例: .../horse/2019104567_basic.bin -> 2019104567_basic
    base_names = [os.path.splitext(os.path.basename(f))[0] for f in bin_files]
    print(f"{len(base_names)}個のファイル（.bin）を見つけました。")

    # ファイル名から horse_id (最初の '_' 以前) を抽出
    # 例: 2019104567_basic -> 2019104567
    # set() で重複を除去し、sorted() で並び替え
    unique_ids = sorted(list(set([name.split('_')[0] for name in base_names])))
    print(f"{len(unique_ids)}個の一意な horse_id を抽出しました。")

    # horse_id が純粋な数字であるものだけをフィルタリング
    horse_ids = [hid for hid in unique_ids if hid.isdigit()]
    
    if len(horse_ids) != len(unique_ids):
        print(f"警告: {len(unique_ids) - len(horse_ids)}個のIDが数字のみでなかったため除外されました。")
    
    print(f"{len(horse_ids)}個の有効な horse_id で処理を続行します。")

    # 現在時刻を全レコード共通のタイムスタンプとして使用
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S.%f')
    
    # DataFrameを作成
    df = pd.DataFrame({'horse_id': horse_ids, 'last_updated': now_str})

    try:
        # CSVファイルとして保存（既存ファイルは上書き）
        df.to_csv(output_csv, index=False, encoding='utf-8')
        print(f"キャッシュログを正常に再生成しました（{len(df)}件のレコード）。")
        print(f"保存先: {output_csv}")
    except ImportError:
        print("エラー: 'pandas' ライブラリが見つかりません。")
        print("'pip install pandas' コマンドでインストールしてください。")
    except Exception as e:
        print(f"CSVファイルの保存中にエラーが発生しました: {e}")

    print("--- 完了 ---")


# --- メイン処理 ---
def main():
    """
    ユーザーに実行したい機能を選択させ、対応する関数を呼び出します。
    'q' が入力されるまで繰り返します。
    """
    while True:
        print("\n========== キャッシュ管理ツール ==========")
        print(" 1: 全ての.binファイルのタイムスタンプを更新する")
        print("    (キャッシュの有効期限を延長します)")
        print(" 2: horse_cache_log.csv の全タイムスタンプを現在時刻に更新する")
        print("    (すべての馬データを「取得済み」として扱います)")
        print(" 3: .binファイルから horse_cache_log.csv を再生成する")
        print("    (馬データの.bin実体ファイルからキャッシュログを再構築します)")
        print(" q: 終了")
        print("=========================================")
        
        choice = input("実行する操作の番号を入力してください: ")
        print() # メニューと実行結果の間に改行を入れる
        
        if choice == '1':
            update_bin_timestamps()
        elif choice == '2':
            update_all_log_timestamps()
        elif choice == '3':
            regenerate_horse_cache_log_from_bins()
        elif choice.lower() == 'q':
            print("ツールを終了します。")
            break
        else:
            print("無効な選択です。'1', '2', '3', 'q' のいずれかを入力してください。")

if __name__ == '__main__':
    # このスクリプトが直接実行された場合にmain関数を呼び出す
    main()
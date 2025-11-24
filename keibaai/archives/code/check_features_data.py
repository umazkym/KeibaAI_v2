#!/usr/bin/env python3
"""
生成された「特徴量(features)Parquetファイル」の
整合性チェック用デバッグコード (修正版 3 - SyntaxError 修正)

実行方法:
1. このスクリプトをプロジェクトのルート（`keibaai/` の親ディレクトリ）に配置する。
2. `python check_features_data.py` を実行する。
3. 出力結果をすべてコピーして私に返信してください。
"""

import pandas as pd
import yaml
import sys
from pathlib import Path

# ANSIカラーコード
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def load_config():
    """設定ファイルをロードする"""
    try:
        base_dir = Path(__file__).resolve().parent
        config_path = base_dir / "keibaai" / "configs" / "default.yaml"
        
        if not config_path.exists():
            print(f"{bcolors.FAIL}エラー: default.yaml が見つかりません: {config_path}{bcolors.ENDC}")
            return None
            
        with open(config_path, "r", encoding="utf-8") as f:
            default_cfg = yaml.safe_load(f)
        
        # keibaai/src/features/generate_features.py のパス定義に合わせる
        data_root = base_dir / "keibaai" / default_cfg['data_path']
        features_data_path = data_root / 'features' / 'parquet'
        
        config = {
            'features_data_path': features_data_path
        }
        return config
    except Exception as e:
        print(f"{bcolors.FAIL}設定ファイルの読み込み中にエラーが発生しました: {e}{bcolors.ENDC}")
        return None

def analyze_features_dataframe(features_dir: Path):
    """特徴量DataFrameを読み込み、shutuba_parser.py の修正が反映されているか確認する"""
    
    print(f"\n{bcolors.HEADER}===== 特徴量(features)Parquet の分析開始 ====={bcolors.ENDC}")
    
    # 日付 (2023-05-14) に基づいてパーティションパスを決定
    target_dir = features_dir / "year=2023" / "month=5"
    
    if not target_dir.exists():
        print(f"{bcolors.FAIL}特徴量ディレクトリが見つかりません: {target_dir}{bcolors.ENDC}")
        # --- ▼▼▼ 構文エラー修正箇所 ▼▼▼ ---
        print(f"""{bcolors.WARNING}
        'keibaai/src/features/generate_features.py' が正常に実行されていない可能性があります。
        ステップ3を（echoなしで）再実行してください。{bcolors.ENDC}""")
        # --- ▲▲▲ 構文エラー修正箇所 ▲▲▲ ---
        print(f"{bcolors.HEADER}===== 特徴量(features) の分析終了 ====={bcolors.ENDC}")
        return

    try:
        # パーティション化されたParquetを読み込む
        df = pd.read_parquet(target_dir)
        
        print(f"{bcolors.OKGREEN}✓ 特徴量ファイル読み込み成功{bcolors.ENDC}")
        print(f"  - ディレクトリ: {target_dir}")
        print(f"  - {bcolors.BOLD}総行数: {len(df)}{bcolors.ENDC}")

        if df.empty:
            print(f"{bcolors.WARNING}DataFrameが空です。{bcolors.ENDC}")
            print(f"{bcolors.HEADER}===== 特徴量(features) の分析終了 ====={bcolors.ENDC}")
            return

        # --- 1. カラム情報とデータ型 ---
        print(f"\n{bcolors.OKCYAN}[1. カラム情報 (df.info())]{bcolors.ENDC}")
        print("---------------------------------")
        df.info(verbose=True)
        print("---------------------------------")

        # --- 2. shutuba_parser.py バグ修正（trainer_id）の確認 ---
        print(f"\n{bcolors.OKCYAN}[2. shutuba_parser.py 修正反映チェック]{bcolors.ENDC}")
        print("---------------------------------")
        
        # 特徴量エンジンは 'trainer_id' をそのまま特徴量に加えるはず
        if 'trainer_id' in df.columns:
            null_count = df['trainer_id'].isnull().sum()
            if null_count == 0:
                print(f"{bcolors.OKGREEN}  - trainer_id: 100% 検出されています (OK) {bcolors.ENDC}")
            elif null_count < len(df):
                 print(f"{bcolors.OKGREEN}  - trainer_id: ほとんど検出されています (Null率: {null_count/len(df):.2%}) (OK) {bcolors.ENDC}")
            else:
                print(f"{bcolors.FAIL}  - trainer_id: 100% Null です。{bcolors.ENDC}")
                print(f"{bcolors.FAIL}  -> [要確認] shutuba_parser.py の修正が反映されていません。{bcolors.ENDC}")
        else:
            print(f"{bcolors.FAIL}  - trainer_id: カラムが特徴量に存在しません。{bcolors.ENDC}")

        # --- 3. race_date の確認 ---
        print(f"\n{bcolors.OKCYAN}[3. 日付範囲チェック]{bcolors.ENDC}")
        print("---------------------------------")
        
        if 'race_date' in df.columns:
            # generate_features.py が日付で絞り込んでいるため、
            # race_date カラムは object (文字列) か datetime 型になっている
            try:
                df['race_date_dt'] = pd.to_datetime(df['race_date'])
                min_date = df['race_date_dt'].min().strftime('%Y-%m-%d')
                max_date = df['race_date_dt'].max().strftime('%Y-%m-%d')
                print(f"  - データ日付範囲: {min_date} から {max_date}")
                
                # 正しい日付(2023-05-14)でフィルタされているか確認
                if min_date == "2023-05-14" and max_date == "2023-05-14":
                     print(f"{bcolors.OKGREEN}  -> 正しい日付 (2023-05-14) のデータが読み込まれています (OK){bcolors.ENDC}")
                else:
                     print(f"{bcolors.WARNING}  -> 予期しない日付 ({min_date} ~ {max_date}) が読み込まれています。{bcolors.ENDC}")
            except Exception as e:
                print(f"{bcolors.WARNING}  - race_date カラムの解析に失敗: {e}{bcolors.ENDC}")
                print(df['race_date'].head())

        else:
            print(f"{bcolors.FAIL}  - race_date: カラムが特徴量に存在しません。{bcolors.ENDC}")

        print(f"{bcolors.HEADER}===== 特徴量(features) の分析終了 ====={bcolors.ENDC}")

    except Exception as e:
        print(f"{bcolors.FAIL}特徴量ファイル {target_dir} の処理中にエラーが発生しました: {e}{bcolors.ENDC}")
        import traceback
        traceback.print_exc()
        print(f"{bcolors.HEADER}===== 特徴量(features) の分析終了 ====={bcolors.ENDC}")


def main():
    """
    メイン実行関数: 特徴量Parquetファイルを分析する
    """
    print(f"{bcolors.BOLD}Keiba AI 特徴量(features) データ検証スクリプト開始... (修正版 3){bcolors.ENDC}")
    
    cfg = load_config()
    if cfg is None:
        sys.exit(1)

    features_dir = cfg['features_data_path']
    analyze_features_dataframe(features_dir)

    print(f"\n{bcolors.BOLD}全ての分析が完了しました。{bcolors.ENDC}")
    print("上記の結果をコピーして、私に返信してください。")

if __name__ == "__main__":
    main()
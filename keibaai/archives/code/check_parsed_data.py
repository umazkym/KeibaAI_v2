#!/usr/bin/env python3
"""
生成されたParquetファイルの整合性チェック用デバッグコード (修正版)

実行方法:
1. `keibaai/src/run_parsing_pipeline_local.py` を実行してParquetファイルを生成する。
2. このスクリプトをプロジェクトのルート（`keibaai/` の親ディレクトリ）に配置する。
3. `python check_parsed_data.py` を実行する。
4. 出力結果をすべてコピーして私に返信してください。
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
        # スクリプトの場所基準で keibaai/configs/default.yaml を探す
        base_dir = Path(__file__).resolve().parent
        config_path = base_dir / "keibaai" / "configs" / "default.yaml"
        
        if not config_path.exists():
            print(f"{bcolors.FAIL}エラー: default.yaml が見つかりません: {config_path}{bcolors.ENDC}")
            print(f"カレントディレクトリ: {Path.cwd()}")
            print(f"スクリプト配置場所: {base_dir}")
            return None
            
        with open(config_path, "r", encoding="utf-8") as f:
            default_cfg = yaml.safe_load(f)
        
        # data_path が絶対パスでない場合は、このスクリプトからの相対パスとして解決
        # (keibaai/src/run_parsing_pipeline_local.py のロジックに合わせる)
        data_root = base_dir / "keibaai" / default_cfg['data_path']
        
        # run_parsing_pipeline_local.py のロジックに合わせてパスを構築
        parsed_data_path = data_root / 'parsed' / 'parquet'
        
        config = {
            'parsed_data_path': parsed_data_path,
            'races_path': parsed_data_path / 'races' / 'races.parquet',
            'shutuba_path': parsed_data_path / 'shutuba' / 'shutuba.parquet',
            'horses_path': parsed_data_path / 'horses' / 'horses.parquet',
            'pedigrees_path': parsed_data_path / 'pedigrees' / 'pedigrees.parquet',
        }
        return config
    except Exception as e:
        print(f"{bcolors.FAIL}設定ファイルの読み込み中にエラーが発生しました: {e}{bcolors.ENDC}")
        return None

def analyze_dataframe(df_name: str, file_path: Path):
    """DataFrameを読み込み、詳細な分析情報を出力する"""
    
    print(f"\n{bcolors.HEADER}===== {df_name} の分析開始 ({file_path.name}) ====={bcolors.ENDC}")
    
    if not file_path.exists():
        print(f"{bcolors.FAIL}ファイルが見つかりません: {file_path}{bcolors.ENDC}")
        # --- ▼▼▼ 構文エラー修正箇所 ▼▼▼ ---
        print(f"""{bcolors.WARNING}
        'keibaai/src/run_parsing_pipeline_local.py' が正常に実行されていない可能性があります。
        (エラー: {bcolors.FAIL}pythpn{bcolors.ENDC} -> 正常: {bcolors.OKGREEN}python{bcolors.ENDC})
        もう一度パース処理を実行してから、このスクリプトを再実行してください。{bcolors.ENDC}""")
        # --- ▲▲▲ 構文エラー修正箇所 ▲▲▲ ---
        print(f"{bcolors.HEADER}===== {df_name} の分析終了 ====={bcolors.ENDC}")
        return

    try:
        df = pd.read_parquet(file_path)
        
        print(f"{bcolors.OKGREEN}✓ ファイル読み込み成功{bcolors.ENDC}")
        print(f"  - ファイルパス: {file_path}")
        print(f"  - {bcolors.BOLD}総行数: {len(df)}{bcolors.ENDC}")

        if df.empty:
            print(f"{bcolors.WARNING}DataFrameが空です。パース対象のHTMLが存在しないか、パースに失敗している可能性があります。{bcolors.ENDC}")
            print(f"{bcolors.HEADER}===== {df_name} の分析終了 ====={bcolors.ENDC}")
            return

        # --- 1. カラム情報とデータ型 ---
        print(f"\n{bcolors.OKCYAN}[1. カラム情報 (df.info())]{bcolors.ENDC}")
        print("---------------------------------")
        df.info(verbose=True)
        print("---------------------------------")

        # --- 2. Null値の割合 ---
        print(f"\n{bcolors.OKCYAN}[2. Null値の割合 (df.isnull().mean())]{bcolors.ENDC}")
        print("---------------------------------")
        null_ratios = df.isnull().mean().sort_values(ascending=False)
        for col, ratio in null_ratios.items():
            color = bcolors.WARNING if ratio > 0 else bcolors.OKGREEN
            if ratio > 0.99:
                color = bcolors.FAIL
            print(f"  - {color}{col:<25}: {ratio:.2%}{bcolors.ENDC}")
        print("---------------------------------")

        # --- 3. 先頭5行のサンプルデータ ---
        print(f"\n{bcolors.OKCYAN}[3. サンプルデータ (df.head())]{bcolors.ENDC}")
        print("---------------------------------")
        with pd.option_context('display.max_columns', None, 'display.width', 1000):
            print(df.head())
        print("---------------------------------")
        
        # --- 4. 仕様書の懸念事項に関する詳細チェック ---
        print(f"\n{bcolors.OKCYAN}[4. 詳細チェック]{bcolors.ENDC}")
        print("---------------------------------")
        
        if df_name == "races.parquet":
            # [仕様書更新 3, 4] trainer, owner, prize_money が取得できているか
            check_cols = ['trainer_name', 'trainer_id', 'owner_name', 'prize_money', 'jockey_id', 'race_date']
            for col in check_cols:
                if col in df.columns:
                    non_null_count = df[col].notnull().sum()
                    print(f"  - {col}: {non_null_count} / {len(df)} 件 (Nullでない)")
                else:
                    print(f"{bcolors.FAIL}  - {col}: カラムが存在しません{bcolors.ENDC}")

        elif df_name == "shutuba.parquet":
            # [仕様書更新 3, 4] 詳細情報が意図通り None (Null) になっているか
            # [仕様書更新 5] jockey_idが取得できているか
            check_cols = ['jockey_id', 'owner_name', 'prize_total', 'morning_odds', 'career_stats', 'last_5_finishes', 'race_date']
            for col in check_cols:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    if col in ['owner_name', 'prize_total', 'morning_odds', 'career_stats', 'last_5_finishes']:
                        if null_count == len(df):
                            print(f"{bcolors.OKGREEN}  - {col}: 意図通りすべてNullです (OK){bcolors.ENDC}")
                        else:
                            print(f"{bcolors.FAIL}  - {col}: Nullであるべきが、値が含まれています ({len(df) - null_count}件){bcolors.ENDC}")
                    elif col == 'jockey_id' or col == 'race_date':
                        non_null_count = df[col].notnull().sum()
                        print(f"  - {col}: {non_null_count} / {len(df)} 件 (Nullでない)")
                else:
                    print(f"{bcolors.FAIL}  - {col}: カラムが存在しません{bcolors.ENDC}")

        elif df_name == "pedigrees.parquet":
            # [仕様書更新 5] 外国馬ID (000など) が除外されているか
            if 'ancestor_id' in df.columns:
                df['ancestor_id'] = df['ancestor_id'].astype(str)
                invalid_ids = df[
                    df['ancestor_id'].str.match(r'^[0]{3,10}$') | # 3桁から10桁の0
                    (df['ancestor_id'].str.len() < 4) |
                    (df['ancestor_id'].str.len() > 10)
                ]
                if invalid_ids.empty:
                    print(f"{bcolors.OKGREEN}  - ancestor_id: 無効なID (000, 桁数不足/超過) は見つかりません (OK){bcolors.ENDC}")
                else:
                    print(f"{bcolors.FAIL}  - ancestor_id: 無効なIDが見つかりました ({len(invalid_ids)}件){bcolors.ENDC}")
                    print(invalid_ids[['horse_id', 'ancestor_id']].head())
            else:
                print(f"{bcolors.FAIL}  - ancestor_id: カラムが存在しません{bcolors.ENDC}")

        print(f"{bcolors.HEADER}===== {df_name} の分析終了 ====={bcolors.ENDC}")

    except Exception as e:
        print(f"{bcolors.FAIL}ファイル {file_path} の処理中にエラーが発生しました: {e}{bcolors.ENDC}")
        import traceback
        traceback.print_exc()
        print(f"{bcolors.HEADER}===== {df_name} の分析終了 ====={bcolors.ENDC}")


def main():
    """
    メイン実行関数: 全てのParquetファイルを分析する
    """
    print(f"{bcolors.BOLD}Keiba AI Parquet データ検証スクリプト開始... (修正版){bcolors.ENDC}")
    
    cfg = load_config()
    if cfg is None:
        sys.exit(1)

    print(f"解析対象ディレクトリ: {bcolors.OKBLUE}{cfg['parsed_data_path']}{bcolors.ENDC}")

    # 分析対象のファイルリスト
    files_to_analyze = {
        "races.parquet": cfg['races_path'],
        "shutuba.parquet": cfg['shutuba_path'],
        "horses.parquet": cfg['horses_path'],
        "pedigrees.parquet": cfg['pedigrees_path'],
    }

    for name, path in files_to_analyze.items():
        analyze_dataframe(name, path)

    print(f"\n{bcolors.BOLD}全ての分析が完了しました。{bcolors.ENDC}")
    print("上記の結果をコピーして、私に返信してください。")

if __name__ == "__main__":
    main()
import pandas as pd
import numpy as np
import os
import json
from pathlib import Path

# スクリプトが実行されているディレクトリ（プロジェクトルートと想定）
# (例: C:\Users\zk-ht\Keiba\Keiba_AI_v2)
current_dir = Path(__file__).parent

# プロジェクトのデータディレクトリへのパスを修正
# (current_dir の中の keibaai/data/...)
base_data_path = current_dir / "keibaai" / "data" / "parsed" / "parquet"

# 分析対象のファイルのフルパス
files_to_check = {
    "races": base_data_path / "races" / "races.parquet",
    "shutuba": base_data_path / "shutuba" / "shutuba.parquet",
    "pedigrees": base_data_path / "pedigrees" / "pedigrees.parquet",
}

# ファイルの存在確認
all_files_found = True
for name, path in files_to_check.items():
    if not path.exists():
        print(f"エラー: ファイルが見つかりません: {path}")
        all_files_found = False

if not all_files_found:
    print("必要なファイルが見つからないため、分析を中断します。")
    print(f"スクリプト実行場所: {Path.cwd()}")
    print(f"想定されるデータパス: {base_data_path}")
    exit()

results = {}

print("--- Parquetファイル分析開始 ---")

try:
    # --- 1. races.parquet の分析 ---
    if "races" in files_to_check:
        df_races = pd.read_parquet(files_to_check["races"])
        report = {}
        
        # jockey_idの欠損率 (Noneまたは空文字)
        report["jockey_id_null_ratio"] = float((df_races['jockey_id'].isnull() | (df_races['jockey_id'] == '')).mean())
        
        # margin_secondsの欠損率
        report["margin_seconds_null_ratio"] = float(df_races['margin_seconds'].isnull().mean())
        non_first = df_races['finish_position'] != 1
        has_time = df_races['finish_time_seconds'].notna()
        no_margin = df_races['margin_seconds'].isnull()
        report["margin_seconds_issue_count (1着以外でタイム有/マージン無)"] = int((non_first & has_time & no_margin).sum())

        # trainer_name / trainer_id の欠損率
        report["trainer_id_null_ratio"] = float((df_races['trainer_id'].isnull() | (df_races['trainer_id'] == '')).mean())
        report["trainer_name_null_ratio"] = float((df_races['trainer_name'].isnull() | (df_races['trainer_name'] == '')).mean())
        
        # owner_name の欠損率
        report["owner_name_null_ratio"] = float((df_races['owner_name'].isnull() | (df_races['owner_name'] == '')).mean())
        
        # prize_money の欠損率
        report["prize_money_null_ratio"] = float(df_races['prize_money'].isnull().mean())
        
        results["races.parquet"] = report

    # --- 2. shutuba.parquet の分析 ---
    if "shutuba" in files_to_check:
        df_shutuba = pd.read_parquet(files_to_check["shutuba"])
        report = {}
        
        # jockey_id の欠損率
        report["jockey_id_null_ratio"] = float((df_shutuba['jockey_id'].isnull() | (df_shutuba['jockey_id'] == '')).mean())
        
        # owner_name の欠損率 (仕様上 100% が期待される)
        report["owner_name_null_ratio (仕様上100%期待)"] = float(df_shutuba['owner_name'].isnull().mean())

        # prize_total の欠損率 (仕様上 100% が期待される)
        report["prize_total_null_ratio (仕様上100%期待)"] = float(df_shutuba['prize_total'].isnull().mean())
        
        # morning_odds の欠損率
        report["morning_odds_null_ratio"] = float(df_shutuba['morning_odds'].isnull().mean())
        
        # morning_popularity の欠損率
        report["morning_popularity_null_ratio"] = float(df_shutuba['morning_popularity'].isnull().mean())

        # career_stats系 (仕様上 100% が期待される)
        report["career_stats_null_ratio (仕様上100%期待)"] = float(df_shutuba['career_stats'].isnull().mean())
        report["career_starts_null_ratio (仕様上100%期待)"] = float(df_shutuba['career_starts'].isnull().mean())
        
        results["shutuba.parquet"] = report

    # --- 3. pedigrees.parquet の分析 ---
    if "pedigrees" in files_to_check:
        df_pedigrees = pd.read_parquet(files_to_check["pedigrees"])
        report = {}
        
        # ancestor_idに '000' や '0' などの無効なIDが存在するか
        invalid_ids = ['0', '00', '000', '0000', '00000', '000000', '0000000', '00000000', '000000000', '0000000000']
        report["invalid_ancestor_id_count ( '000'等 )"] = int(df_pedigrees['ancestor_id'].isin(invalid_ids).sum())
        
        # ancestor_name の欠損率
        report["ancestor_name_null_ratio"] = float(df_pedigrees['ancestor_name'].isnull().mean())
        
        results["pedigrees.parquet"] = report
        
    # --- 結果の表示 ---
    print(json.dumps(results, indent=2, ensure_ascii=False))

except Exception as e:
    print(f"分析中にエラーが発生しました: {e}")

print("--- 分析終了 ---")
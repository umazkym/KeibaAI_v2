# check_features_data.py

import pandas as pd
import sys
import os
import yaml
from pathlib import Path

# --- 検証設定 ---
# generate_features.py で指定した日付（2023-05-14）に合わせて年と月を指定
TARGET_YEAR = 2023
TARGET_MONTH = 5
# --- (ここまで) ---

# --- (スクリプトの残りの部分は変更なし) ---
print("Keiba AI 特徴量(features) データ検証スクリプト開始... (修正版 4)")

# プロジェクトルートの確認
project_root = Path(__file__).resolve().parent
keibaai_dir = project_root / "keibaai"
if not keibaai_dir.exists():
    # もし C:\Users\zk-ht\Keiba\Keiba_AI_v2\check_features_data.py の場合
    project_root = Path(__file__).resolve().parent.parent
    keibaai_dir = project_root / "keibaai"
    if not keibaai_dir.exists():
        print(f"エラー: keibaaiディレクトリが見つかりません: {project_root / 'keibaai'}")
        sys.exit(1)

print(f"プロジェクトルート: {project_root}")
print(f"keibaaiディレクトリ: {keibaai_dir}")

# Parquetファイルのパス
features_dir = keibaai_dir / "data" / "features" / "parquet"
features_path = features_dir / f"year={TARGET_YEAR}" / f"month={TARGET_MONTH}"

# 特徴量リストのパス
features_list_path = keibaai_dir / "data" / "features" / "parquet" / "feature_names.yaml"


print("\n" + "="*40)
print("特徴量データ (features.parquet) の検証")
print("="*40)

if not features_path.exists():
    print(f"特徴量ディレクトリが見つかりません: {features_path}")
    print("\n       'keibaai/src/features/generate_features.py' が正常に実行されていない可能性があります。")
    print("        ステップ1を（echoなしで）再実行してください。")
    sys.exit(1)

try:
    # Parquetディレクトリを読み込む
    df = pd.read_parquet(features_path)
    print(f"Parquetファイルのロード成功: {features_path}")
    print(f"合計 {len(df)} 行のデータを読み込みました。")
    
    print("\n[1. カラム情報 (df.info())]")
    df.info()

    print("\n[2. 修正反映チェック]")
    required_ids = ['jockey_id', 'trainer_id']
    all_ok = True
    for col in required_ids:
        if col not in df.columns:
            print(f"{col}: カラムが特徴量に存在しません。 (feature_engine.pyの修正が反映されていません)")
            all_ok = False
        else:
            print(f"{col}: OK")

    if all_ok:
        print("\n" + "="*40)
        print("検証完了: jockey_id, trainer_id が特徴量データに正しく保持されています。")
        print("="*40)
    else:
        print("\n" + "="*40)
        print("検証失敗: 必要なIDカラムが欠落しています。")
        print("="*40)

except Exception as e:
    print(f"Parquetファイルの読み込みまたは検証中にエラーが発生しました: {e}")
    print(f"パス: {features_path}")

try:
    print("\n[3. 特徴量リスト (feature_names.yaml)]")
    if not features_list_path.exists():
        print(f"特徴量リストファイルが見つかりません: {features_list_path}")
    else:
        with open(features_list_path, 'r', encoding='utf-8') as f:
            feature_names = yaml.safe_load(f)
        print(f"{features_list_path} を読み込みました (特徴量 {len(feature_names)}個)")
        
        # リスト内にjockey_idやtrainer_idが *含まれていない* ことを確認
        if 'jockey_id' in feature_names or 'trainer_id' in feature_names:
            print("警告: feature_names.yaml に jockey_id または trainer_id が含まれています。")
            print("     (_select_features の exclude_cols から削除されているか確認してください)")
        else:
            print("OK: feature_names.yaml にIDは含まれていません。")

except Exception as e:
    print(f"feature_names.yaml の読み込み中にエラーが発生しました: {e}")

print("\n全ての検証が完了しました。")
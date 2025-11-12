#!/usr/bin/env python3
# src/models/train_mu_model.py
"""
μモデル (MuEstimator) 学習実行スクリプト
仕様書 13.3章, 19.3.2章 に基づく実装

実行例:
python src/models/train_mu_model.py \
    --start_date 2020-01-01 \
    --end_date 2023-12-31 \
    --output_dir data/models/mu_model_v1
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
import yaml
import pandas as pd

# --- 修正: プロジェクトルートの定義変更 ---
# スクリプト(keibaai/src/models/train_mu_model.py) の4階層上が Keiba_AI_v2 (実行ルート)
execution_root = Path(__file__).resolve().parent.parent.parent.parent
# keibaai/src を sys.path に追加
sys.path.append(str(execution_root / "keibaai" / "src"))
# --- 修正ここまで ---

try:
    from pipeline_core import setup_logging, load_config
    from utils.data_utils import load_parquet_data_by_date
    from models.model_train import MuEstimator
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    print("プロジェクトルートが正しく設定されているか確認してください。")
    print(f"sys.path: {sys.path}")
    sys.exit(1)

def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI μモデル学習パイプライン')
    parser.add_argument(
        '--start_date',
        type=str,
        required=True,
        help='学習開始日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end_date',
        type=str,
        required=True,
        help='学習終了日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--output_dir',
        type=str,
        required=True,
        help='学習済みモデルの保存先ディレクトリ (例: data/models/mu_model_v1)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='keibaai/configs/default.yaml', # 修正: 実行場所からの相対パス
        help='基本設定ファイルパス'
    )
    parser.add_argument(
        '--models_config',
        type=str,
        default='keibaai/configs/models.yaml', # 修正: 実行場所からの相対パス
        help='モデル設定ファイルパス'
    )
    parser.add_argument(
        '--features_config',
        type=str,
        default='keibaai/configs/features.yaml', # 修正: 実行場所からの相対パス
        help='特徴量設定ファイルパス'
    )
    parser.add_argument(
        '--log_level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='ログレベル'
    )

    args = parser.parse_args()

    # --- 0. 設定とロギング (修正: パス解決) ---
    try:
        # コマンド引数のパス (例: keibaai/configs/default.yaml) は実行場所(execution_root)からの相対パスとして解決
        config_path = execution_root / args.config
        models_config_path = execution_root / args.models_config
        features_config_path = execution_root / args.features_config
        
        config = load_config(str(config_path))
        paths = config.get('paths', {})
        
        with open(models_config_path, 'r', encoding='utf-8') as f:
            models_config = yaml.safe_load(f)
        
        with open(features_config_path, 'r', encoding='utf-8') as f:
            features_config = yaml.safe_load(f)

        # ログパスの ${logs_path} を解決
        logs_path_base = paths.get('logs_path', 'data/logs')
        log_path_template = config.get('logging', {}).get('log_file', 'data/logs/{YYYY}/{MM}/{DD}/training.log')
        log_path_with_base = log_path_template.replace('${logs_path}', logs_path_base)
        
        now = datetime.now()
        log_path = log_path_with_base.format(YYYY=now.year, MM=f"{now.month:02}", DD=f"{now.day:02}")
        
        # ログファイルパスを execution_root からの相対パスとして解決
        # (configのパス例: "keibaai/data/logs/...")
        log_path_abs = execution_root / log_path
        log_path_abs.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=args.log_level.upper(),
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            handlers=[
                logging.FileHandler(log_path_abs, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ],
            force=True
        )
            
    except FileNotFoundError as e:
        logging.error(f"設定ファイルが見つかりません: {e}")
        sys.exit(1)
    except KeyError as e:
        print(f"設定ファイル（{args.config}）の読み込み中にキーエラーが発生しました: {e}")
        print("default.yaml に 'paths' や 'logging' の設定が含まれているか確認してください。")
        sys.exit(1)
    # --- 修正ここまで ---


    logging.info("=" * 60)
    logging.info("Keiba AI μモデル学習パイプライン開始")
    logging.info("=" * 60)
    logging.info(f"学習期間: {args.start_date} - {args.end_date}")

    # --- 1. 日付範囲の決定 ---
    try:
        start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError as e:
        logging.error(f"日付フォーマットエラー: {e}")
        sys.exit(1)

    # --- 2. 特徴量データのロード (修正: パス解決) ---
    # default.yaml の 'features_path' (例: "keibaai/data/features") を execution_root 基準で解決
    
    # ★★★ 修正: 特徴量リスト (feature_names.yaml) のパス ★★★
    features_base_dir = execution_root / paths.get('features_path', 'keibaai/data/features')
    # ★★★ 修正: Parquetデータ本体 (パーティションルート) のパス ★★★
    features_parquet_dir = features_base_dir / 'parquet'
    
    # 特徴量リストを読み込む
    feature_names_list = []
    try:
        # ★★★ 修正: 正しいパスから読み込む ★★★
        feature_names_yaml_path = features_base_dir / "feature_names.yaml"
        with open(feature_names_yaml_path, 'r', encoding='utf-8') as f:
            # YAMLファイルは {'feature_names': [...]} という構造を想定
            feature_names_config = yaml.safe_load(f)
            feature_names_list = feature_names_config.get('feature_names', [])
            
        if not feature_names_list:
             logging.error(f"特徴量リスト (feature_names.yaml) の中身が空です: {feature_names_yaml_path}")
             sys.exit(1)
             
        logging.info(f"{len(feature_names_list)}個の特徴量をロードしました")
        
    except FileNotFoundError:
         logging.error(f"特徴量リスト (feature_names.yaml) が見つかりません: {feature_names_yaml_path}")
         sys.exit(1)
    except Exception as e:
         logging.error(f"特徴量リスト (feature_names.yaml) の読み込みに失敗: {e}")
         sys.exit(1)


    # 学習データをロード
    # ★★★ 修正: Parquetデータ本体のパスを渡す ★★★
    features_df = load_parquet_data_by_date(features_parquet_dir, start_dt, end_dt, date_col='race_date')
    
    if features_df.empty:
        logging.error(f"期間 {args.start_date} - {args.end_date} の特徴量データが見つかりません。")
        logging.error(f"検索パス: {features_parquet_dir}")
        sys.exit(1)
        
    # 学習に必要な目的変数が存在するか確認
    if 'finish_position' not in features_df.columns or 'finish_time_seconds' not in features_df.columns:
        # 特徴量データに目的変数が含まれていない場合は、元の 'races' データからマージする
        logging.warning("特徴量データに目的変数 (finish_position, finish_time_seconds) がありません。")
        logging.warning("元の 'races' Parquetデータからマージを試みます...")
        
        try:
            races_dir = execution_root / paths.get('parsed_parquet_races', 'keibaai/data/parsed/parquet/races')
            races_df = load_parquet_data_by_date(races_dir, start_dt, end_dt, date_col='race_date')
            
            if races_df.empty:
                raise FileNotFoundError("マージ対象の 'races' データが見つかりません。")
            
            # 必要なカラムのみを選択してマージ
            target_cols = ['race_id', 'horse_id', 'finish_position', 'finish_time_seconds']
            if not all(col in races_df.columns for col in target_cols):
                raise ValueError(f"'races' データに必要なカラム {target_cols} が揃っていません。")

            # マージキー（race_id, horse_id）の型を合わせる
            features_df['race_id'] = features_df['race_id'].astype(str)
            features_df['horse_id'] = features_df['horse_id'].astype(str)
            races_df['race_id'] = races_df['race_id'].astype(str)
            races_df['horse_id'] = races_df['horse_id'].astype(str)

            features_df = features_df.merge(
                races_df[target_cols],
                on=['race_id', 'horse_id'],
                how='left'
            )
            logging.info("目的変数のマージ完了。")

        except Exception as e:
            logging.error(f"目的変数のマージに失敗しました: {e}")
            logging.error("特徴量生成パイプラインが 'races' データを正しく処理したか確認してください。")
            sys.exit(1)

    # 欠損値を含む行を削除 (学習のため)
    required_cols = feature_names_list + ['finish_position', 'finish_time_seconds', 'race_id']
    # 存在しないカラムを要求リストから除外 (安全のため)
    required_cols = [col for col in required_cols if col in features_df.columns]
    
    original_rows = len(features_df)
    features_df = features_df.dropna(subset=required_cols)
    rows_dropped = original_rows - len(features_df)
    
    logging.info(f"欠損値除去: {rows_dropped}行を除去しました。")
    logging.info(f"{len(features_df)}行のデータで学習します。")

    if features_df.empty:
        logging.error("欠損値を除去した結果、学習データが0行になりました。")
        sys.exit(1)

    # --- 3. モデル学習 ---
    mu_model_config = models_config.get('mu_estimator', {})
    estimator = MuEstimator(mu_model_config)
    
    # ★★★ 修正: estimator に特徴量リストを渡す ★★★
    estimator.feature_names = feature_names_list
    
    try:
        estimator.train(
            features_df=features_df,
            target_regressor='finish_time_seconds', # 仕様書 参照
            target_ranker='finish_position',     # 仕様書 参照
            group_col='race_id'
        )
    except Exception as e:
        logging.error(f"μモデルの学習中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)

    # --- 4. モデル保存 (修正: パス解決) ---
    try:
        # 保存先パスを execution_root 基準で解決
        output_path_abs = execution_root / args.output_dir
        estimator.save_model(str(output_path_abs))
        
    except Exception as e:
        logging.error(f"学習済みμモデルの保存に失敗しました: {e}", exc_info=True)
        sys.exit(1)

    logging.info("=" * 60)
    logging.info(f"Keiba AI μモデル学習完了。モデルは {args.output_dir} に保存されました。")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
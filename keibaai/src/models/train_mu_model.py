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
    features_dir = execution_root / paths.get('features_path', 'keibaai/data/features') / 'parquet'
    
    # 特徴量リストを読み込む
    try:
        with open(features_dir / "feature_names.yaml", 'r', encoding='utf-8') as f:
            feature_names = yaml.safe_load(f)
        logging.info(f"{len(feature_names)}個の特徴量をロードしました")
    except FileNotFoundError:
         logging.error(f"特徴量リスト (feature_names.yaml) が見つかりません: {features_dir}")
         sys.exit(1)

    # 学習データをロード
    features_df = load_parquet_data_by_date(features_dir, start_dt, end_dt, date_col='race_date')
    
    if features_df.empty:
        logging.error(f"期間 {args.start_date} - {args.end_date} の特徴量データが見つかりません。")
        sys.exit(1)
        
    # 学習に必要な目的変数が存在するか確認
    if 'finish_position' not in features_df.columns or 'finish_time_seconds' not in features_df.columns:
        logging.error("学習に必要な目的変数 (finish_position, finish_time_seconds) が特徴量データにありません。")
        sys.exit(1)
        
    # 欠損値を含む行を削除 (学習のため)
    features_df = features_df.dropna(subset=feature_names + ['finish_position', 'finish_time_seconds', 'race_id'])
    logging.info(f"欠損値除去後、{len(features_df)}行のデータで学習します。")

    if features_df.empty:
        logging.error("欠損値を除去した結果、学習データが0行になりました。")
        sys.exit(1)

    # --- 3. モデル学習 ---
    mu_model_config = models_config.get('mu_estimator', {})
    estimator = MuEstimator(mu_model_config)
    
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
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

# プロジェクトルート (keibaai ディレクトリ) をパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

try:
    from src.pipeline_core import setup_logging, load_config
    from src.utils.data_utils import load_parquet_data_by_date
    from src.models.model_train import MuEstimator
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
        default='configs/default.yaml',
        help='基本設定ファイルパス'
    )
    parser.add_argument(
        '--models_config',
        type=str,
        default='configs/models.yaml',
        help='モデル設定ファイルパス'
    )
    parser.add_argument(
        '--features_config',
        type=str,
        default='configs/features.yaml',
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

    # --- 0. 設定とロギング ---
    try:
        # 【修正箇所】 load_config (encoding なし) の代わりに、encoding を指定して config をロード
        # これにより、Windows (cp932) 環境での config パースエラーを防ぐ
        with open(args.config, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        paths = config.get('paths', {})
        
        # encoding='utf-8' を指定
        with open(args.models_config, 'r', encoding='utf-8') as f:
            models_config = yaml.safe_load(f)
        
        # encoding='utf-8' を指定
        with open(args.features_config, 'r', encoding='utf-8') as f:
            features_config = yaml.safe_load(f)

        # 【!!! 修正箇所 !!!】
        # setup_logging (turn 20) が logging_config から 'log_file' (logging: の下) と
        # 'paths' (トップレベル) の両方を期待しているという矛盾した実装に対応する。
        # config['logging'] 辞書を取得し、それに config['paths'] をマージして渡す。
        logging_config = config.get('logging', {})
        logging_config['paths'] = paths # paths 辞書を logging_config に追加
        
        setup_logging(args.log_level.upper(), logging_config)
            
    except FileNotFoundError as e:
        # ロギング設定前にエラーが発生した場合に備えて print も残す
        print(f"設定ファイルが見つかりません: {e}")
        logging.error(f"設定ファイルが見つかりません: {e}")
        sys.exit(1)
    except Exception as e:
        # 予期せぬエラー（ロギング設定失敗など）をキャッチ
        print(f"初期化中にエラーが発生しました: {e}")
        logging.error(f"初期化中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)


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

    # --- 2. 特徴量データのロード ---
    
    # 【修正箇所】 config が正しくロードされたため、base_dir の定義を
    # コマンド実行時のカレントディレクトリ基準ではなく、config ファイル基準の絶対パスにする
    base_dir = Path(args.config).resolve().parent.parent
    features_dir = base_dir / Path(paths.get('features_path', 'data/features')) / 'parquet'
    
    # 特徴量リストを読み込む
    try:
        # encoding='utf-8' を指定
        with open(features_dir / "feature_names.yaml", 'r', encoding='utf-8') as f:
            feature_names = yaml.safe_load(f)
        logging.info(f"{len(feature_names)}個の特徴量をロードしました")
    except FileNotFoundError:
         # エラーメッセージを修正し、絶対パスを表示
         logging.error(f"特徴量リスト (feature_names.yaml) が見つかりません: {features_dir.resolve()}")
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
            feature_names=feature_names,
            target_regressor='finish_time_seconds', # 仕様書 参照
            target_ranker='finish_position',     # 仕様書 参照
            group_col='race_id'
        )
    except Exception as e:
        logging.error(f"μモデルの学習中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)

    # --- 4. モデル保存 ---
    try:
        # output_dir も Path オブジェクトとして渡す
        output_path = Path(args.output_dir)
        estimator.save_model(output_path)
        
        # (仕様書 3.4.2 に基づき、メタデータもDBに保存するのが望ましいが、ここではファイル保存のみ)
        
    except Exception as e:
        logging.error(f"学習済みμモデルの保存に失敗しました: {e}", exc_info=True)
        sys.exit(1)

    logging.info("=" * 60)
    logging.info(f"Keiba AI μモデル学習完了。モデルは {args.output_dir} に保存されました。")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
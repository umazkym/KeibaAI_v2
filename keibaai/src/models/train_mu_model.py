#!/usr/bin/env python3
# src/models/train_mu_model.py
"""
μモデル (MuEstimator) 学習実行スクリプト (v3 - 堅牢化版)
- 結合キーのクリーンアップ機能を追加
- feature_names.yamlの自動移動機能を追加
- 応急処置的な重複排除処理を、キーのクリーンアップを前提とした正規処理に変更
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
import yaml
import pandas as pd
import shutil

# プロジェクトルート (keibaai ディレクトリ) をパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

try:
    from src.pipeline_core import setup_logging
    from src.utils.data_utils import load_parquet_data_by_date
    from src.models.model_train import MuEstimator
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    sys.exit(1)

def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI μモデル学習パイプライン')
    parser.add_argument('--start_date', type=str, required=True, help='学習開始日 (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='学習終了日 (YYYY-MM-DD)')
    parser.add_argument('--output_dir', type=str, required=True, help='学習済みモデルの保存先ディレクトリ')
    parser.add_argument('--config', type=str, default='configs/default.yaml', help='基本設定ファイルパス')
    parser.add_argument('--models_config', type=str, default='configs/models.yaml', help='モデル設定ファイルパス')
    parser.add_argument('--features_config', type=str, default='configs/features.yaml', help='特徴量設定ファイルパス')
    parser.add_argument('--log_level', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='ログレベル')

    args = parser.parse_args()

    # --- 0. 設定とロギングの初期化 ---
    try:
        base_dir = Path(args.config).resolve().parent.parent
        with open(args.config, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        with open(args.models_config, 'r', encoding='utf-8') as f:
            models_config = yaml.safe_load(f)
        
        data_path = Path(config.get('data_path', 'data'))
        if not data_path.is_absolute():
            data_path = base_dir / data_path
        for key, value in config.items():
            if isinstance(value, str):
                config[key] = value.replace('${data_path}', str(data_path))
            if key.endswith('_path') and not Path(config[key]).is_absolute():
                config[key] = str(base_dir / config[key])
        
        log_conf = config.get('logging', {})
        log_file_template = log_conf.get('log_file', 'logs/pipeline.log')
        logs_path = Path(config.get('logs_path', str(data_path / 'logs')))
        log_file_path = log_file_template.replace('${logs_path}', str(logs_path))
        now = datetime.now()
        log_file_path = now.strftime(log_file_path.replace('{YYYY}', '%Y').replace('{MM}', '%m').replace('{DD}', '%d'))
        
        setup_logging(log_level=args.log_level, log_file=log_file_path, log_format=log_conf.get('format'))

    except Exception as e:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [ERROR] %(message)s')
        logging.error(f"ロギングの初期化に失敗しました: {e}", exc_info=True)
        sys.exit(1)

    logging.info("=" * 60)
    logging.info("Keiba AI μモデル学習パイプライン開始")
    logging.info("=" * 60)
    logging.info(f"学習期間: {args.start_date} - {args.end_date}")

    start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')

    # --- 1. 特徴量リスト(feature_names.yaml)の読み込み ---
    features_path_str = config.get('features_path', str(base_dir / 'data/features'))
    features_dir = Path(features_path_str)
    parquet_dir = features_dir / 'parquet'
    
    # generate_featuresがparquetディレクトリに作成する可能性があるため、存在すれば移動
    source_yaml = parquet_dir / "feature_names.yaml"
    dest_yaml = features_dir / "feature_names.yaml"
    if source_yaml.exists() and not dest_yaml.exists():
        try:
            shutil.move(str(source_yaml), str(dest_yaml))
            logging.info(f"{source_yaml} を {dest_yaml} に移動しました。")
        except Exception as e:
            logging.warning(f"feature_names.yamlの移動に失敗: {e}")

    try:
        with open(dest_yaml, 'r', encoding='utf-8') as f:
            feature_names = yaml.safe_load(f)
        logging.info(f"{len(feature_names)}個の特徴量をロードしました")
    except FileNotFoundError:
         logging.error(f"特徴量リスト (feature_names.yaml) が見つかりません: {dest_yaml}")
         sys.exit(1)

    # --- 2. データロードと前処理 ---
    # 2.1. 特徴量データのロード
    features_df = load_parquet_data_by_date(parquet_dir, start_dt, end_dt, date_col='race_date')
    if features_df.empty:
        logging.error(f"期間 {args.start_date} - {args.end_date} の特徴量データが見つかりません。")
        sys.exit(1)
    
    # 2.2. レース結果データのロード
    parsed_data_path = config.get('parsed_data_path', str(base_dir / 'data/parsed'))
    races_parquet_path = Path(parsed_data_path) / 'parquet' / 'races' / 'races.parquet'
    try:
        races_df = pd.read_parquet(races_parquet_path)
        logging.info(f"全レース結果データをロードしました: {len(races_df)}行")
    except FileNotFoundError:
        logging.error(f"レース結果ファイルが見つかりません: {races_parquet_path}")
        sys.exit(1)

    # 2.3. 結合キーのクリーンアップ (最重要)
    merge_keys = ['race_id', 'horse_id']
    for df in [features_df, races_df]:
        for key in merge_keys:
            if key in df.columns:
                df[key] = df[key].astype(str).str.strip()
    logging.info("結合キーを文字列に変換し、空白を除去しました。")

    # 2.4. 特徴量データの重複排除
    if features_df.duplicated(subset=merge_keys).any():
        logging.warning(f"特徴量データに重複 ({features_df.duplicated(subset=merge_keys).sum()}行) が見つかりました。重複を排除します。")
        features_df = features_df.drop_duplicates(subset=merge_keys, keep='first')
    
    # 2.5. データのマージ
    target_cols = ['finish_position', 'finish_time_seconds']
    races_subset_df = races_df[merge_keys + target_cols].copy()
    merged_df = pd.merge(features_df, races_subset_df, on=merge_keys, how='inner')
    logging.info(f"特徴量とレース結果をマージしました。結果: {len(merged_df)}行")

    if merged_df.empty:
        logging.error("マージの結果、データが0行になりました。race_idやhorse_idが一致しません。")
        sys.exit(1)

    # 2.6. 欠損値処理
    required_cols = target_cols + ['race_id']
    final_df = merged_df.dropna(subset=required_cols)
    logging.info(f"必須カラムの欠損値除去後: {len(final_df)}行")

    if final_df.empty:
        logging.error("必須カラムの欠損値を除去した結果、学習データが0行になりました。")
        sys.exit(1)

    # --- 3. モデル学習 ---
    mu_model_config = models_config.get('mu_estimator', {})
    estimator = MuEstimator(mu_model_config)
    
    try:
        # 学習に使う特徴量カラムの欠損値を0で埋める
        for col in feature_names:
            if col in final_df.columns and final_df[col].dtype == 'object':
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
        
        final_df[feature_names] = final_df[feature_names].fillna(0)
        logging.info("特徴量の欠損値を0で補完し、数値型に変換しました。")

        estimator.train(
            features_df=final_df,
            feature_names=feature_names,
            target_regressor='finish_time_seconds',
            target_ranker='finish_position',
            group_col='race_id'
        )
    except Exception as e:
        logging.error(f"μモデルの学習中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)

    # --- 4. モデル保存 ---
    try:
        output_path = Path(args.output_dir)
        estimator.save_model(output_path)
    except Exception as e:
        logging.error(f"学習済みμモデルの保存に失敗しました: {e}", exc_info=True)
        sys.exit(1)

    logging.info("=" * 60)
    logging.info(f"Keiba AI μモデル学習完了。モデルは {args.output_dir} に保存されました。")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
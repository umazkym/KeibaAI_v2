#!/usr/bin/env python3
# src/models/predict.py
"""
モデル推論 実行スクリプト
指定された日付の特徴量を読み込み、
学習済みモデル（μ, σ, ν）で推論を実行し、
結果を data/predictions/ に保存する。

仕様書 17.3章 に基づく実装

実行例:
python src/models/predict.py --date 2023-10-01 --model_dir data/models/latest
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import numpy as np
import yaml
import joblib
import json

# --- 修正: プロジェクトルートの定義変更 ---
execution_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(execution_root / "keibaai" / "src"))
# --- 修正ここまで ---

try:
    from pipeline_core import setup_logging, load_config
    from utils.data_utils import load_parquet_data_by_date
    from models.model_train import MuEstimator
    from models.sigma_estimator import SigmaEstimator
    from models.nu_estimator import NuEstimator
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    print("プロジェクトルートが正しく設定されているか確認してください。")
    print(f"sys.path: {sys.path}")
    sys.exit(1)


def load_model_safely(model_class, config, model_path_str):
    """モデルをロードする（クラスラッパーを使用）"""
    # 修正: model_path_str は execution_root からの相対パスと見なす
    model_path_abs = execution_root / model_path_str
    if not model_path_abs.exists():
        logging.error(f"モデルディレクトリが見つかりません: {model_path_abs}")
        raise FileNotFoundError(f"モデルディレクトリが見つかりません: {model_path_abs}")
        
    model = model_class(config)
    model.load_model(str(model_path_abs))
    return model

def load_plain_model(model_path_str, meta_path_str):
    """
    プレーンなLGBMモデルファイルとメタデータ（特徴量リスト）をロードする
    """
    # 修正: パスを execution_root 基準で解決
    model_file_abs = execution_root / model_path_str
    meta_file_abs = execution_root / meta_path_str
    
    if not model_file_abs.exists():
        raise FileNotFoundError(f"モデルファイルが見つかりません: {model_file_abs}")
    if not meta_file_abs.exists():
        raise FileNotFoundError(f"メタファイル（特徴量リスト）が見つかりません: {meta_file_abs}")

    model = joblib.load(model_file_abs)
    
    with open(meta_file_abs, 'r', encoding='utf-8') as f:
        try:
            meta_data = json.load(f)
        except json.JSONDecodeError:
            logging.warning(f"メタファイル {meta_file_abs} はJSONではありませんでした。YAMLとして再試行します。")
            f.seek(0)
            meta_data = yaml.safe_load(f)

        if isinstance(meta_data, dict):
            feature_names = meta_data.get('feature_names', [])
        else:
            feature_names = meta_data 
            
    return model, feature_names


def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI モデル推論パイプライン')
    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='推論対象日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--model_dir',
        type=str,
        required=True,
        help='学習済みモデルが格納されているディレクトリ (例: keibaai/data/models/latest)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='keibaai/configs/default.yaml', # 修正: 実行場所からの相対パス
        help='設定ファイルパス'
    )
    parser.add_argument(
        '--models_config',
        type=str,
        default='keibaai/configs/models.yaml', # 修正: 実行場所からの相対パス
        help='モデル設定ファイルパス'
    )
    parser.add_argument(
        '--log_level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='ログレベル'
    )
    parser.add_argument(
        '--output_filename',
        type=str,
        default='mu_predictions.parquet',
        help='μモデルの推論結果の出力ファイル名（σ/ν学習用）'
    )

    args = parser.parse_args()

    # --- 0. 設定とロギング (修正: パス解決) ---
    try:
        config_path = execution_root / args.config
        models_config_path = execution_root / args.models_config

        config = load_config(str(config_path))
        paths = config.get('paths', {})
        
        with open(models_config_path, 'r', encoding='utf-8') as f:
            models_config = yaml.safe_load(f)

        # ログパスの ${logs_path} を解決
        logs_path_base = paths.get('logs_path', 'data/logs')
        log_path_template = config.get('logging', {}).get('log_file', 'data/logs/{YYYY}/{MM}/{DD}/predict.log')
        log_path_with_base = log_path_template.replace('${logs_path}', logs_path_base)

        now = datetime.now()
        log_path = log_path_with_base.format(YYYY=now.year, MM=f"{now.month:02}", DD=f"{now.day:02}")
        
        # ログファイルパスを execution_root からの相対パスとして解決
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
    logging.info("Keiba AI モデル推論パイプライン開始")
    logging.info("=" * 60)
    logging.info(f"対象日: {args.date}, モデル: {args.model_dir}")

    # --- 1. 日付範囲の決定 ---
    try:
        target_dt = datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError as e:
        logging.error(f"日付フォーマットエラー: {e}")
        sys.exit(1)

    # --- 2. 特徴量データのロード (修正: パス解決) ---
    features_dir = execution_root / paths.get('features_path', 'keibaai/data/features') / 'parquet'
    features_df = load_parquet_data_by_date(features_dir, target_dt, target_dt, date_col='race_date')
    
    if features_df.empty:
        logging.warning(f"{args.date} の特徴量データが見つかりません。処理を終了します。")
        sys.exit(0)

    # --- 3. モデルのロード (修正: パス解決) ---
    # args.model_dir (例: keibaai/data/models/mu_model_v1) を execution_root 基準で解決
    model_dir_path_str = args.model_dir
    
    try:
        # 3.1 μ (mu) モデル
        logging.info("μモデルをロード中...")
        mu_model_config = models_config.get('mu_estimator', {})
        mu_model = load_model_safely(
            MuEstimator, 
            mu_model_config, 
            str(Path(model_dir_path_str) / 'mu_model') # 'mu_model' サブディレクトリ
        )

        # 3.2 σ (sigma) モデル
        logging.info("σモデルをロード中...")
        sigma_model, sigma_features = load_plain_model(
            str(Path(model_dir_path_str) / 'sigma_model.pkl'),
            str(Path(model_dir_path_str) / 'sigma_features.json')
        )
        sigma_model.feature_names_ = sigma_features 

        # 3.3 ν (nu) モデル
        logging.info("νモデルをロード中...")
        nu_model, nu_features = load_plain_model(
            str(Path(model_dir_path_str) / 'nu_model.pkl'),
            str(Path(model_dir_path_str) / 'nu_features.json')
        )
        nu_model.feature_names_ = nu_features
        
        logging.info("全モデルのロード完了")

    except Exception as e:
        logging.error(f"モデルのロード中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)
        
    # --- 4. 推論実行 ---
    logging.info("推論実行中...")
    
    predictions_list = []
    race_ids = features_df['race_id'].unique()
    
    for race_id in race_ids:
        race_features_df = features_df[features_df['race_id'] == race_id].copy().reset_index(drop=True)
        
        if race_features_df.empty:
            continue
            
        # 4.1 μ の予測
        mu_pred = mu_model.predict(race_features_df)
        
        # 4.2 σ の予測
        try:
            X_sigma = race_features_df[sigma_model.feature_names_]
            sigma_pred = sigma_model.predict(X_sigma)
            sigma_pred = np.sqrt(np.maximum(sigma_pred, 0.0)) 
        except Exception as e:
            logging.warning(f"レース {race_id} のσ予測に失敗: {e}。グローバル値 (1.0) で代替します。")
            sigma_pred = np.full(len(race_features_df), 1.0)
        
        # 4.3 ν の予測
        try:
            X_nu = race_features_df[nu_model.feature_names_].iloc[0:1] 
            nu_pred = nu_model.predict(X_nu)[0]
        except Exception as e:
            logging.warning(f"レース {race_id} のν予測に失敗: {e}。グローバル値 (1.0) で代替します。")
            nu_pred = 1.0
        
        # 結果を格納
        result_df = pd.DataFrame({
            'race_id': race_id,
            'horse_id': race_features_df['horse_id'],
            'horse_number': race_features_df['horse_number'],
            'mu': mu_pred,
            'sigma': sigma_pred,
            'nu': nu_pred
        })
        
        predictions_list.append(result_df)
        
    if not predictions_list:
        logging.error("推論結果がありません。")
        sys.exit(1)
        
    predictions_df = pd.concat(predictions_list, ignore_index=True)
    
    logging.info(f"{len(predictions_df)}件の推論結果を生成")

    # --- 5. 推論結果の保存 (修正: パス解決) ---
    output_dir = execution_root / paths.get('predictions_path', 'keibaai/data/predictions') / 'parquet'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    predictions_df['year'] = target_dt.year
    predictions_df['month'] = target_dt.month
    predictions_df['day'] = target_dt.day
    
    try:
        predictions_df.to_parquet(
            output_dir,
            engine='pyarrow',
            compression='snappy',
            partition_cols=['year', 'month', 'day'],
            existing_data_behavior='overwrite_or_ignore'
        )
        logging.info(f"推論結果をパーティション形式で {output_dir} に保存しました")

        single_file_path = output_dir / args.output_filename
        predictions_df[['horse_id', 'mu']].to_parquet(
            single_file_path,
            engine='pyarrow',
            compression='snappy'
        )
        logging.info(f"σ/ν学習用のμ推論結果を {single_file_path} に保存しました")

    except Exception as e:
        logging.error(f"推論結果のParquet保存に失敗: {e}", exc_info=True)

    logging.info("=" * 60)
    logging.info("Keiba AI モデル推論パイプライン完了")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
# src/features/generate_features.py
"""
特徴量生成 実行スクリプト
指定された日付（または期間）のパース済みデータを読み込み、
特徴量エンジニアリングを実行し、data/features/ に保存する。

仕様書 17.2 に基づく実装

実行例 (日付指定):
python src/features/generate_features.py --date 2023-10-01

実行例 (期間指定):
python src/features/generate_features.py --start_date 2023-01-01 --end_date 2023-01-31
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import yaml

# --- プロジェクトルート (keibaai/) を基準にする ---
# generate_features.py は keibaai/src/features/ にあるため、2階層上が keibaai/src/、3階層上が keibaai/
script_dir = Path(__file__).resolve().parent  # keibaai/src/features/
src_dir = script_dir.parent  # keibaai/src/
project_root = src_dir.parent  # keibaai/

sys.path.insert(0, str(src_dir))  # keibaai/src/ を追加

try:
    from pipeline_core import setup_logging, load_config
    from utils.data_utils import load_parquet_data_by_date
    from features.feature_engine import FeatureEngine
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    print(f"sys.path: {sys.path}")
    sys.exit(1)


def load_data_for_features(
    paths_config: Dict[str, Any],
    start_dt: datetime,
    end_dt: datetime,
    project_root: Path
) -> Dict[str, pd.DataFrame]:
    """
    特徴量生成に必要なデータをロードする
    仕様書 17.2 に基づく
    """
    logging.info("特徴量生成用のデータをロード中...")
    
    # --- 修正: プロジェクトルート (keibaai/) を基準にパスを構築 ---
    data_path_val = paths_config.get('data_path', 'data')
    parsed_base_dir = project_root / data_path_val / 'parsed' / 'parquet'
    
    logging.info(f"データ検索パス: {parsed_base_dir}")
    
    # 1. 出馬表 (対象期間)
    shutuba_dir = parsed_base_dir / 'shutuba'
    shutuba_df = load_parquet_data_by_date(
        shutuba_dir, start_dt, end_dt, date_col='race_date'
    )
    
    if shutuba_df.empty:
        logging.warning(f"期間 {start_dt.strftime('%Y-%m-%d')} - {end_dt.strftime('%Y-%m-%d')} の出馬表データが見つかりません。処理を中止します。")
        return {}
        
    # 2. 過去成績 (全期間 - 終了日まで)
    results_history_dir = parsed_base_dir / 'races'
    results_history_df = load_parquet_data_by_date(
        results_history_dir, None, end_dt, date_col='race_date'
    ) 

    # 3. 馬プロフィール (全期間)
    horse_profiles_dir = parsed_base_dir / 'horses'
    horse_profiles_df = load_parquet_data_by_date(
        horse_profiles_dir, None, None, date_col='birth_date'
    )

    # 4. 血統 (全期間)
    pedigree_dir = parsed_base_dir / 'pedigrees'
    pedigree_df = load_parquet_data_by_date(
        pedigree_dir, None, None, date_col=None
    )
    
    logging.info("データロード完了")
    
    return {
        "shutuba_df": shutuba_df,
        "results_history_df": results_history_df,
        "horse_profiles_df": horse_profiles_df,
        "pedigree_df": pedigree_df
    }


def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI 特徴量生成パイプライン')
    parser.add_argument(
        '--date',
        type=str,
        help='処理対象日 (YYYY-MM-DD)。指定しない場合は期間指定が必須。'
    )
    parser.add_argument(
        '--start_date',
        type=str,
        help='処理開始日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end_date',
        type=str,
        help='処理終了日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='configs/default.yaml',
        help='設定ファイルパス'
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

    # --- プロジェクトルート (keibaai/) を取得 ---
    project_root = Path(__file__).resolve().parent.parent.parent  # keibaai/

    # --- 0. 設定とロギング ---
    paths_config = {}
    try:
        # 設定ファイルのパスを keibaai/ からの相対パスとして解決
        # args.config は "keibaai/configs/default.yaml" または "configs/default.yaml" の可能性がある
        
        # まず args.config から "keibaai/" プレフィックスを除去
        config_rel = args.config.replace("keibaai/", "").replace("keibaai\\", "")
        features_config_rel = args.features_config.replace("keibaai/", "").replace("keibaai\\", "")
        
        config_path = project_root / config_rel
        features_config_path = project_root / features_config_rel
        
        logging.info(f"設定ファイルパス: {config_path}")
        
        default_config = load_config(str(config_path))
        
        # data_path を取得
        data_path_val = default_config.get('data_path', 'data')
        paths_config = default_config.copy()
        
        # ${data_path} を置換
        for key, value in paths_config.items():
            if isinstance(value, str):
                paths_config[key] = value.replace('${data_path}', data_path_val)
        
        # ログパステンプレートを取得
        logs_path_base = paths_config.get('logs_path', 'data/logs')
        log_path_template = default_config.get('logging', {}).get('log_file', 'data/logs/{YYYY}/{MM}/{DD}/features.log')
        log_path_with_base = log_path_template.replace('${logs_path}', logs_path_base)
        
        now = datetime.now()
        log_path = log_path_with_base.format(YYYY=now.year, MM=f"{now.month:02}", DD=f"{now.day:02}")
        
        # ログファイルパスを絶対パスに変換
        log_path_abs = project_root / log_path
        log_path_abs.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=args.log_level.upper(),
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            handlers=[
                logging.FileHandler(log_path_abs, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ],
            # ★★★ 修正: force=True を追加 ★★★
            force=True
        )
            
    except Exception as e:
        print(f"ロギングと設定の初期化に失敗しました: {e}")
        logging.basicConfig(level=args.log_level.upper(), format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                paths_config_raw = yaml.safe_load(f)
                
            data_path_val = paths_config_raw.get('data_path', 'data')
            paths_config = paths_config_raw.copy()
            for key, value in paths_config.items():
                if isinstance(value, str):
                    paths_config[key] = value.replace('${data_path}', data_path_val)

        except FileNotFoundError:
            logging.error(f"設定ファイルが見つかりません: {config_path}")
            sys.exit(1)
        except Exception as ex:
            logging.error(f"フォールバック設定のロード中にエラー: {ex}")
            sys.exit(1)


    logging.info("=" * 60)
    logging.info("Keiba AI 特徴量生成パイプライン開始")
    logging.info("=" * 60)

    try:
        with open(features_config_path, 'r', encoding='utf-8') as f:
            features_config = yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"特徴量設定ファイルが見つかりません: {features_config_path}")
        sys.exit(1)

    # --- 1. 日付範囲の決定 ---
    try:
        if args.date:
            start_dt = datetime.strptime(args.date, '%Y-%m-%d')
            end_dt = start_dt
        elif args.start_date and args.end_date:
            start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')
        else:
            logging.error("日付 (--date) または期間 (--start_date, --end_date) を指定してください")
            sys.exit(1)
            
        logging.info(f"処理対象期間: {start_dt.strftime('%Y-%m-%d')} - {end_dt.strftime('%Y-%m-%d')}")
            
    except ValueError as e:
        logging.error(f"日付フォーマットエラー: {e}")
        sys.exit(1)

    # --- 2. データロード ---
    data = load_data_for_features(paths_config, start_dt, end_dt, project_root)
    
    if not data:
        logging.warning("ロード対象のデータがありませんでした。処理を終了します。")
        logging.info("=" * 60)
        logging.info("Keiba AI 特徴量生成パイプライン完了 (対象なし)")
        logging.info("=" * 60)
        sys.exit(0)

    # --- 3. 特徴量エンジン初期化 ---
    engine = FeatureEngine(config=features_config)

    # --- 4. 特徴量生成 ---
    features_df = pd.DataFrame()
    if data:
        try:
            features_df = engine.generate_features(
                shutuba_df=data["shutuba_df"],
                results_history_df=data["results_history_df"],
                horse_profiles_df=data["horse_profiles_df"],
                pedigree_df=data["pedigree_df"]
            )
        except Exception as e:
            logging.error(f"特徴量生成中にエラーが発生しました: {e}", exc_info=True)
            sys.exit(1)
        
    
    if features_df.empty:
        logging.warning("特徴量生成の結果が空です。")
    else:
        # --- 5. 特徴量保存 ---
        output_dir_base = paths_config.get('features_path', 'data/features')
        output_dir = project_root / output_dir_base / 'parquet'
        partition_cols = features_config.get('output', {}).get('partition_by', ['year', 'month'])
        
        # 保存のために 'race_date' からパーティションカラムを生成
        if 'race_date' in features_df.columns:
            features_df['race_date'] = pd.to_datetime(features_df['race_date'])
            features_df['year'] = features_df['race_date'].dt.year
            features_df['month'] = features_df['race_date'].dt.month
            features_df['day'] = features_df['race_date'].dt.day
        else:
            logging.warning("race_date カラムが特徴量にありません。パーティション分割が不正確になる可能性があります。")
            if 'race_id' in features_df.columns:
                try:
                    features_df['race_date_str'] = features_df['race_id'].astype(str).str[:8]
                    features_df['race_date'] = pd.to_datetime(features_df['race_date_str'], format='%Y%m%d', errors='coerce')
                    
                    if features_df['race_date'].isnull().any():
                        logging.warning("race_id から日付への変換に失敗したレコードがあります。")
                        valid_dates = features_df['race_date'].notnull()
                        features_df.loc[valid_dates, 'year'] = features_df.loc[valid_dates, 'race_date'].dt.year
                        features_df.loc[valid_dates, 'month'] = features_df.loc[valid_dates, 'race_date'].dt.month
                        features_df.loc[valid_dates, 'day'] = features_df.loc[valid_dates, 'race_date'].dt.day
                    else:
                        features_df['year'] = features_df['race_date'].dt.year
                        features_df['month'] = features_df['race_date'].dt.month
                        features_df['day'] = features_df['race_date'].dt.day
                         
                except Exception as ex_date:
                    logging.error(f"race_id から日付を復元できませんでした: {ex_date}")
            
        engine.save_features(
            features_df=features_df,
            output_dir=str(output_dir),
            partition_cols=partition_cols
        )

    logging.info("=" * 60)
    logging.info("Keiba AI 特徴量生成パイプライン完了")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
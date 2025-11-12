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
import pyarrow as pa
import pyarrow.parquet as pq

# --- ▼▼▼ 修正 ▼▼▼ ---
# スクリプト(keibaai/src/features/generate_features.py) の4階層上が Keiba_AI_v2 (実行ルート)
execution_root = Path(__file__).resolve().parent.parent.parent.parent
# keibaai/src を sys.path に追加
sys.path.append(str(execution_root / "keibaai" / "src"))
# --- ▲▲▲ 修正 ▲▲▲ ---

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
    execution_root: Path
) -> Dict[str, pd.DataFrame]:
    """
    特徴量生成に必要なデータをロードする
    仕様書 17.2 に基づく
    """
    logging.info("特徴量生成用のデータをロード中...")
    
    # --- 修正: プロジェクトルート (keibaai/) を基準にパスを構築 ---
    # default.yaml の 'parsed_parquet_path' (例: "keibaai/data/parsed/parquet") を execution_root 基準で解決
    parsed_base_dir = execution_root / paths_config.get('parsed_parquet_path', 'keibaai/data/parsed/parquet')
    
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
    # --- ▼▼▼ 修正 ▼▼▼ ---
    # ★★★ 修正: 'results' (仕様書) ではなく、run_parsing_pipeline_local.py の
    #             実装に合わせ 'races' ディレクトリを読み込む ★★★
    results_history_dir = parsed_base_dir / 'races' # 'results' から 'races' に変更
    results_history_df = load_parquet_data_by_date(
        results_history_dir, None, end_dt, date_col='race_date'
    )
    
    # ★追加: レース結果がロードできたか確認
    if results_history_df.empty:
        logging.warning(f"レース結果(races)データが {results_history_dir} からロードできませんでした。")
        logging.warning("run_parsing_pipeline_local.py が実行されているか確認してください。")
        # 目的変数なしで続行するが、マージ時に警告が出る
    else:
        logging.info(f"{len(results_history_df)} 行のレース結果(races)をロードしました。")
    # --- ▲▲▲ 修正 ▲▲▲ ---

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
        default='keibaai/configs/default.yaml', # ★修正
        help='設定ファイルパス'
    )
    parser.add_argument(
        '--features_config',
        type=str,
        default='keibaai/configs/features.yaml', # ★修正
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

    # --- 修正 (train_mu_model.py とロジックを統一) ---
    # 実行ルート (Keiba_AI_v2/) を取得
    execution_root = Path(__file__).resolve().parent.parent.parent.parent

    # --- 0. 設定とロギング ---
    paths_config = {}
    try:
        # コマンド引数のパス (例: keibaai/configs/default.yaml) は実行場所(execution_root)からの相対パスとして解決
        config_path = execution_root / args.config
        features_config_path = execution_root / args.features_config
        
        logging.info(f"設定ファイルパス: {config_path}")
        
        default_config = load_config(str(config_path))
        
        # 'paths' セクションを取得
        paths = default_config.get('paths', {})
        paths_config = paths.copy()

        # data_path を取得 (default.yaml に基づく)
        data_path_val = paths_config.get('data_path', 'keibaai/data')
        
        # ${data_path} を置換
        for key, value in paths_config.items():
            if isinstance(value, str):
                paths_config[key] = value.replace('${data_path}', data_path_val)
        
        # ログパステンプレートを取得
        logs_path_base = paths_config.get('logs_path', 'keibaai/data/logs')
        log_path_template = default_config.get('logging', {}).get('log_file', 'keibaai/data/logs/{YYYY}/{MM}/{DD}/features.log')
        log_path_with_base = log_path_template.replace('${logs_path}', logs_path_base)
        
        now = datetime.now()
        log_path = log_path_with_base.format(YYYY=now.year, MM=f"{now.month:02}", DD=f"{now.day:02}")
        
        # ログファイルパスを絶対パスに変換
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
            
    except Exception as e:
        # この時点ではロギングが失敗している可能性があるため print を使用
        print(f"ロギングと設定の初期化に失敗しました: {e}")
        logging.basicConfig(level=args.log_level.upper(), format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        try:
            # フォールバック（設定ファイルの手動読み込み試行）
            with open(config_path, 'r', encoding='utf-8') as f:
                paths_config_raw = yaml.safe_load(f).get('paths', {})
                
            data_path_val = paths_config_raw.get('data_path', 'keibaai/data')
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
    # --- 修正ここまで ---


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
    data = load_data_for_features(paths_config, start_dt, end_dt, execution_root) # ★修正
    
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
            # --- ▼▼▼ 修正 ▼▼▼ ---
            # 目的変数 (finish_position等) を shutuba_df にマージする
            shutuba_df = data["shutuba_df"]
            results_df = data["results_history_df"] # results_history_df には全期間の結果が含まれる

            target_cols = ['race_id', 'horse_id', 'finish_position', 'finish_time_seconds']
            
            # ★修正: results_df が空でないか、カラムが存在するかをチェック
            if results_df.empty:
                logging.warning("レース結果(results)データが空のため、目的変数をマージできません。")
            elif not all(col in results_df.columns for col in target_cols):
                missing_cols = [col for col in target_cols if col not in results_df.columns]
                logging.warning(f"レース結果(results)に目的変数カラム {missing_cols} が不足しているため、マージをスキップします。")
            else:
                # マージキーの型を合わせる
                shutuba_df['race_id'] = shutuba_df['race_id'].astype(str)
                shutuba_df['horse_id'] = shutuba_df['horse_id'].astype(str)
                results_df['race_id'] = results_df['race_id'].astype(str)
                results_df['horse_id'] = results_df['horse_id'].astype(str)

                # shutuba_df (対象期間) に、対応するレース結果 (全期間から) をマージ
                shutuba_df = shutuba_df.merge(
                    results_df[target_cols],
                    on=['race_id', 'horse_id'],
                    how='left' # 出馬表がベース
                )
                logging.info("特徴量生成のため、出馬表にレース結果（目的変数）をマージしました。")
            
            features_df = engine.generate_features(
                shutuba_df=shutuba_df, # ★修正: マージ済みの shutuba_df を渡す
                results_history_df=data["results_history_df"],
                horse_profiles_df=data["horse_profiles_df"],
                pedigree_df=data["pedigree_df"]
            )
            # --- ▲▲▲ 修正 ▲▲▲ ---
            
        except Exception as e:
            logging.error(f"特徴量生成中にエラーが発生しました: {e}", exc_info=True)
            sys.exit(1)
        
    
    if features_df.empty:
        logging.warning("特徴量生成の結果が空です。")
    else:
        # --- 5. 特徴量保存 ---
        # --- 修正 ---
        output_dir_base = paths_config.get('features_path', 'keibaai/data/features')
        output_dir = execution_root / output_dir_base / 'parquet' 
        # --- 修正ここまで ---
        
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
        
        # ★修正: engine.save_features は仕様書になく、実装もないため、ここで直接保存する
        try:
            # --- ▼▼▼ 修正: pandas.to_parquet から pyarrow.write_to_dataset に変更 ▼▼▼
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # DataFrame を Arrow Table に変換
            table = pa.Table.from_pandas(features_df, preserve_index=False)
            
            pq.write_to_dataset(
                table,
                root_path=output_dir,
                partition_cols=partition_cols,
                existing_data_behavior='delete_matching' # 'overwrite_or_ignore' から変更
            )
            # --- ▲▲▲ 修正 ▲▲▲
            
            logging.info(f"{len(features_df)}行を {output_dir} に保存しました")

            # 特徴量リストを保存 (仕様書 6.6)
            feature_names_path = execution_root / output_dir_base / 'feature_names.yaml' # ★修正
            feature_names_data = {'feature_names': engine.feature_names}
            with open(feature_names_path, 'w', encoding='utf-8') as f:
                yaml.dump(feature_names_data, f, allow_unicode=True)
            logging.info(f"特徴量リストを {feature_names_path} に保存しました")

        except Exception as e:
            logging.error(f"特徴量の保存に失敗: {e}", exc_info=True)

    logging.info("=" * 60)
    logging.info("Keiba AI 特徴量生成パイプライン完了")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
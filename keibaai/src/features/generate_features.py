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

# --- 修正箇所 1: sys.path の設定 ---
# プロジェクトの 'src' ディレクトリをパスに追加
# これにより、'pipeline_core' や 'utils' を直接インポート可能にする
src_root = Path(__file__).resolve().parent.parent
if str(src_root) not in sys.path:
    sys.path.append(str(src_root))
# --- 修正箇所 1 終了 ---


# src.pipeline_core が見つからない場合のエラーハンドリング
try:
    # --- 修正箇所 2: インポート文 (src. プレフィックスを削除) ---
    from pipeline_core import setup_logging, load_config
    # --- 修正箇所 2 終了 ---
except ImportError:
    print("エラー: pipeline_core が見つかりません。")
    print("プロジェクトルートが正しく設定されているか確認してください。")
    print(f"sys.path: {sys.path}")
    sys.exit(1)

# --- 修正箇所 3: インポート文 (src. プレフィックスを削除) ---
from utils.data_utils import load_parquet_data_by_date
from features.feature_engine import FeatureEngine
# --- 修正箇所 3 終了 ---


def load_data_for_features(
    paths_config: Dict[str, Any],
    start_dt: datetime,
    end_dt: datetime
) -> Dict[str, pd.DataFrame]:
    """
    特徴量生成に必要なデータをロードする
    (仕様書 17.2 に基づく)
    """
    logging.info("特徴量生成用のデータをロード中...")
    
    # --- 修正箇所 4: paths_config の参照方法を修正 ---
    # paths_config は 'paths' キーを含む辞書、または 'paths' キーそのもの
    # default.yaml に 'paths:' がないため、paths_config そのものを参照する
    paths = paths_config.get('paths', paths_config)
    
    # 変数展開済みのパスを取得
    parsed_data_path = paths.get('parsed_data_path', 'data/parsed')
    parsed_base_dir = Path(parsed_data_path) / 'parquet'
    # --- 修正箇所 4 終了 ---
    
    # 1. 出馬表 (対象期間)
    shutuba_dir = parsed_base_dir / 'shutuba'
    shutuba_df = load_parquet_data_by_date(
        shutuba_dir, start_dt, end_dt, date_col='race_date'
    )
    
    if shutuba_df.empty:
        # ログレベルを ERROR から WARNING に変更 (データがない日は正常終了とするため)
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
        pedigree_dir, None, None, date_col=None # 血統は日付カラムなし
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

    # --- 0. 設定とロギング ---
    paths_config = {}
    try:
        # pipeline_core を使って設定をロード
        default_config = load_config(args.config)

        # --- ★ 修正: 変数展開の実施 ---
        # 1. data_path を取得
        data_path_val = default_config.get('data_path', 'data')

        # 2. paths_config を準備 (default.yaml に 'paths:' がないため、default_config をコピー)
        paths_config = default_config.copy()

        # 3. ${data_path} を置換
        for key, value in paths_config.items():
            if isinstance(value, str):
                paths_config[key] = value.replace('${data_path}', data_path_val)
        
        # 4. logs_path を取得 (置換後の値)
        logs_path_base = paths_config.get('logs_path', 'data/logs')

        # 5. ログパステンプレートを取得
        log_path_template = default_config.get('logging', {}).get('log_file', 'data/logs/{YYYY}/{MM}/{DD}/features.log')

        # 6. ${logs_path} を置換
        log_path_with_base = log_path_template.replace('${logs_path}', logs_path_base)
        
        now = datetime.now()
        # 7. YYYY/MM/DD をフォーマット
        log_path = log_path_with_base.format(YYYY=now.year, MM=f"{now.month:02}", DD=f"{now.day:02}")
        # --- ★ 修正終了 ---

        Path(log_path).parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=args.log_level.upper(),
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            handlers=[
                logging.FileHandler(log_path, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
            
    except Exception as e:
        print(f"ロギングと設定の初期化に失敗しました: {e}")
        # フォールバック (簡易ロギング)
        logging.basicConfig(level=args.log_level.upper(), format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        try:
             with open(args.config, 'r', encoding='utf-8') as f:
                paths_config_raw = yaml.safe_load(f)
                
                # --- ★ 修正: フォールバックでも変数展開 ---
                data_path_val = paths_config_raw.get('data_path', 'data')
                paths_config = paths_config_raw.copy()
                for key, value in paths_config.items():
                    if isinstance(value, str):
                        paths_config[key] = value.replace('${data_path}', data_path_val)
                # --- ★ 修正終了 ---

        except FileNotFoundError:
             logging.error(f"設定ファイルが見つかりません: {args.config}")
             sys.exit(1)
        except Exception as ex:
             logging.error(f"フォールバック設定のロード中にエラー: {ex}")
             sys.exit(1)


    logging.info("=" * 60)
    logging.info("Keiba AI 特徴量生成パイプライン開始")
    logging.info("=" * 60)

    try:
        with open(args.features_config, 'r', encoding='utf-8') as f:
            features_config = yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"特徴量設定ファイルが見つかりません: {args.features_config}")
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
    # ★ 修正: default.yaml に 'paths:' キーがないため、paths_config を直接渡す
    data = load_data_for_features(paths_config, start_dt, end_dt)
    
    if not data:
        # (load_data_for_features内でWarningログ出力済み)
        logging.warning("ロード対象のデータがありませんでした。処理を終了します。")
        logging.info("=" * 60)
        logging.info("Keiba AI 特徴量生成パイプライン完了 (対象なし)")
        logging.info("=" * 60)
        sys.exit(0) # データがない場合は正常終了

    # --- 3. 特徴量エンジン初期化 ---
    engine = FeatureEngine(config=features_config)

    # --- 4. 特徴量生成 ---
    features_df = pd.DataFrame()
    if data: # データがある場合のみ実行
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
        # sys.exit(1) # ログでは警告に留め、パイプラインは停止しない
    else:
        # --- 5. 特徴量保存 ---
        # ★ 修正: default.yaml に 'paths:' キーがないため、paths_config を直接参照
        output_dir_base = paths_config.get('features_path', 'data/features')
        output_dir = Path(output_dir_base) / 'parquet'
        partition_cols = features_config.get('output', {}).get('partition_by', ['year', 'month'])
        
        # 保存のために 'race_date' からパーティションカラムを生成
        if 'race_date' in features_df.columns:
            features_df['race_date'] = pd.to_datetime(features_df['race_date'])
            features_df['year'] = features_df['race_date'].dt.year
            features_df['month'] = features_df['race_date'].dt.month
            features_df['day'] = features_df['race_date'].dt.day
        else:
            logging.warning("race_date カラムが特徴量にありません。パーティション分割が不正確になる可能性があります。")
            # race_id からの復元を試みる
            if 'race_id' in features_df.columns:
                try:
                    # 'race_id' が YYYYMMDD... 形式であることを前提とする (仕様書 3.2)
                    features_df['race_date_str'] = features_df['race_id'].astype(str).str[:8]
                    features_df['race_date'] = pd.to_datetime(features_df['race_date_str'], format='%Y%m%d', errors='coerce')
                    
                    # NaT (日付変換失敗) がないか確認
                    if features_df['race_date'].isnull().any():
                         logging.warning("race_id から日付への変換に失敗したレコードがあります。")
                         # NaT を含まないデータで year/month/day を設定
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
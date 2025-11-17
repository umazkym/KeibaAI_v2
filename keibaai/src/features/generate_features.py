#!/usr/bin/env python3
# src/features/generate_features.py
"""
特徴量生成 実行スクリプト (完全修正版)
指定された日付（または期間）のパース済みデータを読み込み、
特徴量エンジニアリングを実行し、data/features/ に保存する。

実行例 (期間指定):
python src/features/generate_features.py --start_date 2023-01-01 --end_date 2023-12-31
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import yaml
from tqdm import tqdm

# --- プロジェクトルート (keibaai/) を基準にする ---
script_dir = Path(__file__).resolve().parent  # keibaai/src/features/
src_dir = script_dir.parent  # keibaai/src/
project_root = src_dir.parent  # keibaai/

sys.path.insert(0, str(src_dir))

try:
    from pipeline_core import setup_logging
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
    """特徴量生成に必要なデータをロードする"""
    logging.info("=" * 60)
    logging.info("フェーズ1/3: データロード開始")
    logging.info("=" * 60)

    data_path_val = paths_config.get('data_path', 'data')
    parsed_base_dir = project_root / data_path_val / 'parsed' / 'parquet'

    logging.info(f"データ検索パス: {parsed_base_dir}")

    # 1. 出馬表 (対象期間)
    logging.info("  → 出馬表データを読み込み中...")
    shutuba_dir = parsed_base_dir / 'shutuba'
    shutuba_df = load_parquet_data_by_date(
        shutuba_dir, start_dt, end_dt, date_col='race_date'
    )

    if shutuba_df.empty:
        logging.warning(f"期間 {start_dt.strftime('%Y-%m-%d')} - {end_dt.strftime('%Y-%m-%d')} の出馬表データが見つかりません。")
        return {}
    logging.info(f"  ✓ 出馬表: {len(shutuba_df):,}行ロード完了")

    # 2. 過去成績 (全期間 - 終了日まで)
    logging.info("  → 過去成績データを読み込み中...")
    results_history_dir = parsed_base_dir / 'races'
    results_history_df = load_parquet_data_by_date(
        results_history_dir, None, end_dt, date_col='race_date'
    )
    logging.info(f"  ✓ 過去成績: {len(results_history_df):,}行ロード完了")

    # 3. 馬プロフィール (全期間)
    logging.info("  → 馬プロフィールデータを読み込み中...")
    horse_profiles_dir = parsed_base_dir / 'horses'
    horse_profiles_df = load_parquet_data_by_date(
        horse_profiles_dir, None, None, date_col='birth_date'
    )
    logging.info(f"  ✓ 馬プロフィール: {len(horse_profiles_df):,}行ロード完了")

    # 4. 血統 (全期間)
    logging.info("  → 血統データを読み込み中...")
    pedigree_dir = parsed_base_dir / 'pedigrees'
    pedigree_df = load_parquet_data_by_date(
        pedigree_dir, None, None, date_col=None
    )
    logging.info(f"  ✓ 血統: {len(pedigree_df):,}行ロード完了")

    logging.info("=" * 60)
    logging.info(f"データロード完了: 合計 {len(shutuba_df) + len(results_history_df):,}行")
    logging.info("=" * 60)
    
    return {
        "shutuba_df": shutuba_df,
        "results_history_df": results_history_df,
        "horse_profiles_df": horse_profiles_df,
        "pedigree_df": pedigree_df
    }


def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI 特徴量生成パイプライン')
    parser.add_argument('--date', type=str, help='処理対象日 (YYYY-MM-DD)')
    parser.add_argument('--start_date', type=str, help='処理開始日 (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, help='処理終了日 (YYYY-MM-DD)')
    parser.add_argument('--config', type=str, default='configs/default.yaml', help='設定ファイルパス')
    parser.add_argument('--features_config', type=str, default='configs/features.yaml', help='特徴量設定ファイルパス')
    parser.add_argument('--log_level', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])

    args = parser.parse_args()

    # プロジェクトルート (keibaai/) を取得
    project_root = Path(__file__).resolve().parent.parent.parent

    # --- 0. 設定とロギング ---
    paths_config = {}
    try:
        # 設定ファイルパスを解決
        config_rel = args.config.replace("keibaai/", "").replace("keibaai\\", "")
        features_config_rel = args.features_config.replace("keibaai/", "").replace("keibaai\\", "")
        
        config_path = project_root / config_rel
        features_config_path = project_root / features_config_rel
        
        # 設定ファイルをロード
        with open(config_path, 'r', encoding='utf-8') as f:
            default_config = yaml.safe_load(f)
        
        # data_path を取得
        data_path_val = default_config.get('data_path', 'data')
        paths_config = default_config.copy()
        
        # ${data_path} を置換
        for key, value in paths_config.items():
            if isinstance(value, str):
                paths_config[key] = value.replace('${data_path}', data_path_val)
        
        # ロギング設定
        logging_config = default_config.get('logging', {})
        log_file = project_root / logging_config.get('log_file', 'logs/features.log')
        log_format = logging_config.get('log_format', '%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        setup_logging(args.log_level.upper(), str(log_file), log_format)
            
    except Exception as e:
        # フォールバック: 簡易ロギング設定
        print(f"設定の初期化に失敗しました: {e}")
        logging.basicConfig(
            level=args.log_level.upper(),
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        
        # 設定の再ロード試行
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                paths_config_raw = yaml.safe_load(f)
            data_path_val = paths_config_raw.get('data_path', 'data')
            paths_config = paths_config_raw.copy()
            for key, value in paths_config.items():
                if isinstance(value, str):
                    paths_config[key] = value.replace('${data_path}', data_path_val)
        except Exception as ex:
            logging.error(f"設定のロードに完全に失敗しました: {ex}")
            sys.exit(1)

    logging.info("=" * 60)
    logging.info("Keiba AI 特徴量生成パイプライン開始")
    logging.info("=" * 60)

    # 特徴量設定をロード
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
        sys.exit(0)

    # --- 3. 特徴量エンジン初期化とデータ前処理 ---
    logging.info("=" * 60)
    logging.info("フェーズ2/3: 特徴量生成開始")
    logging.info("=" * 60)
    logging.info("  → 特徴量エンジンを初期化中...")

    # history_df に血統情報をマージ (エンティティ集計で sire_id などを使うため)
    if "horse_profiles_df" in data and not data["horse_profiles_df"].empty:
        ped_cols = ['horse_id', 'sire_id', 'damsire_id']
        # 必要なカラムが存在するか確認
        cols_to_merge = [col for col in ped_cols if col in data["horse_profiles_df"].columns]
        if 'horse_id' in cols_to_merge and len(cols_to_merge) > 1:
            logging.info(f"results_history_df に血統情報 {cols_to_merge} をマージします。")
            ped_info = data["horse_profiles_df"][cols_to_merge]
            data["results_history_df"] = data["results_history_df"].merge(ped_info, on='horse_id', how='left')
        else:
            logging.warning("horse_profiles_df に十分な血統情報（sire_idなど）が見つかりませんでした。")

    engine = FeatureEngine(config_path=str(features_config_path))
    logging.info("  ✓ 特徴量エンジン初期化完了")

    # --- 4. 特徴量生成 ---
    logging.info("  → 特徴量を生成中...")
    try:
        features_df = engine.generate_features(
            shutuba_df=data["shutuba_df"],
            results_history_df=data["results_history_df"],
            horse_profiles_df=data["horse_profiles_df"]
        )
    except Exception as e:
        logging.error(f"特徴量生成中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)

    if features_df.empty:
        logging.warning("特徴量生成の結果が空です。")
        sys.exit(0)

    logging.info(f"  ✓ 特徴量生成完了: {len(features_df):,}行 x {len(features_df.columns)}列")
    logging.info("=" * 60)

    # --- 5. 特徴量保存 ---
    logging.info("=" * 60)
    logging.info("フェーズ3/3: 特徴量保存開始")
    logging.info("=" * 60)
    output_dir_base = paths_config.get('features_path', 'data/features')
    output_dir = project_root / output_dir_base / 'parquet'
    partition_cols = features_config.get('output', {}).get('partition_by', ['year', 'month'])
    
    # race_date からパーティションカラムを生成
    if 'race_date' in features_df.columns:
        features_df['race_date'] = pd.to_datetime(features_df['race_date'])
        features_df['year'] = features_df['race_date'].dt.year
        features_df['month'] = features_df['race_date'].dt.month
        features_df['day'] = features_df['race_date'].dt.day
    else:
        logging.warning("race_date カラムがありません。race_id から復元を試みます。")
        if 'race_id' in features_df.columns:
            try:
                features_df['race_date_str'] = features_df['race_id'].astype(str).str[:8]
                features_df['race_date'] = pd.to_datetime(features_df['race_date_str'], format='%Y%m%d', errors='coerce')
                features_df['year'] = features_df['race_date'].dt.year
                features_df['month'] = features_df['race_date'].dt.month
                features_df['day'] = features_df['race_date'].dt.day
            except Exception as ex_date:
                logging.error(f"race_id から日付を復元できませんでした: {ex_date}")
        
    logging.info(f"  → パーティション化して保存中... (partition_by={partition_cols})")
    engine.save_features(
        features_df=features_df,
        output_dir=str(output_dir),
        partition_cols=partition_cols
    )
    logging.info(f"  ✓ 保存完了: {output_dir}")

    logging.info("=" * 60)
    logging.info("✓ Keiba AI 特徴量生成パイプライン完了")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
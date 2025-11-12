#!/usr/bin/env python3
# keibaai/src/features/generate_features.py (修正版)
"""
特徴量生成 実行スクリプト (パス解決を統一)
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import yaml

# パス解決: Keiba_AI_v2 (実行ルート) の特定
execution_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(execution_root / "keibaai" / "src"))

try:
    from pipeline_core import load_config
    from utils.data_utils import load_parquet_data_by_date
    from features.feature_engine import FeatureEngine
except ImportError as e:
    print(f"エラー: {e}")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True)
    parser.add_argument('--end_date', type=str, required=True)
    parser.add_argument('--config', type=str, default='keibaai/configs/default.yaml')
    parser.add_argument('--features_config', type=str, default='keibaai/configs/features.yaml')
    args = parser.parse_args()
    
    # 設定読み込み
    config_path = execution_root / args.config
    config = load_config(str(config_path))
    
    # ログ設定
    now = datetime.now()
    log_path = execution_root / "keibaai" / "data" / "logs" / f"{now.year}" / f"{now.month:02}" / f"{now.day:02}" / "features.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    
    logging.info("=" * 60)
    logging.info("Keiba AI 特徴量生成パイプライン開始")
    logging.info("=" * 60)
    
    # 日付範囲
    start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')
    logging.info(f"処理対象期間: {args.start_date} - {args.end_date}")
    
    # データロード
    logging.info("特徴量生成用のデータをロード中...")
    parsed_base = execution_root / "keibaai" / "data" / "parsed" / "parquet"
    logging.info(f"データ検索パス: {parsed_base}")
    
    # 出馬表
    shutuba_df = load_parquet_data_by_date(
        parsed_base / 'shutuba', start_dt, end_dt, date_col='race_date'
    )
    
    # **修正: 'races' ディレクトリから読み込む**
    results_df = load_parquet_data_by_date(
        parsed_base / 'races',  # ← 'results' から 'races' に変更
        None, end_dt, date_col='race_date'
    )
    
    if results_df.empty:
        logging.error("レース結果(races)データがロードできません")
        sys.exit(1)
    
    logging.info(f"{len(results_df)} 行のレース結果(races)をロードしました。")
    
    # 馬・血統
    horse_profiles_df = load_parquet_data_by_date(
        parsed_base / 'horses', None, None, date_col='birth_date'
    )
    pedigree_df = load_parquet_data_by_date(
        parsed_base / 'pedigrees', None, None, date_col=None
    )
    
    logging.info("データロード完了")
    
    # 目的変数マージ
    shutuba_df['race_id'] = shutuba_df['race_id'].astype(str)
    shutuba_df['horse_id'] = shutuba_df['horse_id'].astype(str)
    results_df['race_id'] = results_df['race_id'].astype(str)
    results_df['horse_id'] = results_df['horse_id'].astype(str)
    
    target_cols = ['race_id', 'horse_id', 'finish_position', 'finish_time_seconds']
    if all(col in results_df.columns for col in target_cols):
        shutuba_df = shutuba_df.merge(
            results_df[target_cols],
            on=['race_id', 'horse_id'],
            how='left'
        )
        logging.info("特徴量生成のため、出馬表にレース結果(目的変数)をマージしました。")
    
    # 特徴量生成
    with open(execution_root / args.features_config, 'r') as f:
        features_config = yaml.safe_load(f)
    
    engine = FeatureEngine(config=features_config)
    
    logging.info("特徴量生成開始")
    features_df = engine.generate_features(
        shutuba_df=shutuba_df,
        results_history_df=results_df,
        horse_profiles_df=horse_profiles_df,
        pedigree_df=pedigree_df
    )
    logging.info(f"特徴量生成完了: {len(engine.feature_names)}個の特徴量を生成しました")
    
    # 保存
    output_dir = execution_root / "keibaai" / "data" / "features" / "parquet"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    features_df.to_parquet(output_dir, partition_cols=['year', 'month'], engine='pyarrow')
    logging.info(f"{len(features_df)}行を {output_dir} に保存しました")
    
    # 特徴量リスト保存
    feature_names_path = execution_root / "keibaai" / "data" / "features" / "feature_names.yaml"
    with open(feature_names_path, 'w', encoding='utf-8') as f:
        yaml.dump({'feature_names': engine.feature_names}, f, allow_unicode=True)
    
    logging.info(f"特徴量リストを {feature_names_path} に保存しました")
    logging.info("=" * 60)
    logging.info("Keiba AI 特徴量生成パイプライン完了")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
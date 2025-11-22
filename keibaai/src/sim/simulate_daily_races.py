#!/usr/bin/env python3
# src/sim/simulate_daily_races.py
"""
日次シミュレーション 実行スクリプト
指定された日付の推論結果（μ, σ, ν）を読み込み、
モンテカルロシミュレーションを実行し、data/simulations/ に保存する。

仕様書 17.4章 に基づく実装

実行例:
python src/sim/simulate_daily_races.py --date 2023-10-01 --K 1000
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import json
import yaml
import pandas as pd

# プロジェクトルート（keibaai/）をパスに追加
# simulate_daily_races.pyの位置: keibaai/src/sim/simulate_daily_races.py
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root)) # keibaai/ を追加
sys.path.insert(0, str(project_root / 'src')) # keibaai/src/ を追加

try:
    from src.pipeline_core import setup_logging, load_config
    from src.utils.data_utils import load_parquet_data_by_date
    from src.sim.simulator import RaceSimulator
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    print("プロジェクトルートが正しく設定されているか確認してください。")
    print(f"sys.path: {sys.path}")
    sys.exit(1)


def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI 日次シミュレーション')
    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='シミュレーション対象日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--K',
        type=int,
        default=1000,
        help='シミュレーション回数 (K)'
    )
    parser.add_argument(
        '--model_id',
        type=str,
        default='latest',
        help='使用したモデルのID (保存用)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='configs/default.yaml',
        help='設定ファイルパス'
    )
    parser.add_argument(
        '--log_level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='ログレベル'
    )
    parser.add_argument(
        '--predictions_path',
        type=str,
        default=None,
        help='予測データのパス (Parquetファイルまたはディレクトリ)'
    )
    parser.add_argument(
        '--output_dir',
        type=str,
        default=None,
        help='シミュレーション結果の出力ディレクトリ'
    )

    args = parser.parse_args()

    # --- 0. 設定とロギング ---
    config = load_config(args.config)
    paths = config.get('paths', {})

    # ログ設定
    log_path_template = config.get('logging', {}).get('log_file', 'data/logs/{YYYY}/{MM}/{DD}/simulation.log')
    logs_path_val = paths.get('logs_path', 'data/logs')
    
    now = datetime.now()
    # {logs_path}が含まれている場合の対策
    log_path = log_path_template.format(
        YYYY=now.year, 
        MM=f"{now.month:02}", 
        DD=f"{now.day:02}",
        logs_path=logs_path_val # これを追加
    )
    
    # project_root基準の絶対パスに変換
    if not Path(log_path).is_absolute():
        log_path = project_root / log_path
        
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=args.log_level.upper(),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True # 既存の設定を上書き
    )
    print("DEBUG: Logging configured.") # デバッグ用print
    
    # Numbaのログを抑制
    logging.getLogger('numba').setLevel(logging.WARNING)

    logging.info("=" * 60)
    logging.info("Keiba AI 日次シミュレーション開始")
    logging.info("=" * 60)
    logging.info(f"対象日: {args.date}, K={args.K}")

    # シミュレーション設定
    sim_config = config.get('simulation', {})
    sim_config['K'] = args.K
    
    # --- 1. 日付範囲の決定 ---
    try:
        target_dt = datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError as e:
        logging.error(f"日付フォーマットエラー: {e}")
        sys.exit(1)

    # --- 2. 推論データのロード ---
    if args.predictions_path:
        predictions_dir = Path(args.predictions_path)
        # 相対パスなら絶対パスに変換
        if not predictions_dir.is_absolute():
            predictions_dir = project_root / predictions_dir
    else:
        predictions_dir = Path(paths.get('predictions_path', 'data/predictions')) / 'parquet'
    
    logging.info(f"推論データパス: {predictions_dir}")
    
    # load_parquet_data_by_date はパーティション化されたディレクトリを読むことを想定
    # 単一ファイルの場合は直接読み込む
    # load_parquet_data_by_date はパーティション化されたディレクトリを読むことを想定
    # 単一ファイルの場合は直接読み込む
    if predictions_dir.is_file():
        # 予測データのロード
        predictions_path = predictions_dir
        logging.info(f"予測データをロード中: {predictions_path}")
        predictions_df = pd.read_parquet(predictions_path)
        
        # 重複削除 (race_id, horse_idの組み合わせでユニークにする)
        before_len = len(predictions_df)
        predictions_df = predictions_df.drop_duplicates(subset=['race_id', 'horse_id'])
        after_len = len(predictions_df)
        if before_len != after_len:
            logging.warning(f"重複行を削除しました: {before_len} -> {after_len} (削除数: {before_len - after_len})")
        
        # 日付フィルタリング
        if 'race_date' in predictions_df.columns:
            predictions_df['race_date'] = pd.to_datetime(predictions_df['race_date'])
            target_date = pd.to_datetime(args.date)
            predictions_df = predictions_df[predictions_df['race_date'] == target_date]
            logging.info(f"日付フィルタリング後の行数: {len(predictions_df)}")
    else:
        predictions_df = load_parquet_data_by_date(predictions_dir, target_dt, target_dt, date_col='race_date')
        # こちらも念のため重複削除
        predictions_df = predictions_df.drop_duplicates(subset=['race_id', 'horse_id'])
    
    if predictions_df.empty:
        logging.error(f"{args.date} の予測データが見つかりません。")
        sys.exit(1)

    # --- 3. シミュレータ初期化 ---
    # logs_pathを明示的に設定（simulator.pyのデフォルト値に依存しない場合）
    if 'logs_path' not in sim_config:
        sim_config['logs_path'] = 'data/logs'
        
    simulator = RaceSimulator(config=sim_config)
    
    # --- 4. レースごとにシミュレーション実行 ---
    race_ids = predictions_df['race_id'].unique()
    logging.info(f"対象レース数: {len(race_ids)}")
    
    logging.info(f"{len(race_ids)} レースのシミュレーションを実行します")
    
    for i, race_id in enumerate(race_ids, 1):
        logging.info(f"--- レース {i}/{len(race_ids)} ({race_id}) ---")
        
        race_data = predictions_df[predictions_df['race_id'] == race_id].copy()
        
        if len(race_data) < 2:
            logging.warning(f"レース {race_id} の出走馬が少なすぎるためスキップします")
            continue
            
        # パラメータ準備
        mu = race_data['mu'].values
        sigma = race_data['sigma'].values
        nu = race_data['nu'].iloc[0] # レース単位で共通
        horse_numbers = race_data['horse_number'].values
        
        seed = sim_config.get('seed', 42) + i # レースごとにシードを変更
        
        try:
            # 実行
            sim_results = simulator.simulate_race(
                mu=mu,
                sigma=sigma,
                nu=nu,
                horse_numbers=horse_numbers,
                K=args.K,
                seed=seed
            )
            
            if not sim_results:
                 logging.warning(f"レース {race_id}: シミュレーション結果が空です。")
                 continue

            # 保存
            if args.output_dir:
                output_dir_str = args.output_dir
            else:
                output_dir_str = paths.get('simulations_path', 'data/simulations')
                
            simulator.save_simulation(
                race_id=race_id,
                model_id=args.model_id,
                simulation_results=sim_results,
                output_dir=output_dir_str
            )
            
        except Exception as e:
            logging.error(f"レース {race_id} のシミュレーションに失敗: {e}", exc_info=True)

    logging.info("=" * 60)
    logging.info("Keiba AI 日次シミュレーション完了")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
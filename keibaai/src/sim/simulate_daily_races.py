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

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

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

    args = parser.parse_args()

    # --- 0. 設定とロギング ---
    try:
        config = load_config(args.config)
        paths = config.get('paths', {})

        # ログ設定 (簡易版)
        log_path_template = config.get('logging', {}).get('log_file', 'data/logs/{YYYY}/{MM}/{DD}/simulation.log')
        now = datetime.now()
        log_path = log_path_template.format(YYYY=now.year, MM=f"{now.month:02}", DD=f"{now.day:02}")
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=args.log_level.upper(),
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            handlers=[
                logging.FileHandler(log_path, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
            
    except FileNotFoundError as e:
        logging.error(f"設定ファイルが見つかりません: {e}")
        sys.exit(1)


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
    predictions_dir = Path(paths.get('predictions_path', 'data/predictions')) / 'parquet'
    
    # load_parquet_data_by_date はパーティション化されたディレクトリを読むことを想定
    predictions_df = load_parquet_data_by_date(predictions_dir, target_dt, target_dt, date_col='race_date')
    
    if predictions_df.empty:
        # もしパーティション化されておらず、単一ファイル (predict.pyのフォールバック) の場合
        single_file_path = predictions_dir / "mu_predictions.parquet" # (これはμのみなので不十分)
        
        # predict.py が 'year', 'month', 'day' カラムを付与していることを期待
        # load_parquet_data_by_date がパーティションを読み、日付でフィルタするはず
        
        logging.warning(f"{args.date} の推論データが見つかりません (ディレクトリ: {predictions_dir})。処理を終了します。")
        sys.exit(0)

    # --- 3. シミュレータ初期化 ---
    simulator = RaceSimulator(config=sim_config)
    
    # --- 4. レースごとにシミュレーション実行 ---
    race_ids = predictions_df['race_id'].unique()
    
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
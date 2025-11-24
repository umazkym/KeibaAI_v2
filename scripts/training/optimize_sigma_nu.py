#!/usr/bin/env python3
# src/models/optimize_sigma_nu.py
"""
σ・ν戦略のパラメータ最適化スクリプト
グリッドサーチにより最適な閾値(ev_threshold, nu_threshold)を探索する
"""

import argparse
import logging
import sys
import os
from datetime import datetime
from pathlib import Path
import yaml
import pandas as pd
import itertools

# プロジェクトルート
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

try:
    from keibaai.src.pipeline_core import setup_logging
    from keibaai.src.models.evaluate_model_advanced import load_data_and_models, evaluate_strategy
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Keiba AI σ・νパラメータ最適化')
    parser.add_argument('--model_dir', type=str, required=True, help='モデルディレクトリ')
    parser.add_argument('--start_date', type=str, required=True, help='評価開始日')
    parser.add_argument('--end_date', type=str, required=True, help='評価終了日')
    parser.add_argument('--config', type=str, default='configs/default.yaml', help='設定ファイル')
    parser.add_argument('--mc_samples', type=int, default=1000, help='モンテカルロサンプル数')
    
    args = parser.parse_args()

    # 設定読み込み
    config_path = project_root / args.config
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # ロギング設定
    data_path = project_root / config.get('data_path', 'data')
    logs_path = data_path / 'logs'
    log_file = logs_path / f"optimize_sigma_nu_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    setup_logging(log_level='INFO', log_file=str(log_file), log_format=log_format)

    logging.info("=" * 60)
    logging.info("σ・νパラメータ最適化開始")
    logging.info("=" * 60)

    # データとモデルのロード（1回だけ実行）
    logging.info("データとモデルをロード中...")
    final_df, mu_model, sigma_model, sigma_features, nu_model, nu_features = load_data_and_models(args, config)
    logging.info("ロード完了")

    # パラメータグリッド
    ev_thresholds = [0.8, 1.0, 1.2, 1.5, 2.0]
    nu_thresholds = [0.8, 0.9, 1.0, 1.1, 1.2, 1.5, 2.0]
    
    results = []
    
    total_combinations = len(ev_thresholds) * len(nu_thresholds)
    current_iter = 0

    logging.info(f"合計 {total_combinations} 通りの組み合わせを評価します")

    for ev_th, nu_th in itertools.product(ev_thresholds, nu_thresholds):
        current_iter += 1
        logging.info(f"[{current_iter}/{total_combinations}] 評価中: EV>{ev_th}, ν<{nu_th}")
        
        # Combined戦略で評価
        res = evaluate_strategy(
            final_df, mu_model, sigma_model, sigma_features, nu_model, nu_features,
            strategy='combined',
            ev_threshold=ev_th,
            nu_threshold=nu_th,
            mc_samples=args.mc_samples
        )
        
        results.append(res)
        hits_count = int(res['hit_rate'] * res['total_races']) if res['total_races'] > 0 else 0
        race_count = res['total_bet'] // 100  # 1レース100円
        logging.info(f"  -> ROI: {res['roi']:.2%}, 的中: {hits_count}/{race_count}レース, 総額: {res['total_bet']:,}円")

    # 結果をDataFrameに変換
    results_df = pd.DataFrame(results)
    
    # 保存
    output_dir = data_path / 'evaluation'
    output_dir.mkdir(exist_ok=True)
    output_csv = output_dir / f"optimization_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    results_df.to_csv(output_csv, index=False)
    logging.info(f"全結果を保存しました: {output_csv}")

    # ベスト結果の表示 (ROI順)
    # ベット数が少なすぎるものは除外 (例: 10レース未満)
    valid_results = results_df[results_df['total_bet'] >= 1000] # 10レース以上 (1レース100円換算)
    
    if not valid_results.empty:
        best_roi = valid_results.sort_values('roi', ascending=False).iloc[0]
        logging.info("=" * 60)
        logging.info("【ベストパラメータ (ROI最大, ベット数>=10)】")
        logging.info(f"EV Threshold: {best_roi['ev_threshold']}")
        logging.info(f"ν Threshold: {best_roi['nu_threshold']}")
        logging.info(f"ROI: {best_roi['roi']:.2%}")
        logging.info(f"Hit Rate: {best_roi['hit_rate']:.2%}")
        logging.info(f"Total Bets: {best_roi['total_bet']}")
        logging.info("=" * 60)
    else:
        logging.warning("有効なベット数（10レース以上）のある結果がありませんでした。")

if __name__ == '__main__':
    main()

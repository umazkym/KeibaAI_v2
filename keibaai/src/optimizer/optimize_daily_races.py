#!/usr/bin/env python3
# src/optimizer/optimize_daily_races.py
"""
日次ポートフォリオ最適化 実行スクリプト
指定された日付のシミュレーション結果とオッズデータを読み込み、
ポートフォリオ最適化を実行し、data/orders/ に保存する。

仕様書 17.5章 に基づく実装

実行例:
python src/optimizer/optimize_daily_races.py --date 2023-10-01 --W_0 100000
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import json
import yaml
import pandas as pd

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

try:
    from src.pipeline_core import setup_logging, load_config
    from src.optimizer.optimizer import PortfolioOptimizer
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    print("プロジェクトルートが正しく設定されているか確認してください。")
    print(f"sys.path: {sys.path}")
    sys.exit(1)


def load_simulation_data(sim_dir: Path, target_date_str: str) -> List[Dict]:
    """
    指定日のシミュレーションJSONファイルをロード
    仕様書 17.5
    """
    results = []
    
    # race_id (YYYYMMDD...) から日付を判断
    target_date_prefix = target_date_str.replace('-', '')
    
    if not sim_dir.exists():
        logging.warning(f"シミュレーションディレクトリが見つかりません: {sim_dir}")
        return []

    all_files = list(sim_dir.glob('*.json'))
    
    for file_path in all_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            race_id = data.get('race_id')
            if not race_id:
                continue
                
            if race_id.startswith(target_date_prefix):
                results.append(data)
                
        except Exception as e:
            logging.warning(f"シミュレーションファイルのロード失敗 ({file_path}): {e}")
            
    return results


def load_odds_data(odds_dir: Path, race_id: str) -> Dict[str, Any]:
    """
    指定レースIDの最新JRAオッズJSONをロード
    仕様書 17.5 (★ダミー実装)
    
    注: 実際の _scrape_jra_odds.py はダミー です。
       ここでは、そのダミーJSONを読み込むことを想定します。
    """
    # ファイル名形式: {race_id}_snapshot_{data_version}.json
    odds_files = sorted(
        list(odds_dir.glob(f"{race_id}_snapshot_*.json")),
        key=lambda p: p.stat().st_mtime,
        reverse=True # 最新のファイルを取得
    )
    
    if not odds_files:
        logging.warning(f"レース {race_id} のオッズファイルが見つかりません (検索パス: {odds_dir}/{race_id}_snapshot_*.json)")
        return {}
        
    try:
        with open(odds_files[0], 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # オプティマイザが期待する形式に変換
        # { 'win': {'1': 5.5, '2': 12.3}, 'place': {'1': 1.8}, 'exacta': {'1-2': 20.5} }
        
        odds_map = {
            'win': {},
            'place': {},
            'exacta': {} # 馬連 (ダミーJSONには含まれていないが枠だけ用意)
        }
        
        # 'market' キーはダミーJSONの構造
        market = data.get('market', {})
        
        if 'win' in market:
            odds_map['win'] = {str(k): v.get('odds') for k, v in market['win'].items() if v.get('odds')}
            
        if 'place' in market:
            # 複勝は下限値を使用 (ダミーは単一値なのでそのまま)
             odds_map['place'] = {str(k): v.get('odds') for k, v in market['place'].items() if v.get('odds')}
                
        return odds_map
        
    except Exception as e:
        logging.error(f"オッズファイルのロード失敗 ({odds_files[0]}): {e}")
        return {}


def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI 日次ポートフォリオ最適化')
    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='最適化対象日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--W_0',
        type=float,
        default=100000.0,
        help='初期（当日）資金'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='configs/default.yaml',
        help='設定ファイルパス'
    )
    parser.add_argument(
        '--optimization_config',
        type=str,
        default='configs/optimization.yaml',
        help='最適化設定ファイルパス'
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
        
        with open(args.optimization_config, 'r') as f:
            optimization_config = yaml.safe_load(f)
            
        # ログ設定 (簡易版)
        log_path_template = config.get('logging', {}).get('log_file', 'data/logs/{YYYY}/{MM}/{DD}/optimization.log')
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
    logging.info("Keiba AI 日次ポートフォリオ最適化開始")
    logging.info("=" * 60)
    logging.info(f"対象日: {args.date}, 当日資金: ¥{args.W_0:,.0f}")

    # --- 1. シミュレーションデータのロード ---
    sim_dir = Path(paths.get('simulations_path', 'data/simulations'))
    simulations = load_simulation_data(sim_dir, args.date)
    
    if not simulations:
        logging.warning(f"{args.date} のシミュレーションデータが見つかりません (検索パス: {sim_dir})。処理を終了します。")
        sys.exit(0)

    # --- 2. オプティマイザ初期化 ---
    optimizer = PortfolioOptimizer(config=optimization_config)

    # --- 3. レースごとに最適化実行 ---
    odds_dir = Path(paths.get('raw_json_jra_odds', 'data/raw/json/jra_odds'))
    
    logging.info(f"{len(simulations)} レースの最適化を実行します")
    
    for i, sim_result in enumerate(simulations, 1):
        race_id = sim_result['race_id']
        logging.info(f"--- レース {i}/{len(simulations)} ({race_id}) ---")

        # 3.1 オッズデータのロード
        odds_data = load_odds_data(odds_dir, race_id)
        
        if not odds_data.get('win'):
            logging.warning(f"レース {race_id} のオッズデータ（単勝）が不十分なためスキップします")
            continue
            
        # 3.2 最適化実行
        try:
            allocation_result = optimizer.optimize(
                simulation_results=sim_result,
                odds_data=odds_data,
                W_0=args.W_0
            )
            
            # 3.3 発注（ログ）保存
            if allocation_result['total_investment'] > 0:
                output_dir_str = paths.get('orders_path', 'data/orders')
                optimizer.save_allocation(
                    race_id=race_id,
                    allocation_result=allocation_result,
                    output_dir=output_dir_str
                )
                
                # コンソールにも結果を表示 (仕様書 10.3)
                print(f"  総投資額: ¥{allocation_result['total_investment']:,.0f}")
                print(f"  期待リターン (シミュレーションベース): {allocation_result['expected_return']:.2%}")
                for bet in allocation_result['bets']:
                    selection_str = '-'.join(map(str, bet['selection']))
                    print(f"    {bet['type']:<7} {selection_str:<6} ¥{bet['amount']:>6,.0f} (Odds:{bet['odds']:>5.1f}, EV:{bet['ev']:>4.2f})")

            else:
                logging.info(f"レース {race_id}: 投資対象なし")
                
        except Exception as e:
            logging.error(f"レース {race_id} の最適化に失敗: {e}", exc_info=True)

    logging.info("=" * 60)
    logging.info("Keiba AI 日次ポートフォリオ最適化完了")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
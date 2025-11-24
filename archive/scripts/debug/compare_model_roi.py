# -*- coding: utf-8 -*-
"""
モデルごとのROI（投資回収率）を比較するためのバックテストスクリプト

このスクリプトは、指定された複数のモデルディレクトリに対して、
2024年のレースデータを用いたバックテストをそれぞれ実行し、
最終的なROIを比較して出力します。

主な処理フロー:
1. 2024年のレース開催日リストを取得します。
2. 指定された各モデルディレクトリに対して、以下をループ実行します。
    a. モデルごとに総投資額・総回収額を初期化します。
    b. 開催日ごとに、予測・シミュレーション・投資最適化を実行します。
    c. 投資判断（Order）と実際のレース結果を照合し、回収額を計算します。
    d. モデルごとの総投資額・総回収額を更新します。
3. 全モデルのバックテスト完了後、ROIの結果をまとめて表示します。
"""
import pandas as pd
from pathlib import Path
import subprocess
import sys
import logging
import argparse
from datetime import datetime
import json

# ロギング設定
log_file_path = f"compare_model_roi_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file_path, encoding='utf-8')
    ]
)

# グローバル変数
PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON_EXECUTABLE = sys.executable

def run_command(command: list, description: str) -> subprocess.CompletedProcess:
    """指定されたコマンドを実行し、結果を返す"""
    logging.info(f"  > 実行中: {description}")
    # Windows用にエンコーディングを指定
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding='cp932',
        errors='replace',
        cwd=PROJECT_ROOT
    )
    if result.returncode != 0:
        logging.error(f"  [!] 失敗: {description}")
        logging.error(f"    STDOUT: {result.stdout.strip()}")
        logging.error(f"    STDERR: {result.stderr.strip()}")
    return result

def get_2024_race_dates() -> list:
    ""`shutuba.parquet` から2024年のレース開催日リストを取得する"""
    shutuba_file = PROJECT_ROOT / 'keibaai' / 'data' / 'parsed' / 'parquet' / 'shutuba' / 'shutuba.parquet'
    if not shutuba_file.exists():
        logging.error(f"出馬表ファイルが見つかりません: {shutuba_file}")
        return []
    
    logging.info(f"出馬表ファイルから2024年の開催日を読み込み中: {shutuba_file}")
    df = pd.read_parquet(shutuba_file)
    df['race_date'] = pd.to_datetime(df['race_date'])
    dates = sorted(df[df['race_date'].dt.year == 2024]['race_date'].unique())
    return [pd.to_datetime(d).strftime('%Y-%m-%d') for d in dates]

def get_race_results(date_str: str) -> pd.DataFrame:
    """指定された日付のレース結果（払い戻し情報を含む）を取得する"""
    results_file = PROJECT_ROOT / 'keibaai' / 'data' / 'parsed' / 'parquet' / 'races' / 'races.parquet'
    if not results_file.exists():
        logging.error(f"レース結果ファイルが見つかりません: {results_file}")
        return pd.DataFrame()
        
    df = pd.read_parquet(results_file)
    df['race_date'] = pd.to_datetime(df['race_date'])
    target_date = pd.to_datetime(date_str)
    
    # 該当日のレース結果を抽出
    date_results = df[df['race_date'] == target_date].copy()
    
    # 払い戻し情報を抽出 (カラム名はCLAUDE.mdやschema.mdに基づく想定)
    payout_cols = [col for col in df.columns if 'payout' in col]
    return date_results[['race_id', 'horse_number', 'finish_position'] + payout_cols]

def calculate_payout(order: dict, results_df: pd.DataFrame) -> float:
    """投資判断(order)とレース結果から回収額を計算する"""
    total_payout = 0.0
    for bet in order.get('bets', []):
        bet_type = bet.get('type')
        selection = bet.get('selection')
        amount = bet.get('amount', 0)
        
        # 100円あたりの賭け金に変換
        bet_unit = amount / 100.0
        
        race_id = order['race_id']
        race_result = results_df[results_df['race_id'] == race_id]
        
        if race_result.empty:
            continue

        if bet_type == 'win': # 単勝
            winner = race_result[race_result['finish_position'] == 1]
            if not winner.empty and str(winner.iloc[0]['horse_number']) == str(selection[0]):
                payout_col = 'win_payout'
                if payout_col in race_result.columns:
                    payout_val = race_result.iloc[0][payout_col]
                    total_payout += bet_unit * payout_val
    
    return total_payout

def main(model_dirs: list, start_date: str, end_date: str):
    """メインのバックテスト処理"""
    
    race_dates = get_2024_race_dates()
    if not race_dates:
        return
        
    # 日付でフィルタリング
    if start_date:
        race_dates = [d for d in race_dates if d >= start_date]
    if end_date:
        race_dates = [d for d in race_dates if d <= end_date]

    logging.info(f"バックテスト対象期間: {race_dates[0]} から {race_dates[-1]} ({len(race_dates)}日間)")

    model_results = {}

    # --- モデルごとのループ ---
    for model_dir in model_dirs:
        model_name = Path(model_dir).name
        logging.info(f"\n{'='*20} バックテスト開始: {model_name} {'='*20}")
        
        total_investment = 0.0
        total_payout = 0.0
        
        # --- 開催日ごとのループ ---
        for i, target_date in enumerate(race_dates):
            logging.info(f"\n--- [{model_name}] {i+1}/{len(race_dates)}: {target_date} ---")
            
            # --- 1. 予測 ---
            pred_path = f"data/predictions/parquet/predictions_{target_date.replace('-', '')}.parquet"
            cmd_predict = [
                PYTHON_EXECUTABLE, 'keibaai/src/models/predict.py',
                '--date', target_date,
                '--model_dir', model_dir,
                '--output', pred_path,
                '--config', 'keibaai/configs/default.yaml'
            ]
            run_command(cmd_predict, "予測生成")

            # --- 2. シミュレーション ---
            sim_path_prefix = f"data/simulations/{model_name}_{target_date.replace('-', '')}"
            cmd_sim = [
                PYTHON_EXECUTABLE, 'keibaai/src/sim/simulate_daily_races.py',
                '--date', target_date,
                '--K', '100', # テスト用に回数を少なく設定
                '--predictions_path', pred_path,
                '--output_prefix', sim_path_prefix, # モデルごとにシミュレーション結果を区別
                '--config', 'keibaai/configs/default.yaml'
            ]
            run_command(cmd_sim, "シミュレーション")

            # --- 3. 投資最適化 ---
            # このスクリプトはコンソールに出力するだけなので、それをキャプチャするのは難しい
            # 代わりに、生成される order ファイルを読む
            order_dir = PROJECT_ROOT / 'keibaai' / 'data' / 'orders'
            # 既存のorderファイルを削除しておく
            for f in order_dir.glob(f"*{target_date.replace('-', '')}*.json"):
                f.unlink()

            cmd_opt = [
                PYTHON_EXECUTABLE, 'keibaai/src/optimizer/optimize_daily_races.py',
                '--date', target_date,
                '--W_0', '10000', # 1日の予算
                '--config', 'keibaai/configs/default.yaml',
                 # ★★★ シミュレーション結果のパスを正しく指定する必要がある。
                 # optimize_daily_races.pyがパスを引数で受け取れないので、
                 # ここではデフォルトのパス命名規則に依存する。これは改善点。
                 # 今回はsim/simulate_daily_races.pyの--output_prefixに合わせて、
                 # files = sim_dir.glob(f"{output_prefix}*.json") のように読み込んでもらう必要がある
                 # が、optimizerはその機能がないので、シミュレーション結果をデフォルトパスにコピーする
            ]
            # シミュレーション結果を optimizer が期待する場所にコピーするハック
            temp_sim_dir = PROJECT_ROOT / 'keibaai' / 'data' / 'simulations'
            for f in temp_sim_dir.glob(f"{model_name}*.json"):
                 f.rename(temp_sim_dir / f.name.replace(f"{model_name}_", ""))
            
            run_command(cmd_opt, "投資最適化")

            # --- 4. 結果照合と回収額計算 ---
            daily_results_df = get_race_results(target_date)
            if daily_results_df.empty:
                logging.warning(f"{target_date} のレース結果データが見つかりません。")
                continue

            # orderファイルは race_id_order.json という命名規則
            order_files = list(order_dir.glob(f"*{target_date.replace('-', '')}*_order.json"))
            logging.info(f"  {len(order_files)} 件の投資判断ファイルを処理中...")

            for order_file in order_files:
                with open(order_file, 'r', encoding='utf-8') as f:
                    order_data = json.load(f)
                
                investment = order_data.get('total_investment', 0.0)
                if investment > 0:
                    total_investment += investment
                    payout = calculate_payout(order_data, daily_results_df)
                    if payout > 0:
                        total_payout += payout
                        logging.info(f"    -> {order_data['race_id']}: 投資 ¥{investment:,.0f}, 回収 ¥{payout:,.0f} (的中！)")
                    else:
                        logging.info(f"    -> {order_data['race_id']}: 投資 ¥{investment:,.0f}, 回収 ¥0")

        # モデルごとの結果を保存
        roi = (total_payout / total_investment) if total_investment > 0 else 0
        model_results[model_name] = {
            "total_investment": total_investment,
            "total_payout": total_payout,
            "roi": roi
        }
        logging.info(f"\n--- バックテスト完了: {model_name} ---")
        logging.info(f"  総投資額: ¥{total_investment:,.0f}")
        logging.info(f"  総回収額: ¥{total_payout:,.0f}")
        logging.info(f"  回収率 (ROI): {roi:.2%}")


    # --- 最終結果の表示 ---
    logging.info(f"\n{'='*25} 最終バックテスト結果 {'='*25}")
    sorted_results = sorted(model_results.items(), key=lambda item: item[1]['roi'], reverse=True)
    
    for name, result in sorted_results:
        logging.info(f"モデル: {name}")
        logging.info(f"  - 総投資額: ¥{result['total_investment']:>12,.0f}")
        logging.info(f"  - 総回収額: ¥{result['total_payout']:>12,.0f}")
        logging.info(f"  - 回収率 (ROI): {result['roi']:>9.2%}")
        logging.info("-" * 40)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="モデルごとのROIを比較するバックテストを実行します。")
    parser.add_argument(
        'model_dirs',
        nargs='+',
        help="比較対象のモデルが格納されているディレクトリのパス（複数指定可）。例: keibaai/data/models/model1 keibaai/data/models/model2"
    )
    parser.add_argument('--start_date', help="バックテスト開始日 (YYYY-MM-DD)")
    parser.add_argument('--end_date', help="バックテスト終了日 (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    main(args.model_dirs, args.start_date, args.end_date)

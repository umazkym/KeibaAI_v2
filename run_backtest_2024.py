# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
import subprocess
import sys
import logging
from datetime import datetime

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('backtest_2024.log', encoding='utf-8')
    ]
)

def main():
    project_root = Path(__file__).resolve().parent
    shutuba_dir = project_root / 'keibaai/data/parsed/parquet/shutuba'
    
    logging.info(f"出馬表データを検索中: {shutuba_dir}")
    if not shutuba_dir.exists():
        logging.error("出馬表ディレクトリが見つかりません。")
        return

    parquet_files = list(shutuba_dir.glob('*.parquet'))
    if not parquet_files:
        logging.error("出馬表データ(.parquet)が見つかりません。")
        return
        
    shutuba_path = parquet_files[0]
    logging.info(f"読み込みファイル: {shutuba_path.name}")

    try:
        df = pd.read_parquet(shutuba_path)
        
        # 2024年の開催日を抽出
        if 'race_date' not in df.columns:
             # race_idから日付を推測する必要があるが、今回はfeaturesが正しく生成されている前提で
             # featuresディレクトリから日付リストを取得する方が確実かもしれない
             # しかし、shutubaにはrace_dateがあるはず
             logging.error("shutubaデータにrace_dateカラムがありません。")
             return

        # 日付型に変換
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # 2024年のデータをフィルタリング
        dates_2024 = df[df['race_date'].dt.year == 2024]['race_date'].unique()
        dates_2024 = sorted([pd.to_datetime(d).strftime('%Y-%m-%d') for d in dates_2024])
        
        logging.info(f"2024年の開催日数: {len(dates_2024)}日")
        
        # 実行ループ
        for i, target_date in enumerate(dates_2024):
            logging.info(f"[{i+1}/{len(dates_2024)}] 処理中: {target_date}")
            
            # 1. 予測生成 (predict.py)
            # パス指定に注意: --model_dir data/models
            cmd_predict = [
                sys.executable,
                str(project_root / 'keibaai/src/models/predict.py'),
                '--date', target_date,
                '--model_dir', 'data/models',
                '--config', 'keibaai/configs/default.yaml'
            ]
            
            logging.info(f"  予測生成実行: {' '.join(cmd_predict)}")
            # Windows環境用に encoding='cp932', errors='replace' に変更
            result_predict = subprocess.run(cmd_predict, capture_output=True, text=True, encoding='cp932', errors='replace')
            
            if result_predict.returncode != 0:
                logging.error(f"  予測生成失敗: {target_date}")
                logging.error(f"STDERR: {result_predict.stderr}")
                logging.error(f"STDOUT: {result_predict.stdout}")
                continue
            
            # 予測ファイルのパスを構築
            # predict.pyはデフォルトで data/predictions/parquet/predictions_YYYYMMDD.parquet に保存する
            date_obj = datetime.strptime(target_date, '%Y-%m-%d')
            pred_filename = f"predictions_{date_obj.strftime('%Y%m%d')}.parquet"
            pred_path = f"data/predictions/parquet/{pred_filename}"
            
            # 2. シミュレーション実行 (simulate_daily_races.py)
            cmd_sim = [
                sys.executable,
                str(project_root / 'keibaai/src/sim/simulate_daily_races.py'),
                '--date', target_date,
                '--K', '100', # テスト用に100回（本番は1000回推奨）
                '--model_id', 'v2_backtest',
                '--predictions_path', pred_path,
                '--config', 'keibaai/configs/default.yaml'
            ]
            
            logging.info(f"  シミュレーション実行: {' '.join(cmd_sim)}")
            # Windows環境用に encoding='cp932', errors='replace' に変更
            result_sim = subprocess.run(cmd_sim, capture_output=True, text=True, encoding='cp932', errors='replace')
            
            if result_sim.returncode != 0:
                logging.error(f"  シミュレーション失敗: {target_date}")
                logging.error(result_sim.stderr)
            else:
                logging.info(f"  完了: {target_date}")

    except Exception as e:
        logging.error(f"エラーが発生しました: {e}", exc_info=True)

if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""
μモデルで2020-2023年の実際のレース開催日の予測を生成するスクリプト（改訂版）

races.parquetから実際の開催日を取得し、その日付に対して予測を実行する。

使用方法:
  python generate_mu_predictions_2020_2023_v2.py
"""
import subprocess
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('generate_mu_predictions_2020_2023_v2.log', encoding='utf-8')
    ]
)

def get_race_dates(races_path, start_year, end_year):
    """races.parquetから指定期間のユニークな開催日を取得"""
    logging.info(f"レース開催日を取得中: {races_path}")

    df = pd.read_parquet(races_path)
    df['race_date'] = pd.to_datetime(df['race_date'])

    # 2020-2023年のデータをフィルタ
    df_filtered = df[(df['race_date'].dt.year >= start_year) &
                     (df['race_date'].dt.year <= end_year)]

    # ユニークな開催日を取得してソート
    race_dates = sorted(df_filtered['race_date'].dt.date.unique())

    logging.info(f"  → {len(race_dates)}日分の開催日を取得しました")
    return race_dates

def main():
    project_root = Path(__file__).resolve().parent
    predict_script = project_root / 'keibaai/src/models/predict.py'
    # predict.pyはkeibaai/を基準とするため、相対パスから'keibaai/'を除く
    model_dir = 'data/models/mu_model_20241122_final'
    predictions_dir = project_root / 'keibaai/data/predictions/parquet'
    races_path = project_root / 'keibaai/data/parsed/parquet/races/races.parquet'

    predictions_dir.mkdir(parents=True, exist_ok=True)

    if not predict_script.exists():
        logging.error(f"予測スクリプトが見つかりません: {predict_script}")
        return

    # model_dirは相対パス文字列なので、exists()チェックは predict.py に任せる

    if not races_path.exists():
        logging.error(f"races.parquetが見つかりません: {races_path}")
        return

    # 2020-2023年の実際の開催日を取得
    race_dates = get_race_dates(races_path, 2020, 2023)
    total_dates = len(race_dates)

    logging.info("=" * 70)
    logging.info("μモデル 予測生成（2020-2023年）")
    logging.info("=" * 70)
    logging.info(f"対象期間: 2020-01-01 〜 2023-12-31")
    logging.info(f"処理日数: {total_dates}日")
    logging.info(f"モデルディレクトリ: {model_dir}")
    logging.info("=" * 70)

    success_count = 0
    failed_dates = []
    prediction_files = []

    # 開催日ごとに予測を生成（tqdmでプログレス表示）
    pbar = tqdm(race_dates, desc="μ予測生成", unit="日")

    for race_date in pbar:
        date_str = race_date.strftime('%Y-%m-%d')
        output_filename = f"mu_predictions_{race_date.strftime('%Y%m%d')}.parquet"

        pbar.set_postfix_str(f"{date_str} | 成功: {success_count}/{len(prediction_files) + len(failed_dates)}")

        cmd = [
            sys.executable,
            str(predict_script),
            '--date', date_str,
            '--model_dir', str(model_dir),
            '--output_filename', output_filename
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=300  # 5分タイムアウト
            )

            if result.returncode == 0:
                prediction_file = predictions_dir / output_filename
                if prediction_file.exists():
                    success_count += 1
                    prediction_files.append(prediction_file)
                else:
                    tqdm.write(f"⚠️  出力ファイルなし: {date_str}")
                    failed_dates.append(date_str)
            else:
                tqdm.write(f"❌ 失敗: {date_str}")
                if result.stderr:
                    # エラーメッセージの重要部分を抽出（最後の5行程度）
                    stderr_lines = result.stderr.strip().split('\n')
                    important_lines = stderr_lines[-5:] if len(stderr_lines) > 5 else stderr_lines
                    tqdm.write(f"   STDERR: {chr(10).join(important_lines)}")
                failed_dates.append(date_str)

        except subprocess.TimeoutExpired:
            tqdm.write(f"❌ タイムアウト: {date_str}")
            failed_dates.append(f"{date_str}（タイムアウト）")
        except Exception as e:
            tqdm.write(f"❌ エラー: {date_str} - {e}")
            failed_dates.append(f"{date_str}（エラー）")

    pbar.close()

    # 全ての予測ファイルを統合
    logging.info("\n" + "=" * 70)
    logging.info("予測ファイルの統合")
    logging.info("=" * 70)

    if prediction_files:
        logging.info(f"統合するファイル数: {len(prediction_files)}")

        dfs = []
        total_rows = 0
        for pf in prediction_files:
            try:
                df = pd.read_parquet(pf)
                dfs.append(df)
                total_rows += len(df)
            except Exception as e:
                logging.error(f"  ✗ {pf.name}: 読み込みエラー - {e}")

        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            output_path = predictions_dir / 'mu_predictions.parquet'
            combined_df.to_parquet(output_path, index=False)
            logging.info(f"\n✅ 統合完了: {output_path}")
            logging.info(f"   ファイル数: {len(dfs)}")
            logging.info(f"   総行数: {len(combined_df):,}行")
        else:
            logging.error("統合可能なファイルがありませんでした")
    else:
        logging.error("予測ファイルが1つも生成されませんでした")

    # 最終結果
    logging.info("\n" + "=" * 70)
    logging.info("✅ μモデル予測生成 完了")
    logging.info("=" * 70)
    logging.info(f"成功: {success_count}/{total_dates}日")

    if failed_dates:
        logging.warning(f"失敗: {len(failed_dates)}日")
        if len(failed_dates) <= 10:
            logging.warning("失敗した日:")
            for fd in failed_dates:
                logging.warning(f"  - {fd}")
    else:
        logging.info("全ての開催日の予測に成功しました！")

    logging.info("=" * 70)
    logging.info("\n次のステップ:")
    logging.info("  python keibaai\\src\\models\\train_sigma_nu_models.py")
    logging.info("=" * 70)

if __name__ == '__main__':
    main()

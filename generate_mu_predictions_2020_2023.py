# -*- coding: utf-8 -*-
"""
μモデルで2020-2023年の全期間の予測を生成するスクリプト

σ/νモデル学習のために、μモデルの予測結果が必要。
月ごとに予測を実行し、最後に統合したファイルを生成する。

使用方法:
  python generate_mu_predictions_2020_2023.py
"""
import subprocess
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from calendar import monthrange

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('generate_mu_predictions_2020_2023.log', encoding='utf-8')
    ]
)

def generate_month_ranges(start_year, end_year):
    """指定期間の月ごとの日付リストを生成"""
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            start_date = datetime(year, month, 1)
            _, last_day = monthrange(year, month)
            end_date = datetime(year, month, last_day)
            months.append({
                'year': year,
                'month': month,
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d'),
                'date': start_date.strftime('%Y-%m-%d')  # predict.py用（月の最初の日）
            })
    return months

def main():
    project_root = Path(__file__).resolve().parent
    predict_script = project_root / 'keibaai/src/models/predict.py'
    model_dir = project_root / 'keibaai/data/models/mu_model_20241122_complete'
    predictions_dir = project_root / 'keibaai/data/predictions/parquet'
    predictions_dir.mkdir(parents=True, exist_ok=True)

    if not predict_script.exists():
        logging.error(f"予測スクリプトが見つかりません: {predict_script}")
        return

    if not model_dir.exists():
        logging.error(f"μモデルが見つかりません: {model_dir}")
        return

    # 2020-2023年の全月
    months = generate_month_ranges(2020, 2023)
    total_months = len(months)

    logging.info("=" * 70)
    logging.info("μモデル 予測生成（2020-2023年）")
    logging.info("=" * 70)
    logging.info(f"対象期間: 2020年1月 〜 2023年12月")
    logging.info(f"処理月数: {total_months}ヶ月")
    logging.info(f"モデルディレクトリ: {model_dir}")
    logging.info("=" * 70)

    success_count = 0
    failed_months = []
    prediction_files = []

    # 月ごとに予測を生成
    for i, month_info in enumerate(months, 1):
        year = month_info['year']
        month = month_info['month']
        date = month_info['date']

        logging.info(f"\n[{i}/{total_months}] 処理中: {year}年{month}月 ({date})")

        # predict.pyは月全体ではなく日付を指定するため、月の最初の日を指定
        # ただし、実際には月全体のデータを予測するように実装されている必要がある
        # ここでは、月の最初の日を指定して、その月のデータを予測する
        output_filename = f"mu_predictions_{year:04d}{month:02d}.parquet"

        cmd = [
            sys.executable,
            str(predict_script),
            '--date', date,
            '--model_dir', str(model_dir),
            '--output_filename', output_filename
        ]

        logging.info(f"  実行コマンド: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=600  # 10分タイムアウト
            )

            if result.returncode == 0:
                prediction_file = predictions_dir / output_filename
                if prediction_file.exists():
                    logging.info(f"  ✅ {year}年{month}月: 成功")
                    success_count += 1
                    prediction_files.append(prediction_file)
                else:
                    logging.warning(f"  ⚠️  {year}年{month}月: コマンドは成功したが、出力ファイルが見つかりません")
            else:
                logging.error(f"  ❌ {year}年{month}月: 失敗")
                logging.error(f"STDERR: {result.stderr}")
                failed_months.append(f"{year}年{month}月")

        except subprocess.TimeoutExpired:
            logging.error(f"  ❌ {year}年{month}月: タイムアウト（10分超過）")
            failed_months.append(f"{year}年{month}月（タイムアウト）")
        except Exception as e:
            logging.error(f"  ❌ {year}年{month}月: エラー発生 - {e}")
            failed_months.append(f"{year}年{month}月（エラー）")

    # 全ての予測ファイルを統合
    logging.info("\n" + "=" * 70)
    logging.info("予測ファイルの統合")
    logging.info("=" * 70)

    if prediction_files:
        logging.info(f"統合するファイル数: {len(prediction_files)}")

        dfs = []
        for pf in prediction_files:
            try:
                df = pd.read_parquet(pf)
                dfs.append(df)
                logging.info(f"  ✓ {pf.name}: {len(df)}行")
            except Exception as e:
                logging.error(f"  ✗ {pf.name}: 読み込みエラー - {e}")

        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            output_path = predictions_dir / 'mu_predictions.parquet'
            combined_df.to_parquet(output_path, index=False)
            logging.info(f"\n✅ 統合完了: {output_path}")
            logging.info(f"   総行数: {len(combined_df):,}行")
        else:
            logging.error("統合可能なファイルがありませんでした")
    else:
        logging.error("予測ファイルが1つも生成されませんでした")

    # 最終結果
    logging.info("\n" + "=" * 70)
    logging.info("✅ μモデル予測生成 完了")
    logging.info("=" * 70)
    logging.info(f"成功: {success_count}/{total_months}ヶ月")

    if failed_months:
        logging.warning(f"失敗: {len(failed_months)}ヶ月")
        logging.warning("失敗した月:")
        for fm in failed_months:
            logging.warning(f"  - {fm}")
    else:
        logging.info("全ての月の予測に成功しました！")

    logging.info("=" * 70)
    logging.info("\n次のステップ:")
    logging.info("  python keibaai\\src\\models\\train_sigma_nu_models.py")
    logging.info("=" * 70)

if __name__ == '__main__':
    main()

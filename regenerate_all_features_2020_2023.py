# -*- coding: utf-8 -*-
"""
2020-2023年の特徴量を月ごとに完全再生成するスクリプト

各月を個別に処理することで:
- メモリ負荷を軽減
- 進捗を可視化
- エラー発生時のリカバリが容易

使用方法:
  python regenerate_all_features_2020_2023.py
"""
import subprocess
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
from calendar import monthrange

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('regenerate_features_2020_2023.log', encoding='utf-8')
    ]
)

def generate_month_ranges(start_year, end_year):
    """指定期間の月ごとの開始日・終了日リストを生成"""
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # 月の最初の日
            start_date = datetime(year, month, 1)
            # 月の最後の日
            _, last_day = monthrange(year, month)
            end_date = datetime(year, month, last_day)

            months.append({
                'year': year,
                'month': month,
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            })
    return months

def main():
    project_root = Path(__file__).resolve().parent
    generate_script = project_root / 'keibaai/src/features/generate_features.py'

    if not generate_script.exists():
        logging.error(f"特徴量生成スクリプトが見つかりません: {generate_script}")
        return

    # 2020-2023年の全月をリストアップ
    months = generate_month_ranges(2020, 2023)
    total_months = len(months)

    logging.info("=" * 70)
    logging.info("2020-2023年 特徴量 完全再生成")
    logging.info("=" * 70)
    logging.info(f"対象期間: 2020年1月 〜 2023年12月")
    logging.info(f"処理月数: {total_months}ヶ月")
    logging.info("=" * 70)

    success_count = 0
    failed_months = []

    # 月ごとに特徴量を生成
    for i, month_info in enumerate(months, 1):
        year = month_info['year']
        month = month_info['month']
        start_date = month_info['start']
        end_date = month_info['end']

        logging.info(f"\n[{i}/{total_months}] 処理中: {year}年{month}月 ({start_date} 〜 {end_date})")

        cmd = [
            sys.executable,
            str(generate_script),
            '--start_date', start_date,
            '--end_date', end_date
        ]

        logging.info(f"  実行コマンド: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=1800  # 30分タイムアウト
            )

            if result.returncode == 0:
                logging.info(f"  ✅ {year}年{month}月: 成功")
                success_count += 1
            else:
                logging.error(f"  ❌ {year}年{month}月: 失敗")
                logging.error(f"STDERR: {result.stderr}")
                failed_months.append(f"{year}年{month}月")

        except subprocess.TimeoutExpired:
            logging.error(f"  ❌ {year}年{month}月: タイムアウト（30分超過）")
            failed_months.append(f"{year}年{month}月（タイムアウト）")
        except Exception as e:
            logging.error(f"  ❌ {year}年{month}月: エラー発生 - {e}")
            failed_months.append(f"{year}年{month}月（エラー）")

    # 最終結果
    logging.info("\n" + "=" * 70)
    logging.info("✅ 特徴量再生成 完了")
    logging.info("=" * 70)
    logging.info(f"成功: {success_count}/{total_months}ヶ月")

    if failed_months:
        logging.warning(f"失敗: {len(failed_months)}ヶ月")
        logging.warning("失敗した月:")
        for fm in failed_months:
            logging.warning(f"  - {fm}")
    else:
        logging.info("全ての月の生成に成功しました！")

    logging.info("=" * 70)
    logging.info("\n次のステップ:")
    logging.info("  python check_feature_coverage.py  # 全月のデータが生成されたか確認")
    logging.info("=" * 70)

if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""
特徴量データの年月カバレッジを確認するスクリプト
"""
import pandas as pd
from pathlib import Path
import sys

def main():
    project_root = Path(__file__).resolve().parent
    features_base = project_root / 'keibaai/data/features/parquet'

    if not features_base.exists():
        print(f"❌ 特徴量ディレクトリが存在しません: {features_base}")
        return

    print("=" * 60)
    print("特徴量データ カバレッジ確認")
    print("=" * 60)

    # 年ごとに確認
    for year in range(2020, 2025):
        year_dir = features_base / f'year={year}'
        if not year_dir.exists():
            print(f"\n❌ {year}年: ディレクトリなし")
            continue

        print(f"\n📅 {year}年:")

        # 月ごとに確認
        months_found = []
        months_missing = []
        month_stats = []

        for month in range(1, 13):
            month_dir = year_dir / f'month={month}'
            if month_dir.exists():
                # Parquetファイル数と総行数を確認
                parquet_files = list(month_dir.glob('*.parquet'))
                if parquet_files:
                    total_rows = 0
                    for pf in parquet_files:
                        try:
                            df = pd.read_parquet(pf)
                            total_rows += len(df)
                        except:
                            pass
                    months_found.append(month)
                    month_stats.append(f"  ✓ {month:2d}月: {len(parquet_files):2d}ファイル, {total_rows:5d}行")
                else:
                    months_missing.append(month)
                    month_stats.append(f"  ❌ {month:2d}月: ファイルなし")
            else:
                months_missing.append(month)
                month_stats.append(f"  ❌ {month:2d}月: ディレクトリなし")

        # 統計表示
        for stat in month_stats:
            print(stat)

        print(f"\n  📊 {year}年 サマリー:")
        print(f"     存在する月: {len(months_found)}/12ヶ月")
        print(f"     欠損月: {months_missing if months_missing else 'なし'}")

    print("\n" + "=" * 60)
    print("✅ カバレッジ確認完了")
    print("=" * 60)

if __name__ == '__main__':
    main()

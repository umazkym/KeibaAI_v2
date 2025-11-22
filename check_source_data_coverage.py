# -*- coding: utf-8 -*-
"""
元データ（races.parquet, shutuba.parquet）の年月別カバレッジを確認
"""
import pandas as pd
from pathlib import Path

def main():
    project_root = Path(__file__).resolve().parent
    races_path = project_root / 'keibaai/data/parsed/parquet/races/races.parquet'
    shutuba_path = project_root / 'keibaai/data/parsed/parquet/shutuba/shutuba.parquet'

    print("=" * 70)
    print("元データ（races.parquet, shutuba.parquet）の年月別カバレッジ確認")
    print("=" * 70)

    # races.parquetの確認
    if races_path.exists():
        print("\n📊 races.parquet の年月別データ数")
        print("-" * 70)
        df = pd.read_parquet(races_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        df['month'] = df['race_date'].dt.month

        summary = df.groupby(['year', 'month']).size().reset_index(name='race_count')

        for year in range(2020, 2025):
            year_data = summary[summary['year'] == year]
            print(f'\n{year}年:')

            if len(year_data) == 0:
                print('  データなし')
                continue

            total_races = 0
            missing_months = []

            for month in range(1, 13):
                month_data = year_data[year_data['month'] == month]
                if len(month_data) > 0:
                    count = month_data.iloc[0]['race_count']
                    total_races += count
                    print(f'  {month:2d}月: {count:5d}レース')
                else:
                    print(f'  {month:2d}月: データなし ❌')
                    missing_months.append(month)

            print(f'\n  📈 {year}年合計: {total_races}レース')
            if missing_months:
                print(f'  ⚠️  欠損月: {missing_months}')
    else:
        print(f"\n❌ races.parquet が見つかりません: {races_path}")

    # shutuba.parquetの確認
    if shutuba_path.exists():
        print("\n" + "=" * 70)
        print("📊 shutuba.parquet の年月別データ数")
        print("-" * 70)
        df = pd.read_parquet(shutuba_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        df['month'] = df['race_date'].dt.month

        summary = df.groupby(['year', 'month']).size().reset_index(name='entry_count')

        for year in range(2020, 2025):
            year_data = summary[summary['year'] == year]
            print(f'\n{year}年:')

            if len(year_data) == 0:
                print('  データなし')
                continue

            total_entries = 0
            missing_months = []

            for month in range(1, 13):
                month_data = year_data[year_data['month'] == month]
                if len(month_data) > 0:
                    count = month_data.iloc[0]['entry_count']
                    total_entries += count
                    print(f'  {month:2d}月: {count:5d}エントリ')
                else:
                    print(f'  {month:2d}月: データなし ❌')
                    missing_months.append(month)

            print(f'\n  📈 {year}年合計: {total_entries}エントリ')
            if missing_months:
                print(f'  ⚠️  欠損月: {missing_months}')
    else:
        print(f"\n❌ shutuba.parquet が見つかりません: {shutuba_path}")

    print("\n" + "=" * 70)
    print("✅ 元データのカバレッジ確認完了")
    print("=" * 70)

if __name__ == '__main__':
    main()

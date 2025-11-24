#!/usr/bin/env python3
"""
デバッグスクリプト03: パース失敗原因の徹底調査

なぜ20,157件中1,000件しかパースされていないのかを特定します
"""

import os
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
import pandas as pd

def analyze_html_files():
    """RAW HTMLファイルの詳細分析"""
    print("=" * 80)
    print("📁 RAW HTMLファイルの詳細分析")
    print("=" * 80)

    html_base = Path("keibaai/data/raw/html")

    # レース結果HTMLの分析
    race_dir = html_base / "race"
    if race_dir.exists():
        race_files = list(race_dir.glob("*.bin"))
        race_ids = [f.stem for f in race_files]

        # 年月別の集計
        year_month_count = defaultdict(int)
        year_count = defaultdict(int)

        for race_id in race_ids:
            # race_id = YYYYPPNNDDRR
            if len(race_id) == 12 and race_id.isdigit():
                year = race_id[:4]
                month = race_id[4:6]
                year_count[year] += 1
                year_month_count[f"{year}-{month}"] += 1

        print(f"\n📊 レース結果HTMLファイル: 合計 {len(race_files)} 件")
        print("\n年別ファイル数:")
        for year in sorted(year_count.keys()):
            print(f"  {year}年: {year_count[year]:>5} 件")

        print("\n年月別ファイル数（上位20件）:")
        sorted_ym = sorted(year_month_count.items(), key=lambda x: x[0])
        for ym, count in sorted_ym[:20]:
            print(f"  {ym}: {count:>4} 件")

    # 馬データHTMLの分析
    horse_dir = html_base / "horse"
    if horse_dir.exists():
        horse_files = list(horse_dir.glob("*.bin"))
        profile_files = [f for f in horse_files if "_profile" in f.name]
        perf_files = [f for f in horse_files if "_perf" in f.name]

        print(f"\n📊 馬データHTMLファイル: 合計 {len(horse_files)} 件")
        print(f"  - プロフィール: {len(profile_files)} 件")
        print(f"  - 過去成績: {len(perf_files)} 件")


def analyze_parsed_parquet():
    """パース済みParquetの詳細分析"""
    print("\n" + "=" * 80)
    print("📊 パース済みParquetの詳細分析")
    print("=" * 80)

    parquet_base = Path("keibaai/data/parsed/parquet")

    # races.parquet の分析
    races_file = parquet_base / "races" / "races.parquet"
    if races_file.exists():
        df = pd.read_parquet(races_file)
        print(f"\n📄 races.parquet:")
        print(f"  総行数: {len(df):,}")

        # race_idから年月を抽出
        df['year'] = df['race_id'].astype(str).str[:4]
        df['year_month'] = df['race_id'].astype(str).str[:6]

        print("\n  年別レース数:")
        for year, count in df['year'].value_counts().sort_index().items():
            print(f"    {year}年: {count:>5} レース")

        print("\n  年月別レース数（上位20件）:")
        for ym, count in df['year_month'].value_counts().sort_index().head(20).items():
            year = ym[:4]
            month = ym[4:6]
            print(f"    {year}-{month}: {count:>4} レース")

        # 日付範囲の確認
        print(f"\n  日付範囲:")
        print(f"    最小: {df['race_date'].min()}")
        print(f"    最大: {df['race_date'].max()}")

        # ユニークレース数
        unique_races = df['race_id'].nunique()
        print(f"\n  ユニークレース数: {unique_races:,}")
        print(f"  レースあたり平均頭数: {len(df) / unique_races:.2f}")


def check_parse_logs():
    """パース処理のログを確認"""
    print("\n" + "=" * 80)
    print("📋 パースログの確認")
    print("=" * 80)

    log_dir = Path("keibaai/data/logs")
    if log_dir.exists():
        log_files = sorted(log_dir.glob("*.log"))
        if log_files:
            print(f"\n見つかったログファイル: {len(log_files)} 件")
            for log_file in log_files[-5:]:  # 最新5件
                print(f"  - {log_file.name} ({log_file.stat().st_size} bytes)")

            # 最新のログファイルを読む
            latest_log = log_files[-1]
            print(f"\n📄 最新ログファイルの内容（{latest_log.name}）:")
            print("-" * 80)
            with open(latest_log, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                # エラー行を抽出
                error_lines = [line for line in lines if 'ERROR' in line or 'Exception' in line]
                if error_lines:
                    print("⚠️ エラーメッセージ:")
                    for line in error_lines[:20]:  # 最初の20件
                        print(f"  {line.strip()}")
                else:
                    print("✅ エラーは見つかりませんでした")

                # 最後の20行を表示
                print("\n最後の20行:")
                for line in lines[-20:]:
                    print(f"  {line.strip()}")
        else:
            print("❌ ログファイルが見つかりません")
    else:
        print("❌ ログディレクトリが存在しません")


def check_parse_script_config():
    """パース処理スクリプトの設定確認"""
    print("\n" + "=" * 80)
    print("⚙️ パース処理スクリプトの設定確認")
    print("=" * 80)

    script_path = Path("keibaai/src/run_parsing_pipeline_local.py")
    if script_path.exists():
        print(f"\n📄 {script_path}")
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()

            # 日付範囲の設定を探す
            date_patterns = [
                r'start_date\s*=\s*["\'](\d{4}-\d{2}-\d{2})["\']',
                r'end_date\s*=\s*["\'](\d{4}-\d{2}-\d{2})["\']',
                r'date_range\s*=\s*\(',
                r'limit\s*=\s*(\d+)',
            ]

            print("\n🔍 設定値の検索:")
            for pattern in date_patterns:
                matches = re.findall(pattern, content)
                if matches:
                    print(f"  {pattern}: {matches}")

            # ファイル内に "1000" という数字があるか確認
            if "1000" in content:
                print("\n⚠️ スクリプト内に '1000' という数値が見つかりました")
                # 該当行を表示
                lines = content.split('\n')
                for i, line in enumerate(lines, 1):
                    if "1000" in line:
                        print(f"  行{i}: {line.strip()}")
    else:
        print("❌ パース処理スクリプトが見つかりません")


def check_config_files():
    """設定ファイルの確認"""
    print("\n" + "=" * 80)
    print("📝 設定ファイルの確認")
    print("=" * 80)

    config_dir = Path("keibaai/configs")
    if config_dir.exists():
        config_files = list(config_dir.glob("*.yaml"))
        print(f"\n見つかった設定ファイル: {len(config_files)} 件")
        for config_file in config_files:
            print(f"  - {config_file.name}")

            # scraping.yaml と default.yaml を詳しく見る
            if config_file.name in ['scraping.yaml', 'default.yaml']:
                with open(config_file, 'r', encoding='utf-8') as f:
                    print(f"\n📄 {config_file.name} の内容:")
                    print("-" * 80)
                    content = f.read()
                    print(content[:500])  # 最初の500文字
                    if len(content) > 500:
                        print("... (省略)")
    else:
        print("❌ 設定ディレクトリが見つかりません")


def compare_raw_vs_parsed():
    """RAWとパース済みの詳細比較"""
    print("\n" + "=" * 80)
    print("🔍 RAW vs パース済み詳細比較")
    print("=" * 80)

    # RAWのrace_idリストを取得
    race_dir = Path("keibaai/data/raw/html/race")
    if race_dir.exists():
        raw_race_ids = set(f.stem for f in race_dir.glob("*.bin"))
        print(f"\nRAW HTMLのレースID数: {len(raw_race_ids):,}")
    else:
        raw_race_ids = set()

    # パース済みのrace_idリストを取得
    races_file = Path("keibaai/data/parsed/parquet/races/races.parquet")
    if races_file.exists():
        df = pd.read_parquet(races_file)
        parsed_race_ids = set(df['race_id'].unique())
        print(f"パース済みのレースID数: {len(parsed_race_ids):,}")
    else:
        parsed_race_ids = set()

    # 差分を分析
    if raw_race_ids and parsed_race_ids:
        missing_race_ids = raw_race_ids - parsed_race_ids
        print(f"\n⚠️ 未パースのレースID数: {len(missing_race_ids):,}")

        if missing_race_ids:
            # 未パースレースの年別分布
            year_count = defaultdict(int)
            for race_id in missing_race_ids:
                if len(race_id) == 12 and race_id.isdigit():
                    year = race_id[:4]
                    year_count[year] += 1

            print("\n未パースレースの年別分布:")
            for year in sorted(year_count.keys()):
                print(f"  {year}年: {year_count[year]:>5} 件")

            # サンプルとして最初の10件を表示
            print("\n未パースレースIDのサンプル（最初の10件）:")
            for race_id in sorted(missing_race_ids)[:10]:
                print(f"  {race_id}")


def main():
    """メイン処理"""
    print("=" * 80)
    print("🔍 KeibaAI_v2 パース失敗原因の徹底調査")
    print(f"⏰ 実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    analyze_html_files()
    analyze_parsed_parquet()
    check_parse_logs()
    check_parse_script_config()
    check_config_files()
    compare_raw_vs_parsed()

    print("\n" + "=" * 80)
    print("📋 調査結果サマリー")
    print("=" * 80)
    print("""
💡 次のステップ:
  1. パース処理スクリプトに日付制限やlimit指定がないか確認
  2. エラーログで異常終了の原因を特定
  3. 未パースのレースIDリストを生成し、差分パースを実行
  4. パーサーコード（results_parser.py等）の問題点を確認
    """)

    print("=" * 80)
    print("✅ デバッグスクリプト03完了")
    print("=" * 80)


if __name__ == "__main__":
    main()

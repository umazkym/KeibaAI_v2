# -*- coding: utf-8 -*-
"""
既存のbinファイルから直接パースするデバッグスクリプト

使用方法:
    # 指定日付のbinファイルを全て読み込んでパース
    python debug_parse_from_bins.py --date 2023-10-09

    # カスタムディレクトリ指定
    python debug_parse_from_bins.py --date 2023-10-09 --bin-dir data/raw/html/race

特徴:
- スクレイピングなし（既存binファイルのみ使用）
- 高速（ネットワーク不要）
- RaceData01対応済み
"""

import os
import re
import sys
import argparse
from pathlib import Path
from typing import List
import pandas as pd
from bs4 import BeautifulSoup

# --- debug_scraping_and_parsing.pyから関数をインポート ---
from debug_scraping_and_parsing import (
    parse_html_content,
    parse_int_or_none,
    parse_sex_age,
    parse_float_or_none,
    safe_strip,
    parse_horse_weight,
    parse_time_to_seconds,
    get_id_from_href,
    parse_margin_to_seconds,
    add_derived_features
)

# --- 設定 ---
DEFAULT_BIN_DIR = 'data/raw/html/race'
OUTPUT_CSV_PATH = 'debug_scraped_data.csv'


def find_bin_files_for_date(bin_dir: str, target_date: str) -> List[str]:
    """指定日付のbinファイルを全て検索

    Args:
        bin_dir: binファイルが格納されているディレクトリ
        target_date: 対象日付（YYYY-MM-DD形式）

    Returns:
        binファイルパスのリスト
    """
    # 日付をYYYYMMDD形式に変換
    date_formatted = target_date.replace('-', '')

    bin_path = Path(bin_dir)

    if not bin_path.exists():
        print(f"[!] ディレクトリが存在しません: {bin_dir}")
        return []

    # パターン: YYYYMMDDNNRR.bin (8桁の日付 + 4桁)
    # 例: 20231009*.bin
    pattern = f"{date_formatted}*.bin"

    bin_files = list(bin_path.glob(pattern))

    # race_idでソート
    bin_files.sort()

    return [str(f) for f in bin_files]


def extract_race_id_from_filename(file_path: str) -> str:
    """ファイル名からrace_idを抽出

    Args:
        file_path: binファイルのパス

    Returns:
        race_id（拡張子なし）
    """
    filename = os.path.basename(file_path)
    return filename.replace('.bin', '')


def parse_bin_file(file_path: str, race_id: str) -> pd.DataFrame:
    """binファイルをパースしてDataFrameを返す

    Args:
        file_path: binファイルのパス
        race_id: レースID

    Returns:
        パース結果のDataFrame
    """
    try:
        with open(file_path, 'rb') as f:
            html_bytes = f.read()

        # debug_scraping_and_parsing.pyのparse_html_contentを使用
        df = parse_html_content(html_bytes, race_id)

        return df

    except Exception as e:
        print(f"  [!] パースエラー: {file_path} - {e}")
        return pd.DataFrame()


def main():
    """メイン実行処理"""
    parser = argparse.ArgumentParser(
        description='既存のbinファイルから直接パースするデバッグスクリプト'
    )
    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='対象日付（YYYY-MM-DD形式）例: 2023-10-09'
    )
    parser.add_argument(
        '--bin-dir',
        type=str,
        default=DEFAULT_BIN_DIR,
        help=f'binファイルのディレクトリ（デフォルト: {DEFAULT_BIN_DIR}）'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=OUTPUT_CSV_PATH,
        help=f'出力CSVファイル名（デフォルト: {OUTPUT_CSV_PATH}）'
    )

    args = parser.parse_args()

    print(f"=== 既存binファイルからのパース開始 ===")
    print(f"対象日付: {args.date}")
    print(f"binディレクトリ: {args.bin_dir}")
    print(f"出力ファイル: {args.output}")

    # 1. 指定日付のbinファイルを検索
    bin_files = find_bin_files_for_date(args.bin_dir, args.date)

    if not bin_files:
        print(f"\n[!] 指定日付のbinファイルが見つかりませんでした: {args.date}")
        print(f"[!] ディレクトリを確認してください: {args.bin_dir}")
        return

    print(f"\n[+] {len(bin_files)} 件のbinファイルを検出しました")

    # 2. 各binファイルをパース
    all_races_data = []

    for i, file_path in enumerate(bin_files):
        race_id = extract_race_id_from_filename(file_path)
        filename = os.path.basename(file_path)

        print(f"\n--- レース {i+1}/{len(bin_files)} (ID: {race_id}) ---")
        print(f"  ファイル: {filename}")

        df = parse_bin_file(file_path, race_id)

        if not df.empty:
            print(f"  [✓] パース成功: {len(df)}頭のデータを取得")
            all_races_data.append(df)
        else:
            print(f"  [!] パース失敗またはデータなし")

    # 3. 全データを統合してCSV出力
    if all_races_data:
        final_df = pd.concat(all_races_data, ignore_index=True)
        final_df.to_csv(args.output, index=False, encoding='utf-8-sig')

        print(f"\n=== 処理完了 ===")
        print(f"[✓] {len(final_df)} 行のデータを {args.output} に保存しました")

        # 簡易統計を表示
        print(f"\n--- 簡易統計 ---")
        print(f"総行数: {len(final_df)}")
        print(f"ユニークレース数: {final_df['race_id'].nunique()}")

        # distance_m の欠損確認
        distance_missing = final_df['distance_m'].isna().sum()
        distance_missing_pct = (distance_missing / len(final_df) * 100)
        print(f"distance_m 欠損: {distance_missing}行 ({distance_missing_pct:.2f}%)")

        # track_surface の欠損確認
        surface_missing = final_df['track_surface'].isna().sum()
        surface_missing_pct = (surface_missing / len(final_df) * 100)
        print(f"track_surface 欠損: {surface_missing}行 ({surface_missing_pct:.2f}%)")

        # race_class の分布
        if 'race_class' in final_df.columns:
            print(f"\nrace_class 分布:")
            race_class_counts = final_df['race_class'].value_counts()
            for cls, count in race_class_counts.items():
                unique_races = final_df[final_df['race_class'] == cls]['race_id'].nunique()
                print(f"  {cls}: {count}行 ({unique_races}レース)")

        # 欠損があるレースを表示
        if distance_missing > 0:
            print(f"\n距離情報が欠損しているレース:")
            missing_races = final_df[final_df['distance_m'].isna()]['race_id'].unique()
            for race_id in missing_races[:5]:  # 最初の5件
                race_rows = final_df[final_df['race_id'] == race_id]
                if not race_rows.empty:
                    race_name = race_rows.iloc[0].get('race_name', 'N/A')
                    print(f"  {race_id}: {race_name} ({len(race_rows)}頭)")
    else:
        print(f"\n[!] データを取得できませんでした")


if __name__ == "__main__":
    main()

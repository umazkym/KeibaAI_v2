# -*- coding: utf-8 -*-
"""
複数のbinファイルを分析し、test/test_outputのような複数CSVを出力するスクリプト

使用方法:
    python analyze_multiple_bins.py [binファイルディレクトリ] [出力ディレクトリ]

    例: python analyze_multiple_bins.py test output
"""

import os
import sys
import re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

# プロジェクトルートをパスに追加
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# debug_scraping_and_parsing.pyから関数をインポート
# ※ 実際の環境ではpandasが必要ですが、ここでは構造のみ示します

# --- 設定 ---
DEFAULT_BIN_DIR = 'test'
DEFAULT_OUTPUT_DIR = 'test_output'

def parse_race_result_bin(file_path: str) -> Optional[pd.DataFrame]:
    """レース結果binファイルをパースしてDataFrameを返す"""
    from debug_scraping_and_parsing import parse_html_content, get_id_from_href

    # ファイル名からrace_idを抽出
    filename = os.path.basename(file_path)
    race_id = filename.replace('.bin', '')

    # HTMLを読み込み
    try:
        with open(file_path, 'rb') as f:
            html_bytes = f.read()

        # パース
        df = parse_html_content(html_bytes, race_id)
        return df

    except Exception as e:
        print(f"  [!] パースエラー: {file_path} - {e}")
        return None

def extract_shutuba_metadata(html_bytes: bytes) -> Dict:
    """出馬表HTMLからメタデータを抽出"""
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except Exception:
        html_text = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_text, 'html.parser')

    metadata = {
        'distance_m': None, 'track_surface': None, 'weather': None,
        'track_condition': None, 'post_time': None
    }

    # RaceData01 を探す（出馬表形式）
    race_data01 = soup.find('div', class_='RaceData01')
    if race_data01:
        race_data01_str = race_data01.get_text()

        # 距離と馬場
        distance_match = re.search(r'(芝|ダ|障)(?:右|左|)?(\d+)m', race_data01_str)
        if distance_match:
            surface_char = distance_match.group(1)
            surface_map = {'芝': '芝', 'ダ': 'ダート', '障': '障害'}
            metadata['track_surface'] = surface_map.get(surface_char)
            metadata['distance_m'] = int(distance_match.group(2))

        # 天候
        weather_match = re.search(r'天候:(\S+)', race_data01_str)
        if weather_match:
            metadata['weather'] = weather_match.group(1).strip()

        # 馬場状態
        condition_match = re.search(r'馬場:(\S+)', race_data01_str)
        if condition_match:
            metadata['track_condition'] = condition_match.group(1).strip()

        # 発走時刻
        time_match = re.search(r'(\d{1,2}:\d{2})発走', race_data01_str)
        if time_match:
            metadata['post_time'] = time_match.group(1)

    return metadata

def parse_shutuba_bin(file_path: str) -> Optional[pd.DataFrame]:
    """出馬表binファイルをパースしてDataFrameを返す"""
    filename = os.path.basename(file_path)
    race_id = filename.replace('.bin', '')

    try:
        with open(file_path, 'rb') as f:
            html_bytes = f.read()

        metadata = extract_shutuba_metadata(html_bytes)

        # 簡易的なDataFrame作成（実際にはテーブルをパースする必要あり）
        # ここでは構造のみ示す
        df = pd.DataFrame([{
            'race_id': race_id,
            **metadata
        }])

        return df

    except Exception as e:
        print(f"  [!] パースエラー: {file_path} - {e}")
        return None

def extract_horse_info(html_bytes: bytes, horse_id: str) -> Dict:
    """馬プロフィールHTMLから情報を抽出"""
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except Exception:
        html_text = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_text, 'html.parser')

    profile = {'horse_id': horse_id}

    # 馬名
    horse_title = soup.find('div', class_='horse_title')
    if horse_title:
        h1 = horse_title.find('h1')
        if h1:
            profile['horse_name'] = h1.get_text(strip=True)

    # その他の情報も抽出可能（簡略化）

    return profile

def parse_horse_profile_bin(file_path: str) -> Optional[pd.DataFrame]:
    """馬プロフィールbinファイルをパースしてDataFrameを返す"""
    filename = os.path.basename(file_path)
    horse_id = filename.replace('_profile.bin', '')

    try:
        with open(file_path, 'rb') as f:
            html_bytes = f.read()

        profile = extract_horse_info(html_bytes, horse_id)

        df = pd.DataFrame([profile])
        return df

    except Exception as e:
        print(f"  [!] パースエラー: {file_path} - {e}")
        return None

def extract_horse_performance(html_bytes: bytes, horse_id: str) -> pd.DataFrame:
    """馬の過去成績HTMLから情報を抽出"""
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except Exception:
        html_text = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_text, 'html.parser')

    # db_h_race_results テーブルを探す
    perf_table = soup.find('table', class_='db_h_race_results')

    if not perf_table:
        return pd.DataFrame()

    rows = []
    tbody = perf_table.find('tbody') if perf_table.find('tbody') else perf_table

    for tr in tbody.find_all('tr'):
        cells = tr.find_all('td')
        if len(cells) < 10:
            continue

        row_data = {
            'horse_id': horse_id,
            'race_date': cells[0].get_text(strip=True),
            'venue': cells[1].get_text(strip=True),
            'weather': cells[2].get_text(strip=True),
            'race_number': cells[3].get_text(strip=True),
            'race_name': cells[4].get_text(strip=True),
        }

        # 距離情報を抽出（障害レース対応）
        distance_cell = cells[5] if len(cells) > 5 else None
        if distance_cell:
            distance_text = distance_cell.get_text(strip=True)
            # "障2860" や "芝1800" などを抽出
            distance_match = re.search(r'(芝|ダ|障)(\d+)', distance_text)
            if distance_match:
                surface_map = {'芝': '芝', 'ダ': 'ダート', '障': '障害'}
                row_data['track_surface'] = surface_map.get(distance_match.group(1))
                row_data['distance_m'] = int(distance_match.group(2))

        rows.append(row_data)

    return pd.DataFrame(rows)

def parse_horse_performance_bin(file_path: str) -> Optional[pd.DataFrame]:
    """馬の過去成績binファイルをパースしてDataFrameを返す"""
    filename = os.path.basename(file_path)
    horse_id = filename.replace('_perf.bin', '')

    try:
        with open(file_path, 'rb') as f:
            html_bytes = f.read()

        df = extract_horse_performance(html_bytes, horse_id)
        return df

    except Exception as e:
        print(f"  [!] パースエラー: {file_path} - {e}")
        return None

def parse_pedigree_bin(file_path: str) -> Optional[pd.DataFrame]:
    """血統binファイルをパースしてDataFrameを返す"""
    filename = os.path.basename(file_path)
    horse_id = filename.replace('.bin', '')

    try:
        with open(file_path, 'rb') as f:
            html_bytes = f.read()

        # 簡易的な実装（実際には血統テーブルをパースする必要あり）
        df = pd.DataFrame([{'horse_id': horse_id, 'pedigree_data': 'placeholder'}])
        return df

    except Exception as e:
        print(f"  [!] パースエラー: {file_path} - {e}")
        return None

def analyze_directory(bin_dir: str, output_dir: str):
    """ディレクトリ内の全binファイルを分析して、複数のCSVを出力"""

    bin_path = Path(bin_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"=== 複数binファイル分析開始 ===")
    print(f"入力ディレクトリ: {bin_dir}")
    print(f"出力ディレクトリ: {output_dir}")

    # ファイルを分類
    race_result_files = []
    shutuba_files = []
    horse_profile_files = []
    horse_perf_files = []
    pedigree_files = []

    for file_path in bin_path.glob('*.bin'):
        filename = file_path.name

        # ファイル名パターンで分類
        if '_profile.bin' in filename:
            horse_profile_files.append(str(file_path))
        elif '_perf.bin' in filename:
            horse_perf_files.append(str(file_path))
        elif len(filename) == 16 and filename.endswith('.bin'):  # 12桁のrace_id
            # さらに細分化: 末尾が01なら出馬表、02なら結果など
            race_id = filename.replace('.bin', '')
            if race_id.endswith('02'):
                shutuba_files.append(str(file_path))
            else:
                race_result_files.append(str(file_path))
        else:
            # その他（血統など）
            pedigree_files.append(str(file_path))

    print(f"\n検出ファイル:")
    print(f"  レース結果: {len(race_result_files)}")
    print(f"  出馬表: {len(shutuba_files)}")
    print(f"  馬プロフィール: {len(horse_profile_files)}")
    print(f"  馬過去成績: {len(horse_perf_files)}")
    print(f"  血統: {len(pedigree_files)}")

    # 1. レース結果をパース
    if race_result_files:
        print(f"\n--- レース結果のパース ---")
        all_results = []
        for i, file_path in enumerate(race_result_files):
            print(f"  [{i+1}/{len(race_result_files)}] {os.path.basename(file_path)}")
            df = parse_race_result_bin(file_path)
            if df is not None and not df.empty:
                all_results.append(df)

        if all_results:
            results_df = pd.concat(all_results, ignore_index=True)
            output_file = output_path / 'race_results.csv'
            results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"  [✓] 保存完了: {output_file} ({len(results_df)}行)")

    # 2. 出馬表をパース
    if shutuba_files:
        print(f"\n--- 出馬表のパース ---")
        all_shutuba = []
        for i, file_path in enumerate(shutuba_files):
            print(f"  [{i+1}/{len(shutuba_files)}] {os.path.basename(file_path)}")
            df = parse_shutuba_bin(file_path)
            if df is not None and not df.empty:
                all_shutuba.append(df)

        if all_shutuba:
            shutuba_df = pd.concat(all_shutuba, ignore_index=True)
            output_file = output_path / 'shutuba.csv'
            shutuba_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"  [✓] 保存完了: {output_file} ({len(shutuba_df)}行)")

    # 3. 馬プロフィールをパース
    if horse_profile_files:
        print(f"\n--- 馬プロフィールのパース ---")
        all_profiles = []
        for i, file_path in enumerate(horse_profile_files[:10]):  # 最初の10件のみ
            print(f"  [{i+1}/{min(10, len(horse_profile_files))}] {os.path.basename(file_path)}")
            df = parse_horse_profile_bin(file_path)
            if df is not None and not df.empty:
                all_profiles.append(df)

        if all_profiles:
            profiles_df = pd.concat(all_profiles, ignore_index=True)
            output_file = output_path / 'horse_profiles.csv'
            profiles_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"  [✓] 保存完了: {output_file} ({len(profiles_df)}行)")

    # 4. 馬過去成績をパース
    if horse_perf_files:
        print(f"\n--- 馬過去成績のパース ---")
        all_perfs = []
        for i, file_path in enumerate(horse_perf_files[:10]):  # 最初の10件のみ
            print(f"  [{i+1}/{min(10, len(horse_perf_files))}] {os.path.basename(file_path)}")
            df = parse_horse_performance_bin(file_path)
            if df is not None and not df.empty:
                all_perfs.append(df)

        if all_perfs:
            perfs_df = pd.concat(all_perfs, ignore_index=True)
            output_file = output_path / 'horses_performance.csv'
            perfs_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"  [✓] 保存完了: {output_file} ({len(perfs_df)}行)")

    print(f"\n=== 分析完了 ===")
    print(f"出力先: {output_dir}")

def main():
    """メイン実行処理"""

    # コマンドライン引数から取得
    if len(sys.argv) >= 3:
        bin_dir = sys.argv[1]
        output_dir = sys.argv[2]
    elif len(sys.argv) == 2:
        bin_dir = sys.argv[1]
        output_dir = DEFAULT_OUTPUT_DIR
    else:
        bin_dir = DEFAULT_BIN_DIR
        output_dir = DEFAULT_OUTPUT_DIR

    # 実行
    analyze_directory(bin_dir, output_dir)

if __name__ == "__main__":
    main()

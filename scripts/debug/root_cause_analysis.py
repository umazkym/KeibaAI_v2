"""
問題1-4の根本原因分析スクリプト
生データ（binファイル）を直接確認して原因を特定

問題1: cornersに対応がないレース（286件）
問題2: horsesに登録がない馬（2頭）
問題3: C4データなし馬（10頭）
問題4: passing_order_3/4のNULL率50%超
"""

import pandas as pd
import numpy as np
from pathlib import Path
from bs4 import BeautifulSoup
import re

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"
RAW_DIR = PROJECT_ROOT / "keibaai" / "data" / "raw" / "html"

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def read_html_file(file_path):
    """HTMLファイルを読み込む"""
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    
    return html_text

def investigate_corners_missing_races():
    """問題1: cornersに対応がないレース（286件）の根本原因"""
    print_section("問題1: cornersに対応がないレースの根本原因分析")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
    race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")
    
    races_race_ids = set(races['race_id'].unique())
    corners_race_ids = set(corners['race_id'].unique())
    
    missing_race_ids = list(races_race_ids - corners_race_ids)[:10]  # サンプル10件
    
    print(f"\n  欠損レースのサンプル（10件）を詳細分析")
    
    for race_id in missing_race_ids:
        print(f"\n  【race_id: {race_id}】")
        
        # races.parquetから情報取得
        race_info = races[races['race_id'] == race_id].iloc[0]
        print(f"    競馬場: {race_info.get('venue', 'N/A')}")
        print(f"    距離: {race_info.get('distance_m', 'N/A')}m")
        print(f"    馬場: {race_info.get('track_surface', 'N/A')}")
        
        # race_detailsからコーナー生データを確認
        detail = race_details[race_details['race_id'] == race_id]
        if len(detail) > 0:
            detail_row = detail.iloc[0]
            c1 = detail_row.get('corner_1_raw', None)
            c2 = detail_row.get('corner_2_raw', None)
            c3 = detail_row.get('corner_3_raw', None)
            c4 = detail_row.get('corner_4_raw', None)
            print(f"    corner_1_raw: {c1}")
            print(f"    corner_2_raw: {c2}")
            print(f"    corner_3_raw: {c3}")
            print(f"    corner_4_raw: {c4}")
        else:
            print(f"    race_detailsにデータなし")
        
        # 生HTMLファイルを確認
        race_html_path = RAW_DIR / "race" / f"{race_id}.bin"
        if race_html_path.exists():
            html_text = read_html_file(race_html_path)
            soup = BeautifulSoup(html_text, 'html.parser')
            
            # コーナー通過順を探す
            corner_section = soup.find('th', string=re.compile('コーナー通過順'))
            if corner_section:
                print(f"    生HTML: コーナー通過順セクションあり")
                # 親テーブルを探す
                parent_tr = corner_section.find_parent('tr')
                if parent_tr:
                    tds = parent_tr.find_all('td')
                    for i, td in enumerate(tds):
                        print(f"      TD[{i}]: {td.get_text()[:100]}")
            else:
                print(f"    生HTML: コーナー通過順セクションなし")
                
                # 障害レースかどうか確認
                title = soup.find('title')
                if title:
                    print(f"    タイトル: {title.get_text()[:80]}")
        else:
            print(f"    生HTMLファイルが見つからない: {race_html_path}")
    
    # 共通の特徴を分析
    print(f"\n  【欠損レースの共通特徴分析】")
    all_missing = races[races['race_id'].isin(races_race_ids - corners_race_ids)]
    
    # 馬場別
    surface_dist = all_missing['track_surface'].value_counts()
    print(f"    馬場別分布:")
    for surface, count in surface_dist.items():
        print(f"      {surface}: {count}件")

def investigate_horses_missing():
    """問題2: horsesに登録がない馬（2頭）の根本原因"""
    print_section("問題2: horsesに登録がない馬の根本原因分析")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    horses = pd.read_parquet(DATA_DIR / "horses" / "horses.parquet")
    
    racing_horses = set(races['horse_id'].dropna().unique())
    registered_horses = set(horses['horse_id'].unique())
    
    missing_horses = racing_horses - registered_horses
    
    for horse_id in missing_horses:
        print(f"\n  【馬ID: {horse_id}】")
        
        # races.parquetから情報取得
        horse_races = races[races['horse_id'] == horse_id]
        horse_name = horse_races['horse_name'].iloc[0] if 'horse_name' in horse_races.columns else 'N/A'
        print(f"    馬名: {horse_name}")
        print(f"    出走レース数: {len(horse_races)}件")
        
        # 生HTMLファイル（馬プロフィール）を確認
        horse_html_path = RAW_DIR / "horse" / f"{horse_id}.bin"
        horse_profile_path = RAW_DIR / "horse" / f"{horse_id}_profile.bin"
        
        if horse_html_path.exists():
            print(f"    生HTMLファイル: あり ({horse_html_path.name})")
            html_text = read_html_file(horse_html_path)
            
            # ファイルサイズ
            file_size = horse_html_path.stat().st_size
            print(f"    ファイルサイズ: {file_size:,} bytes")
            
            # HTMLの内容を確認
            soup = BeautifulSoup(html_text, 'html.parser')
            title = soup.find('title')
            if title:
                print(f"    タイトル: {title.get_text()[:80]}")
            
            # 馬名が含まれているか
            if horse_name and horse_name in html_text:
                print(f"    馬名「{horse_name}」: HTMLに含まれている")
            else:
                print(f"    馬名「{horse_name}」: HTMLに含まれていない可能性")
            
            # エラーページかどうか
            if 'エラー' in html_text or '見つかりません' in html_text or '404' in html_text:
                print(f"    ⚠️ エラーページの可能性あり")
        elif horse_profile_path.exists():
            print(f"    生HTMLファイル: あり ({horse_profile_path.name})")
        else:
            print(f"    ⚠️ 生HTMLファイルがない")
            print(f"      期待パス1: {horse_html_path}")
            print(f"      期待パス2: {horse_profile_path}")
            
            # horse/ディレクトリ内で検索
            horse_dir = RAW_DIR / "horse"
            matching_files = list(horse_dir.glob(f"*{horse_id}*"))
            if matching_files:
                print(f"    類似ファイル: {[f.name for f in matching_files]}")

def investigate_c4_missing_horses():
    """問題3: C4データなし馬（10頭）の根本原因"""
    print_section("問題3: C4データなし馬の根本原因分析")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
    race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")
    
    c4 = corners[corners['corner'] == 4]
    
    # C4データを馬ごとに集計
    c4_merged = c4.merge(
        races[['race_id', 'horse_number', 'horse_id']].drop_duplicates(),
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    horses_with_c4 = set(c4_merged['horse_id'].dropna().unique())
    all_racing_horses = set(races['horse_id'].dropna().unique())
    
    horses_without_c4 = all_racing_horses - horses_with_c4
    
    print(f"\n  C4データなし馬: {len(horses_without_c4)}頭")
    
    for horse_id in list(horses_without_c4)[:5]:  # サンプル5頭
        print(f"\n  【馬ID: {horse_id}】")
        
        horse_races = races[races['horse_id'] == horse_id]
        horse_name = horse_races['horse_name'].iloc[0] if 'horse_name' in horse_races.columns else 'N/A'
        print(f"    馬名: {horse_name}")
        print(f"    出走レース数: {len(horse_races)}件")
        
        # このレースのrace_detailsを確認
        for _, race in horse_races.iterrows():
            race_id = race['race_id']
            print(f"\n    レースID: {race_id}")
            print(f"      距離: {race.get('distance_m', 'N/A')}m")
            print(f"      馬場: {race.get('track_surface', 'N/A')}")
            
            # race_detailsを確認
            detail = race_details[race_details['race_id'] == race_id]
            if len(detail) > 0:
                detail_row = detail.iloc[0]
                c4_raw = detail_row.get('corner_4_raw', None)
                print(f"      corner_4_raw: {c4_raw}")
            else:
                print(f"      race_detailsにデータなし")
            
            # 生HTMLを確認
            race_html_path = RAW_DIR / "race" / f"{race_id}.bin"
            if race_html_path.exists():
                html_text = read_html_file(race_html_path)
                soup = BeautifulSoup(html_text, 'html.parser')
                
                # タイトルからレースタイプを確認
                title = soup.find('title')
                if title:
                    title_text = title.get_text()
                    print(f"      タイトル: {title_text[:60]}")
                    if '障害' in title_text:
                        print(f"      ⚠️ 障害レースです")

def investigate_passing_order_null():
    """問題4: passing_order_3/4のNULL率の根本原因"""
    print_section("問題4: passing_order_3/4のNULL率の根本原因分析")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")
    
    # passing_order_4がNULLのレースをサンプリング
    null_pass4 = races[races['passing_order_4'].isnull()]
    
    # 距離別に分類
    short_null = null_pass4[null_pass4['distance_m'] <= 1400]
    mid_null = null_pass4[(null_pass4['distance_m'] > 1400) & (null_pass4['distance_m'] <= 1800)]
    long_null = null_pass4[null_pass4['distance_m'] > 1800]
    
    print(f"\n  passing_order_4がNULLのレコード総数: {len(null_pass4):,}")
    print(f"    短距離(~1400m): {len(short_null):,}件")
    print(f"    中距離(1400-1800m): {len(mid_null):,}件")
    print(f"    長距離(1800m+): {len(long_null):,}件")
    
    # 長距離でNULLのケースを詳細調査（これが異常）
    print(f"\n  【長距離（1800m+）でpassing_order_4がNULLのケース詳細】")
    
    sample_long_null = long_null.head(5)
    for _, row in sample_long_null.iterrows():
        race_id = row['race_id']
        horse_number = row['horse_number']
        
        print(f"\n    race_id: {race_id}, 馬番: {horse_number}")
        print(f"      距離: {row.get('distance_m', 'N/A')}m")
        print(f"      馬場: {row.get('track_surface', 'N/A')}")
        print(f"      passing_order_1: {row.get('passing_order_1', 'N/A')}")
        print(f"      passing_order_2: {row.get('passing_order_2', 'N/A')}")
        print(f"      passing_order_3: {row.get('passing_order_3', 'N/A')}")
        print(f"      passing_order_4: {row.get('passing_order_4', 'N/A')}")
        
        # race_detailsを確認
        detail = race_details[race_details['race_id'] == race_id]
        if len(detail) > 0:
            detail_row = detail.iloc[0]
            print(f"      corner_3_raw: {detail_row.get('corner_3_raw', 'N/A')}")
            print(f"      corner_4_raw: {detail_row.get('corner_4_raw', 'N/A')}")
        
        # 生HTMLを確認
        race_html_path = RAW_DIR / "race" / f"{race_id}.bin"
        if race_html_path.exists():
            html_text = read_html_file(race_html_path)
            soup = BeautifulSoup(html_text, 'html.parser')
            
            title = soup.find('title')
            if title:
                title_text = title.get_text()
                print(f"      タイトル: {title_text[:60]}")
                if '障害' in title_text:
                    print(f"      ⚠️ 障害レースです")
    
    # 短距離でNULLは正常であることを確認
    print(f"\n  【短距離（~1400m）でのNULLは正常】")
    print(f"    短距離レースはコーナー数が少ないため、3/4コーナーが存在しない")
    print(f"    これはデータの欠陥ではなく、レース構造の特性")

def main():
    print("=" * 80)
    print("  問題1-4の根本原因分析（生データ確認）")
    print("=" * 80)
    
    investigate_corners_missing_races()
    investigate_horses_missing()
    investigate_c4_missing_horses()
    investigate_passing_order_null()
    
    print("\n" + "=" * 80)
    print("  分析完了")
    print("=" * 80)

if __name__ == "__main__":
    main()

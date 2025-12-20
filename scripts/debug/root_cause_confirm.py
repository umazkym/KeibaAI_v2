"""
根本原因の確定調査：新潟直線1000mレースの確認
"""

import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

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

def investigate_straight_course():
    """新潟直線コースの調査"""
    print_section("新潟直線コース（1000m）レースの調査")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
    
    races_race_ids = set(races['race_id'].unique())
    corners_race_ids = set(corners['race_id'].unique())
    missing_race_ids = races_race_ids - corners_race_ids
    
    # 欠損レースの詳細
    missing_races = races[races['race_id'].isin(missing_race_ids)]
    
    print(f"\n  cornersに対応がないレース総数: {len(missing_race_ids)}件")
    
    # 競馬場別分布
    print(f"\n  【競馬場別分布】")
    venue_counts = missing_races.groupby('venue')['race_id'].nunique()
    for venue, count in venue_counts.items():
        print(f"    {venue}: {count}件")
    
    # 新潟の欠損レースを詳細調査
    niigata_missing = missing_races[missing_races['venue'] == '新潟']
    print(f"\n  新潟の欠損レース: {len(niigata_missing['race_id'].unique())}件")
    
    # サンプルの生HTMLを見て距離を特定
    print(f"\n  【新潟欠損レースの生HTML調査】")
    sample_race_ids = niigata_missing['race_id'].unique()[:5]
    
    for race_id in sample_race_ids:
        race_html_path = RAW_DIR / "race" / f"{race_id}.bin"
        if race_html_path.exists():
            html_text = read_html_file(race_html_path)
            soup = BeautifulSoup(html_text, 'html.parser')
            
            # レース名と距離を取得
            diary_snap = soup.find('div', class_='RaceData01')
            if diary_snap:
                text = diary_snap.get_text()
                print(f"\n    race_id: {race_id}")
                print(f"    RaceData01: {text[:100].strip()}")
            
            # 距離情報
            dist_match = soup.find(string=lambda x: x and ('芝' in str(x) or 'ダート' in str(x)) if x else False)
            if dist_match:
                print(f"    距離情報: {str(dist_match)[:50]}")
            
            # コーナー通過順がないことを確認
            corner_th = soup.find('th', string=lambda x: x and 'コーナー通過順' in str(x) if x else False)
            if corner_th:
                print(f"    コーナー通過順: あり")
            else:
                print(f"    コーナー通過順: なし（直線コースの可能性大）")

def investigate_distance_na():
    """distance_m=NAのレースを調査"""
    print_section("distance_m=NAのレース調査")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    
    na_distance = races[races['distance_m'].isna()]
    
    print(f"\n  distance_m=NAのレース数: {len(na_distance)}件")
    print(f"  ユニークレース数: {na_distance['race_id'].nunique()}件")
    
    # 競馬場別
    venue_counts = na_distance.groupby('venue')['race_id'].nunique()
    print(f"\n  【競馬場別分布】")
    for venue, count in venue_counts.items():
        print(f"    {venue}: {count}件")
    
    # これらのレースの生HTMLを調査
    print(f"\n  【distance_m=NAレースの生HTML調査】")
    sample_race_ids = na_distance['race_id'].unique()[:5]
    
    for race_id in sample_race_ids:
        race_html_path = RAW_DIR / "race" / f"{race_id}.bin"
        if race_html_path.exists():
            html_text = read_html_file(race_html_path)
            soup = BeautifulSoup(html_text, 'html.parser')
            
            print(f"\n    race_id: {race_id}")
            
            # レース距離を直接探す
            race_data = soup.find('span', class_='RaceData01')
            if race_data:
                print(f"    RaceData01: {race_data.get_text()[:80].strip()}")
            
            # diary_snap_cutの内容
            diary = soup.find('diary_snap_cut')
            if diary:
                print(f"    diary_snap_cut: {diary.get_text()[:80].strip()}")
            
            # タイトル
            title = soup.find('title')
            if title:
                print(f"    title: {title.get_text()[:60]}")
            
            # 「直」または「直線」を探す
            if '直' in html_text or '芝1000' in html_text:
                print(f"    ※「直」または「芝1000」を含む")

def investigate_horses_files():
    """馬ファイルの存在を詳細調査"""
    print_section("horses欠損馬のファイル存在調査")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    horses = pd.read_parquet(DATA_DIR / "horses" / "horses.parquet")
    
    racing_horses = set(races['horse_id'].dropna().unique())
    registered_horses = set(horses['horse_id'].unique())
    missing_horses = racing_horses - registered_horses
    
    horse_dir = RAW_DIR / "horse"
    
    for horse_id in missing_horses:
        print(f"\n  【馬ID: {horse_id}】")
        
        # ファイル存在確認
        patterns = [
            f"{horse_id}.bin",
            f"{horse_id}.html",
            f"{horse_id}_profile.bin",
            f"{horse_id}_profile.html",
            f"{horse_id}_perf.bin",
        ]
        
        for pattern in patterns:
            path = horse_dir / pattern
            if path.exists():
                print(f"    ✓ {pattern} あり (size: {path.stat().st_size} bytes)")
            else:
                print(f"    ✗ {pattern} なし")
        
        # ワイルドカード検索
        matching = list(horse_dir.glob(f"*{horse_id}*"))
        if matching:
            print(f"    関連ファイル: {[f.name for f in matching]}")
        
        # _perf.binがあれば内容を確認
        perf_path = horse_dir / f"{horse_id}_perf.bin"
        if perf_path.exists():
            html_text = read_html_file(perf_path)
            soup = BeautifulSoup(html_text, 'html.parser')
            
            # 馬名を探す
            horse_title = soup.find('div', class_='horse_title')
            if horse_title:
                print(f"    馬名（_perf.binから）: {horse_title.get_text()[:50].strip()}")

def check_passing_order_parsing():
    """passing_orderのパース状況を確認"""
    print_section("passing_orderパース状況の詳細確認")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")
    
    # 長距離でpassing_order_4=NAのケース
    long_null = races[(races['distance_m'] > 1800) & (races['passing_order_4'].isna())]
    
    print(f"\n  長距離（1800m+）でpassing_order_4=NAのレコード: {len(long_null)}件")
    
    # race_idごとに調査
    sample_race_ids = long_null['race_id'].unique()[:3]
    
    for race_id in sample_race_ids:
        print(f"\n  【race_id: {race_id}】")
        
        # race_detailsからcorner_4_rawを確認
        detail = race_details[race_details['race_id'] == race_id]
        if len(detail) > 0:
            c4_raw = detail.iloc[0].get('corner_4_raw', None)
            print(f"    corner_4_raw: {c4_raw}")
        
        # racesの該当レコード
        race_records = races[races['race_id'] == race_id]
        print(f"    レコード数: {len(race_records)}件")
        print(f"    passing_order_4がNULLの馬番: {list(race_records[race_records['passing_order_4'].isna()]['horse_number'])}")
        print(f"    passing_order_4がある馬番: {list(race_records[race_records['passing_order_4'].notna()]['horse_number'])}")
        
        # corner_4_rawの形式を解析
        if detail.get('corner_4_raw', pd.Series([None])).iloc[0]:
            c4_raw = detail.iloc[0]['corner_4_raw']
            print(f"    corner_4_rawの解析:")
            print(f"      「=」の数: {c4_raw.count('=')}")
            print(f"      空白区切り: {len(c4_raw.split())}")

def main():
    print("=" * 80)
    print("  根本原因の確定調査")
    print("=" * 80)
    
    investigate_straight_course()
    investigate_distance_na()
    investigate_horses_files()
    check_passing_order_parsing()
    
    print("\n" + "=" * 80)
    print("  調査完了")
    print("=" * 80)

if __name__ == "__main__":
    main()

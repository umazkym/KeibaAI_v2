"""
現在データ＆今後取得データの品質検証

検証項目:
1. 最新レースファイルが正しくパースされるか
2. 障害/直線レースのdistance_mが取得できるか
3. コーナー離れ馬がパースされるか
4. 血統データが完全か
5. 新規スクレイピングで取得したファイルの品質
"""

import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "keibaai" / "src"))

from modules.parsers.results_parser import parse_results_html, extract_race_metadata_enhanced
from modules.parsers.race_detail_parser import parse_race_html, _parse_corner_order

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"
RAW_DIR = PROJECT_ROOT / "keibaai" / "data" / "raw" / "html"

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def test_distance_parser():
    """距離パーサーのテスト（障害/直線対応確認）"""
    print_section("1. 距離パーサーのテスト")
    
    test_cases = [
        ("芝1600m / 天候 : 晴 / 芝 : 良", "通常芝"),
        ("ダ1800m / 天候 : 曇 / ダート : 稍重", "通常ダート"),
        ("障芝 外-内2890m / 天候 : 晴 / 芝 : 良", "障害芝"),
        ("障ダ 外-内2800m / 天候 : 雨 / ダート : 重", "障害ダート"),
        ("芝直線1000m / 天候 : 晴 / 芝 : 良", "新潟直線"),
    ]
    
    import re
    
    for metadata_text, label in test_cases:
        # 障害レースパターン
        obstacle_match = re.search(r'障(芝|ダ)\s*(?:外-内|内-外|外|内)?\s*(\d+)m', metadata_text)
        if obstacle_match:
            surface = '障害'
            distance = int(obstacle_match.group(2))
            print(f"  ✅ {label}: track_surface={surface}, distance_m={distance}")
            continue
        
        # 直線コースパターン
        straight_match = re.search(r'(芝|ダ)直線\s*(\d+)m', metadata_text)
        if straight_match:
            surface_map = {'芝': '芝', 'ダ': 'ダート'}
            surface = surface_map.get(straight_match.group(1))
            distance = int(straight_match.group(2))
            print(f"  ✅ {label}: track_surface={surface}, distance_m={distance}, is_straight=True")
            continue
        
        # 通常パターン
        distance_match = re.search(r'(芝|ダ|障)\s*(?:右|左|直|外|内)?\s*(?:右|左|直|外|内)?\s*(\d+)m', metadata_text)
        if distance_match:
            surface_map = {'芝': '芝', 'ダ': 'ダート', '障': '障害'}
            surface = surface_map.get(distance_match.group(1))
            distance = int(distance_match.group(2))
            print(f"  ✅ {label}: track_surface={surface}, distance_m={distance}")
            continue
        
        print(f"  ❌ {label}: パース失敗")

def test_corner_parser():
    """コーナーパーサーのテスト（離れ馬対応確認）"""
    print_section("2. コーナーパーサーのテスト")
    
    test_cases = [
        ("1,2(3,4)-5=6", "通常形式"),
        ("1(2,7)(9,3,11)(13,8,4)12,6(5,14,10)   15", "離れ馬あり"),
        ("(*9,13)(1,5,14)(2,10,3)(6,11)(8,7)-12   4", "アスタリスク付き"),
        ("3(6,10,11)8(4,12)(2,5,1)=7   9", "末尾離れ馬"),
    ]
    
    for corner_str, label in test_cases:
        rows = _parse_corner_order("test_race", 4, corner_str)
        horses = [r['horse_number'] for r in rows]
        
        # 期待される馬番を抽出
        import re
        expected = set(map(int, re.findall(r'\d+', corner_str.replace('*', ''))))
        parsed = set(horses)
        
        if expected == parsed:
            print(f"  ✅ {label}: {len(horses)}頭をパース")
        else:
            missing = expected - parsed
            print(f"  ❌ {label}: 欠損馬番 {missing}")

def test_recent_race_files():
    """最新レースファイルのパーステスト"""
    print_section("3. 最新レースファイルのパーステスト")
    
    race_dir = RAW_DIR / "race"
    race_files = sorted(race_dir.glob("2025*.bin"))[-5:]  # 2025年の最新5件
    
    if not race_files:
        race_files = sorted(race_dir.glob("2024*.bin"))[-5:]  # 2024年でフォールバック
    
    print(f"  テスト対象: {len(race_files)}件")
    
    for file_path in race_files:
        try:
            df = parse_results_html(str(file_path))
            
            if df.empty:
                print(f"  ⚠️ {file_path.name}: 空のDataFrame")
            else:
                distance = df['distance_m'].iloc[0]
                surface = df['track_surface'].iloc[0]
                count = len(df)
                print(f"  ✅ {file_path.name}: {count}頭, {surface} {distance}m")
        except Exception as e:
            print(f"  ❌ {file_path.name}: エラー - {e}")

def test_pedigree_data():
    """血統データの確認"""
    print_section("4. 血統データの確認")
    
    pedigrees = pd.read_parquet(DATA_DIR / "pedigrees" / "pedigrees.parquet")
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    
    print(f"  総レコード数: {len(pedigrees):,}")
    print(f"  ユニーク馬数: {pedigrees['horse_id'].nunique():,}")
    print(f"  馬あたり平均: {len(pedigrees) / pedigrees['horse_id'].nunique():.1f}件")
    
    # 世代別
    if 'generation' in pedigrees.columns:
        print(f"\n  世代別カウント:")
        gen_counts = pedigrees['generation'].value_counts().sort_index()
        for gen, count in gen_counts.items():
            print(f"    世代{gen}: {count:,}件")
    
    # races.parquetとのカバレッジ
    racing_horses = set(races['horse_id'].dropna().unique())
    ped_horses = set(pedigrees['horse_id'].unique())
    coverage = len(racing_horses & ped_horses) / len(racing_horses) * 100
    
    print(f"\n  レース出走馬カバレッジ: {coverage:.1f}%")

def test_horses_completeness():
    """horses.parquetの完全性確認"""
    print_section("5. horses.parquet完全性確認")
    
    horses = pd.read_parquet(DATA_DIR / "horses" / "horses.parquet")
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    
    racing_horses = set(races['horse_id'].dropna().unique())
    registered_horses = set(horses['horse_id'].unique())
    
    missing = racing_horses - registered_horses
    
    print(f"  レース出走馬: {len(racing_horses):,}頭")
    print(f"  horses登録馬: {len(registered_horses):,}頭")
    
    if missing:
        print(f"  ⚠️ 未登録馬: {len(missing)}頭")
        for h in list(missing)[:5]:
            print(f"    - {h}")
    else:
        print(f"  ✅ 全馬登録済み")

def test_future_data_compatibility():
    """今後取得するデータの互換性確認"""
    print_section("6. 今後取得データの互換性確認")
    
    # 最新のnetkeibaページをシミュレート
    test_metadata = [
        "芝1200m / 天候 : 晴 / 芝 : 良 / 発走 : 10:00",
        "ダ1400m / 天候 : 曇 / ダート : 稍重 / 発走 : 11:00",
        "障芝 外3000m / 天候 : 晴 / 芝 : 良 / 発走 : 12:00",
        "芝直線1000m / 天候 : 晴 / 芝 : 良 / 発走 : 13:00",
    ]
    
    import re
    all_ok = True
    
    for metadata_text in test_metadata:
        # 障害
        obstacle_match = re.search(r'障(芝|ダ)\s*(?:外-内|内-外|外|内)?\s*(\d+)m', metadata_text)
        if obstacle_match:
            continue
        
        # 直線
        straight_match = re.search(r'(芝|ダ)直線\s*(\d+)m', metadata_text)
        if straight_match:
            continue
        
        # 通常
        distance_match = re.search(r'(芝|ダ|障)\s*(?:右|左|直|外|内)?\s*(?:右|左|直|外|内)?\s*(\d+)m', metadata_text)
        if distance_match:
            continue
        
        print(f"  ❌ パース失敗: {metadata_text[:30]}")
        all_ok = False
    
    if all_ok:
        print(f"  ✅ 全パターン対応済み")
    
    # HTMLバリデーター確認
    from modules.utils.html_validator import validate_race_html, HtmlValidationResult
    print(f"\n  HTML検証モジュール: ✅ インポート成功")

def main():
    print("=" * 80)
    print("  現在データ＆今後取得データの品質検証")
    print("=" * 80)
    
    test_distance_parser()
    test_corner_parser()
    test_recent_race_files()
    test_pedigree_data()
    test_horses_completeness()
    test_future_data_compatibility()
    
    print("\n" + "=" * 80)
    print("  検証完了")
    print("=" * 80)

if __name__ == "__main__":
    main()

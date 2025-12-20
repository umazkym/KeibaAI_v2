"""
マスターデータとbin調査結果の照合
不足しているコースを特定
"""

import yaml
from pathlib import Path
import re
from collections import defaultdict
from tqdm import tqdm
import concurrent.futures
import os

BIN_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\race")
YAML_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\configs\course_master.yaml")

def extract_info(bin_file):
    """1ファイルからコース情報を抽出"""
    try:
        with open(bin_file, 'rb') as f:
            content = f.read().decode('euc-jp', errors='ignore')
        
        venue_match = re.search(r'\d回\s*(札幌|函館|福島|新潟|東京|中山|中京|京都|阪神|小倉)\s*\d日目', content)
        if not venue_match:
            return None
        venue = venue_match.group(1)
        
        # 障害レースもチェック
        obstacle_match = re.search(r'障(芝|ダ)\s*(?:外-内|内-外|外|内)?\s*(\d+)m', content)
        if obstacle_match:
            return {
                'venue': venue,
                'surface': '障害',
                'distance': int(obstacle_match.group(2)),
            }
        
        # 直線コース
        straight_match = re.search(r'(芝|ダ)直線\s*(\d+)m', content)
        if straight_match:
            surface_map = {'芝': '芝', 'ダ': 'ダート'}
            return {
                'venue': venue,
                'surface': surface_map.get(straight_match.group(1)),
                'distance': int(straight_match.group(2)),
                'is_straight': True
            }
        
        # 通常コース
        course_match = re.search(r'(芝|ダ)\s*(右|左)?\s*(内|外)?\s*(\d+)m', content)
        if course_match:
            surface_map = {'芝': '芝', 'ダ': 'ダート'}
            return {
                'venue': venue,
                'surface': surface_map.get(course_match.group(1)),
                'distance': int(course_match.group(4)),
            }
        
        return None
    except Exception:
        return None

def main():
    print("=" * 80)
    print("マスターデータとbin調査結果の照合")
    print("=" * 80)
    
    # マスターデータ読み込み
    with open(YAML_PATH, 'r', encoding='utf-8') as f:
        master = yaml.safe_load(f)
    
    master_courses = set()
    for venue, surfaces in master.items():
        if venue == 'venue_meta':
            continue
        for surface, distances in surfaces.items():
            for distance in distances.keys():
                master_courses.add((venue, surface, distance))
    
    print(f"マスターに定義されたコース数: {len(master_courses)}")
    
    # binファイルから全コースを抽出
    bin_files = list(BIN_DIR.glob('*.bin'))
    print(f"binファイル数: {len(bin_files)}")
    
    bin_courses = defaultdict(int)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        results = list(tqdm(executor.map(extract_info, bin_files), total=len(bin_files), desc="処理中"))
    
    for info in results:
        if info:
            key = (info['venue'], info['surface'], info['distance'])
            bin_courses[key] += 1
    
    print(f"\nbinファイルから見つかったコース数: {len(bin_courses)}")
    
    # マスターにないコースを特定
    missing_in_master = []
    for key, count in sorted(bin_courses.items()):
        venue, surface, distance = key
        if (venue, surface, distance) not in master_courses:
            missing_in_master.append((venue, surface, distance, count))
    
    print(f"\n{'='*80}")
    print(f"マスターに存在しないコース: {len(missing_in_master)}件")
    print(f"{'='*80}")
    
    for venue, surface, distance, count in missing_in_master:
        print(f"  {venue} {surface}{distance}m: {count}レース")
    
    # マスターにあるがbinにないコース
    bin_course_set = set(bin_courses.keys())
    missing_in_bin = []
    for venue, surface, distance in master_courses:
        if (venue, surface, distance) not in bin_course_set:
            missing_in_bin.append((venue, surface, distance))
    
    print(f"\n{'='*80}")
    print(f"マスターにあるがbinにないコース: {len(missing_in_bin)}件")
    print(f"{'='*80}")
    
    for venue, surface, distance in sorted(missing_in_bin):
        print(f"  {venue} {surface}{distance}m")

if __name__ == '__main__':
    main()

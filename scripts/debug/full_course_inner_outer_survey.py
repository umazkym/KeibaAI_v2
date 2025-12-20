"""
全binファイルから競馬場×距離×芝/ダート×内/外の完全調査（高速版）
HTMLからvenue情報も抽出
"""

from pathlib import Path
import re
from collections import defaultdict
from tqdm import tqdm
import concurrent.futures
import os

BIN_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\race")

def extract_info(bin_file):
    """1ファイルからコース情報を抽出"""
    try:
        with open(bin_file, 'rb') as f:
            content = f.read().decode('euc-jp', errors='ignore')
        
        # 競馬場を抽出（例: 3回 新潟 4日目）
        venue_match = re.search(r'\d回\s*(札幌|函館|福島|新潟|東京|中山|中京|京都|阪神|小倉)\s*\d日目', content)
        if not venue_match:
            return None
        venue = venue_match.group(1)
        
        # コース情報を抽出（例: 芝左 外2000m）
        course_match = re.search(r'(芝|ダート?)(左|右)\s*(内|外)?\s*(\d+)m?', content)
        if not course_match:
            return None
            
        surface, direction, inner_outer, distance = course_match.groups()
        if surface == 'ダ':
            surface = 'ダート'
        
        return {
            'race_id': bin_file.stem,
            'venue': venue,
            'surface': surface,
            'direction': direction,
            'inner_outer': inner_outer if inner_outer else '無印',
            'distance': int(distance)
        }
    except Exception:
        return None

def main():
    print("=" * 100)
    print("全binファイルから競馬場×距離×芝/ダート×内/外の完全調査")
    print("=" * 100)
    
    bin_files = list(BIN_DIR.glob('*.bin'))
    print(f"全ファイル数: {len(bin_files)}")
    
    # 並列処理で高速化
    course_data = defaultdict(lambda: {'内': 0, '外': 0, '無印': 0, 'samples': {'内': [], '外': [], '無印': []}})
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        results = list(tqdm(executor.map(extract_info, bin_files), total=len(bin_files), desc="処理中"))
    
    for info in results:
        if info:
            key = (info['venue'], info['surface'], info['distance'])
            io = info['inner_outer']
            course_data[key][io] += 1
            if len(course_data[key]['samples'][io]) < 2:
                course_data[key]['samples'][io].append(info['race_id'])
    
    # 結果を競馬場順に出力
    print("\n" + "=" * 100)
    print("全競馬場×距離×芝/ダート×内/外の組み合わせ")
    print("=" * 100)
    
    venues = ['札幌', '函館', '福島', '新潟', '東京', '中山', '中京', '京都', '阪神', '小倉']
    
    for venue in venues:
        print(f"\n{'='*80}")
        print(f"【{venue}競馬場】")
        print(f"{'='*80}")
        
        venue_courses = {k: v for k, v in course_data.items() if k[0] == venue}
        
        for surface in ['芝', 'ダート']:
            surface_courses = {k: v for k, v in venue_courses.items() if k[1] == surface}
            if not surface_courses:
                continue
            
            print(f"\n  [{surface}]")
            print(f"  {'距離':>8} {'内':>8} {'外':>8} {'無印':>8} {'備考':>10}")
            print(f"  " + "-" * 50)
            
            for key in sorted(surface_courses.keys(), key=lambda x: x[2]):
                _, _, distance = key
                counts = surface_courses[key]
                
                if counts['内'] > 0 and counts['外'] > 0:
                    note = '★内外混在'
                elif counts['内'] > 0:
                    note = '内のみ'
                elif counts['外'] > 0:
                    note = '外のみ'
                else:
                    note = ''
                
                print(f"  {distance:>7}m {counts['内']:>8} {counts['外']:>8} {counts['無印']:>8} {note:>10}")
                
                if counts['内'] > 0 and counts['外'] > 0:
                    print(f"           内sample: {counts['samples']['内']}")
                    print(f"           外sample: {counts['samples']['外']}")
    
    # まとめ
    print("\n" + "=" * 100)
    print("★ 内/外が両方存在するコース（混在コース）")
    print("=" * 100)
    
    for key, counts in sorted(course_data.items()):
        venue, surface, distance = key
        if counts['内'] > 0 and counts['外'] > 0:
            print(f"  {venue} {surface}{distance}m: 内={counts['内']}件, 外={counts['外']}件")
    
    print("\n" + "=" * 100)
    print("外または内の指定があるコース一覧")
    print("=" * 100)
    
    for key, counts in sorted(course_data.items()):
        venue, surface, distance = key
        if counts['内'] > 0 or counts['外'] > 0:
            print(f"  {venue} {surface}{distance}m: 内={counts['内']}, 外={counts['外']}, 無印={counts['無印']}")

if __name__ == '__main__':
    main()

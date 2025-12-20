"""
全binファイルから完全なコース情報を抽出（改善版）
- 芝/ダート両方を確実に抽出
- 全ての情報をファイルに保存
"""

from pathlib import Path
import re
from collections import defaultdict
from tqdm import tqdm
import concurrent.futures
import os

BIN_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\race")
OUTPUT_FILE = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\scripts\debug\complete_course_survey_results.txt")

def extract_info(bin_file):
    """1ファイルから全コース情報を抽出"""
    try:
        with open(bin_file, 'rb') as f:
            content = f.read().decode('euc-jp', errors='ignore')
        
        # 競馬場を抽出
        venue_match = re.search(r'\d回\s*(札幌|函館|福島|新潟|東京|中山|中京|京都|阪神|小倉)\s*\d日目', content)
        if not venue_match:
            return None
        venue = venue_match.group(1)
        
        # コース情報を抽出（複数パターン対応）
        # パターン1: 芝左 外2000m, 芝右1800m
        # パターン2: ダート左1400m, ダ右1800m
        patterns = [
            r'(芝)(左|右)\s*(内|外)?\s*(\d+)m',
            r'(ダート|ダ)(左|右)\s*(内|外)?\s*(\d+)m',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                surface, direction, inner_outer, distance = match.groups()
                # 正規化
                if surface == 'ダ':
                    surface = 'ダート'
                
                return {
                    'race_id': bin_file.stem,
                    'venue': venue,
                    'surface': surface,
                    'direction': direction,
                    'inner_outer': inner_outer if inner_outer else '無印',
                    'distance': int(distance),
                    'raw_match': match.group(0)
                }
        
        return None
    except Exception as e:
        return {'error': str(e), 'race_id': bin_file.stem}

def main():
    lines = []
    lines.append("=" * 100)
    lines.append("全binファイルから完全なコース情報を抽出")
    lines.append("=" * 100)
    
    bin_files = list(BIN_DIR.glob('*.bin'))
    lines.append(f"全ファイル数: {len(bin_files)}")
    print(f"全ファイル数: {len(bin_files)}")
    
    # 並列処理
    course_data = defaultdict(lambda: {'内': 0, '外': 0, '無印': 0, 'samples': {'内': [], '外': [], '無印': []}})
    errors = []
    no_match = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        results = list(tqdm(executor.map(extract_info, bin_files), total=len(bin_files), desc="処理中"))
    
    for info in results:
        if info is None:
            no_match.append("unknown")
        elif 'error' in info:
            errors.append(info)
        else:
            key = (info['venue'], info['surface'], info['distance'], info['direction'])
            io = info['inner_outer']
            course_data[key][io] += 1
            if len(course_data[key]['samples'][io]) < 2:
                course_data[key]['samples'][io].append(info['race_id'])
    
    lines.append(f"エラー: {len(errors)}件")
    lines.append(f"マッチなし: {len(no_match)}件")
    lines.append(f"正常: {len(results) - len(errors) - len(no_match)}件")
    
    # 競馬場順に出力
    lines.append("\n" + "=" * 100)
    lines.append("全競馬場×距離×芝/ダート×回り×内/外の組み合わせ")
    lines.append("=" * 100)
    
    venues = ['札幌', '函館', '福島', '新潟', '東京', '中山', '中京', '京都', '阪神', '小倉']
    
    for venue in venues:
        lines.append(f"\n{'='*80}")
        lines.append(f"【{venue}競馬場】")
        lines.append(f"{'='*80}")
        
        venue_courses = {k: v for k, v in course_data.items() if k[0] == venue}
        
        for surface in ['芝', 'ダート']:
            surface_courses = {k: v for k, v in venue_courses.items() if k[1] == surface}
            if not surface_courses:
                continue
            
            lines.append(f"\n  [{surface}]")
            lines.append(f"  {'距離':>8} {'回り':>4} {'内':>6} {'外':>6} {'無印':>6} {'備考':>12}")
            lines.append(f"  " + "-" * 55)
            
            for key in sorted(surface_courses.keys(), key=lambda x: (x[2], x[3])):
                _, _, distance, direction = key
                counts = surface_courses[key]
                total = counts['内'] + counts['外'] + counts['無印']
                
                if counts['内'] > 0 and counts['外'] > 0:
                    note = '★内外混在'
                elif counts['内'] > 0:
                    note = '内のみ'
                elif counts['外'] > 0:
                    note = '外あり'
                else:
                    note = ''
                
                lines.append(f"  {distance:>7}m {direction:>4} {counts['内']:>6} {counts['外']:>6} {counts['無印']:>6} {note:>12}")
    
    # まとめ
    lines.append("\n" + "=" * 100)
    lines.append("★ 内/外が両方存在するコース（混在コース）")
    lines.append("=" * 100)
    
    mixed = []
    for key, counts in sorted(course_data.items()):
        venue, surface, distance, direction = key
        if counts['内'] > 0 and counts['外'] > 0:
            mixed.append(f"  {venue} {surface}{distance}m ({direction}): 内={counts['内']}, 外={counts['外']}")
            lines.append(f"  {venue} {surface}{distance}m ({direction}): 内={counts['内']}件, 外={counts['外']}件")
    
    if not mixed:
        lines.append("  なし")
    
    lines.append("\n" + "=" * 100)
    lines.append("外または内の指定があるコース一覧")
    lines.append("=" * 100)
    
    for key, counts in sorted(course_data.items()):
        venue, surface, distance, direction = key
        if counts['内'] > 0 or counts['外'] > 0:
            lines.append(f"  {venue} {surface}{distance}m ({direction}): 内={counts['内']}, 外={counts['外']}, 無印={counts['無印']}")
    
    # ファイル保存
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print('\n'.join(lines))
    print(f"\n結果を {OUTPUT_FILE} に保存しました")

if __name__ == '__main__':
    main()

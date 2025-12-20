"""
全binファイルからコース情報（回り、内/外）を抽出 - 結果ファイル出力版
"""

from pathlib import Path
import re
from collections import defaultdict
import random
from tqdm import tqdm

BIN_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\race")
OUTPUT_FILE = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\scripts\debug\course_info_results.txt")

def extract_course_info(content):
    pattern = r'(芝|ダート?)(左|右)\s*(内|外)?\s*(\d+)m?'
    matches = re.findall(pattern, content)
    if matches:
        surface, direction, inner_outer, distance = matches[0]
        return {
            'surface': surface,
            'direction': direction,
            'inner_outer': inner_outer if inner_outer else None,
            'distance': int(distance)
        }
    return None

def main():
    results_lines = []
    results_lines.append("=" * 80)
    results_lines.append("binファイルからコース情報を抽出")
    results_lines.append("=" * 80)
    
    bin_files = list(BIN_DIR.glob('*.bin'))
    results_lines.append(f"全ファイル数: {len(bin_files)}")
    
    sample_size = min(4000, len(bin_files) // 10)
    sampled_files = random.sample(bin_files, sample_size)
    results_lines.append(f"サンプル数: {sample_size}")
    
    courses_with_inner_outer = defaultdict(lambda: {'inner': 0, 'outer': 0, 'none': 0})
    sample_races = defaultdict(list)
    
    for bin_file in tqdm(sampled_files, desc="処理中"):
        try:
            with open(bin_file, 'rb') as f:
                content = f.read().decode('euc-jp', errors='ignore')
            
            info = extract_course_info(content)
            if info:
                key = (info['surface'], info['direction'], info['distance'])
                if info['inner_outer'] == '内':
                    courses_with_inner_outer[key]['inner'] += 1
                    if len(sample_races[(*key, '内')]) < 3:
                        sample_races[(*key, '内')].append(bin_file.stem)
                elif info['inner_outer'] == '外':
                    courses_with_inner_outer[key]['outer'] += 1
                    if len(sample_races[(*key, '外')]) < 3:
                        sample_races[(*key, '外')].append(bin_file.stem)
                else:
                    courses_with_inner_outer[key]['none'] += 1
        except Exception:
            pass
    
    results_lines.append("")
    results_lines.append("=" * 80)
    results_lines.append("内/外情報があるコース一覧")
    results_lines.append("=" * 80)
    
    result_count = 0
    for key, counts in sorted(courses_with_inner_outer.items()):
        surface, direction, distance = key
        if counts['inner'] > 0 or counts['outer'] > 0:
            result_count += 1
            results_lines.append(f"\n{surface} {direction} {distance}m:")
            results_lines.append(f"  内: {counts['inner']}件, 外: {counts['outer']}件, 指定なし: {counts['none']}件")
            
            if counts['inner'] > 0:
                results_lines.append(f"  内サンプル: {sample_races[(*key, '内')]}")
            if counts['outer'] > 0:
                results_lines.append(f"  外サンプル: {sample_races[(*key, '外')]}")
    
    results_lines.append("")
    results_lines.append("=" * 80)
    results_lines.append(f"まとめ: 内/外情報があるコース数: {result_count}")
    results_lines.append("=" * 80)
    
    # ファイルに出力
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(results_lines))
    
    print('\n'.join(results_lines))
    print(f"\n結果を {OUTPUT_FILE} に保存しました")

if __name__ == '__main__':
    main()

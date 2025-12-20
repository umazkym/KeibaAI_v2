"""course_master.yaml の検証スクリプト"""

import yaml
from pathlib import Path

yaml_path = Path(r'c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\configs\course_master.yaml')

with open(yaml_path, 'r', encoding='utf-8') as f:
    data = yaml.safe_load(f)

print('YAMLロード成功')

# コース数をカウント
total_courses = 0
mixed_courses = []

for venue, surfaces in data.items():
    if venue == 'venue_meta':
        continue
    for surface, distances in surfaces.items():
        for distance, course_configs in distances.items():
            if 'inner' in course_configs and 'outer' in course_configs:
                mixed_courses.append(f'{venue} {surface}{distance}m')
                total_courses += 2
            else:
                total_courses += 1

print(f'総コース数: {total_courses}')
print(f'内外混在コース: {mixed_courses}')

# 新潟2000mのテスト
niigata_2000 = data['新潟']['芝'][2000]
print(f"\n新潟2000m内: {niigata_2000['inner']}")
print(f"新潟2000m外: {niigata_2000['outer']}")

# 京都1600mのテスト
kyoto_1600 = data['京都']['芝'][1600]
print(f"\n京都1600m内: {kyoto_1600['inner']}")
print(f"京都1600m外: {kyoto_1600['outer']}")

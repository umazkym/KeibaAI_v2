import re
from pathlib import Path

file_path = Path('keibaai/data/raw/html/odds/sanrentan/202406050810.bin')
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Count AXIS markers
axis_count = content.count('<!-- AXIS')
print(f'AXIS markers: {axis_count}')

# Count odds spans with real values vs placeholders
real_odds = re.findall(r'id="odds-[^"]+">([0-9,.]+)</span>', content)
placeholders = len(re.findall(r'---\.-', content))

print(f'Real odds count: {len(real_odds)}')
print(f'Placeholder count: {placeholders}')

if real_odds:
    print(f'Sample real odds: {real_odds[:10]}')
    
# Check file size
print(f'File size: {file_path.stat().st_size:,} bytes')

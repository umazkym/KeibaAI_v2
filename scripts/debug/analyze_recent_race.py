import re
from pathlib import Path

for race_id in ['202406010102', '202406050810']:
    file_path = Path(f'keibaai/data/raw/html/odds/sanrentan/{race_id}.bin')
    if file_path.exists():
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f'\n=== Race: {race_id} ===')
        
        # Find all odds spans and extract their content
        # Pattern: <span id="odds-8-010203">VALUE</span>
        spans = re.findall(r'id="(odds-[78]-\d{6})">([^<]*)</span>', content)
        
        # Count numeric vs placeholder
        numeric = [s for s in spans if re.match(r'[\d,.]+$', s[1].strip())]
        placeholder = [s for s in spans if s[1].strip() == '---.-' or s[1].strip() == '']
        
        print(f'Total spans: {len(spans)}')
        print(f'Numeric values: {len(numeric)}')
        print(f'Placeholders/empty: {len(placeholder)}')
        print(f'Other: {len(spans) - len(numeric) - len(placeholder)}')
        
        if numeric:
            print(f'Sample numeric: {numeric[:5]}')
        if placeholder:
            print(f'Sample placeholders: {placeholder[:5]}')

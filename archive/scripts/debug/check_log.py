import re
from pathlib import Path

log_path = Path(r'c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\logs\2024\01\01\pipeline.log')

if log_path.exists():
    with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
        
    # Find relevant lines
    matches = re.findall(r'.*finish_position.*', content)
    for m in matches:
        print(m)
        
    matches_target = re.findall(r'.*ターゲット変数のマージ.*', content)
    for m in matches_target:
        print(m)
else:
    print("Log file not found")

from pathlib import Path

log_file = Path("keibaai/data/logs/2024/01/01/pipeline.log")

if log_file.exists():
    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
        
    print(f"ログ行数: {len(lines)}")
    
    keywords = ["DEBUG:"]
    
    found_count = 0
    for i, line in enumerate(lines):
        for kw in keywords:
            if kw in line:
                print(f"Line {i+1}: {line.strip()}")
                found_count += 1
                
    if found_count == 0:
        print("キーワードが見つかりませんでした。")
else:
    print("ログファイルが見つかりません。")

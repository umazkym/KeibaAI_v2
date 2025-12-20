import sys
sys.path.insert(0, '.')

from pathlib import Path
from keibaai.src.modules.parsers import shutuba_parser

# 2014年と2020年のshutubaファイルをパースして比較
html_dir = Path('keibaai/data/raw/html/shutuba')

# 2014年のファイルを取得
files_2014 = sorted([f for f in html_dir.glob('2014*.bin')])[:3]
files_2020 = sorted([f for f in html_dir.glob('2020*.bin')])[:3]

print("=== 2014年のHTMLパース ===")
for f in files_2014:
    print(f"\nFile: {f.name}")
    try:
        df = shutuba_parser.parse_shutuba_html(str(f))
        print(f"  結果: {len(df)} rows")
        if len(df) > 0:
            print(f"  カラム: {list(df.columns)[:8]}")
    except Exception as e:
        print(f"  エラー: {e}")

print("\n=== 2020年のHTMLパース ===")
for f in files_2020:
    print(f"\nFile: {f.name}")
    try:
        df = shutuba_parser.parse_shutuba_html(str(f))
        print(f"  結果: {len(df)} rows")
        if len(df) > 0:
            print(f"  カラム: {list(df.columns)[:8]}")
    except Exception as e:
        print(f"  エラー: {e}")

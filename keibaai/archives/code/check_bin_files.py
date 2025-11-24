# binファイル保存場所確認スクリプト

import os
from pathlib import Path
from datetime import datetime

print("=" * 60)
print("binファイル保存場所確認")
print("=" * 60)

# 確認するディレクトリ
bin_dirs = [
    'data/raw/html/race',
    'data\\raw\\html\\race',  # Windows
    'data/raw/html/shutuba',
    'data\\raw\\html\\shutuba',  # Windows
]

for bin_dir in bin_dirs:
    bin_path = Path(bin_dir)

    if bin_path.exists():
        print(f"\n[✓] ディレクトリ存在: {bin_path}")

        # binファイル数をカウント
        bin_files = list(bin_path.glob('*.bin'))
        print(f"    総binファイル数: {len(bin_files)}")

        # 2023-10-09のbinファイルを検索
        date_pattern = '20231009*.bin'
        date_files = list(bin_path.glob(date_pattern))
        print(f"    2023-10-09のbinファイル: {len(date_files)}")

        if date_files:
            print(f"\n    最初の5件:")
            for f in sorted(date_files)[:5]:
                size_kb = f.stat().st_size / 1024
                mtime = datetime.fromtimestamp(f.stat().st_mtime)
                print(f"      {f.name} ({size_kb:.1f} KB, {mtime.strftime('%Y-%m-%d %H:%M:%S')})")

        # 202305040301-202305040312 のbinファイルを検索（東京）
        tokyo_pattern = '202305040*.bin'
        tokyo_files = list(bin_path.glob(tokyo_pattern))
        print(f"    202305040xxx のbinファイル: {len(tokyo_files)}")

        # 202308020301-202308020312 のbinファイルを検索（京都）
        kyoto_pattern = '202308020*.bin'
        kyoto_files = list(bin_path.glob(kyoto_pattern))
        print(f"    202308020xxx のbinファイル: {len(kyoto_files)}")

    else:
        print(f"\n[X] ディレクトリなし: {bin_path}")

# output_20231009 の確認
print(f"\n{'=' * 60}")
print("output_20231009 ディレクトリ確認")
print(f"{'=' * 60}")

output_dir = Path('output_20231009')
if output_dir.exists():
    print(f"[✓] ディレクトリ存在: {output_dir}")

    csv_files = list(output_dir.glob('*.csv'))
    print(f"    CSVファイル数: {len(csv_files)}")

    for csv_file in csv_files:
        size_kb = csv_file.stat().st_size / 1024
        print(f"      {csv_file.name} ({size_kb:.1f} KB)")
else:
    print(f"[X] ディレクトリなし: {output_dir}")

print(f"\n{'=' * 60}")
print("確認完了")
print(f"{'=' * 60}")

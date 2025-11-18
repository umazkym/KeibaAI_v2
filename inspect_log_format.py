#!/usr/bin/env python3
"""
ログファイルのフォーマットを確認するツール
"""

import sys
from pathlib import Path


def inspect_log_file(log_file: Path, num_lines: int = 50):
    """ログファイルの最初のN行を表示"""
    print(f"=" * 80)
    print(f"📄 ファイル: {log_file.name}")
    print(f"📊 サイズ: {log_file.stat().st_size:,} bytes")
    print(f"=" * 80)
    print()

    try:
        with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
            lines = []
            for i, line in enumerate(f, 1):
                if i <= num_lines:
                    lines.append((i, line.rstrip()))
                else:
                    break

            print(f"最初の {len(lines)} 行:")
            print("-" * 80)
            for line_num, line in lines:
                print(f"{line_num:4d}: {line}")
            print("-" * 80)

            # 総行数をカウント
            f.seek(0)
            total_lines = sum(1 for _ in f)
            print(f"\n総行数: {total_lines:,}")

            # 最後の数行も表示
            f.seek(0)
            last_lines = []
            for line in f:
                last_lines.append(line.rstrip())
                if len(last_lines) > 10:
                    last_lines.pop(0)

            print(f"\n最後の {len(last_lines)} 行:")
            print("-" * 80)
            for i, line in enumerate(last_lines, total_lines - len(last_lines) + 1):
                print(f"{i:4d}: {line}")
            print("-" * 80)

    except Exception as e:
        print(f"❌ エラー: {e}")


def main():
    if len(sys.argv) > 1:
        log_dir = Path(sys.argv[1])
    else:
        from datetime import date
        today = date.today()
        log_dir = Path(f"keibaai/data/logs/{today.year}/{today.month:02d}/{today.day:02d}")

    print(f"🔍 ログフォーマット確認ツール")
    print(f"📁 ログディレクトリ: {log_dir}\n")

    if not log_dir.exists():
        print(f"⚠️  ディレクトリが存在しません: {log_dir}")
        return

    log_files = sorted(log_dir.glob("*.log"))

    if not log_files:
        print(f"⚠️  ログファイルが見つかりません")
        return

    for log_file in log_files:
        inspect_log_file(log_file)
        print("\n")


if __name__ == "__main__":
    main()

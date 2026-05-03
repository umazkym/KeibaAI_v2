#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
デバッグ: Excel生成プロセスのエラーを直接確認する

Usage:
  python scripts/debug/debug_excel_gen.py --date 20260403 --venue 中山
"""

import sys
import os
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True)
    parser.add_argument('--venue', required=True)
    args = parser.parse_args()
    
    excel_script = PROJECT_ROOT / 'archive' / 'scripts' / 'generate_race_report.py'
    print(f"Script: {excel_script}")
    print(f"Exists: {excel_script.exists()}")
    print(f"Date: {args.date}, Venue: {args.venue}")
    print("=" * 60)
    print("Running generate_race_report.py with full output...")
    print("=" * 60)
    
    # capture_output=False で直接出力を見せる
    result = subprocess.run(
        [sys.executable, str(excel_script), '--date', args.date, '--venue', args.venue],
        cwd=str(PROJECT_ROOT),
        capture_output=True, text=True, encoding='utf-8'
    )
    
    print("\n=== STDOUT ===")
    print(result.stdout[-5000:] if len(result.stdout) > 5000 else result.stdout)
    
    print("\n=== STDERR ===")
    print(result.stderr[-5000:] if len(result.stderr) > 5000 else result.stderr)
    
    print(f"\n=== Return Code: {result.returncode} ===")
    
    # Excelファイルの存在確認
    excel_name = f"race_analysis_{args.date}_{args.venue}.xlsx"
    for search_dir in [PROJECT_ROOT, PROJECT_ROOT / 'outputs' / 'reports']:
        path = search_dir / excel_name
        if path.exists():
            print(f"✅ Found: {path} ({path.stat().st_size:,} bytes)")
        else:
            print(f"❌ Not found: {path}")

if __name__ == '__main__':
    main()

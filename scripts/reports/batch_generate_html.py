#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指定日・指定会場のHTMLレポートを非対話で一括生成するスクリプト

Usage:
  python scripts/reports/batch_generate_html.py --date 20260329 --venues 阪神 中京 中山
"""

import sys
import os
import shutil
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'scripts' / '_archive_legacy'))

OUTPUT_DIR = PROJECT_ROOT / 'outputs' / 'reports'


def generate_one(date_str: str, venue: str):
    """1会場分のHTML生成"""
    print(f"\n{'=' * 60}")
    print(f"  {venue} ({date_str}) レポート生成中...")
    print(f"{'=' * 60}")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    excel_name = f"race_analysis_{date_str}_{venue}.xlsx"
    html_name = f"race_analysis_{date_str}_{venue}_interactive.html"
    excel_path = OUTPUT_DIR / excel_name
    html_path = OUTPUT_DIR / html_name
    
    # Step 1: Excel生成
    print("[1/2] Excelレポート生成中...")
    excel_script = PROJECT_ROOT / 'scripts' / '_archive_legacy' / 'generate_race_report.py'
    result = subprocess.run(
        [sys.executable, str(excel_script), '--date', date_str, '--venue', venue],
        cwd=str(PROJECT_ROOT),
        capture_output=True, text=True, encoding='utf-8', errors='replace'
    )
    
    if result.returncode != 0:
        print(f"  ⚠️ Excel生成でエラー:")
        # 最後の10行を表示
        stderr_lines = result.stderr.strip().split('\n')[-10:]
        for line in stderr_lines:
            print(f"    {line}")
    
    # Excelファイルの移動
    excel_src = PROJECT_ROOT / excel_name
    if excel_src.exists():
        shutil.move(str(excel_src), str(excel_path))
        print(f"  → Excel: {excel_path}")
    elif not excel_path.exists():
        print(f"  ❌ Excelファイルが生成されませんでした")
        return None
    
    # Step 2: HTML変換
    print("[2/2] HTML変換中...")
    html_script = PROJECT_ROOT / 'scripts' / 'reports' / 'create_interactive_charts.py'
    result = subprocess.run(
        [sys.executable, str(html_script), str(excel_path)],
        cwd=str(OUTPUT_DIR),
        capture_output=True, text=True, encoding='utf-8', errors='replace'
    )
    
    if result.returncode != 0:
        print(f"  ⚠️ HTML変換でエラー:")
        stderr_lines = result.stderr.strip().split('\n')[-10:]
        for line in stderr_lines:
            print(f"    {line}")
    
    if html_path.exists():
        print(f"  ✅ 完了: {html_path}")
        # 中間Excelを削除
        if excel_path.exists():
            excel_path.unlink()
        return html_path
    else:
        print(f"  ❌ HTMLファイルが生成されませんでした")
        return None


def main():
    parser = argparse.ArgumentParser(description='HTML1括レポート生成')
    parser.add_argument('--date', required=True, help='日付 (YYYYMMDD)')
    parser.add_argument('--venues', nargs='+', required=True, help='会場名 (スペース区切り)')
    args = parser.parse_args()
    
    print(f"日付: {args.date}")
    print(f"会場: {', '.join(args.venues)}")
    
    results = []
    for venue in args.venues:
        html_path = generate_one(args.date, venue)
        results.append((venue, html_path))
    
    print(f"\n{'=' * 60}")
    print(f"  生成結果サマリー")
    print(f"{'=' * 60}")
    for venue, path in results:
        if path:
            print(f"  ✅ {venue}: {path}")
        else:
            print(f"  ❌ {venue}: 失敗")


if __name__ == "__main__":
    main()

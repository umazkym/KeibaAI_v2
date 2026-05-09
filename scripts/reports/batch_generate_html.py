#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指定日・指定会場のHTMLレポートを非対話で一括生成するスクリプト

Usage:
  python scripts/reports/batch_generate_html.py --date 20260329 --venues 阪神 中京 中山
  python scripts/reports/batch_generate_html.py --date 20260329 --venues 阪神 --delete-excel
  python scripts/reports/batch_generate_html.py --date 20260329 --venues 阪神 --reuse-existing
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


def generate_one(date_str: str, venue: str, keep_excel: bool = True, reuse_existing: bool = False):
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
        print("  [WARN] Excel生成でエラー:")
        # 最後の10行を表示
        stderr_lines = result.stderr.strip().split('\n')[-10:]
        for line in stderr_lines:
            print(f"    {line}")
        if not reuse_existing:
            print("  [ERROR] Excel生成に失敗したため、既存Excelは再利用しません")
            return None
    
    # Excelファイルの移動
    excel_src = PROJECT_ROOT / excel_name
    if excel_src.exists():
        shutil.move(str(excel_src), str(excel_path))
        print(f"  -> Excel: {excel_path}")
    elif excel_path.exists() and reuse_existing:
        print(f"  -> 既存Excelを再利用: {excel_path}")
    elif not excel_path.exists():
        print("  [ERROR] Excelファイルが生成されませんでした")
        return None
    else:
        print("  [ERROR] 新しいExcelファイルを確認できませんでした")
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
        print("  [WARN] HTML変換でエラー:")
        stderr_lines = result.stderr.strip().split('\n')[-10:]
        for line in stderr_lines:
            print(f"    {line}")
    
    if html_path.exists():
        print(f"  [OK] 完了: {html_path}")
        if excel_path.exists() and not keep_excel:
            excel_path.unlink()
            print(f"  -> Excel削除: {excel_path}")
        elif excel_path.exists():
            print(f"  -> Excel保持: {excel_path}")
        return html_path
    else:
        print("  [ERROR] HTMLファイルが生成されませんでした")
        return None


def main():
    parser = argparse.ArgumentParser(description='HTML1括レポート生成')
    parser.add_argument('--date', required=True, help='日付 (YYYYMMDD)')
    parser.add_argument('--venues', nargs='+', required=True, help='会場名 (スペース区切り)')
    parser.add_argument('--delete-excel', action='store_true', help='HTML生成後に中間Excelを削除する')
    parser.add_argument('--reuse-existing', action='store_true', help='Excel生成失敗時に既存Excelを使ってHTMLだけ再生成する')
    args = parser.parse_args()
    
    print(f"日付: {args.date}")
    print(f"会場: {', '.join(args.venues)}")
    
    results = []
    for venue in args.venues:
        html_path = generate_one(
            args.date,
            venue,
            keep_excel=not args.delete_excel,
            reuse_existing=args.reuse_existing,
        )
        results.append((venue, html_path))
    
    print(f"\n{'=' * 60}")
    print(f"  生成結果サマリー")
    print(f"{'=' * 60}")
    for venue, path in results:
        if path:
            print(f"  [OK] {venue}: {path}")
        else:
            print(f"  [ERROR] {venue}: 失敗")


if __name__ == "__main__":
    main()

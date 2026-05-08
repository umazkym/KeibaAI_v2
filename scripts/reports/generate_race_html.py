#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
レース分析HTMLレポート生成（対話形式）

netkeibaからスクレイピングし、直接HTMLレポートを生成。
Excel中間ファイル不要。
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# プロジェクトルート設定
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'scripts' / '_archive_legacy'))

def ask_input(prompt, default=None):
    """対話形式で入力を受け取る"""
    if default:
        user_input = input(f"{prompt} [{default}]: ").strip()
        return user_input if user_input else default
    else:
        return input(f"{prompt}: ").strip()

def main():
    print("=" * 60)
    print("  レース分析HTMLレポート生成")
    print("=" * 60)
    print()
    
    # 日付入力
    today = datetime.now().strftime('%Y%m%d')
    date_input = ask_input("対象日付 (YYYYMMDD)", today)
    
    # 会場入力
    venues = ["中山", "阪神", "中京", "東京", "京都", "札幌", "函館", "福島", "新潟", "小倉"]
    print(f"\n会場選択肢: {', '.join(venues)}")
    venue_input = ask_input("会場名")
    
    if venue_input not in venues:
        print(f"警告: '{venue_input}' は標準会場リストにありません。続行しますか？")
        if input("続行 (y/n): ").lower() != 'y':
            return
    
    # 出力フォルダ入力
    default_output = str(PROJECT_ROOT / 'outputs' / 'reports')
    output_dir = ask_input("出力フォルダ", default_output)
    os.makedirs(output_dir, exist_ok=True)
    
    print()
    print("-" * 40)
    print(f"日付:   {date_input}")
    print(f"会場:   {venue_input}")
    print(f"出力先: {output_dir}")
    print("-" * 40)
    
    if input("\nこの設定で実行しますか？ (y/n): ").lower() != 'y':
        print("キャンセルしました。")
        return
    
    print("\n処理を開始します...\n")
    
    # 既存スクリプトをインポートして実行
    try:
        from generate_race_report import NetkeibaAnalyzer
        from scripts.reports.create_interactive_charts import create_interactive_charts
    except ImportError as e:
        print(f"モジュールインポートエラー: {e}")
        print("代替方法で実行します...")
        run_legacy(date_input, venue_input, output_dir)
        return
    
    # Excel生成
    analyzer = NetkeibaAnalyzer(date_input, venue_input)
    excel_path = os.path.join(output_dir, f"race_analysis_{date_input}_{venue_input}.xlsx")
    analyzer.run()
    
    # 生成されたExcelをHTMLに変換
    if os.path.exists(f"race_analysis_{date_input}_{venue_input}.xlsx"):
        import shutil
        shutil.move(f"race_analysis_{date_input}_{venue_input}.xlsx", excel_path)
    
    if os.path.exists(excel_path):
        html_path = create_interactive_charts(excel_path)
        print(f"\n完了: {html_path}")
        
        # Excelファイルを削除するか確認
        if input("\n中間Excelファイルを削除しますか？ (y/n): ").lower() == 'y':
            os.remove(excel_path)
            print(f"削除: {excel_path}")
    else:
        print(f"エラー: Excelファイルが生成されませんでした")


def run_legacy(date_input, venue_input, output_dir):
    """既存スクリプトをサブプロセスで実行"""
    import subprocess
    
    # Step 1: Excel生成
    print("[1/2] Excelレポート生成中...")
    excel_script = PROJECT_ROOT / 'scripts' / '_archive_legacy' / 'generate_race_report.py'
    result = subprocess.run([
        sys.executable, str(excel_script),
        '--date', date_input,
        '--venue', venue_input
    ], cwd=str(PROJECT_ROOT), capture_output=False)
    
    excel_name = f"race_analysis_{date_input}_{venue_input}.xlsx"
    excel_src = PROJECT_ROOT / excel_name
    excel_dst = Path(output_dir) / excel_name
    
    if excel_src.exists():
        import shutil
        shutil.move(str(excel_src), str(excel_dst))
        print(f"  -> {excel_dst}")
    elif excel_dst.exists():
        pass  # 既に正しい場所にある
    else:
        print(f"エラー: {excel_name} が見つかりません")
        return
    
    # Step 2: HTML変換
    print("[2/2] HTML変換中...")
    html_script = PROJECT_ROOT / 'scripts' / 'reports' / 'create_interactive_charts.py'
    result = subprocess.run([
        sys.executable, str(html_script),
        str(excel_dst)
    ], cwd=str(output_dir), capture_output=False)
    
    html_path = excel_dst.with_name(excel_dst.stem + '_interactive.html')
    if html_path.exists():
        print(f"\n完了: {html_path}")
        
        # Excelファイルを削除するか確認
        if input("\n中間Excelファイルを削除しますか？ (y/n): ").lower() == 'y':
            excel_dst.unlink()
            print(f"削除: {excel_dst}")
    else:
        print("警告: HTMLファイルの生成を確認できませんでした")


if __name__ == "__main__":
    main()

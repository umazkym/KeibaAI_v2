#!/usr/bin/env python3
"""
デバッグスクリプト03: パースログの確認
パース処理のログファイルを分析し、エラー原因を特定します
"""

import os
from pathlib import Path
from datetime import datetime
import re
from collections import Counter

def find_latest_log():
    """最新のparsing.logを探す"""
    print("=" * 80)
    print("📁 パースログファイルの検索")
    print("=" * 80)

    log_base_dir = Path("keibaai/data/logs")

    if not log_base_dir.exists():
        print(f"\n❌ ログディレクトリが存在しません: {log_base_dir}")
        return None

    # すべてのparsing.logを探す
    log_files = list(log_base_dir.rglob("parsing.log"))

    if not log_files:
        print(f"\n❌ parsing.logファイルが見つかりませんでした")
        return None

    # 最新のログファイルを選択（更新日時でソート）
    latest_log = max(log_files, key=lambda f: f.stat().st_mtime)

    print(f"\n✅ 最新のログファイルを発見:")
    print(f"   パス: {latest_log}")
    print(f"   サイズ: {latest_log.stat().st_size / 1024:.2f} KB")
    print(f"   更新日時: {datetime.fromtimestamp(latest_log.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")

    return latest_log

def analyze_log_file(log_path: Path):
    """ログファイルを詳細分析"""
    print("\n" + "=" * 80)
    print("📊 ログファイルの詳細分析")
    print("=" * 80)

    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        print(f"\n📄 ログファイル総行数: {len(lines):,} 行")

        # ログレベル別カウント
        level_counts = Counter()
        error_messages = []
        warning_messages = []
        info_messages = []

        for line in lines:
            if 'ERROR' in line:
                level_counts['ERROR'] += 1
                error_messages.append(line.strip())
            elif 'WARNING' in line:
                level_counts['WARNING'] += 1
                warning_messages.append(line.strip())
            elif 'INFO' in line:
                level_counts['INFO'] += 1
                info_messages.append(line.strip())

        # ログレベル別集計
        print(f"\n📊 ログレベル別集計:")
        print(f"  INFO:    {level_counts.get('INFO', 0):>8,} 件")
        print(f"  WARNING: {level_counts.get('WARNING', 0):>8,} 件")
        print(f"  ERROR:   {level_counts.get('ERROR', 0):>8,} 件")

        # エラーメッセージの分析
        if error_messages:
            print(f"\n🚨 エラーメッセージの分析 ({len(error_messages)}件):")
            print("-" * 80)

            # エラーパターンの抽出
            error_patterns = Counter()

            for msg in error_messages:
                # エラーの種類を抽出（正規表現で）
                if 'Traceback' in msg:
                    error_patterns['Traceback'] += 1
                elif 'KeyError' in msg:
                    match = re.search(r"KeyError: '(\w+)'", msg)
                    if match:
                        error_patterns[f"KeyError: {match.group(1)}"] += 1
                    else:
                        error_patterns['KeyError (その他)'] += 1
                elif 'AttributeError' in msg:
                    error_patterns['AttributeError'] += 1
                elif 'ValueError' in msg:
                    error_patterns['ValueError'] += 1
                elif 'IndexError' in msg:
                    error_patterns['IndexError'] += 1
                elif 'TypeError' in msg:
                    error_patterns['TypeError'] += 1
                elif 'NoneType' in msg:
                    error_patterns['NoneType エラー'] += 1
                else:
                    # エラーメッセージの最初の50文字をパターンとして登録
                    short_msg = msg[:100] + "..." if len(msg) > 100 else msg
                    error_patterns[short_msg] += 1

            # 頻出エラーパターンTOP10
            print(f"\n  頻出エラーパターン TOP10:")
            for pattern, count in error_patterns.most_common(10):
                print(f"    [{count:>6,}件] {pattern}")

            # 最初のエラーメッセージを10件表示
            print(f"\n  最初のエラーメッセージ（10件）:")
            for i, msg in enumerate(error_messages[:10], 1):
                # 長すぎる場合は省略
                display_msg = msg if len(msg) <= 150 else msg[:150] + "..."
                print(f"    {i:>2}. {display_msg}")

        # ワーニングメッセージの分析
        if warning_messages:
            print(f"\n⚠️ ワーニングメッセージの分析 ({len(warning_messages)}件):")
            print("-" * 80)

            # 最初の10件を表示
            for i, msg in enumerate(warning_messages[:10], 1):
                display_msg = msg if len(msg) <= 150 else msg[:150] + "..."
                print(f"    {i:>2}. {display_msg}")

        # パース成功/失敗のカウント
        print(f"\n📈 パース処理の統計:")
        print("-" * 80)

        # INFOメッセージから件数を抽出
        for msg in info_messages:
            if "件のレース結果HTMLファイルが見つかりました" in msg:
                match = re.search(r'(\d+)件', msg)
                if match:
                    print(f"  レース結果HTML: {int(match.group(1)):>8,} 件発見")

            elif "件の出馬表HTMLファイルが見つかりました" in msg:
                match = re.search(r'(\d+)件', msg)
                if match:
                    print(f"  出馬表HTML:     {int(match.group(1)):>8,} 件発見")

            elif "件の馬プロフィールHTMLファイルが見つかりました" in msg:
                match = re.search(r'(\d+)件', msg)
                if match:
                    print(f"  馬プロフィール:  {int(match.group(1)):>8,} 件発見")

            elif "件の血統HTMLファイルが見つかりました" in msg:
                match = re.search(r'(\d+)件', msg)
                if match:
                    print(f"  血統HTML:       {int(match.group(1)):>8,} 件発見")

            elif "件の馬過去成績ファイルが見つかりました" in msg:
                match = re.search(r'(\d+)件', msg)
                if match:
                    print(f"  馬過去成績:     {int(match.group(1)):>8,} 件発見")

            elif "レコード)" in msg and "保存しました" in msg:
                match = re.search(r'\((\d+)レコード\)', msg)
                if match:
                    if "レース結果" in msg:
                        print(f"  → レース結果:   {int(match.group(1)):>8,} レコード保存")
                    elif "出馬表" in msg:
                        print(f"  → 出馬表:       {int(match.group(1)):>8,} レコード保存")
                    elif "馬プロフィール" in msg:
                        print(f"  → 馬プロフィール: {int(match.group(1)):>8,} レコード保存")
                    elif "血統" in msg:
                        print(f"  → 血統:         {int(match.group(1)):>8,} レコード保存")
                    elif "馬過去成績" in msg:
                        print(f"  → 馬過去成績:   {int(match.group(1)):>8,} レコード保存")

        # 特定のエラーパターンを検索
        print(f"\n🔍 特定エラーパターンの検索:")
        print("-" * 80)

        # テーブル未発見エラー
        table_not_found = sum(1 for msg in error_messages if 'テーブルが見つかりません' in msg or 'table' in msg.lower() and 'not found' in msg.lower())
        if table_not_found > 0:
            print(f"  ⚠️ テーブル未発見エラー: {table_not_found:>6,} 件")

        # エンコーディングエラー
        encoding_errors = sum(1 for msg in error_messages if 'encoding' in msg.lower() or 'decode' in msg.lower())
        if encoding_errors > 0:
            print(f"  ⚠️ エンコーディングエラー: {encoding_errors:>6,} 件")

        # NoneTypeエラー
        nonetype_errors = sum(1 for msg in error_messages if 'NoneType' in msg)
        if nonetype_errors > 0:
            print(f"  ⚠️ NoneType エラー: {nonetype_errors:>6,} 件")

    except Exception as e:
        print(f"\n❌ ログファイル読み込みエラー: {e}")
        import traceback
        traceback.print_exc()

def check_parse_errors_db():
    """パースエラーログ（データベース）の確認"""
    print("\n" + "=" * 80)
    print("💾 パースエラーデータベースの確認")
    print("=" * 80)

    db_path = Path("keibaai/data/metadata/db.sqlite3")

    if not db_path.exists():
        print(f"\n❌ データベースが存在しません: {db_path}")
        return

    try:
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # テーブル一覧を確認
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if not tables:
            print("\n⚠️ データベースにテーブルが存在しません")
            conn.close()
            return

        print(f"\n📋 テーブル一覧:")
        for (table_name,) in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  - {table_name:30s} : {count:>10,} レコード")

            # parse_errorsテーブルがあれば詳細を表示
            if 'parse_error' in table_name.lower() or 'error' in table_name.lower():
                print(f"\n    💡 エラーログテーブルを発見: {table_name}")

                # 最初の10件を表示
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
                rows = cursor.fetchall()

                # カラム名を取得
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in cursor.fetchall()]

                print(f"    カラム: {', '.join(columns)}")
                print(f"\n    最初の10件:")

                for i, row in enumerate(rows, 1):
                    print(f"      {i:>2}. {row}")

        conn.close()

    except Exception as e:
        print(f"\n❌ データベース読み込みエラー: {e}")

def main():
    """メイン処理"""
    print("\n")
    print("🔍 KeibaAI_v2 パースログ分析")
    print(f"⏰ 実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n")

    # 1. 最新ログファイルの検索
    log_path = find_latest_log()

    if log_path:
        # 2. ログファイルの詳細分析
        analyze_log_file(log_path)

    # 3. エラーデータベースの確認
    check_parse_errors_db()

    # まとめ
    print("\n" + "=" * 80)
    print("📋 分析完了")
    print("=" * 80)
    print("\n💡 次のアクション:")
    print("  1. エラーパターンを確認し、パーサーの修正箇所を特定")
    print("  2. 最も頻出するエラーから優先的に対応")
    print("  3. 修正後、パース処理を再実行")
    print("\n" + "=" * 80)
    print("\n")

if __name__ == "__main__":
    main()

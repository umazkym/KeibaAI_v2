#!/usr/bin/env python3
"""
デバッグスクリプト01: データの存在確認
KeibaAI_v2プロジェクトのデータ状況を詳細に調査します
"""

import os
from pathlib import Path
from datetime import datetime

def check_directory_structure():
    """データディレクトリ構造を確認"""
    print("=" * 80)
    print("データディレクトリ構造の確認")
    print("=" * 80)

    base_paths = [
        "keibaai/data/raw/html/race",
        "keibaai/data/raw/html/shutuba",
        "keibaai/data/raw/html/horse",
        "keibaai/data/raw/html/ped",
        "keibaai/data/parsed/parquet/races",
        "keibaai/data/parsed/parquet/shutuba",
        "keibaai/data/parsed/parquet/horses",
        "keibaai/data/parsed/parquet/pedigrees",
        "keibaai/data/features/parquet",
        "keibaai/data/models",
        "keibaai/data/metadata",
    ]

    results = {}

    for path_str in base_paths:
        path = Path(path_str)
        exists = path.exists()

        if exists:
            # ファイル数をカウント
            if path.is_dir():
                files = list(path.rglob("*"))
                file_count = len([f for f in files if f.is_file()])
                dir_count = len([f for f in files if f.is_dir()])

                # ファイルサイズの合計
                total_size = sum(f.stat().st_size for f in files if f.is_file())
                size_mb = total_size / (1024 * 1024)

                results[path_str] = {
                    "exists": True,
                    "files": file_count,
                    "dirs": dir_count,
                    "size_mb": size_mb
                }
            else:
                results[path_str] = {
                    "exists": True,
                    "is_file": True
                }
        else:
            results[path_str] = {"exists": False}

    # 結果を表示
    print("\n📁 ディレクトリ別の状況:\n")
    for path_str, info in results.items():
        if info["exists"]:
            if "files" in info:
                status = f"✅ 存在 | ファイル: {info['files']}件 | サイズ: {info['size_mb']:.2f} MB"
            else:
                status = "✅ 存在（ファイル）"
        else:
            status = "❌ 存在しない"

        print(f"{path_str:50s} -> {status}")

    return results

def check_raw_html_samples():
    """raw HTMLファイルのサンプルを確認"""
    print("\n" + "=" * 80)
    print("RAW HTMLファイルのサンプル確認")
    print("=" * 80)

    html_dirs = {
        "race": "keibaai/data/raw/html/race",
        "shutuba": "keibaai/data/raw/html/shutuba",
        "horse": "keibaai/data/raw/html/horse",
        "ped": "keibaai/data/raw/html/ped",
    }

    for name, dir_path in html_dirs.items():
        path = Path(dir_path)
        print(f"\n📄 {name} HTMLファイル:")

        if not path.exists():
            print(f"  ❌ ディレクトリが存在しません: {dir_path}")
            continue

        # 最初の5ファイルを表示
        files = sorted(path.glob("*"))[:5]

        if not files:
            print(f"  ⚠️ ファイルが見つかりませんでした")
        else:
            for file in files:
                size_kb = file.stat().st_size / 1024
                mtime = datetime.fromtimestamp(file.stat().st_mtime)
                print(f"  - {file.name:40s} | {size_kb:>8.2f} KB | {mtime.strftime('%Y-%m-%d %H:%M:%S')}")

            total_files = len(list(path.glob("*")))
            print(f"  📊 合計: {total_files}件")

def check_parquet_files():
    """Parquetファイルの存在確認"""
    print("\n" + "=" * 80)
    print("Parquetファイルの確認")
    print("=" * 80)

    parquet_files = [
        "keibaai/data/parsed/parquet/races/races.parquet",
        "keibaai/data/parsed/parquet/shutuba/shutuba.parquet",
        "keibaai/data/parsed/parquet/horses/horses.parquet",
        "keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet",
    ]

    print("\n📊 主要なParquetファイル:\n")

    for file_path_str in parquet_files:
        file_path = Path(file_path_str)

        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)

            try:
                import pandas as pd
                df = pd.read_parquet(file_path)
                rows = len(df)
                cols = len(df.columns)
                status = f"✅ {rows:,}行 × {cols}列 | {size_mb:.2f} MB | 更新: {mtime.strftime('%Y-%m-%d %H:%M')}"
            except Exception as e:
                status = f"⚠️ 読み込みエラー: {str(e)[:50]}"
        else:
            status = "❌ ファイルが存在しません"

        print(f"{file_path.name:30s} -> {status}")

def check_metadata_db():
    """メタデータDBの確認"""
    print("\n" + "=" * 80)
    print("メタデータDBの確認")
    print("=" * 80)

    db_path = Path("keibaai/data/metadata/db.sqlite3")

    if not db_path.exists():
        print(f"\n❌ データベースが存在しません: {db_path}")
        return

    print(f"\n✅ データベースが存在します: {db_path}")
    print(f"   サイズ: {db_path.stat().st_size / 1024:.2f} KB")

    try:
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # テーブル一覧を取得
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        print(f"\n📋 テーブル一覧 ({len(tables)}件):\n")

        for (table_name,) in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  - {table_name:30s} : {count:>10,} レコード")

        conn.close()

    except Exception as e:
        print(f"\n⚠️ データベース読み込みエラー: {e}")

def main():
    """メイン処理"""
    print("\n")
    print("🔍 KeibaAI_v2 データ存在確認デバッグスクリプト")
    print(f"⏰ 実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n")

    # 1. ディレクトリ構造確認
    dir_results = check_directory_structure()

    # 2. RAW HTMLサンプル確認
    check_raw_html_samples()

    # 3. Parquetファイル確認
    check_parquet_files()

    # 4. メタデータDB確認
    check_metadata_db()

    # サマリー
    print("\n" + "=" * 80)
    print("📊 サマリー")
    print("=" * 80)

    raw_exists = any(
        dir_results.get(f"keibaai/data/raw/html/{t}", {}).get("exists", False)
        for t in ["race", "shutuba", "horse", "ped"]
    )

    parsed_exists = any(
        dir_results.get(f"keibaai/data/parsed/parquet/{t}", {}).get("exists", False)
        for t in ["races", "shutuba", "horses", "pedigrees"]
    )

    print(f"\n✅ RAWデータ（HTML）: {'存在する' if raw_exists else '存在しない'}")
    print(f"✅ パース済みデータ（Parquet）: {'存在する' if parsed_exists else '存在しない'}")

    if raw_exists and not parsed_exists:
        print("\n💡 推奨アクション: パース処理を実行してください")
        print("   → python keibaai/src/run_parsing_pipeline_local.py")
    elif not raw_exists:
        print("\n💡 推奨アクション: スクレイピング処理を実行してください")
        print("   → python keibaai/src/run_scraping_pipeline_local.py")
    elif parsed_exists:
        print("\n💡 推奨アクション: データが揃っています。特徴量生成やモデル学習に進めます")

    print("\n" + "=" * 80)
    print("✅ デバッグスクリプト01完了")
    print("=" * 80)
    print("\n")

if __name__ == "__main__":
    main()

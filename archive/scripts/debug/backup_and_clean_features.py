# -*- coding: utf-8 -*-
"""
2020-2023年の特徴量データをバックアップして削除するスクリプト

使用方法:
1. Dry-run（削除せず確認のみ）: python backup_and_clean_features.py
2. 実際に削除: python backup_and_clean_features.py --execute
"""
import shutil
import argparse
from pathlib import Path
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='2020-2023年の特徴量データをバックアップ＆削除')
    parser.add_argument('--execute', action='store_true', help='実際に削除を実行する（デフォルトはDry-run）')
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    features_base = project_root / 'keibaai/data/features/parquet'

    if not features_base.exists():
        print(f"❌ 特徴量ディレクトリが存在しません: {features_base}")
        return

    print("=" * 70)
    print("2020-2023年 特徴量データ バックアップ＆クリーンアップ")
    print("=" * 70)

    if args.execute:
        print("⚠️  モード: 実行モード（実際に削除します）")
    else:
        print("ℹ️  モード: Dry-runモード（削除はしません）")

    # バックアップ先
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_base = project_root / 'keibaai/data/features' / f'parquet_backup_{timestamp}'

    # 削除対象の年
    target_years = [2020, 2021, 2022, 2023]

    total_size = 0
    dirs_to_backup = []

    # 対象ディレクトリの確認
    print("\n📂 削除対象ディレクトリ:")
    for year in target_years:
        year_dir = features_base / f'year={year}'
        if year_dir.exists():
            # ディレクトリサイズを計算
            dir_size = sum(f.stat().st_size for f in year_dir.rglob('*') if f.is_file())
            total_size += dir_size
            dirs_to_backup.append(year_dir)
            print(f"  ✓ year={year}: {dir_size / (1024**2):.2f} MB")
        else:
            print(f"  - year={year}: 存在しない")

    if not dirs_to_backup:
        print("\n✅ 削除対象のディレクトリがありません。")
        return

    print(f"\n📊 合計サイズ: {total_size / (1024**2):.2f} MB")
    print(f"📁 バックアップ先: {backup_base}")

    if not args.execute:
        print("\n" + "=" * 70)
        print("⚠️  Dry-runモード: 実際の削除は行いませんでした。")
        print("削除を実行する場合は --execute オプションを付けて再実行してください:")
        print("  python backup_and_clean_features.py --execute")
        print("=" * 70)
        return

    # 実行モード
    print("\n" + "=" * 70)
    print("🚀 バックアップ＆削除を開始します...")
    print("=" * 70)

    # バックアップ
    print("\n📦 バックアップ中...")
    backup_base.mkdir(parents=True, exist_ok=True)

    for year_dir in dirs_to_backup:
        year_name = year_dir.name
        backup_dest = backup_base / year_name
        print(f"  → {year_name} をバックアップ中...")
        shutil.copytree(year_dir, backup_dest)
        print(f"  ✓ {year_name} バックアップ完了")

    print(f"\n✅ バックアップ完了: {backup_base}")

    # 削除
    print("\n🗑️  削除中...")
    for year_dir in dirs_to_backup:
        year_name = year_dir.name
        print(f"  → {year_name} を削除中...")
        shutil.rmtree(year_dir)
        print(f"  ✓ {year_name} 削除完了")

    print("\n" + "=" * 70)
    print("✅ バックアップ＆削除が完了しました")
    print("=" * 70)
    print(f"\nバックアップ場所: {backup_base}")
    print("\n次のステップ:")
    print("  python regenerate_all_features_2020_2023.py")
    print("=" * 70)

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
効率的な再取得・リカバリースクリプト

失敗したスクレイピングやパースを効率的に再実行：
1. 空ファイル・異常ファイルの検出
2. パースエラーが発生したファイルのリスト化
3. 選択的な再スクレイピング
4. 再パース処理
"""

import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import json
import argparse

# プロジェクトルート
project_root = Path(__file__).resolve().parent / "keibaai"
sys.path.append(str(project_root))

from src.modules.preparing import _scrape_html


class RecoveryManager:
    """リカバリー処理を管理するクラス"""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.html_dir = data_dir / "raw" / "html"
        self.failed_files = {
            'empty': [],
            'too_small': [],
            'parse_error': [],
        }

    def detect_failed_files(self, min_size: int = 1024):
        """失敗したファイルを検出"""
        print("🔍 失敗ファイルの検出中...")
        print("=" * 80)

        for file_type in ['race', 'shutuba', 'horse', 'ped']:
            dir_path = self.html_dir / file_type
            if not dir_path.exists():
                continue

            print(f"\n📁 {file_type}ディレクトリをチェック中...")
            files = list(dir_path.glob("*.bin"))

            empty_count = 0
            small_count = 0

            for file_path in files:
                size = file_path.stat().st_size

                # 空ファイル
                if size == 0:
                    self.failed_files['empty'].append({
                        'type': file_type,
                        'path': str(file_path),
                        'id': self._extract_id(file_path),
                        'size': 0
                    })
                    empty_count += 1

                # 小さすぎるファイル
                elif size < min_size:
                    self.failed_files['too_small'].append({
                        'type': file_type,
                        'path': str(file_path),
                        'id': self._extract_id(file_path),
                        'size': size
                    })
                    small_count += 1

            print(f"   空ファイル: {empty_count:,}件")
            print(f"   小さすぎるファイル（<{min_size}B）: {small_count:,}件")

        # サマリー
        total_empty = len(self.failed_files['empty'])
        total_small = len(self.failed_files['too_small'])
        print("\n" + "=" * 80)
        print(f"📊 検出結果:")
        print(f"   空ファイル: {total_empty:,}件")
        print(f"   小さすぎるファイル: {total_small:,}件")
        print(f"   合計: {total_empty + total_small:,}件")

    def _extract_id(self, file_path: Path) -> str:
        """ファイルパスからIDを抽出"""
        filename = file_path.stem
        # _がある場合は分割（馬ファイル）
        if '_' in filename:
            return filename.split('_')[0]
        return filename

    def generate_recovery_list(self, output_path: Path):
        """リカバリー対象のリストを生成"""
        print("\n📝 リカバリーリストを生成中...")

        recovery_list = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'data_directory': str(self.data_dir),
            },
            'summary': {
                'empty_files': len(self.failed_files['empty']),
                'too_small_files': len(self.failed_files['too_small']),
                'total': len(self.failed_files['empty']) + len(self.failed_files['too_small']),
            },
            'recovery_targets': {
                'race_ids': [],
                'horse_ids': [],
            },
            'details': self.failed_files,
        }

        # race_idとhorse_idに分類
        race_ids = set()
        horse_ids = set()

        for category in ['empty', 'too_small']:
            for file_info in self.failed_files[category]:
                file_type = file_info['type']
                file_id = file_info['id']

                if file_type in ['race', 'shutuba']:
                    race_ids.add(file_id)
                elif file_type in ['horse', 'ped']:
                    horse_ids.add(file_id)

        recovery_list['recovery_targets']['race_ids'] = sorted(list(race_ids))
        recovery_list['recovery_targets']['horse_ids'] = sorted(list(horse_ids))

        # 保存
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(recovery_list, f, ensure_ascii=False, indent=2)

        print(f"💾 リカバリーリストを保存: {output_path}")
        print(f"   レースID: {len(race_ids):,}件")
        print(f"   馬ID: {len(horse_ids):,}件")

        return recovery_list

    def delete_failed_files(self, dry_run: bool = True):
        """失敗したファイルを削除"""
        print("\n🗑️  失敗ファイルの削除")
        print("=" * 80)

        if dry_run:
            print("⚠️  DRY RUNモード（実際には削除しません）\n")
        else:
            print("⚠️  実際にファイルを削除します！\n")

        total_deleted = 0
        total_size_freed = 0

        for category in ['empty', 'too_small']:
            print(f"\n■ {category}ファイル:")
            for file_info in self.failed_files[category]:
                file_path = Path(file_info['path'])
                size = file_info['size']

                if dry_run:
                    print(f"   [DRY RUN] 削除対象: {file_path.name} ({size} bytes)")
                else:
                    try:
                        file_path.unlink()
                        print(f"   ✅ 削除: {file_path.name} ({size} bytes)")
                        total_deleted += 1
                        total_size_freed += size
                    except Exception as e:
                        print(f"   ❌ 削除失敗: {file_path.name} - {e}")

        print("\n" + "=" * 80)
        if dry_run:
            print(f"[DRY RUN] 削除対象: {len(self.failed_files['empty']) + len(self.failed_files['too_small']):,}件")
        else:
            print(f"✅ 削除完了: {total_deleted:,}件（{self._format_size(total_size_freed)}解放）")

    def _format_size(self, size: float) -> str:
        """ファイルサイズをフォーマット"""
        if size < 1024:
            return f"{size:.0f} B"
        elif size < 1024 * 1024:
            return f"{size/1024:.1f} KB"
        else:
            return f"{size/(1024*1024):.2f} MB"


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='失敗したスクレイピングのリカバリー',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 失敗ファイルを検出してリストを生成
  python %(prog)s --detect

  # 失敗ファイルを削除（DRY RUN）
  python %(prog)s --detect --delete --dry-run

  # 失敗ファイルを実際に削除
  python %(prog)s --detect --delete

  # 最小ファイルサイズを指定（デフォルト: 1024）
  python %(prog)s --detect --min-size 2048
        """
    )
    parser.add_argument('--detect', action='store_true', help='失敗ファイルを検出')
    parser.add_argument('--delete', action='store_true', help='失敗ファイルを削除')
    parser.add_argument('--dry-run', action='store_true', help='削除のDRY RUN（実際には削除しない）')
    parser.add_argument('--min-size', type=int, default=1024, help='最小ファイルサイズ（bytes、デフォルト: 1024）')

    args = parser.parse_args()

    print("🔧 KeibaAI_v2 スクレイピング・リカバリーツール")
    print("=" * 80)

    data_dir = Path("keibaai/data")

    if not data_dir.exists():
        print(f"❌ データディレクトリが見つかりません: {data_dir}")
        return

    manager = RecoveryManager(data_dir)

    # 失敗ファイルの検出
    if args.detect:
        manager.detect_failed_files(min_size=args.min_size)

        # リカバリーリスト生成
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = data_dir / "logs" / "recovery" / f"recovery_list_{timestamp}.json"
        recovery_list = manager.generate_recovery_list(output_path)

        # 削除処理
        if args.delete:
            manager.delete_failed_files(dry_run=args.dry_run)

    else:
        print("⚠️  --detect オプションを指定してください")
        parser.print_help()

    print("\n" + "=" * 80)
    print("✅ 処理完了!")
    print("=" * 80)


if __name__ == "__main__":
    main()

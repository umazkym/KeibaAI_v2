#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
パースエラーの詳細分析スクリプト

実際にパーサーを実行して、どこでエラーが発生するかを特定：
1. エラーが発生したファイルのリスト
2. エラーの種類と頻度
3. エラーファイルの詳細内容
4. 正常ファイルとの比較
"""

import sys
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
import json
import traceback
from typing import Dict, List, Optional

# プロジェクトルート
project_root = Path(__file__).resolve().parent / "keibaai"
sys.path.append(str(project_root))

from src.modules.parsers import results_parser, shutuba_parser


class ParseErrorAnalyzer:
    """パースエラーを分析するクラス"""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.html_dir = data_dir / "raw" / "html"
        self.error_details = []
        self.success_count = 0
        self.error_count = 0
        self.error_types = Counter()

    def analyze_race_files(self, year_filter: Optional[int] = None, max_files: int = 100):
        """レース結果ファイルをパース分析"""
        print(f"\n📄 レース結果ファイルのパース分析")
        print("=" * 80)

        race_dir = self.html_dir / "race"
        if not race_dir.exists():
            print(f"❌ ディレクトリが見つかりません: {race_dir}")
            return

        files = list(race_dir.glob("*.bin"))

        # 年でフィルター
        if year_filter:
            files = [f for f in files if f.stem.startswith(str(year_filter))]
            print(f"🔍 {year_filter}年のファイルに絞り込み: {len(files)}件")

        # 最大件数制限
        if len(files) > max_files:
            print(f"⚠️  ファイル数が多いため、最初の{max_files}件のみ分析します")
            files = files[:max_files]

        print(f"📊 分析対象: {len(files)}件\n")

        # パース実行
        for i, file_path in enumerate(files, 1):
            race_id = file_path.stem
            print(f"  [{i}/{len(files)}] {race_id}...", end=" ")

            try:
                # パース実行
                df = results_parser.parse_race_results_from_file(str(file_path))

                if df is None or df.empty:
                    print("❌ 空のDataFrame")
                    self.error_count += 1
                    self.error_types['empty_dataframe'] += 1
                    self._record_error(file_path, 'empty_dataframe', 'パース結果が空')
                else:
                    print(f"✅ {len(df)}行")
                    self.success_count += 1

            except Exception as e:
                print(f"❌ {type(e).__name__}")
                self.error_count += 1
                error_type = type(e).__name__
                self.error_types[error_type] += 1
                self._record_error(file_path, error_type, str(e), traceback.format_exc())

        print("\n" + "-" * 80)
        print(f"✅ 成功: {self.success_count}件")
        print(f"❌ 失敗: {self.error_count}件")

    def analyze_shutuba_files(self, year_filter: Optional[int] = None, max_files: int = 100):
        """出馬表ファイルをパース分析"""
        print(f"\n📄 出馬表ファイルのパース分析")
        print("=" * 80)

        shutuba_dir = self.html_dir / "shutuba"
        if not shutuba_dir.exists():
            print(f"❌ ディレクトリが見つかりません: {shutuba_dir}")
            return

        files = list(shutuba_dir.glob("*.bin"))

        # 年でフィルター
        if year_filter:
            files = [f for f in files if f.stem.startswith(str(year_filter))]
            print(f"🔍 {year_filter}年のファイルに絞り込み: {len(files)}件")

        # 最大件数制限
        if len(files) > max_files:
            print(f"⚠️  ファイル数が多いため、最初の{max_files}件のみ分析します")
            files = files[:max_files]

        print(f"📊 分析対象: {len(files)}件\n")

        # パース実行
        success = 0
        error = 0

        for i, file_path in enumerate(files, 1):
            race_id = file_path.stem
            print(f"  [{i}/{len(files)}] {race_id}...", end=" ")

            try:
                # パース実行
                df = shutuba_parser.parse_shutuba_from_file(str(file_path))

                if df is None or df.empty:
                    print("❌ 空のDataFrame")
                    error += 1
                    self.error_types['shutuba_empty_dataframe'] += 1
                    self._record_error(file_path, 'shutuba_empty_dataframe', 'パース結果が空')
                else:
                    print(f"✅ {len(df)}行")
                    success += 1

            except Exception as e:
                print(f"❌ {type(e).__name__}")
                error += 1
                error_type = f"shutuba_{type(e).__name__}"
                self.error_types[error_type] += 1
                self._record_error(file_path, error_type, str(e), traceback.format_exc())

        print("\n" + "-" * 80)
        print(f"✅ 成功: {success}件")
        print(f"❌ 失敗: {error}件")

        self.success_count += success
        self.error_count += error

    def _record_error(self, file_path: Path, error_type: str, error_message: str, traceback_str: str = ""):
        """エラー詳細を記録"""
        error_info = {
            'file_path': str(file_path),
            'file_name': file_path.name,
            'file_id': file_path.stem,
            'file_size': file_path.stat().st_size,
            'error_type': error_type,
            'error_message': error_message,
            'traceback': traceback_str,
        }

        # ファイルの最初の500文字を記録
        try:
            with open(file_path, 'rb') as f:
                content_bytes = f.read(1000)
            try:
                content = content_bytes.decode('euc_jp', errors='replace')
            except:
                content = content_bytes.decode('utf-8', errors='replace')
            error_info['file_preview'] = content[:500]
        except Exception as e:
            error_info['file_preview'] = f"[ファイル読み込みエラー: {e}]"

        self.error_details.append(error_info)

    def print_error_summary(self):
        """エラーサマリーを出力"""
        print("\n" + "=" * 80)
        print("📊 エラーサマリー")
        print("=" * 80)

        print(f"\n■ エラー統計:")
        print(f"  総ファイル数: {self.success_count + self.error_count:,}")
        print(f"  成功: {self.success_count:,} ({self.success_count/(self.success_count+self.error_count)*100:.1f}%)")
        print(f"  失敗: {self.error_count:,} ({self.error_count/(self.success_count+self.error_count)*100:.1f}%)")

        if self.error_types:
            print(f"\n■ エラータイプ別集計:")
            print(f"  {'エラータイプ':<30} {'件数':>10}")
            print("  " + "-" * 42)
            for error_type, count in self.error_types.most_common():
                print(f"  {error_type:<30} {count:>10,}")

        if self.error_details:
            print(f"\n■ エラー詳細（最初の10件）:")
            for i, error in enumerate(self.error_details[:10], 1):
                print(f"\n  {i}. {error['file_name']} ({error['file_size']} bytes)")
                print(f"     エラー: {error['error_type']}")
                print(f"     メッセージ: {error['error_message'][:100]}")
                if error['file_preview']:
                    preview_lines = error['file_preview'].split('\n')[:3]
                    print(f"     ファイル内容: {preview_lines[0][:60]}...")

    def save_report(self, output_path: Path):
        """詳細レポートをJSONで保存"""
        report = {
            'metadata': {
                'analysis_datetime': datetime.now().isoformat(),
                'data_directory': str(self.data_dir),
            },
            'summary': {
                'total_files': self.success_count + self.error_count,
                'success_count': self.success_count,
                'error_count': self.error_count,
                'success_rate': self.success_count / (self.success_count + self.error_count) * 100
                               if (self.success_count + self.error_count) > 0 else 0,
            },
            'error_types': dict(self.error_types),
            'error_details': self.error_details,
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\n💾 詳細レポートを保存: {output_path}")


def main():
    """メイン処理"""
    import argparse

    parser = argparse.ArgumentParser(
        description='パースエラーの詳細分析',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--year', type=int, help='分析対象の年（例: 2025）')
    parser.add_argument('--max-files', type=int, default=100, help='最大分析ファイル数（デフォルト: 100）')
    parser.add_argument('--race', action='store_true', help='レース結果を分析')
    parser.add_argument('--shutuba', action='store_true', help='出馬表を分析')

    args = parser.parse_args()

    print("🔍 KeibaAI_v2 パースエラー詳細分析")
    print("=" * 80)

    data_dir = Path("keibaai/data")

    if not data_dir.exists():
        print(f"❌ データディレクトリが見つかりません: {data_dir}")
        return

    analyzer = ParseErrorAnalyzer(data_dir)

    # デフォルトでは両方分析
    analyze_race = args.race or not (args.race or args.shutuba)
    analyze_shutuba = args.shutuba or not (args.race or args.shutuba)

    if analyze_race:
        analyzer.analyze_race_files(year_filter=args.year, max_files=args.max_files)

    if analyze_shutuba:
        analyzer.analyze_shutuba_files(year_filter=args.year, max_files=args.max_files)

    # サマリー出力
    analyzer.print_error_summary()

    # レポート保存
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    year_suffix = f"_{args.year}" if args.year else ""
    output_path = data_dir / "logs" / "analysis" / f"parse_errors{year_suffix}_{timestamp}.json"
    analyzer.save_report(output_path)

    print("\n" + "=" * 80)
    print("✅ 分析完了!")
    print("=" * 80)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTMLファイルの詳細分析スクリプト

スクレイピングエラーの原因特定のため、以下を調査：
1. ファイルサイズの統計（年別、種類別）
2. 空ファイル・異常ファイルの検出
3. 2025年ファイルの詳細調査
4. サンプルHTMLの内容確認
"""

import sys
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
import json

# プロジェクトルート
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))


class HTMLFileAnalyzer:
    """HTMLファイルを分析するクラス"""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.html_dir = data_dir / "raw" / "html"
        self.stats = {
            'race': defaultdict(list),
            'shutuba': defaultdict(list),
            'horse': defaultdict(list),
            'ped': defaultdict(list),
        }
        self.suspicious_files = []
        self.empty_files = []

    def analyze_all(self):
        """全HTMLファイルを分析"""
        print("🔍 HTMLファイルの詳細分析を開始\n")
        print("=" * 80)

        # 各ディレクトリを分析
        for file_type in ['race', 'shutuba', 'horse', 'ped']:
            dir_path = self.html_dir / file_type
            if dir_path.exists():
                print(f"\n📁 {file_type}ディレクトリを分析中...")
                self._analyze_directory(dir_path, file_type)

        # 統計レポート
        self._print_statistics()
        self._print_suspicious_files()
        self._print_sample_contents()

    def _analyze_directory(self, dir_path: Path, file_type: str):
        """ディレクトリ内のファイルを分析"""
        files = list(dir_path.glob("*.bin"))
        print(f"   総ファイル数: {len(files):,}")

        for file_path in files:
            # ファイルサイズ取得
            size = file_path.stat().st_size

            # race_idまたはhorse_idを抽出
            file_id = self._extract_id(file_path.stem)
            year = self._extract_year(file_id)

            # 統計に追加
            self.stats[file_type][year].append({
                'file_path': str(file_path),
                'file_id': file_id,
                'size': size,
                'name': file_path.name
            })

            # 空ファイルチェック
            if size == 0:
                self.empty_files.append({
                    'type': file_type,
                    'path': str(file_path),
                    'id': file_id,
                    'size': 0
                })
            # 異常に小さいファイル（1KB未満）
            elif size < 1024:
                self.suspicious_files.append({
                    'type': file_type,
                    'path': str(file_path),
                    'id': file_id,
                    'size': size,
                    'reason': f'異常に小さい ({size} bytes)'
                })

    def _extract_id(self, filename: str) -> str:
        """ファイル名からID部分を抽出"""
        # race, shutuba: 202310010201.bin
        # horse: 2009100502_profile.bin, 2009100502_perf.bin
        # ped: 2009100502.bin

        # _がある場合は分割
        if '_' in filename:
            return filename.split('_')[0]
        return filename

    def _extract_year(self, file_id: str) -> int:
        """IDから年を抽出"""
        # 最初の4桁を年とする
        if len(file_id) >= 4:
            try:
                return int(file_id[:4])
            except ValueError:
                return 0
        return 0

    def _print_statistics(self):
        """統計情報を出力"""
        print("\n" + "=" * 80)
        print("📊 ファイル統計（年別）")
        print("=" * 80)

        for file_type in ['race', 'shutuba', 'horse', 'ped']:
            if not self.stats[file_type]:
                continue

            print(f"\n■ {file_type.upper()} ファイル:")
            print(f"{'年':<8} {'ファイル数':>10} {'平均サイズ':>15} {'最小':>12} {'最大':>12}")
            print("-" * 80)

            years = sorted(self.stats[file_type].keys())
            total_files = 0
            total_size = 0

            for year in years:
                files = self.stats[file_type][year]
                count = len(files)
                sizes = [f['size'] for f in files]
                avg_size = sum(sizes) / len(sizes) if sizes else 0
                min_size = min(sizes) if sizes else 0
                max_size = max(sizes) if sizes else 0

                total_files += count
                total_size += sum(sizes)

                print(f"{year:<8} {count:>10,} {self._format_size(avg_size):>15} "
                      f"{self._format_size(min_size):>12} {self._format_size(max_size):>12}")

            print("-" * 80)
            print(f"{'合計':<8} {total_files:>10,} {self._format_size(total_size/total_files if total_files else 0):>15} "
                  f"{'':>12} {self._format_size(total_size):>12}")

    def _print_suspicious_files(self):
        """疑わしいファイルを出力"""
        print("\n" + "=" * 80)
        print("⚠️  疑わしいファイル")
        print("=" * 80)

        # 空ファイル
        if self.empty_files:
            print(f"\n■ 空ファイル: {len(self.empty_files):,}件")
            print(f"{'種別':<10} {'ファイルID':<15} {'パス'}")
            print("-" * 80)
            for file_info in self.empty_files[:20]:  # 最初の20件のみ
                print(f"{file_info['type']:<10} {file_info['id']:<15} {Path(file_info['path']).name}")

            if len(self.empty_files) > 20:
                print(f"... 他 {len(self.empty_files) - 20:,}件")
        else:
            print("\n✅ 空ファイルは見つかりませんでした")

        # 異常に小さいファイル
        if self.suspicious_files:
            print(f"\n■ 異常に小さいファイル（<1KB）: {len(self.suspicious_files):,}件")
            print(f"{'種別':<10} {'ファイルID':<15} {'サイズ':<12} {'パス'}")
            print("-" * 80)
            for file_info in self.suspicious_files[:20]:  # 最初の20件のみ
                print(f"{file_info['type']:<10} {file_info['id']:<15} "
                      f"{self._format_size(file_info['size']):<12} {Path(file_info['path']).name}")

            if len(self.suspicious_files) > 20:
                print(f"... 他 {len(self.suspicious_files) - 20:,}件")
        else:
            print("\n✅ 異常に小さいファイルは見つかりませんでした")

    def _print_sample_contents(self):
        """サンプルファイルの内容を表示"""
        print("\n" + "=" * 80)
        print("📄 サンプルファイルの内容確認")
        print("=" * 80)

        # 2025年のファイルをピックアップ
        samples_2025 = []
        for file_type in ['race', 'shutuba']:
            if 2025 in self.stats[file_type]:
                files = self.stats[file_type][2025]
                if files:
                    samples_2025.append((file_type, files[0]))

        # 正常なファイルもピックアップ（2023年）
        samples_normal = []
        for file_type in ['race', 'shutuba']:
            if 2023 in self.stats[file_type]:
                files = self.stats[file_type][2023]
                # サイズが大きいもの（正常と思われる）
                large_files = [f for f in files if f['size'] > 10000]
                if large_files:
                    samples_normal.append((file_type, large_files[0]))

        # 2025年サンプル
        if samples_2025:
            print("\n■ 2025年ファイルのサンプル（最初の500文字）:")
            for file_type, file_info in samples_2025[:2]:
                self._print_file_content(file_info['file_path'], file_type, 500)

        # 正常ファイルサンプル
        if samples_normal:
            print("\n■ 正常ファイル（2023年）のサンプル（最初の500文字）:")
            for file_type, file_info in samples_normal[:2]:
                self._print_file_content(file_info['file_path'], file_type, 500)

    def _print_file_content(self, file_path: str, file_type: str, max_chars: int = 500):
        """ファイルの内容を表示"""
        path = Path(file_path)
        print(f"\n  📄 {file_type} - {path.name} ({self._format_size(path.stat().st_size)})")
        print("  " + "-" * 76)

        try:
            with open(path, 'rb') as f:
                content_bytes = f.read(max_chars * 2)  # 余裕を持って読む

            # EUC-JPでデコード試行
            try:
                content = content_bytes.decode('euc_jp', errors='replace')
            except:
                try:
                    content = content_bytes.decode('utf-8', errors='replace')
                except:
                    content = str(content_bytes[:max_chars])

            # 最初のmax_chars文字を表示
            content_preview = content[:max_chars]

            # 改行で分割して最初の10行のみ
            lines = content_preview.split('\n')[:10]
            for line in lines:
                print(f"  {line[:74]}")

            if len(content) > max_chars:
                print(f"  ... (以下省略)")

        except Exception as e:
            print(f"  ❌ ファイル読み込みエラー: {e}")

    def _format_size(self, size: float) -> str:
        """ファイルサイズをフォーマット"""
        if size < 1024:
            return f"{size:.0f} B"
        elif size < 1024 * 1024:
            return f"{size/1024:.1f} KB"
        else:
            return f"{size/(1024*1024):.2f} MB"

    def save_report(self, output_path: Path):
        """詳細レポートをJSONで保存"""
        report = {
            'metadata': {
                'analysis_datetime': datetime.now().isoformat(),
                'data_directory': str(self.data_dir),
            },
            'statistics': {},
            'empty_files': self.empty_files,
            'suspicious_files': self.suspicious_files,
        }

        # 統計を変換
        for file_type, years_data in self.stats.items():
            report['statistics'][file_type] = {}
            for year, files in years_data.items():
                report['statistics'][file_type][str(year)] = {
                    'count': len(files),
                    'total_size': sum(f['size'] for f in files),
                    'avg_size': sum(f['size'] for f in files) / len(files) if files else 0,
                    'files': [{'id': f['file_id'], 'size': f['size'], 'name': f['name']} for f in files[:100]]
                }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\n💾 詳細レポートを保存: {output_path}")


def main():
    """メイン処理"""
    print("🔍 KeibaAI_v2 HTMLファイル詳細分析")
    print("=" * 80)

    data_dir = Path("keibaai/data")

    if not data_dir.exists():
        print(f"❌ データディレクトリが見つかりません: {data_dir}")
        return

    analyzer = HTMLFileAnalyzer(data_dir)
    analyzer.analyze_all()

    # レポート保存
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = data_dir / "logs" / "analysis" / f"html_analysis_{timestamp}.json"
    analyzer.save_report(output_path)

    print("\n" + "=" * 80)
    print("✅ 分析完了!")
    print("=" * 80)


if __name__ == "__main__":
    main()

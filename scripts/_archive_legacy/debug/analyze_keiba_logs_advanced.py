#!/usr/bin/env python3
"""
KeibaAI_v2 Advanced Log Analyzer
より詳細な分析とグラフ生成機能を含む拡張版ログアナライザー
"""

import re
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import Dict, List, Optional
import json


class AdvancedKeibaLogAnalyzer:
    """拡張版ログアナライザー"""

    def __init__(self, log_dir: Path):
        self.log_dir = Path(log_dir)
        self.log_entries = []
        self.timeline = []  # 時系列データ
        self.stats = {
            'total_entries': 0,
            'by_level': Counter(),
            'errors': [],
            'warnings': [],
            'scraping_stats': {
                'races': {'scraped': 0, 'failed': 0, 'skipped': 0},
                'shutuba': {'scraped': 0, 'failed': 0, 'skipped': 0},
                'horses': {'scraped': 0, 'failed': 0, 'skipped': 0},
                'pedigrees': {'scraped': 0, 'failed': 0, 'skipped': 0},
                'http_errors': Counter(),
                'retry_count': 0,
            },
            'parsing_stats': {
                'races': {'parsed': 0, 'failed': 0},
                'shutuba': {'parsed': 0, 'failed': 0},
                'horses': {'parsed': 0, 'failed': 0},
                'pedigrees': {'parsed': 0, 'failed': 0},
                'missing_fields': Counter(),
                'data_quality': defaultdict(list),
            },
            'performance': {
                'scraping_rate': [],  # (timestamp, count) pairs
                'parsing_rate': [],
                'execution_stages': {},
            },
        }

    def find_log_files(self) -> List[Path]:
        """ログファイルを検索"""
        if not self.log_dir.exists():
            print(f"⚠️  ログディレクトリが存在しません: {self.log_dir}")
            return []

        log_files = sorted(self.log_dir.glob("*.log"))
        return log_files

    def parse_log_line(self, line: str) -> Optional[Dict]:
        """ログ行をパース"""
        pattern = r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})\s+-\s+(\w+)\s+-\s+(.+)'
        match = re.match(pattern, line)

        if match:
            timestamp_str, level, message = match.groups()
            try:
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
            except:
                timestamp = None

            return {
                'timestamp': timestamp,
                'level': level,
                'message': message.strip(),
                'raw': line
            }
        return None

    def analyze_log_file(self, log_file: Path):
        """ログファイルを分析"""
        print(f"📄 分析中: {log_file.name}")

        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    entry = self.parse_log_line(line)
                    if entry:
                        self.log_entries.append(entry)
                        self.stats['total_entries'] += 1
                        self.stats['by_level'][entry['level']] += 1

                        if entry['level'] == 'ERROR':
                            self.stats['errors'].append(entry)
                        elif entry['level'] == 'WARNING':
                            self.stats['warnings'].append(entry)

                        self._analyze_scraping(entry)
                        self._analyze_parsing(entry)
                        self._analyze_performance(entry)

        except Exception as e:
            print(f"❌ ファイル読み込みエラー: {log_file.name} - {e}")

    def _analyze_scraping(self, entry: Dict):
        """スクレイピングの詳細分析"""
        msg = entry['message']
        msg_lower = msg.lower()

        # レース結果
        if 'race' in msg_lower and 'scraping' in msg_lower:
            if 'success' in msg_lower or 'completed' in msg_lower:
                self.stats['scraping_stats']['races']['scraped'] += 1
            elif 'failed' in msg_lower or 'error' in msg_lower:
                self.stats['scraping_stats']['races']['failed'] += 1
            elif 'skip' in msg_lower:
                self.stats['scraping_stats']['races']['skipped'] += 1

        # 出馬表
        if 'shutuba' in msg_lower and 'scraping' in msg_lower:
            if 'success' in msg_lower or 'completed' in msg_lower:
                self.stats['scraping_stats']['shutuba']['scraped'] += 1
            elif 'failed' in msg_lower or 'error' in msg_lower:
                self.stats['scraping_stats']['shutuba']['failed'] += 1
            elif 'skip' in msg_lower:
                self.stats['scraping_stats']['shutuba']['skipped'] += 1

        # 馬情報
        if 'horse' in msg_lower and 'scraping' in msg_lower and 'pedigree' not in msg_lower:
            if 'success' in msg_lower or 'completed' in msg_lower:
                self.stats['scraping_stats']['horses']['scraped'] += 1
            elif 'failed' in msg_lower or 'error' in msg_lower:
                self.stats['scraping_stats']['horses']['failed'] += 1
            elif 'skip' in msg_lower:
                self.stats['scraping_stats']['horses']['skipped'] += 1

        # 血統情報
        if 'pedigree' in msg_lower and 'scraping' in msg_lower:
            if 'success' in msg_lower or 'completed' in msg_lower:
                self.stats['scraping_stats']['pedigrees']['scraped'] += 1
            elif 'failed' in msg_lower or 'error' in msg_lower:
                self.stats['scraping_stats']['pedigrees']['failed'] += 1
            elif 'skip' in msg_lower:
                self.stats['scraping_stats']['pedigrees']['skipped'] += 1

        # HTTPエラー
        http_match = re.search(r'http[s]?\s+(\d{3})', msg_lower)
        if http_match:
            status_code = http_match.group(1)
            self.stats['scraping_stats']['http_errors'][status_code] += 1

        # リトライ
        if 'retry' in msg_lower or 'retrying' in msg_lower:
            self.stats['scraping_stats']['retry_count'] += 1

    def _analyze_parsing(self, entry: Dict):
        """パースの詳細分析"""
        msg = entry['message']
        msg_lower = msg.lower()

        # レース結果パース
        if 'race' in msg_lower and 'pars' in msg_lower:
            if 'success' in msg_lower or 'completed' in msg_lower:
                self.stats['parsing_stats']['races']['parsed'] += 1
            elif 'failed' in msg_lower or 'error' in msg_lower:
                self.stats['parsing_stats']['races']['failed'] += 1

        # 出馬表パース
        if 'shutuba' in msg_lower and 'pars' in msg_lower:
            if 'success' in msg_lower or 'completed' in msg_lower:
                self.stats['parsing_stats']['shutuba']['parsed'] += 1
            elif 'failed' in msg_lower or 'error' in msg_lower:
                self.stats['parsing_stats']['shutuba']['failed'] += 1

        # 馬情報パース
        if 'horse' in msg_lower and 'pars' in msg_lower and 'pedigree' not in msg_lower:
            if 'success' in msg_lower or 'completed' in msg_lower:
                self.stats['parsing_stats']['horses']['parsed'] += 1
            elif 'failed' in msg_lower or 'error' in msg_lower:
                self.stats['parsing_stats']['horses']['failed'] += 1

        # 血統パース
        if 'pedigree' in msg_lower and 'pars' in msg_lower:
            if 'success' in msg_lower or 'completed' in msg_lower:
                self.stats['parsing_stats']['pedigrees']['parsed'] += 1
            elif 'failed' in msg_lower or 'error' in msg_lower:
                self.stats['parsing_stats']['pedigrees']['failed'] += 1

        # 欠損フィールド
        missing_match = re.search(r'missing\s+field[s]?:\s*(\w+)', msg_lower)
        if missing_match:
            field = missing_match.group(1)
            self.stats['parsing_stats']['missing_fields'][field] += 1

        # データ品質
        quality_match = re.search(r'(distance|surface|weather|venue|class).*not found', msg_lower)
        if quality_match:
            field = quality_match.group(1)
            self.stats['parsing_stats']['data_quality'][field].append(entry)

    def _analyze_performance(self, entry: Dict):
        """パフォーマンス分析"""
        msg = entry['message']

        # 処理完了時間
        time_match = re.search(r'completed in\s+([\d.]+)\s+seconds?', msg, re.IGNORECASE)
        if time_match:
            seconds = float(time_match.group(1))

            if 'scraping' in msg.lower():
                stage = 'scraping'
            elif 'parsing' in msg.lower():
                stage = 'parsing'
            elif 'pipeline' in msg.lower():
                stage = 'total_pipeline'
            else:
                stage = 'other'

            self.stats['performance']['execution_stages'][stage] = seconds

        # スループット（時間あたりの処理数）
        throughput_match = re.search(r'(\d+)\s+items?\s+in\s+([\d.]+)\s+seconds?', msg, re.IGNORECASE)
        if throughput_match:
            items = int(throughput_match.group(1))
            seconds = float(throughput_match.group(2))
            rate = items / seconds if seconds > 0 else 0

            if entry['timestamp']:
                if 'scraping' in msg.lower():
                    self.stats['performance']['scraping_rate'].append((entry['timestamp'], rate))
                elif 'parsing' in msg.lower():
                    self.stats['performance']['parsing_rate'].append((entry['timestamp'], rate))

    def generate_detailed_report(self) -> str:
        """詳細レポート生成"""
        report = []
        report.append("=" * 100)
        report.append("📊 KeibaAI_v2 詳細ログ分析レポート")
        report.append("=" * 100)
        report.append(f"📁 ログディレクトリ: {self.log_dir}")
        report.append(f"📅 分析日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"📝 総ログエントリ数: {self.stats['total_entries']:,}")
        report.append("")

        # ログレベル別統計
        report.append("■ ログレベル別統計")
        report.append("┌─────────────┬──────────┬─────────┐")
        report.append("│ レベル      │ 件数     │ 割合    │")
        report.append("├─────────────┼──────────┼─────────┤")
        for level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            count = self.stats['by_level'].get(level, 0)
            if self.stats['total_entries'] > 0:
                percentage = (count / self.stats['total_entries'] * 100)
                report.append(f"│ {level:11s} │ {count:8,} │ {percentage:6.2f}% │")
        report.append("└─────────────┴──────────┴─────────┘")
        report.append("")

        # スクレイピング統計
        report.append("■ スクレイピング詳細統計")
        report.append("┌──────────────┬──────────┬──────────┬──────────┬──────────┐")
        report.append("│ データ種別   │ 成功     │ 失敗     │ スキップ │ 合計     │")
        report.append("├──────────────┼──────────┼──────────┼──────────┼──────────┤")

        for data_type in ['races', 'shutuba', 'horses', 'pedigrees']:
            stats = self.stats['scraping_stats'][data_type]
            total = stats['scraped'] + stats['failed'] + stats['skipped']
            report.append(
                f"│ {data_type:12s} │ {stats['scraped']:8,} │ "
                f"{stats['failed']:8,} │ {stats['skipped']:8,} │ {total:8,} │"
            )

        report.append("└──────────────┴──────────┴──────────┴──────────┴──────────┘")

        # HTTPエラー
        if self.stats['scraping_stats']['http_errors']:
            report.append("")
            report.append("  HTTPエラー統計:")
            for status, count in sorted(self.stats['scraping_stats']['http_errors'].items()):
                report.append(f"    • HTTP {status}: {count:,} 件")

        report.append(f"\n  リトライ回数: {self.stats['scraping_stats']['retry_count']:,} 回")
        report.append("")

        # パース統計
        report.append("■ パース詳細統計")
        report.append("┌──────────────┬──────────┬──────────┬──────────┬──────────┐")
        report.append("│ データ種別   │ 成功     │ 失敗     │ 成功率   │ 合計     │")
        report.append("├──────────────┼──────────┼──────────┼──────────┼──────────┤")

        for data_type in ['races', 'shutuba', 'horses', 'pedigrees']:
            stats = self.stats['parsing_stats'][data_type]
            total = stats['parsed'] + stats['failed']
            success_rate = (stats['parsed'] / total * 100) if total > 0 else 0
            report.append(
                f"│ {data_type:12s} │ {stats['parsed']:8,} │ "
                f"{stats['failed']:8,} │ {success_rate:7.2f}% │ {total:8,} │"
            )

        report.append("└──────────────┴──────────┴──────────┴──────────┴──────────┘")

        # 欠損フィールド
        if self.stats['parsing_stats']['missing_fields']:
            report.append("")
            report.append("  欠損フィールド (上位15件):")
            for field, count in self.stats['parsing_stats']['missing_fields'].most_common(15):
                report.append(f"    • {field:30s}: {count:6,} 件")

        report.append("")

        # パフォーマンス統計
        if self.stats['performance']['execution_stages']:
            report.append("■ パフォーマンス統計")
            for stage, seconds in sorted(self.stats['performance']['execution_stages'].items()):
                minutes = seconds / 60
                hours = minutes / 60

                if hours >= 1:
                    time_str = f"{hours:6.2f} 時間 ({seconds:10,.1f} 秒)"
                elif minutes >= 1:
                    time_str = f"{minutes:6.2f} 分   ({seconds:10,.1f} 秒)"
                else:
                    time_str = f"{seconds:10,.1f} 秒"

                report.append(f"  • {stage:20s}: {time_str}")

            report.append("")

        # スループット
        if self.stats['performance']['scraping_rate']:
            avg_scraping_rate = sum(r for _, r in self.stats['performance']['scraping_rate']) / len(
                self.stats['performance']['scraping_rate'])
            report.append(f"  平均スクレイピング速度: {avg_scraping_rate:.2f} items/秒")

        if self.stats['performance']['parsing_rate']:
            avg_parsing_rate = sum(r for _, r in self.stats['performance']['parsing_rate']) / len(
                self.stats['performance']['parsing_rate'])
            report.append(f"  平均パース速度: {avg_parsing_rate:.2f} items/秒")

        report.append("")

        # エラー詳細
        if self.stats['errors']:
            report.append("■ エラー詳細 (最新20件)")
            report.append("─" * 100)
            for i, error in enumerate(self.stats['errors'][-20:], 1):
                timestamp = error['timestamp'].strftime('%H:%M:%S') if error['timestamp'] else 'N/A'
                report.append(f"  {i:2d}. [{timestamp}] {error['message'][:90]}")
            report.append("")

        # 警告詳細
        if self.stats['warnings']:
            report.append("■ 警告詳細 (最新20件)")
            report.append("─" * 100)
            for i, warning in enumerate(self.stats['warnings'][-20:], 1):
                timestamp = warning['timestamp'].strftime('%H:%M:%S') if warning['timestamp'] else 'N/A'
                report.append(f"  {i:2d}. [{timestamp}] {warning['message'][:90]}")
            report.append("")

        # データ品質問題
        if self.stats['parsing_stats']['data_quality']:
            report.append("■ データ品質問題")
            for field, entries in self.stats['parsing_stats']['data_quality'].items():
                report.append(f"  • {field}: {len(entries):,} 件の問題")
            report.append("")

        report.append("=" * 100)
        return "\n".join(report)

    def save_json_report(self, output_path: Path):
        """JSON形式でレポート保存"""
        report = {
            'metadata': {
                'log_directory': str(self.log_dir),
                'analysis_datetime': datetime.now().isoformat(),
                'total_entries': self.stats['total_entries'],
            },
            'log_levels': dict(self.stats['by_level']),
            'scraping_stats': {
                k: v if not isinstance(v, Counter) else dict(v)
                for k, v in self.stats['scraping_stats'].items()
            },
            'parsing_stats': {
                k: v if not isinstance(v, (Counter, defaultdict)) else dict(v)
                for k, v in self.stats['parsing_stats'].items()
                if k != 'data_quality'  # 除外: Entryオブジェクトを含む
            },
            'performance': {
                k: v for k, v in self.stats['performance'].items()
                if k not in ['scraping_rate', 'parsing_rate']  # 除外: datetimeオブジェクトを含む
            },
            'error_count': len(self.stats['errors']),
            'warning_count': len(self.stats['warnings']),
        }

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"💾 JSON レポートを保存: {output_path}")

    def analyze(self):
        """分析実行"""
        log_files = self.find_log_files()

        if not log_files:
            print("⚠️  ログファイルが見つかりませんでした")
            return

        print(f"📊 {len(log_files)} 個のログファイルを分析します\n")

        for log_file in log_files:
            self.analyze_log_file(log_file)

        print("\n✅ 分析完了!\n")


def main():
    """メイン関数"""
    import sys
    from datetime import date

    today = date.today()
    default_log_dir = Path(f"keibaai/data/logs/{today.year}/{today.month:02d}/{today.day:02d}")

    if len(sys.argv) > 1:
        log_dir = Path(sys.argv[1])
    else:
        log_dir = default_log_dir

    print("🔍 KeibaAI_v2 拡張ログアナライザー")
    print(f"📁 ログディレクトリ: {log_dir}\n")

    analyzer = AdvancedKeibaLogAnalyzer(log_dir)
    analyzer.analyze()

    # レポート生成
    report = analyzer.generate_detailed_report()
    print(report)

    # 保存
    output_dir = Path("keibaai/data/logs/analysis")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # テキストレポート
    txt_output = output_dir / f"advanced_log_analysis_{timestamp}.txt"
    with open(txt_output, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"💾 詳細レポート保存: {txt_output}")

    # JSONレポート
    json_output = output_dir / f"advanced_log_analysis_{timestamp}.json"
    analyzer.save_json_report(json_output)

    print("\n" + "=" * 100)
    print("📈 分析完了!")
    print("=" * 100)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
KeibaAI_v2 Log Analyzer
スクレイピングとパースパイプラインのログを分析し、統計情報を出力するツール
"""

import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, List, Tuple
import json


class KeibaLogAnalyzer:
    """ログファイルを分析するクラス"""

    def __init__(self, log_dir: Path):
        self.log_dir = Path(log_dir)
        self.log_entries = []
        self.stats = {
            'total_entries': 0,
            'by_level': Counter(),
            'by_module': Counter(),
            'errors': [],
            'warnings': [],
            'scraping_stats': {
                'races_scraped': 0,
                'shutuba_scraped': 0,
                'horses_scraped': 0,
                'pedigrees_scraped': 0,
                'failed_scrapes': 0,
                'http_errors': Counter(),
            },
            'parsing_stats': {
                'races_parsed': 0,
                'shutuba_parsed': 0,
                'horses_parsed': 0,
                'pedigrees_parsed': 0,
                'parse_errors': 0,
                'missing_data': Counter(),
            },
            'execution_time': {},
        }

    def find_log_files(self) -> List[Path]:
        """ログディレクトリ内のすべてのログファイルを検索"""
        if not self.log_dir.exists():
            print(f"⚠️  ログディレクトリが存在しません: {self.log_dir}")
            return []

        log_files = sorted(self.log_dir.glob("*.log"))
        return log_files

    def parse_log_line(self, line: str) -> Dict:
        """ログ行をパースして構造化データに変換"""
        # 標準フォーマット: "2025-11-18 10:30:45,123 - INFO - module_name - message"
        # または: "2025-11-18 10:30:45,123 - INFO - message"
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
        """単一のログファイルを分析"""
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

                        # エラーと警告を記録
                        if entry['level'] == 'ERROR':
                            self.stats['errors'].append(entry)
                        elif entry['level'] == 'WARNING':
                            self.stats['warnings'].append(entry)

                        # スクレイピング統計を抽出
                        self._extract_scraping_stats(entry)

                        # パース統計を抽出
                        self._extract_parsing_stats(entry)

                        # 実行時間を抽出
                        self._extract_execution_time(entry)

        except Exception as e:
            print(f"❌ ファイル読み込みエラー: {log_file.name} - {e}")

    def _extract_scraping_stats(self, entry: Dict):
        """スクレイピング関連の統計を抽出"""
        msg = entry['message'].lower()

        # レース結果のスクレイピング
        if 'scraping race' in msg or 'scraped race' in msg:
            self.stats['scraping_stats']['races_scraped'] += 1

        # 出馬表のスクレイピング
        if 'scraping shutuba' in msg or 'scraped shutuba' in msg:
            self.stats['scraping_stats']['shutuba_scraped'] += 1

        # 馬情報のスクレイピング
        if 'scraping horse' in msg or 'scraped horse' in msg:
            self.stats['scraping_stats']['horses_scraped'] += 1

        # 血統情報のスクレイピング
        if 'scraping pedigree' in msg or 'scraped pedigree' in msg:
            self.stats['scraping_stats']['pedigrees_scraped'] += 1

        # HTTPエラー
        if 'http' in msg and ('error' in msg or 'failed' in msg):
            self.stats['scraping_stats']['failed_scrapes'] += 1

            # HTTPステータスコードを抽出
            status_match = re.search(r'(\d{3})', entry['message'])
            if status_match:
                status_code = status_match.group(1)
                self.stats['scraping_stats']['http_errors'][status_code] += 1

    def _extract_parsing_stats(self, entry: Dict):
        """パース関連の統計を抽出"""
        msg = entry['message'].lower()

        # レース結果のパース
        if 'parsing race' in msg or 'parsed race' in msg:
            self.stats['parsing_stats']['races_parsed'] += 1

        # 出馬表のパース
        if 'parsing shutuba' in msg or 'parsed shutuba' in msg:
            self.stats['parsing_stats']['shutuba_parsed'] += 1

        # 馬情報のパース
        if 'parsing horse' in msg or 'parsed horse' in msg:
            self.stats['parsing_stats']['horses_parsed'] += 1

        # 血統情報のパース
        if 'parsing pedigree' in msg or 'parsed pedigree' in msg:
            self.stats['parsing_stats']['pedigrees_parsed'] += 1

        # パースエラー
        if entry['level'] == 'ERROR' and 'pars' in msg:
            self.stats['parsing_stats']['parse_errors'] += 1

        # 欠損データ
        if 'missing' in msg or 'not found' in msg:
            # メッセージから欠損フィールドを抽出
            field_match = re.search(r'(\w+)\s+(missing|not found)', entry['message'])
            if field_match:
                field = field_match.group(1)
                self.stats['parsing_stats']['missing_data'][field] += 1

    def _extract_execution_time(self, entry: Dict):
        """実行時間を抽出"""
        msg = entry['message']

        # "Completed in X seconds" パターン
        time_match = re.search(r'completed in\s+([\d.]+)\s+seconds', msg, re.IGNORECASE)
        if time_match:
            seconds = float(time_match.group(1))

            # どの処理の時間か判定
            if 'scraping' in msg.lower():
                self.stats['execution_time']['scraping'] = seconds
            elif 'parsing' in msg.lower():
                self.stats['execution_time']['parsing'] = seconds
            elif 'total' in msg.lower() or 'pipeline' in msg.lower():
                self.stats['execution_time']['total'] = seconds

    def generate_report(self) -> str:
        """分析結果のレポートを生成"""
        report = []
        report.append("=" * 80)
        report.append("📊 KeibaAI_v2 ログ分析レポート")
        report.append("=" * 80)
        report.append(f"📁 ログディレクトリ: {self.log_dir}")
        report.append(f"📅 分析日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        # 総合統計
        report.append("■ 総合統計")
        report.append(f"  • 総ログエントリ数: {self.stats['total_entries']:,}")
        report.append("")

        # ログレベル別
        report.append("■ ログレベル別集計")
        for level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            count = self.stats['by_level'].get(level, 0)
            if count > 0:
                percentage = (count / self.stats['total_entries'] * 100) if self.stats['total_entries'] > 0 else 0
                report.append(f"  • {level:8s}: {count:6,} ({percentage:5.1f}%)")
        report.append("")

        # スクレイピング統計
        report.append("■ スクレイピング統計")
        scraping = self.stats['scraping_stats']
        report.append(f"  • レース結果: {scraping['races_scraped']:,} 件")
        report.append(f"  • 出馬表: {scraping['shutuba_scraped']:,} 件")
        report.append(f"  • 馬情報: {scraping['horses_scraped']:,} 件")
        report.append(f"  • 血統情報: {scraping['pedigrees_scraped']:,} 件")
        report.append(f"  • 失敗: {scraping['failed_scrapes']:,} 件")

        if scraping['http_errors']:
            report.append("  • HTTPエラー:")
            for status, count in sorted(scraping['http_errors'].items()):
                report.append(f"    - {status}: {count:,} 件")
        report.append("")

        # パース統計
        report.append("■ パース統計")
        parsing = self.stats['parsing_stats']
        report.append(f"  • レース結果: {parsing['races_parsed']:,} 件")
        report.append(f"  • 出馬表: {parsing['shutuba_parsed']:,} 件")
        report.append(f"  • 馬情報: {parsing['horses_parsed']:,} 件")
        report.append(f"  • 血統情報: {parsing['pedigrees_parsed']:,} 件")
        report.append(f"  • パースエラー: {parsing['parse_errors']:,} 件")

        if parsing['missing_data']:
            report.append("  • 欠損データ (上位10件):")
            for field, count in parsing['missing_data'].most_common(10):
                report.append(f"    - {field}: {count:,} 件")
        report.append("")

        # 実行時間
        if self.stats['execution_time']:
            report.append("■ 実行時間")
            for process, seconds in self.stats['execution_time'].items():
                minutes = seconds / 60
                hours = minutes / 60
                if hours >= 1:
                    report.append(f"  • {process}: {hours:.2f} 時間 ({seconds:,.1f} 秒)")
                elif minutes >= 1:
                    report.append(f"  • {process}: {minutes:.2f} 分 ({seconds:,.1f} 秒)")
                else:
                    report.append(f"  • {process}: {seconds:.1f} 秒")
            report.append("")

        # エラー詳細（最新10件）
        if self.stats['errors']:
            report.append("■ エラー詳細 (最新10件)")
            for i, error in enumerate(self.stats['errors'][-10:], 1):
                timestamp = error['timestamp'].strftime('%H:%M:%S') if error['timestamp'] else 'N/A'
                report.append(f"  {i}. [{timestamp}] {error['message'][:100]}")
                if len(error['message']) > 100:
                    report.append(f"     ... (省略)")
            report.append("")

        # 警告詳細（最新10件）
        if self.stats['warnings']:
            report.append("■ 警告詳細 (最新10件)")
            for i, warning in enumerate(self.stats['warnings'][-10:], 1):
                timestamp = warning['timestamp'].strftime('%H:%M:%S') if warning['timestamp'] else 'N/A'
                report.append(f"  {i}. [{timestamp}] {warning['message'][:100]}")
                if len(warning['message']) > 100:
                    report.append(f"     ... (省略)")
            report.append("")

        report.append("=" * 80)
        return "\n".join(report)

    def save_detailed_report(self, output_path: Path):
        """詳細なJSON形式のレポートを保存"""
        detailed_report = {
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
                k: v if not isinstance(v, Counter) else dict(v)
                for k, v in self.stats['parsing_stats'].items()
            },
            'execution_time': self.stats['execution_time'],
            'errors': [
                {
                    'timestamp': e['timestamp'].isoformat() if e['timestamp'] else None,
                    'message': e['message']
                }
                for e in self.stats['errors']
            ],
            'warnings': [
                {
                    'timestamp': w['timestamp'].isoformat() if w['timestamp'] else None,
                    'message': w['message']
                }
                for w in self.stats['warnings']
            ],
        }

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(detailed_report, f, ensure_ascii=False, indent=2)

        print(f"💾 詳細レポートを保存: {output_path}")

    def analyze(self):
        """ログファイルの分析を実行"""
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

    # デフォルトのログディレクトリ（今日の日付）
    today = date.today()
    default_log_dir = Path(f"keibaai/data/logs/{today.year}/{today.month:02d}/{today.day:02d}")

    # コマンドライン引数でディレクトリを指定可能
    if len(sys.argv) > 1:
        log_dir = Path(sys.argv[1])
    else:
        log_dir = default_log_dir

    print(f"🔍 KeibaAI_v2 ログアナライザー")
    print(f"📁 ログディレクトリ: {log_dir}\n")

    # アナライザーを初期化して実行
    analyzer = KeibaLogAnalyzer(log_dir)
    analyzer.analyze()

    # レポート生成
    report = analyzer.generate_report()
    print(report)

    # 詳細レポートをJSON形式で保存
    output_dir = Path("keibaai/data/logs/analysis")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_output = output_dir / f"log_analysis_{timestamp}.json"
    analyzer.save_detailed_report(json_output)

    # サマリーをテキストファイルにも保存
    txt_output = output_dir / f"log_analysis_{timestamp}.txt"
    with open(txt_output, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"💾 サマリーレポートを保存: {txt_output}")

    print("\n" + "=" * 80)
    print("📈 分析完了! 詳細は上記ファイルを参照してください。")
    print("=" * 80)


if __name__ == "__main__":
    main()

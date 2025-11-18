#!/usr/bin/env python3
"""
KeibaAI_v2 Log Analyzer (Fixed Version)
実際のログフォーマットと日本語メッセージに対応したバージョン
"""

import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, List, Optional
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
            'criticals': [],
            'scraping_stats': {
                'races_scraped': 0,
                'shutuba_scraped': 0,
                'horses_scraped': 0,
                'horses_performance_scraped': 0,
                'pedigrees_scraped': 0,
                'failed_scrapes': 0,
                'http_errors': Counter(),
                'ip_ban_warnings': 0,
            },
            'parsing_stats': {
                'races_parsed': 0,
                'shutuba_parsed': 0,
                'horses_parsed': 0,
                'horses_profile_parsed': 0,
                'horses_performance_parsed': 0,
                'pedigrees_parsed': 0,
                'parse_errors': 0,
                'total_records': Counter(),
            },
            'execution_time': {},
            'file_counts': {},
        }

    def find_log_files(self) -> List[Path]:
        """ログディレクトリ内のすべてのログファイルを検索"""
        if not self.log_dir.exists():
            print(f"⚠️  ログディレクトリが存在しません: {self.log_dir}")
            return []

        log_files = sorted(self.log_dir.glob("*.log"))
        return log_files

    def parse_log_line(self, line: str) -> Optional[Dict]:
        """ログ行をパースして構造化データに変換

        実際のフォーマット: YYYY-MM-DD HH:MM:SS,mmm - <module> - <level> - <message>
        例: 2025-11-18 09:02:52,233 - __main__ - INFO - データ整形パイプラインを開始します...
        """
        # モジュール名付きフォーマット
        pattern = r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})\s+-\s+([^\s]+)\s+-\s+(\w+)\s+-\s+(.+)'
        match = re.match(pattern, line)

        if match:
            timestamp_str, module, level, message = match.groups()
            try:
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
            except:
                timestamp = None

            return {
                'timestamp': timestamp,
                'module': module,
                'level': level,
                'message': message.strip(),
                'raw': line
            }
        return None

    def analyze_log_file(self, log_file: Path):
        """単一のログファイルを分析"""
        print(f"📄 分析中: {log_file.name}")

        try:
            with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    entry = self.parse_log_line(line)
                    if entry:
                        self.log_entries.append(entry)
                        self.stats['total_entries'] += 1
                        self.stats['by_level'][entry['level']] += 1
                        self.stats['by_module'][entry['module']] += 1

                        # エラー、警告、クリティカルを記録
                        if entry['level'] == 'ERROR':
                            self.stats['errors'].append(entry)
                        elif entry['level'] == 'WARNING':
                            self.stats['warnings'].append(entry)
                        elif entry['level'] == 'CRITICAL':
                            self.stats['criticals'].append(entry)

                        # スクレイピング統計を抽出
                        self._extract_scraping_stats(entry)

                        # パース統計を抽出
                        self._extract_parsing_stats(entry)

                        # 実行時間とファイル数を抽出
                        self._extract_metadata(entry)

        except Exception as e:
            print(f"❌ ファイル読み込みエラー: {log_file.name} - {e}")

    def _extract_scraping_stats(self, entry: Dict):
        """スクレイピング関連の統計を抽出"""
        msg = entry['message']

        # HTTPエラー（CRITICAL）
        if 'HTTP 400' in msg or 'HTTP 403' in msg or 'HTTP 404' in msg:
            status_match = re.search(r'HTTP (\d{3})', msg)
            if status_match:
                status_code = status_match.group(1)
                self.stats['scraping_stats']['http_errors'][status_code] += 1
                self.stats['scraping_stats']['failed_scrapes'] += 1

        # IP BAN警告
        if 'IP BAN' in msg or 'IP BANの可能性' in msg:
            self.stats['scraping_stats']['ip_ban_warnings'] += 1

        # 開催日取得
        if '個の開催日を取得' in msg:
            count_match = re.search(r'(\d+)個の開催日', msg)
            if count_match:
                self.stats['scraping_stats']['kaisai_days'] = int(count_match.group(1))

    def _extract_parsing_stats(self, entry: Dict):
        """パース関連の統計を抽出（日本語キーワードに対応）"""
        msg = entry['message']

        # レース結果パース
        if 'レース結果パース完了' in msg:
            self.stats['parsing_stats']['races_parsed'] += 1
            # 行数を抽出
            rows_match = re.search(r'(\d+)行', msg)
            if rows_match:
                rows = int(rows_match.group(1))
                self.stats['parsing_stats']['total_records']['races'] += rows

        # 出馬表パース
        if '出馬表パース完了' in msg:
            self.stats['parsing_stats']['shutuba_parsed'] += 1
            rows_match = re.search(r'(\d+)行', msg)
            if rows_match:
                rows = int(rows_match.group(1))
                self.stats['parsing_stats']['total_records']['shutuba'] += rows

        # 馬プロフィールパース
        if '馬プロフィールパース完了' in msg:
            self.stats['parsing_stats']['horses_profile_parsed'] += 1

        # 馬過去成績パース
        if '馬過去成績パース完了' in msg:
            self.stats['parsing_stats']['horses_performance_parsed'] += 1
            races_match = re.search(r'(\d+)レース', msg)
            if races_match:
                races = int(races_match.group(1))
                self.stats['parsing_stats']['total_records']['horse_performance'] += races

        # 血統パース
        if '血統パース完了' in msg:
            self.stats['parsing_stats']['pedigrees_parsed'] += 1

        # パースエラー
        if entry['level'] == 'ERROR' and ('パース' in msg or 'parse' in msg.lower()):
            self.stats['parsing_stats']['parse_errors'] += 1

    def _extract_metadata(self, entry: Dict):
        """メタデータ（ファイル数、実行時間など）を抽出"""
        msg = entry['message']

        # ファイル数
        file_count_match = re.search(r'(\d{1,3}(?:,\d{3})*)件の.*(HTML|ファイル)', msg)
        if file_count_match:
            count_str = file_count_match.group(1).replace(',', '')
            count = int(count_str)
            if 'レース結果' in msg:
                self.stats['file_counts']['race_files'] = count
            elif '出馬表' in msg:
                self.stats['file_counts']['shutuba_files'] = count
            elif '馬プロフィール' in msg:
                self.stats['file_counts']['horse_profile_files'] = count
            elif '馬過去成績' in msg:
                self.stats['file_counts']['horse_performance_files'] = count

        # 保存完了（レコード数）
        save_match = re.search(r'保存完了.*\((\d{1,3}(?:,\d{3})*)レコード\)', msg)
        if save_match:
            count_str = save_match.group(1).replace(',', '')
            count = int(count_str)
            if 'races' in msg or 'レース結果' in msg:
                self.stats['parsing_stats']['total_records']['races_saved'] = count
            elif 'shutuba' in msg or '出馬表' in msg:
                self.stats['parsing_stats']['total_records']['shutuba_saved'] = count
            elif 'horses' in msg or '馬情報' in msg:
                self.stats['parsing_stats']['total_records']['horses_saved'] = count
            elif 'performance' in msg or '過去成績' in msg:
                self.stats['parsing_stats']['total_records']['horse_performance_saved'] = count
            elif 'pedigree' in msg or '血統' in msg:
                self.stats['parsing_stats']['total_records']['pedigrees_saved'] = count

    def generate_report(self) -> str:
        """分析結果のレポートを生成"""
        report = []
        report.append("=" * 80)
        report.append("📊 KeibaAI_v2 ログ分析レポート (修正版)")
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
                report.append(f"  • {level:8s}: {count:8,} ({percentage:5.1f}%)")
        report.append("")

        # モジュール別（上位10件）
        if self.stats['by_module']:
            report.append("■ モジュール別集計 (上位10件)")
            for module, count in self.stats['by_module'].most_common(10):
                percentage = (count / self.stats['total_entries'] * 100) if self.stats['total_entries'] > 0 else 0
                report.append(f"  • {module:40s}: {count:8,} ({percentage:5.1f}%)")
            report.append("")

        # スクレイピング統計
        report.append("■ スクレイピング統計")
        scraping = self.stats['scraping_stats']

        if 'kaisai_days' in scraping:
            report.append(f"  • 開催日: {scraping['kaisai_days']:,} 日")

        if scraping['failed_scrapes'] > 0:
            report.append(f"  • スクレイピング失敗: {scraping['failed_scrapes']:,} 件")

        if scraping['ip_ban_warnings'] > 0:
            report.append(f"  • ⚠️  IP BAN警告: {scraping['ip_ban_warnings']:,} 回")

        if scraping['http_errors']:
            report.append("  • HTTPエラー:")
            for status, count in sorted(scraping['http_errors'].items()):
                error_name = {
                    '400': 'Bad Request',
                    '403': 'Forbidden',
                    '404': 'Not Found',
                    '500': 'Internal Server Error',
                    '503': 'Service Unavailable'
                }.get(status, 'Unknown')
                report.append(f"    - HTTP {status} ({error_name}): {count:,} 件")
        report.append("")

        # ファイル数統計
        if self.stats['file_counts']:
            report.append("■ 処理ファイル数")
            for file_type, count in self.stats['file_counts'].items():
                file_type_jp = {
                    'race_files': 'レース結果',
                    'shutuba_files': '出馬表',
                    'horse_profile_files': '馬プロフィール',
                    'horse_performance_files': '馬過去成績',
                }.get(file_type, file_type)
                report.append(f"  • {file_type_jp}: {count:,} ファイル")
            report.append("")

        # パース統計
        report.append("■ パース統計")
        parsing = self.stats['parsing_stats']
        report.append(f"  • レース結果: {parsing['races_parsed']:,} 件")
        report.append(f"  • 出馬表: {parsing['shutuba_parsed']:,} 件")
        report.append(f"  • 馬プロフィール: {parsing['horses_profile_parsed']:,} 件")
        report.append(f"  • 馬過去成績: {parsing['horses_performance_parsed']:,} 件")
        report.append(f"  • 血統: {parsing['pedigrees_parsed']:,} 件")

        if parsing['parse_errors'] > 0:
            report.append(f"  • パースエラー: {parsing['parse_errors']:,} 件")

        # 保存済みレコード数
        if parsing['total_records']:
            report.append("")
            report.append("■ 保存済みレコード数")
            for data_type, count in parsing['total_records'].items():
                data_type_jp = {
                    'races': 'レース結果（行）',
                    'shutuba': '出馬表（行）',
                    'horse_performance': '馬過去成績（レース）',
                    'races_saved': 'レース結果（最終保存）',
                    'shutuba_saved': '出馬表（最終保存）',
                    'horses_saved': '馬情報（最終保存）',
                    'horse_performance_saved': '馬過去成績（最終保存）',
                    'pedigrees_saved': '血統（最終保存）',
                }.get(data_type, data_type)
                report.append(f"  • {data_type_jp}: {count:,} レコード")
        report.append("")

        # クリティカルエラー
        if self.stats['criticals']:
            report.append("■ クリティカルエラー (全件)")
            report.append("─" * 80)
            for i, critical in enumerate(self.stats['criticals'], 1):
                timestamp = critical['timestamp'].strftime('%H:%M:%S') if critical['timestamp'] else 'N/A'
                report.append(f"  {i:2d}. [{timestamp}] {critical['message'][:90]}")
                if len(critical['message']) > 90:
                    report.append(f"      {critical['message'][90:180]}")
            report.append("")

        # エラー詳細（最新15件）
        if self.stats['errors']:
            report.append(f"■ エラー詳細 (最新15件 / 全{len(self.stats['errors'])}件)")
            report.append("─" * 80)
            for i, error in enumerate(self.stats['errors'][-15:], 1):
                timestamp = error['timestamp'].strftime('%H:%M:%S') if error['timestamp'] else 'N/A'
                report.append(f"  {i:2d}. [{timestamp}] {error['message'][:90]}")
            report.append("")

        # 警告詳細（最新15件）
        if self.stats['warnings']:
            report.append(f"■ 警告詳細 (最新15件 / 全{len(self.stats['warnings'])}件)")
            report.append("─" * 80)
            for i, warning in enumerate(self.stats['warnings'][-15:], 1):
                timestamp = warning['timestamp'].strftime('%H:%M:%S') if warning['timestamp'] else 'N/A'
                report.append(f"  {i:2d}. [{timestamp}] {warning['message'][:90]}")
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
            'modules': dict(self.stats['by_module'].most_common(20)),
            'scraping_stats': {
                k: v if not isinstance(v, Counter) else dict(v)
                for k, v in self.stats['scraping_stats'].items()
            },
            'parsing_stats': {
                k: v if not isinstance(v, Counter) else dict(v)
                for k, v in self.stats['parsing_stats'].items()
            },
            'file_counts': self.stats['file_counts'],
            'error_count': len(self.stats['errors']),
            'warning_count': len(self.stats['warnings']),
            'critical_count': len(self.stats['criticals']),
            'errors': [
                {
                    'timestamp': e['timestamp'].isoformat() if e['timestamp'] else None,
                    'module': e['module'],
                    'message': e['message']
                }
                for e in self.stats['errors']
            ],
            'warnings': [
                {
                    'timestamp': w['timestamp'].isoformat() if w['timestamp'] else None,
                    'module': w['module'],
                    'message': w['message']
                }
                for w in self.stats['warnings']
            ],
            'criticals': [
                {
                    'timestamp': c['timestamp'].isoformat() if c['timestamp'] else None,
                    'module': c['module'],
                    'message': c['message']
                }
                for c in self.stats['criticals']
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

    print(f"🔍 KeibaAI_v2 ログアナライザー (修正版)")
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
    json_output = output_dir / f"log_analysis_fixed_{timestamp}.json"
    analyzer.save_detailed_report(json_output)

    # サマリーをテキストファイルにも保存
    txt_output = output_dir / f"log_analysis_fixed_{timestamp}.txt"
    with open(txt_output, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"💾 サマリーレポートを保存: {txt_output}")

    print("\n" + "=" * 80)
    print("📈 分析完了! 詳細は上記ファイルを参照してください。")
    print("=" * 80)


if __name__ == "__main__":
    main()

# src/modules/validation/validation_pipeline.py
"""
包括的なデータ品質検証パイプライン

スクレイピング、パース、特徴量生成の各段階でデータ品質を自動チェックし、
詳細なレポートを生成する。
"""

import logging
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """検証結果を格納するデータクラス"""
    check_name: str
    status: str  # 'pass', 'warning', 'fail'
    message: str
    details: Dict[str, Any]
    timestamp: str


class ValidationPipeline:
    """包括的なデータ品質検証パイプライン"""

    def __init__(self, data_path: Path, config: Dict[str, Any]):
        """
        Args:
            data_path: データディレクトリのパス
            config: 設定辞書
        """
        self.data_path = Path(data_path)
        self.config = config
        self.results: List[ValidationResult] = []

        # 品質しきい値（設定可能）
        self.thresholds = {
            'max_404_rate': 0.01,  # 404エラー率の上限（1%）
            'max_parse_error_rate': 0.02,  # パースエラー率の上限（2%）
            'max_missing_rate': 0.10,  # 欠損率の上限（10%）
            'min_data_count': 1000,  # 最小データ件数
            'max_outlier_rate': 0.05,  # 外れ値率の上限（5%）
        }

    def validate_all(self, start_date: Optional[str] = None,
                     end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        すべての検証を実行

        Args:
            start_date: 検証開始日（YYYY-MM-DD）
            end_date: 検証終了日（YYYY-MM-DD）

        Returns:
            検証結果の辞書
        """
        logger.info("=" * 80)
        logger.info("データ品質検証パイプライン開始")
        logger.info("=" * 80)

        if start_date and end_date:
            logger.info(f"検証期間: {start_date} - {end_date}")

        # Phase 1: スクレイピングデータの検証
        logger.info("\n[Phase 1] スクレイピングデータの検証")
        logger.info("-" * 80)
        self._validate_scraping_data()

        # Phase 2: パースデータの検証
        logger.info("\n[Phase 2] パースデータの検証")
        logger.info("-" * 80)
        self._validate_parsed_data()

        # Phase 3: 特徴量データの検証
        logger.info("\n[Phase 3] 特徴量データの検証")
        logger.info("-" * 80)
        self._validate_features_data(start_date, end_date)

        # 結果の集計
        summary = self._generate_summary()

        logger.info("=" * 80)
        logger.info("データ品質検証パイプライン完了")
        logger.info("=" * 80)

        return summary

    def _validate_scraping_data(self):
        """スクレイピングデータの品質検証"""
        html_path = self.data_path / 'raw' / 'html'

        if not html_path.exists():
            self._add_result('scraping_existence', 'fail',
                           'HTML データディレクトリが存在しません', {})
            return

        # 各カテゴリのHTMLファイルを検証
        categories = ['race', 'shutuba', 'horse', 'ped']

        for category in categories:
            category_path = html_path / category
            if not category_path.exists():
                self._add_result(f'scraping_{category}_existence', 'warning',
                               f'{category} ディレクトリが存在しません', {})
                continue

            # ファイル数とサイズの統計
            files = list(category_path.glob('*.bin'))
            if not files:
                self._add_result(f'scraping_{category}_count', 'warning',
                               f'{category} ディレクトリにファイルが存在しません', {})
                continue

            # ファイルサイズの統計
            sizes = [f.stat().st_size for f in files]

            details = {
                'file_count': len(files),
                'total_size_mb': sum(sizes) / (1024 * 1024),
                'avg_size_kb': np.mean(sizes) / 1024,
                'min_size_kb': min(sizes) / 1024,
                'max_size_kb': max(sizes) / 1024,
            }

            # 異常に小さいファイル（404エラーの可能性）を検出
            small_files = [f for f in files if f.stat().st_size < 1000]  # 1KB未満
            error_rate = len(small_files) / len(files)

            details['small_file_count'] = len(small_files)
            details['error_rate'] = error_rate

            if error_rate > self.thresholds['max_404_rate']:
                status = 'warning'
                message = f'{category} に {len(small_files)} 個の異常に小さいファイル（404の可能性）が存在'
            else:
                status = 'pass'
                message = f'{category} のスクレイピングデータは正常'

            self._add_result(f'scraping_{category}', status, message, details)

    def _validate_parsed_data(self):
        """パースデータの品質検証"""
        parquet_path = self.data_path / 'parsed' / 'parquet'

        if not parquet_path.exists():
            self._add_result('parsed_existence', 'fail',
                           'Parquet データディレクトリが存在しません', {})
            return

        # 各Parquetファイルを検証
        parquet_files = {
            'races': parquet_path / 'races' / 'races.parquet',
            'shutuba': parquet_path / 'shutuba' / 'shutuba.parquet',
            'horses': parquet_path / 'horses' / 'horses.parquet',
            'pedigrees': parquet_path / 'pedigrees' / 'pedigrees.parquet',
        }

        for name, file_path in parquet_files.items():
            if not file_path.exists():
                self._add_result(f'parsed_{name}_existence', 'warning',
                               f'{name}.parquet が存在しません', {})
                continue

            try:
                df = pd.read_parquet(file_path)

                # 基本統計
                details = {
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'memory_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
                }

                # データ件数チェック
                if len(df) < self.thresholds['min_data_count']:
                    status = 'warning'
                    message = f'{name} のデータ件数が少ない（{len(df)}件）'
                else:
                    status = 'pass'
                    message = f'{name} のデータ件数は正常'

                # スキーマ検証
                schema_issues = self._validate_schema(name, df)
                if schema_issues:
                    details['schema_issues'] = schema_issues
                    status = 'warning'
                    message += f' | スキーマに {len(schema_issues)} 個の問題'

                # 欠損率の計算
                missing_rates = (df.isnull().sum() / len(df) * 100).to_dict()
                high_missing = {col: rate for col, rate in missing_rates.items()
                              if rate > self.thresholds['max_missing_rate'] * 100}

                if high_missing:
                    details['high_missing_columns'] = high_missing
                    if status == 'pass':
                        status = 'warning'
                        message += f' | {len(high_missing)} 個のカラムで高欠損率'

                # 論理整合性チェック（racesの場合）
                if name == 'races':
                    consistency_issues = self._check_race_consistency(df)
                    if consistency_issues:
                        details['consistency_issues'] = consistency_issues
                        status = 'warning'
                        message += f' | {len(consistency_issues)} 個の整合性エラー'

                self._add_result(f'parsed_{name}', status, message, details)

            except Exception as e:
                self._add_result(f'parsed_{name}', 'fail',
                               f'{name}.parquet の読み込みエラー: {str(e)}', {})

    def _validate_features_data(self, start_date: Optional[str],
                               end_date: Optional[str]):
        """特徴量データの品質検証"""
        features_path = self.data_path / 'features' / 'parquet'

        if not features_path.exists():
            self._add_result('features_existence', 'warning',
                           '特徴量ディレクトリが存在しません', {})
            return

        try:
            # パーティション構造の確認
            year_dirs = sorted([d for d in features_path.glob('year=*') if d.is_dir()])

            if not year_dirs:
                self._add_result('features_partitions', 'warning',
                               '特徴量パーティションが存在しません', {})
                return

            details = {
                'year_count': len(year_dirs),
                'years': [d.name.split('=')[1] for d in year_dirs],
            }

            # サンプルデータを読み込んで検証
            sample_df = pd.read_parquet(features_path,
                                       filters=[('year', '=', int(year_dirs[-1].name.split('=')[1]))])

            details['sample_row_count'] = len(sample_df)
            details['feature_count'] = len(sample_df.columns)

            # データリークチェック（禁止カラムの検出）
            forbidden_columns = ['win_odds', 'place_odds', 'popularity', 'finish_position',
                               'finish_time_seconds', 'prize_money']
            leaks = [col for col in forbidden_columns if col in sample_df.columns]

            if leaks:
                details['data_leaks'] = leaks
                self._add_result('features_data_leak', 'fail',
                               f'データリークを検出: {leaks}', details)
            else:
                self._add_result('features_data_leak', 'pass',
                               'データリークなし', {})

            # 欠損率チェック
            missing_rates = (sample_df.isnull().sum() / len(sample_df) * 100).to_dict()
            high_missing = {col: rate for col, rate in missing_rates.items()
                          if rate > self.thresholds['max_missing_rate'] * 100}

            if high_missing:
                details['high_missing_features'] = high_missing
                status = 'warning'
                message = f'{len(high_missing)} 個の特徴量で高欠損率'
            else:
                status = 'pass'
                message = '特徴量の欠損率は正常'

            # 無限値・NaNチェック
            inf_cols = []
            for col in sample_df.select_dtypes(include=[np.number]).columns:
                if np.isinf(sample_df[col]).any():
                    inf_cols.append(col)

            if inf_cols:
                details['infinite_value_features'] = inf_cols
                status = 'warning'
                message += f' | {len(inf_cols)} 個の特徴量に無限値'

            self._add_result('features_quality', status, message, details)

        except Exception as e:
            self._add_result('features_quality', 'fail',
                           f'特徴量検証エラー: {str(e)}', {})

    def _validate_schema(self, name: str, df: pd.DataFrame) -> List[str]:
        """スキーマの検証"""
        issues = []

        # 期待されるカラムの定義
        expected_schemas = {
            'races': {
                'required': ['race_id', 'race_date', 'venue', 'distance_m',
                           'track_surface', 'finish_position', 'horse_id'],
                'numeric': ['distance_m', 'head_count', 'finish_position'],
            },
            'shutuba': {
                'required': ['race_id', 'horse_id', 'horse_number'],
                'numeric': ['horse_number', 'morning_odds'],
            },
            'horses': {
                'required': ['horse_id', 'horse_name'],
                'numeric': [],
            },
            'pedigrees': {
                'required': ['horse_id', 'generation', 'position'],
                'numeric': ['generation', 'position'],
            },
        }

        if name not in expected_schemas:
            return issues

        schema = expected_schemas[name]

        # 必須カラムのチェック
        for col in schema['required']:
            if col not in df.columns:
                issues.append(f'必須カラム不在: {col}')

        # 数値カラムの型チェック
        for col in schema['numeric']:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    issues.append(f'数値型エラー: {col} ({df[col].dtype})')

        return issues

    def _check_race_consistency(self, df: pd.DataFrame) -> List[str]:
        """レースデータの論理整合性チェック"""
        issues = []

        # 着順 <= 出走頭数
        if 'finish_position' in df.columns and 'head_count' in df.columns:
            invalid_positions = df[df['finish_position'] > df['head_count']]
            if len(invalid_positions) > 0:
                issues.append(f'{len(invalid_positions)} 件の着順が出走頭数を超過')

        # 距離の範囲チェック（100m-4000m）
        if 'distance_m' in df.columns:
            invalid_distances = df[(df['distance_m'] < 100) | (df['distance_m'] > 4000)]
            if len(invalid_distances) > 0:
                issues.append(f'{len(invalid_distances)} 件の異常な距離')

        # タイムの範囲チェック
        if 'finish_time_seconds' in df.columns:
            invalid_times = df[(df['finish_time_seconds'] < 50) | (df['finish_time_seconds'] > 500)]
            if len(invalid_times) > 0:
                issues.append(f'{len(invalid_times)} 件の異常なタイム')

        return issues

    def _add_result(self, check_name: str, status: str,
                   message: str, details: Dict[str, Any]):
        """検証結果を追加"""
        result = ValidationResult(
            check_name=check_name,
            status=status,
            message=message,
            details=details,
            timestamp=datetime.now().isoformat()
        )
        self.results.append(result)

        # ログ出力
        level = {
            'pass': logging.INFO,
            'warning': logging.WARNING,
            'fail': logging.ERROR,
        }.get(status, logging.INFO)

        logger.log(level, f"[{status.upper()}] {check_name}: {message}")

    def _generate_summary(self) -> Dict[str, Any]:
        """検証結果のサマリを生成"""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_checks': len(self.results),
            'passed': sum(1 for r in self.results if r.status == 'pass'),
            'warnings': sum(1 for r in self.results if r.status == 'warning'),
            'failed': sum(1 for r in self.results if r.status == 'fail'),
            'results': [asdict(r) for r in self.results],
        }

        # サマリのログ出力
        logger.info("\n" + "=" * 80)
        logger.info("検証結果サマリ")
        logger.info("=" * 80)
        logger.info(f"総チェック数: {summary['total_checks']}")
        logger.info(f"✓ 合格: {summary['passed']}")
        logger.info(f"⚠ 警告: {summary['warnings']}")
        logger.info(f"✗ 失敗: {summary['failed']}")

        # 失敗・警告のある項目を詳細表示
        if summary['warnings'] > 0 or summary['failed'] > 0:
            logger.info("\n問題のある項目:")
            for result in self.results:
                if result.status in ['warning', 'fail']:
                    logger.info(f"  [{result.status.upper()}] {result.check_name}")
                    logger.info(f"    {result.message}")

        return summary

    def save_report(self, output_path: Path, format: str = 'json'):
        """
        検証レポートを保存

        Args:
            output_path: 出力パス
            format: 出力フォーマット ('json' or 'markdown')
        """
        summary = self._generate_summary()

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format == 'json':
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            logger.info(f"レポートを保存: {output_path}")

        elif format == 'markdown':
            md_content = self._generate_markdown_report(summary)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(md_content)
            logger.info(f"Markdownレポートを保存: {output_path}")

    def _generate_markdown_report(self, summary: Dict[str, Any]) -> str:
        """Markdown形式のレポートを生成"""
        lines = [
            "# データ品質検証レポート",
            "",
            f"**生成日時**: {summary['timestamp']}",
            "",
            "## サマリ",
            "",
            f"- 総チェック数: {summary['total_checks']}",
            f"- ✓ 合格: {summary['passed']}",
            f"- ⚠ 警告: {summary['warnings']}",
            f"- ✗ 失敗: {summary['failed']}",
            "",
            "## 詳細結果",
            "",
        ]

        # ステータス別にグループ化
        for status in ['fail', 'warning', 'pass']:
            status_results = [r for r in self.results if r.status == status]
            if not status_results:
                continue

            status_icon = {'pass': '✓', 'warning': '⚠', 'fail': '✗'}[status]
            lines.append(f"### {status_icon} {status.upper()}")
            lines.append("")

            for result in status_results:
                lines.append(f"#### {result.check_name}")
                lines.append(f"**メッセージ**: {result.message}")

                if result.details:
                    lines.append("**詳細**:")
                    for key, value in result.details.items():
                        if isinstance(value, (int, float)):
                            lines.append(f"- {key}: {value:.2f}" if isinstance(value, float) else f"- {key}: {value}")
                        elif isinstance(value, (list, dict)) and len(str(value)) < 200:
                            lines.append(f"- {key}: {value}")
                lines.append("")

        return "\n".join(lines)


# コマンドライン実行用の関数
def run_validation(data_path: str, config_path: Optional[str] = None,
                  output_dir: Optional[str] = None):
    """
    コマンドラインから検証を実行

    Args:
        data_path: データディレクトリのパス
        config_path: 設定ファイルのパス
        output_dir: レポート出力先ディレクトリ
    """
    import yaml

    # 設定の読み込み
    if config_path:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    else:
        config = {}

    # 検証の実行
    pipeline = ValidationPipeline(data_path, config)
    summary = pipeline.validate_all()

    # レポートの保存
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # JSON形式
        json_path = output_dir / f'validation_report_{timestamp}.json'
        pipeline.save_report(json_path, format='json')

        # Markdown形式
        md_path = output_dir / f'validation_report_{timestamp}.md'
        pipeline.save_report(md_path, format='markdown')

    return summary

#!/usr/bin/env python3
# keibaai/scripts/run_quality_checks.py
"""
データ品質チェック統合実行スクリプト

スクレイピング、パース、特徴量、モデルの品質を包括的にチェックし、
詳細なレポートを自動生成する。
"""

import argparse
import logging
import sys
import yaml
from pathlib import Path
from datetime import datetime

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

try:
    from keibaai.src.pipeline_core import setup_logging
    from keibaai.src.modules.validation.validation_pipeline import ValidationPipeline
    from keibaai.src.modules.monitoring.monitoring_local import MonitoringSystem
    from keibaai.src.modules.monitoring.model_analyzer import ModelAnalyzer
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    print(f"プロジェクトルート: {project_root}")
    sys.exit(1)


def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(
        description='KeibaAI データ品質チェック統合実行スクリプト',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 全チェックを実行
  python keibaai/scripts/run_quality_checks.py --config keibaai/configs/default.yaml

  # バリデーションのみ実行
  python keibaai/scripts/run_quality_checks.py --config keibaai/configs/default.yaml --validation-only

  # モデル分析のみ実行（予測データが必要）
  python keibaai/scripts/run_quality_checks.py --config keibaai/configs/default.yaml \\
    --model-analysis-only \\
    --model-dir keibaai/data/models/latest \\
    --predictions keibaai/data/predictions/2023-10-01.parquet \\
    --actuals keibaai/data/parsed/parquet/races/races.parquet

  # 定期実行用（cron対応）
  0 3 * * * cd /path/to/KeibaAI_v2 && python keibaai/scripts/run_quality_checks.py --config keibaai/configs/default.yaml
        """
    )

    parser.add_argument(
        '--config',
        type=str,
        default='keibaai/configs/default.yaml',
        help='設定ファイルのパス (デフォルト: keibaai/configs/default.yaml)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='レポート出力先ディレクトリ (デフォルト: data/quality_reports/YYYY/MM/DD/)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='ログレベル (デフォルト: INFO)'
    )

    # チェック範囲の指定
    parser.add_argument(
        '--validation-only',
        action='store_true',
        help='バリデーションのみ実行'
    )

    parser.add_argument(
        '--monitoring-only',
        action='store_true',
        help='モニタリングのみ実行'
    )

    parser.add_argument(
        '--model-analysis-only',
        action='store_true',
        help='モデル分析のみ実行'
    )

    # モデル分析用のオプション
    parser.add_argument(
        '--model-dir',
        type=str,
        help='モデルディレクトリのパス（モデル分析時に必要）'
    )

    parser.add_argument(
        '--predictions',
        type=str,
        help='予測結果のParquetファイルパス（モデル分析時に必要）'
    )

    parser.add_argument(
        '--actuals',
        type=str,
        help='実績データのParquetファイルパス（モデル分析時に必要）'
    )

    # 日付範囲
    parser.add_argument(
        '--start-date',
        type=str,
        help='検証開始日 (YYYY-MM-DD形式)'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        help='検証終了日 (YYYY-MM-DD形式)'
    )

    args = parser.parse_args()

    # --- 0. 設定とロギングの初期化 ---
    try:
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = project_root / config_path

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        data_path = project_root / config.get('data_path', 'keibaai/data')

        # パスの正規化
        for key, value in config.items():
            if isinstance(value, str):
                config[key] = value.replace('${data_path}', str(data_path))
            if key.endswith('_path') and not Path(config[key]).is_absolute():
                config[key] = str(project_root / config[key])

        # ロギングの設定
        log_conf = config.get('logging', {})
        now = datetime.now()
        log_file_path = now.strftime(
            log_conf.get('log_file', 'keibaai/data/logs/{YYYY}/{MM}/{DD}/quality_check.log')
            .replace('{YYYY}', '%Y')
            .replace('{MM}', '%m')
            .replace('{DD}', '%d')
        )

        log_file_path = project_root / log_file_path
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        setup_logging(
            log_level=args.log_level,
            log_file=str(log_file_path),
            log_format=log_conf.get('format', '%(asctime)s - %(levelname)s - %(message)s')
        )

    except Exception as e:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [ERROR] %(message)s')
        logging.error(f"初期化に失敗しました: {e}", exc_info=True)
        sys.exit(1)

    logging.info("=" * 100)
    logging.info("KeibaAI データ品質チェック統合実行スクリプト開始")
    logging.info("=" * 100)
    logging.info(f"設定ファイル: {config_path}")
    logging.info(f"データパス: {data_path}")

    # 出力ディレクトリの設定
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = data_path / 'quality_reports' / now.strftime('%Y/%m/%d')

    output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"レポート出力先: {output_dir}")

    # チェック範囲の決定
    run_validation = not (args.monitoring_only or args.model_analysis_only) or args.validation_only
    run_monitoring = not (args.validation_only or args.model_analysis_only) or args.monitoring_only
    run_model_analysis = args.model_analysis_only

    # --- 1. バリデーション実行 ---
    if run_validation:
        logging.info("\n" + "=" * 100)
        logging.info("[Phase 1] データバリデーション")
        logging.info("=" * 100)

        try:
            validation_pipeline = ValidationPipeline(data_path, config)
            validation_summary = validation_pipeline.validate_all(
                start_date=args.start_date,
                end_date=args.end_date
            )

            # レポート保存
            timestamp = now.strftime('%Y%m%d_%H%M%S')
            validation_pipeline.save_report(
                output_dir / f'validation_report_{timestamp}.json',
                format='json'
            )
            validation_pipeline.save_report(
                output_dir / f'validation_report_{timestamp}.md',
                format='markdown'
            )

            logging.info(f"✓ バリデーション完了")
            logging.info(f"  - 合格: {validation_summary['passed']}")
            logging.info(f"  - 警告: {validation_summary['warnings']}")
            logging.info(f"  - 失敗: {validation_summary['failed']}")

        except Exception as e:
            logging.error(f"バリデーション実行中にエラー: {e}", exc_info=True)

    # --- 2. モニタリング実行 ---
    if run_monitoring:
        logging.info("\n" + "=" * 100)
        logging.info("[Phase 2] リアルタイムモニタリング")
        logging.info("=" * 100)

        try:
            monitoring_system = MonitoringSystem(data_path, config)

            # 過去のメトリクスを読み込み（存在する場合）
            metrics_path = data_path / 'metrics' / 'monitoring_metrics.json'
            if metrics_path.exists():
                monitoring_system.load_metrics(metrics_path)

            # スクレイピングメトリクスの追跡
            if args.start_date and args.end_date:
                monitoring_system.track_scraping_metrics(args.start_date, args.end_date)

            # パースメトリクスの追跡
            monitoring_system.track_parsing_metrics()

            # ベースラインの計算
            monitoring_system.calculate_baselines()

            # メトリクスの保存
            timestamp = now.strftime('%Y%m%d_%H%M%S')
            monitoring_system.save_metrics(output_dir / f'monitoring_metrics_{timestamp}.json')

            # サマリの表示
            summary = monitoring_system.get_summary()
            logging.info(f"✓ モニタリング完了")
            logging.info(f"  - 総メトリクス数: {summary['total_metrics']}")
            logging.info(f"  - 総アラート数: {summary['total_alerts']}")

            for severity, count in summary['alert_by_severity'].items():
                logging.info(f"    - {severity}: {count}")

        except Exception as e:
            logging.error(f"モニタリング実行中にエラー: {e}", exc_info=True)

    # --- 3. モデル分析実行 ---
    if run_model_analysis:
        logging.info("\n" + "=" * 100)
        logging.info("[Phase 3] モデル分析")
        logging.info("=" * 100)

        if not args.model_dir or not args.predictions or not args.actuals:
            logging.error("モデル分析には --model-dir, --predictions, --actuals が必要です")
        else:
            try:
                import pandas as pd

                model_dir = Path(args.model_dir)
                if not model_dir.is_absolute():
                    model_dir = project_root / model_dir

                predictions_path = Path(args.predictions)
                if not predictions_path.is_absolute():
                    predictions_path = project_root / predictions_path

                actuals_path = Path(args.actuals)
                if not actuals_path.is_absolute():
                    actuals_path = project_root / actuals_path

                # データの読み込み
                predictions_df = pd.read_parquet(predictions_path)
                actuals_df = pd.read_parquet(actuals_path)

                # モデル分析
                analyzer = ModelAnalyzer(model_dir, data_path)
                report = analyzer.analyze(predictions_df, actuals_df)

                # レポート保存
                timestamp = now.strftime('%Y%m%d_%H%M%S')
                analyzer.save_report(report, output_dir / f'model_analysis_{timestamp}.json')

                logging.info(f"✓ モデル分析完了")
                logging.info(f"  - 全体相関: {report.overall_metrics.get('spearman_mean', 'N/A'):.4f}")
                logging.info(f"  - 改善提案数: {len(report.recommendations)}")

            except Exception as e:
                logging.error(f"モデル分析実行中にエラー: {e}", exc_info=True)

    # --- 4. 統合レポートの生成 ---
    logging.info("\n" + "=" * 100)
    logging.info("[Phase 4] 統合レポート生成")
    logging.info("=" * 100)

    try:
        # 統合レポートのMarkdown生成
        integrated_report_path = output_dir / f'integrated_report_{now.strftime("%Y%m%d_%H%M%S")}.md'

        with open(integrated_report_path, 'w', encoding='utf-8') as f:
            f.write("# KeibaAI データ品質チェック統合レポート\n\n")
            f.write(f"**生成日時**: {now.isoformat()}\n\n")

            if run_validation:
                f.write("## 1. データバリデーション\n\n")
                f.write(f"- ✓ 合格: {validation_summary['passed']}\n")
                f.write(f"- ⚠ 警告: {validation_summary['warnings']}\n")
                f.write(f"- ✗ 失敗: {validation_summary['failed']}\n\n")
                f.write(f"詳細は `validation_report_{timestamp}.md` を参照してください。\n\n")

            if run_monitoring:
                f.write("## 2. リアルタイムモニタリング\n\n")
                f.write(f"- 総メトリクス数: {summary['total_metrics']}\n")
                f.write(f"- 総アラート数: {summary['total_alerts']}\n\n")
                f.write("### アラート内訳\n\n")
                for severity, count in summary['alert_by_severity'].items():
                    f.write(f"- {severity}: {count}\n")
                f.write(f"\n詳細は `monitoring_metrics_{timestamp}.json` を参照してください。\n\n")

            if run_model_analysis and 'report' in locals():
                f.write("## 3. モデル分析\n\n")
                f.write("### 全体的な評価指標\n\n")
                for metric, value in report.overall_metrics.items():
                    f.write(f"- **{metric}**: {value:.4f}\n")
                f.write("\n### 改善提案\n\n")
                for i, rec in enumerate(report.recommendations, 1):
                    f.write(f"{i}. {rec}\n")
                f.write(f"\n詳細は `model_analysis_{timestamp}.md` を参照してください。\n\n")

            f.write("---\n\n")
            f.write("このレポートは自動生成されました。\n")

        logging.info(f"✓ 統合レポート生成完了: {integrated_report_path}")

    except Exception as e:
        logging.error(f"統合レポート生成中にエラー: {e}", exc_info=True)

    # --- 5. 終了処理 ---
    logging.info("\n" + "=" * 100)
    logging.info("KeibaAI データ品質チェック統合実行スクリプト完了")
    logging.info("=" * 100)
    logging.info(f"すべてのレポートは {output_dir} に保存されました")

    # 重大なエラーがあった場合は終了コード1を返す
    if run_validation and validation_summary.get('failed', 0) > 0:
        logging.warning("⚠ バリデーションで失敗が検出されました")
        sys.exit(1)


if __name__ == '__main__':
    main()

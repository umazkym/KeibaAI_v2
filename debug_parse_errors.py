#!/usr/bin/env python3
"""
パースエラーの詳細分析スクリプト
エラーが発生したレースファイルを詳細に調査
"""

import sys
from pathlib import Path
import logging
from collections import defaultdict
from datetime import datetime

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('debug_parse_errors.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / 'keibaai'))

from keibaai.src.modules.parsers import results_parser
from bs4 import BeautifulSoup


def analyze_race_file(file_path: Path) -> dict:
    """レースファイルを詳細に分析"""
    analysis = {
        'file_name': file_path.name,
        'file_size': file_path.stat().st_size,
        'race_id': file_path.stem,
        'encoding': None,
        'html_structure': {},
        'errors': [],
        'warnings': []
    }

    try:
        # ファイルを読み込み
        with open(file_path, 'rb') as f:
            html_bytes = f.read()

        # エンコーディングを試行
        for encoding in ['euc_jp', 'utf-8', 'shift_jis']:
            try:
                html_text = html_bytes.decode(encoding, errors='replace')
                analysis['encoding'] = encoding
                break
            except Exception:
                continue

        if not analysis['encoding']:
            analysis['errors'].append("エンコーディングの検出に失敗")
            return analysis

        # HTML解析
        soup = BeautifulSoup(html_text, "html.parser")

        # タイトル
        title = soup.find('title')
        analysis['html_structure']['title'] = title.text if title else None

        # エラーページのチェック
        if "404" in html_text or "Not Found" in html_text:
            analysis['errors'].append("404 エラーページ")
        if "レース情報が見つかりません" in html_text:
            analysis['errors'].append("レース情報が見つかりません")
        if "該当するデータがありません" in html_text:
            analysis['errors'].append("該当するデータがありません")

        # レース結果テーブルの確認
        result_table = soup.find('table', class_='race_table_01')
        analysis['html_structure']['race_table_01'] = result_table is not None

        if not result_table:
            # 代替パターンを探す
            all_tables = soup.find_all('table')
            analysis['html_structure']['total_tables'] = len(all_tables)
            analysis['html_structure']['table_classes'] = [
                table.get('class', []) for table in all_tables[:5]
            ]

        # レースデータの確認
        race_data = soup.find('div', class_='data_intro')
        analysis['html_structure']['data_intro'] = race_data is not None

        if not race_data:
            race_data = soup.find('diary', class_='diary_snap_cut')
            analysis['html_structure']['diary_snap_cut'] = race_data is not None

        # メタデータの確認
        meta_data = soup.find('dl', class_='racedata')
        analysis['html_structure']['racedata'] = meta_data is not None

        # パース試行
        try:
            df = results_parser.parse_race_results(file_path)
            if df.empty:
                analysis['warnings'].append("パース結果が空のDataFrame")
            else:
                analysis['html_structure']['parsed_rows'] = len(df)
        except Exception as e:
            analysis['errors'].append(f"パースエラー: {str(e)}")

    except Exception as e:
        analysis['errors'].append(f"ファイル読み込みエラー: {str(e)}")

    return analysis


def main():
    """パースエラーの分析を実行"""
    logger.info("=" * 80)
    logger.info("KeibaAI_v2 パースエラー詳細分析")
    logger.info(f"実行日時: {datetime.now()}")
    logger.info("=" * 80)

    # レースディレクトリ
    race_dir = Path("keibaai/data/raw/html/race")

    if not race_dir.exists():
        logger.error(f"❌ レースディレクトリが存在しません: {race_dir}")
        return

    # 全レースファイルを取得
    all_race_files = sorted(race_dir.glob("*.bin"))
    logger.info(f"総レースファイル数: {len(all_race_files)}")

    # 年別に分類
    files_by_year = defaultdict(list)
    for file_path in all_race_files:
        year = file_path.stem[:4]
        files_by_year[year].append(file_path)

    logger.info("\n年別ファイル数:")
    for year in sorted(files_by_year.keys()):
        logger.info(f"  {year}年: {len(files_by_year[year])} ファイル")

    # 2025年のファイルを重点的に分析
    if '2025' in files_by_year:
        logger.info("\n" + "=" * 80)
        logger.info("2025年のレースファイルを詳細分析")
        logger.info("=" * 80)

        files_2025 = files_by_year['2025']
        logger.info(f"分析対象: {len(files_2025)} ファイル")

        # 最初の10ファイルを詳細分析
        sample_size = min(10, len(files_2025))
        logger.info(f"サンプル: 最初の {sample_size} ファイル")

        error_summary = defaultdict(int)
        warning_summary = defaultdict(int)

        for i, file_path in enumerate(files_2025[:sample_size], 1):
            logger.info(f"\n--- ファイル {i}/{sample_size}: {file_path.name} ---")

            analysis = analyze_race_file(file_path)

            logger.info(f"ファイルサイズ: {analysis['file_size']} bytes")
            logger.info(f"エンコーディング: {analysis['encoding']}")
            logger.info(f"タイトル: {analysis['html_structure'].get('title', 'N/A')}")

            # HTML構造
            logger.info("HTML構造:")
            for key, value in analysis['html_structure'].items():
                if key != 'title':
                    logger.info(f"  {key}: {value}")

            # エラー
            if analysis['errors']:
                logger.warning(f"エラー ({len(analysis['errors'])}件):")
                for error in analysis['errors']:
                    logger.warning(f"  ❌ {error}")
                    error_summary[error] += 1
            else:
                logger.info("✅ エラーなし")

            # 警告
            if analysis['warnings']:
                logger.info(f"警告 ({len(analysis['warnings'])}件):")
                for warning in analysis['warnings']:
                    logger.info(f"  ⚠️ {warning}")
                    warning_summary[warning] += 1

        # サマリー
        logger.info("\n" + "=" * 80)
        logger.info("エラーサマリー (2025年サンプル)")
        logger.info("=" * 80)

        if error_summary:
            for error, count in sorted(error_summary.items(), key=lambda x: -x[1]):
                logger.info(f"  {count}/{sample_size} ファイル: {error}")
        else:
            logger.info("  エラーなし")

        if warning_summary:
            logger.info("\n警告サマリー:")
            for warning, count in sorted(warning_summary.items(), key=lambda x: -x[1]):
                logger.info(f"  {count}/{sample_size} ファイル: {warning}")

    # 全年度のランダムサンプル分析
    logger.info("\n" + "=" * 80)
    logger.info("全年度のランダムサンプル分析")
    logger.info("=" * 80)

    import random
    random.seed(42)

    # 各年から2ファイルずつサンプリング
    sample_files = []
    for year in sorted(files_by_year.keys())[:5]:  # 最初の5年
        files = files_by_year[year]
        sample_files.extend(random.sample(files, min(2, len(files))))

    logger.info(f"サンプル数: {len(sample_files)} ファイル")

    success_count = 0
    error_count = 0

    for file_path in sample_files:
        analysis = analyze_race_file(file_path)
        if not analysis['errors']:
            success_count += 1
        else:
            error_count += 1

    logger.info(f"\n結果:")
    logger.info(f"  ✅ 成功: {success_count}/{len(sample_files)} ({success_count/len(sample_files)*100:.1f}%)")
    logger.info(f"  ❌ エラー: {error_count}/{len(sample_files)} ({error_count/len(sample_files)*100:.1f}%)")

    logger.info("\n" + "=" * 80)
    logger.info("分析完了")
    logger.info("詳細ログ: debug_parse_errors.log")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()

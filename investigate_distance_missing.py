#!/usr/bin/env python3
"""
distance_m欠損の原因を調査
"""

import sys
import logging
from pathlib import Path

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('investigate_distance_missing.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / 'keibaai'))

from bs4 import BeautifulSoup


def investigate_metadata_extraction(race_id: str):
    """distance_mメタデータ抽出の詳細調査"""
    logger.info("=" * 80)
    logger.info(f"レースID: {race_id} のメタデータ抽出調査")
    logger.info("=" * 80)

    file_path = Path(f"keibaai/data/raw/html/race/{race_id}.bin")

    if not file_path.exists():
        logger.error(f"❌ ファイルが存在しません: {file_path}")
        return

    try:
        with open(file_path, 'rb') as f:
            html_bytes = f.read()

        html_text = html_bytes.decode('euc_jp', errors='replace')
        soup = BeautifulSoup(html_text, "html.parser")

        # メタデータ抽出の各パターンを試行
        logger.info("\n1. data_intro の確認:")
        data_intro = soup.find('div', class_='data_intro')
        if data_intro:
            logger.info(f"  ✅ data_intro 発見")
            logger.info(f"  内容: {data_intro.get_text()[:200]}")

            # diary_snap内の確認
            diary_snap = data_intro.find('p', class_='diary_snap')
            if diary_snap:
                logger.info(f"\n  ✅ diary_snap 発見")
                logger.info(f"  テキスト: {diary_snap.get_text()}")

                # 距離抽出のテスト
                import re
                distance_match = re.search(r'(\d+)m', diary_snap.get_text())
                if distance_match:
                    logger.info(f"  ✅ 距離抽出成功: {distance_match.group(1)}m")
                else:
                    logger.warning(f"  ❌ 距離抽出失敗")
            else:
                logger.warning(f"  ❌ diary_snap が見つかりません")
        else:
            logger.warning("  ❌ data_intro が見つかりません")

        logger.info("\n2. diary_snap_cut の確認:")
        diary_snap_cut = soup.find('div', class_='diary_snap_cut')
        if diary_snap_cut:
            logger.info(f"  ✅ diary_snap_cut 発見")
            logger.info(f"  内容: {diary_snap_cut.get_text()[:200]}")
        else:
            logger.warning("  ❌ diary_snap_cut が見つかりません")

        logger.info("\n3. racedata の確認:")
        racedata = soup.find('dl', class_='racedata')
        if racedata:
            logger.info(f"  ✅ racedata 発見")

            # dd要素の確認
            dd_elements = racedata.find_all('dd')
            logger.info(f"  dd要素の数: {len(dd_elements)}")

            for i, dd in enumerate(dd_elements, 1):
                logger.info(f"\n  dd[{i}]: {dd.get_text().strip()[:100]}")

                # 距離抽出のテスト
                import re
                distance_match = re.search(r'(\d+)m', dd.get_text())
                if distance_match:
                    logger.info(f"    → 距離候補: {distance_match.group(1)}m")
        else:
            logger.warning("  ❌ racedata が見つかりません")

        logger.info("\n4. span.racedata01 の確認:")
        racedata01 = soup.find('span', class_='racedata01')
        if racedata01:
            logger.info(f"  ✅ racedata01 発見")
            logger.info(f"  内容: {racedata01.get_text()[:200]}")
        else:
            logger.warning("  ❌ racedata01 が見つかりません")

        logger.info("\n5. HTMLの全体構造:")
        # すべてのdivのクラスを列挙
        divs = soup.find_all('div', class_=True)
        div_classes = set()
        for div in divs:
            classes = div.get('class', [])
            div_classes.update(classes)

        logger.info(f"  ページ内のdivクラス (一部):")
        for cls in sorted(list(div_classes))[:20]:
            logger.info(f"    - {cls}")

        logger.info("\n6. 距離情報の全文検索:")
        # HTML全体から距離情報を探す
        import re
        distance_patterns = [
            r'(\d+)m',
            r'距離[：:]\s*(\d+)',
            r'(\d{4})メートル',
        ]

        for pattern in distance_patterns:
            matches = re.finditer(pattern, html_text)
            found = [match.group() for match in list(matches)[:5]]
            if found:
                logger.info(f"  パターン '{pattern}': {found}")

    except Exception as e:
        logger.error(f"❌ 調査中にエラー: {str(e)}", exc_info=True)

    logger.info("\n")


def main():
    """distance_m欠損ファイルを調査"""
    logger.info("distance_m欠損原因の調査")
    logger.info("=" * 80)

    # 欠損が確認されたファイル
    missing_files = [
        "202109060502",  # 2021年9月 阪神
        "202304010208",  # 2023年4月 中山
    ]

    for race_id in missing_files:
        investigate_metadata_extraction(race_id)

    logger.info("=" * 80)
    logger.info("調査完了")
    logger.info("詳細ログ: investigate_distance_missing.log")


if __name__ == "__main__":
    main()

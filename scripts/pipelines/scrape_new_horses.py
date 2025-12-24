#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新規馬IDのみを対象にした馬情報・血統スクレイピング
horse_id_list_new.txt から馬IDを読み込み、既存データをスキップして取得
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from keibaai.src.preparing import _scrape_html
from keibaai.src.constants import LocalPaths


def setup_logging():
    """ログ設定"""
    log_dir = project_root / "keibaai" / "data" / "logs" / datetime.now().strftime("%Y/%m/%d")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "scrape_new_horses.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def load_new_horse_ids(filepath: Path) -> list:
    """新規馬IDリストを読み込む"""
    if not filepath.exists():
        raise FileNotFoundError(f"馬IDリストが見つかりません: {filepath}")
    
    with open(filepath, 'r') as f:
        horse_ids = [line.strip() for line in f if line.strip()]
    
    return horse_ids


def get_existing_horse_ids() -> set:
    """既存の馬プロフィールファイルからIDを取得"""
    horse_dir = Path(LocalPaths.HTML_HORSE_DIR)
    if not horse_dir.exists():
        return set()
    
    existing_ids = set()
    for f in horse_dir.glob('*_profile.bin'):
        existing_ids.add(f.stem.replace('_profile', ''))
    
    return existing_ids


def get_existing_pedigree_ids() -> set:
    """既存の血統ファイルからIDを取得"""
    ped_dir = Path(LocalPaths.HTML_PED_DIR)
    if not ped_dir.exists():
        return set()
    
    existing_ids = set()
    for f in ped_dir.glob('*.bin'):
        existing_ids.add(f.stem)
    
    return existing_ids


def main():
    log = setup_logging()
    
    log.info("=" * 80)
    log.info("新規馬データスクレイピングを開始します")
    log.info("=" * 80)
    
    # 新規馬IDを読み込む
    horse_id_file = project_root / "horse_id_list_new.txt"
    horse_ids = load_new_horse_ids(horse_id_file)
    log.info(f"読み込んだ馬ID: {len(horse_ids)}頭")
    
    # 既存データを確認
    existing_horse_ids = get_existing_horse_ids()
    existing_ped_ids = get_existing_pedigree_ids()
    
    log.info(f"既存馬プロフィール: {len(existing_horse_ids)}頭")
    log.info(f"既存血統データ: {len(existing_ped_ids)}頭")
    
    # 新規馬のみフィルタ（念のため再チェック）
    new_horse_ids = [hid for hid in horse_ids if hid not in existing_horse_ids]
    new_ped_ids = [hid for hid in horse_ids if hid not in existing_ped_ids]
    
    log.info(f"新規馬プロフィール取得対象: {len(new_horse_ids)}頭")
    log.info(f"新規血統取得対象: {len(new_ped_ids)}頭")
    
    # フェーズ1: 馬プロフィール・成績を取得
    if new_horse_ids:
        log.info("\n" + "=" * 80)
        log.info("【フェーズ1/2】馬プロフィール・成績HTMLのスクレイピング")
        log.info("=" * 80)
        
        horse_paths = _scrape_html.scrape_html_horse(new_horse_ids, skip=True)
        log.info(f"  ✅ {len(horse_paths)}件の馬データを取得しました")
    else:
        log.info("馬プロフィールは全て取得済みです")
    
    # フェーズ2: 血統を取得
    if new_ped_ids:
        log.info("\n" + "=" * 80)
        log.info("【フェーズ2/2】血統HTMLのスクレイピング")
        log.info("=" * 80)
        
        ped_paths = _scrape_html.scrape_html_ped(new_ped_ids, skip=True)
        log.info(f"  ✅ {len(ped_paths)}件の血統データを取得しました")
    else:
        log.info("血統データは全て取得済みです")
    
    log.info("\n" + "=" * 80)
    log.info("✅ 新規馬データスクレイピングが完了しました")
    log.info("=" * 80)


if __name__ == "__main__":
    main()

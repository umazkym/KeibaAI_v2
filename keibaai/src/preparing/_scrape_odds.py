# -*- coding: utf-8 -*-
"""
オッズスクレイピング関数群（JSON形式で保存）
Netkeibaのオッズページから全券種のオッズを取得

対応券種:
- 単勝・複勝 (b1)
- 馬連 (b4)
- ワイド (b5)
- 馬単 (b6)
- 三連複 (b7)
- 三連単 (b8)
"""
import os
import re
import time
import random
import logging
import json
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from tqdm import tqdm

from ._scrape_html import prepare_chrome_driver, MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS

# ログ設定
logger = logging.getLogger(__name__)

# オッズページのURL構造
ODDS_BASE_URL = "https://race.netkeiba.com/odds/index.html"

# 券種コード
BET_TYPES = {
    'win_place': 'b1',      # 単勝・複勝
    'quinella': 'b4',       # 馬連
    'quinella_place': 'b5', # ワイド
    'exacta': 'b6',         # 馬単
    'trio': 'b7',           # 三連複
    'trifecta': 'b8',       # 三連単
}

# 保存ディレクトリ
ODDS_DATA_DIR = Path("keibaai/data/parsed/odds")


def scrape_odds_win_place(driver: webdriver.Chrome, race_id: str) -> Dict[str, Any]:
    """
    単勝・複勝オッズを取得
    
    Returns:
        {
            'race_id': str,
            'bet_type': 'win_place',
            'data': [{'horse_number': int, 'win_odds': float, 'place_odds_min': float, 'place_odds_max': float}, ...]
        }
    """
    url = f"{ODDS_BASE_URL}?type=b1&race_id={race_id}&rf=shutuba_submenu"
    logger.info(f"単勝・複勝オッズを取得: {race_id}")
    
    time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))
    driver.get(url)
    
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.RaceOdds_HorseList_Table span.Odds"))
        )
        time.sleep(1)  # 追加の待機（動的コンテンツ読み込み用）
    except TimeoutException:
        logger.warning(f"ページロードタイムアウト: {url}")
        return {'race_id': race_id, 'bet_type': 'win_place', 'data': []}
    
    # JavaScript（単勝ページ: 2つのテーブルが並んでいる構造）
    script = """
    return (() => {
        const data = [];
        // 単勝テーブル（1番目のテーブル）から取得
        const tables = document.querySelectorAll('.RaceOdds_HorseList_Table');
        if (tables.length === 0) return data;
        
        const winTable = tables[0]; // 単勝テーブル
        const placeTable = tables.length > 1 ? tables[1] : null; // 複勝テーブル
        
        const rows = winTable.querySelectorAll('tr');
        rows.forEach(tr => {
            // 馬番を取得（2番目のtd）
            const cells = tr.querySelectorAll('td');
            if (cells.length < 6) return;
            
            // cells[0]=枠, cells[1]=馬番, cells[4]=馬名, cells[5]=オッズ
            const horseNumber = parseInt(cells[1].innerText.trim());
            if (isNaN(horseNumber) || horseNumber < 1 || horseNumber > 18) return;
            
            // 単勝オッズ（spanの中）
            const oddsSpan = cells[5].querySelector('span');
            const winOdds = oddsSpan ? parseFloat(oddsSpan.innerText.trim().replace(/,/g, '')) : null;
            
            if (winOdds === null || isNaN(winOdds)) return;
            
            // 複勝オッズを取得（別テーブルから同じ馬番を探す）
            let placeOddsMin = null, placeOddsMax = null;
            if (placeTable) {
                const placeRows = placeTable.querySelectorAll('tr');
                placeRows.forEach(ptr => {
                    const pCells = ptr.querySelectorAll('td');
                    if (pCells.length < 6) return;
                    const pHorseNum = parseInt(pCells[1].innerText.trim());
                    if (pHorseNum === horseNumber) {
                        const pOddsSpan = pCells[5].querySelector('span');
                        if (pOddsSpan) {
                            const text = pOddsSpan.innerText.trim();
                            const match = text.match(/([\\d.]+)\\s*-\\s*([\\d.]+)/);
                            if (match) {
                                placeOddsMin = parseFloat(match[1]);
                                placeOddsMax = parseFloat(match[2]);
                            }
                        }
                    }
                });
            }
            
            data.push({
                horse_number: horseNumber,
                win_odds: winOdds,
                place_odds_min: placeOddsMin,
                place_odds_max: placeOddsMax
            });
        });
        return data.sort((a, b) => a.horse_number - b.horse_number);
    })()
    """
    
    try:
        data = driver.execute_script(script)
        return {'race_id': race_id, 'bet_type': 'win_place', 'data': data, 'fetched_at': datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"データ抽出エラー (win_place): {e}")
        return {'race_id': race_id, 'bet_type': 'win_place', 'data': [], 'error': str(e)}


def scrape_odds_matrix(driver: webdriver.Chrome, race_id: str, bet_type: str) -> Dict[str, Any]:
    """
    マトリクス形式のオッズを取得（馬連/ワイド/馬単）
    
    Args:
        bet_type: 'quinella', 'quinella_place', 'exacta'
    """
    type_code = BET_TYPES[bet_type]
    url = f"{ODDS_BASE_URL}?type={type_code}&race_id={race_id}&housiki=c0&rf=shutuba_submenu"
    logger.info(f"{bet_type}オッズを取得: {race_id}")
    
    time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))
    driver.get(url)
    
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "td[id^='check_']"))
        )
    except TimeoutException:
        logger.warning(f"ページロードタイムアウト: {url}")
        return {'race_id': race_id, 'bet_type': bet_type, 'data': []}
    
    # JavaScriptでマトリクスデータ抽出
    script = """
    return (() => {
        const data = [];
        const cells = document.querySelectorAll('td[id^="check_"]');
        
        cells.forEach(td => {
            const id = td.id;
            // ID形式: check_b4_c0_H1_H2 (馬連等)
            const parts = id.split('_');
            if (parts.length >= 2) {
                const h2 = parseInt(parts.pop());
                const h1 = parseInt(parts.pop());
                
                const span = td.querySelector('span');
                if (span) {
                    const oddsText = span.innerText.trim().replace(/,/g, '');
                    const odds = parseFloat(oddsText);
                    if (!isNaN(odds)) {
                        data.push({ horse1: h1, horse2: h2, odds: odds });
                    }
                }
            }
        });
        
        data.sort((a, b) => (a.horse1 - b.horse1) || (a.horse2 - b.horse2));
        return data;
    })()
    """
    
    try:
        data = driver.execute_script(script)
        return {'race_id': race_id, 'bet_type': bet_type, 'data': data, 'fetched_at': datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"データ抽出エラー ({bet_type}): {e}")
        return {'race_id': race_id, 'bet_type': bet_type, 'data': [], 'error': str(e)}


def scrape_odds_popularity(driver: webdriver.Chrome, race_id: str, bet_type: str) -> Dict[str, Any]:
    """
    人気順リスト形式のオッズを取得（三連複/三連単）
    ページングを行い全組み合わせを取得
    
    Args:
        bet_type: 'trio', 'trifecta'
    """
    type_code = BET_TYPES[bet_type]
    url = f"{ODDS_BASE_URL}?type={type_code}&race_id={race_id}&housiki=c99&rf=shutuba_submenu"
    logger.info(f"{bet_type}オッズを取得 (人気順): {race_id}")
    
    time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))
    driver.get(url)
    
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "RaceOdds_HorseList_Table"))
        )
    except TimeoutException:
        logger.warning(f"ページロードタイムアウト: {url}")
        return {'race_id': race_id, 'bet_type': bet_type, 'data': []}
    
    all_data = []
    
    # ドロップダウンのオプションを取得
    try:
        select_elem = driver.find_element(By.ID, "ninki_select")
        options = [opt.get_attribute("value") for opt in select_elem.find_elements(By.TAG_NAME, "option")]
    except NoSuchElementException:
        options = ["0"]  # ドロップダウンがない場合は1ページのみ
    
    # 各ページからデータを抽出
    for page_value in options:
        if page_value != "0":
            # ページ切り替え
            try:
                select = Select(driver.find_element(By.ID, "ninki_select"))
                select.select_by_value(page_value)
                time.sleep(1.5)  # ページ読み込み待機
            except Exception as e:
                logger.warning(f"ページ切り替えエラー: {e}")
                continue
        
        # データ抽出（人気順リスト - 正確なセル位置を使用）
        script = """
        return (() => {
            const data = [];
            // テーブルの行を全て取得
            const table = document.querySelector('table.RaceOdds_HorseList_Table');
            if (!table) return data;
            
            const rows = table.querySelectorAll('tr');
            rows.forEach(tr => {
                const cells = tr.querySelectorAll('td');
                if (cells.length < 4) return;
                
                // 最初のセルが人気順（数字）かチェック
                const rankSpan = cells[0].querySelector('span');
                const rankText = rankSpan ? rankSpan.innerText.trim() : cells[0].innerText.trim();
                const rank = parseInt(rankText);
                if (isNaN(rank) || rank < 1) return;
                
                // 組み合わせを取得（div.Combi01内のspan.UmaBan span）
                const combiDiv = cells[2].querySelector('div.Combi01');
                if (!combiDiv) return;
                
                const umaBanSpans = combiDiv.querySelectorAll('span.UmaBan span');
                const nums = Array.from(umaBanSpans).map(s => parseInt(s.innerText.trim())).filter(n => !isNaN(n) && n >= 1);
                
                if (nums.length < 3) return;
                
                // オッズを取得（cells[3]内のspan）
                const oddsSpan = cells[3].querySelector('span');
                const oddsText = oddsSpan ? oddsSpan.innerText.trim().replace(/,/g, '') : cells[3].innerText.trim().replace(/,/g, '');
                const odds = parseFloat(oddsText);
                
                if (!isNaN(odds)) {
                    data.push({
                        rank: rank,
                        horse1: nums[0],
                        horse2: nums[1],
                        horse3: nums[2],
                        odds: odds
                    });
                }
            });
            return data;
        })()
        """
        
        try:
            page_data = driver.execute_script(script)
            all_data.extend(page_data)
        except Exception as e:
            logger.warning(f"データ抽出エラー (ページ {page_value}): {e}")
    
    # 重複除去（ランクでユニーク化）
    unique_data = {item['rank']: item for item in all_data}
    sorted_data = sorted(unique_data.values(), key=lambda x: x['rank'])
    
    return {
        'race_id': race_id, 
        'bet_type': bet_type, 
        'data': sorted_data, 
        'fetched_at': datetime.now().isoformat(),
        'total_combinations': len(sorted_data)
    }


def scrape_all_odds(race_id: str, save: bool = True, output_dir: Optional[Path] = None) -> Dict[str, Dict]:
    """
    指定レースの全券種オッズを取得
    
    Args:
        race_id: レースID
        save: JSONファイルに保存するか
        output_dir: 出力ディレクトリ（Noneの場合はデフォルト）
    
    Returns:
        {
            'win_place': {...},
            'quinella': {...},
            'quinella_place': {...},
            'exacta': {...},
            'trio': {...},
            'trifecta': {...}
        }
    """
    logger.info(f"=== 全券種オッズ取得開始: {race_id} ===")
    
    if output_dir is None:
        output_dir = ODDS_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = {}
    driver = None
    
    try:
        driver = prepare_chrome_driver(headless=True)
        
        # 1. 単勝・複勝
        results['win_place'] = scrape_odds_win_place(driver, race_id)
        
        # 2. 馬連
        results['quinella'] = scrape_odds_matrix(driver, race_id, 'quinella')
        
        # 3. ワイド
        results['quinella_place'] = scrape_odds_matrix(driver, race_id, 'quinella_place')
        
        # 4. 馬単
        results['exacta'] = scrape_odds_matrix(driver, race_id, 'exacta')
        
        # 5. 三連複
        results['trio'] = scrape_odds_popularity(driver, race_id, 'trio')
        
        # 6. 三連単
        results['trifecta'] = scrape_odds_popularity(driver, race_id, 'trifecta')
        
    except Exception as e:
        logger.error(f"オッズ取得中にエラー: {e}")
        results['error'] = str(e)
        
    finally:
        if driver:
            driver.quit()
    
    # ファイル保存
    if save:
        output_file = output_dir / f"{race_id}_odds.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"オッズデータを保存: {output_file}")
    
    # サマリー出力
    logger.info("=== 取得結果サマリー ===")
    for bet_type, data in results.items():
        if bet_type == 'error':
            continue
        count = len(data.get('data', []))
        logger.info(f"  {bet_type}: {count}件")
    
    return results


def scrape_odds_batch(race_id_list: List[str], skip: bool = True, output_dir: Optional[Path] = None) -> List[str]:
    """
    複数レースのオッズを一括取得
    
    Args:
        race_id_list: レースIDリスト
        skip: 既存ファイルをスキップするか
        output_dir: 出力ディレクトリ
    
    Returns:
        保存したファイルパスのリスト
    """
    logger.info(f"オッズ一括取得開始: {len(race_id_list)}レース")
    
    if output_dir is None:
        output_dir = ODDS_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    
    saved_files = []
    
    for race_id in tqdm(race_id_list, desc="オッズ取得", unit="レース"):
        output_file = output_dir / f"{race_id}_odds.json"
        
        if skip and output_file.exists():
            logger.debug(f"スキップ: {race_id} (既存)")
            continue
        
        try:
            scrape_all_odds(race_id, save=True, output_dir=output_dir)
            saved_files.append(str(output_file))
        except Exception as e:
            logger.error(f"レース {race_id} のオッズ取得失敗: {e}")
    
    return saved_files


def scrape_odds_by_date(
    kaisai_date: str, 
    skip: bool = True, 
    output_dir: Optional[Path] = None,
    exclude_trifecta: bool = False
) -> Dict[str, Any]:
    """
    開催日を指定して全レースのオッズを取得
    
    Args:
        kaisai_date: 開催日（YYYYMMDD形式、例: 20251228）
        skip: 既存ファイルをスキップするか
        output_dir: 出力ディレクトリ
        exclude_trifecta: 三連単を除外（時間短縮のため）
    
    Returns:
        {
            'kaisai_date': str,
            'race_count': int,
            'saved_files': List[str],
            'errors': List[str]
        }
    """
    from ._scrape_html import scrape_race_id_list
    
    logger.info(f"=== 開催日 {kaisai_date} の全レースオッズ取得開始 ===")
    
    # 1. レースIDリストを取得
    race_id_list = scrape_race_id_list([kaisai_date])
    logger.info(f"レースID取得完了: {len(race_id_list)}レース")
    
    if not race_id_list:
        logger.warning(f"レースIDが見つかりません: {kaisai_date}")
        return {
            'kaisai_date': kaisai_date,
            'race_count': 0,
            'saved_files': [],
            'errors': ['レースIDが見つかりません']
        }
    
    if output_dir is None:
        output_dir = ODDS_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 2. 各レースのオッズを取得
    saved_files = []
    errors = []
    driver = None
    
    try:
        driver = prepare_chrome_driver(headless=True)
        
        for race_id in tqdm(race_id_list, desc=f"オッズ取得 ({kaisai_date})", unit="レース"):
            output_file = output_dir / f"{race_id}_odds.json"
            
            if skip and output_file.exists():
                logger.debug(f"スキップ: {race_id} (既存)")
                saved_files.append(str(output_file))
                continue
            
            try:
                results = {}
                
                # 1. 単勝・複勝
                results['win_place'] = scrape_odds_win_place(driver, race_id)
                
                # 2. 馬連
                results['quinella'] = scrape_odds_matrix(driver, race_id, 'quinella')
                
                # 3. ワイド
                results['quinella_place'] = scrape_odds_matrix(driver, race_id, 'quinella_place')
                
                # 4. 馬単
                results['exacta'] = scrape_odds_matrix(driver, race_id, 'exacta')
                
                # 5. 三連複
                results['trio'] = scrape_odds_popularity(driver, race_id, 'trio')
                
                # 6. 三連単（オプション）
                if not exclude_trifecta:
                    results['trifecta'] = scrape_odds_popularity(driver, race_id, 'trifecta')
                
                # 保存
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                saved_files.append(str(output_file))
                logger.info(f"保存: {output_file}")
                
            except Exception as e:
                logger.error(f"レース {race_id} のオッズ取得失敗: {e}")
                errors.append(f"{race_id}: {str(e)}")
                
    finally:
        if driver:
            driver.quit()
    
    logger.info(f"=== 完了: {len(saved_files)}/{len(race_id_list)}レース ===")
    
    return {
        'kaisai_date': kaisai_date,
        'race_count': len(race_id_list),
        'saved_files': saved_files,
        'errors': errors
    }


# CLI用エントリーポイント
if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    parser = argparse.ArgumentParser(description="Netkeibaオッズスクレイパー")
    subparsers = parser.add_subparsers(dest='command', help='コマンド')
    
    # 単一レースモード
    race_parser = subparsers.add_parser('race', help='単一レースのオッズを取得')
    race_parser.add_argument("race_id", help="レースID (例: 202506050810)")
    race_parser.add_argument("--output", "-o", help="出力ディレクトリ", default=None)
    
    # 日付指定モード
    date_parser = subparsers.add_parser('date', help='開催日の全レースオッズを取得')
    date_parser.add_argument("kaisai_date", help="開催日 (YYYYMMDD形式、例: 20251228)")
    date_parser.add_argument("--output", "-o", help="出力ディレクトリ", default=None)
    date_parser.add_argument("--no-skip", action="store_true", help="既存ファイルを再取得")
    date_parser.add_argument("--exclude-trifecta", action="store_true", help="三連単を除外（高速化）")
    
    args = parser.parse_args()
    
    if args.command == 'race':
        output_dir = Path(args.output) if args.output else None
        results = scrape_all_odds(args.race_id, save=True, output_dir=output_dir)
        
        print("\n=== 取得完了 ===")
        for bet_type, data in results.items():
            if bet_type == 'error':
                continue
            print(f"  {bet_type}: {len(data.get('data', []))}件")
            
    elif args.command == 'date':
        output_dir = Path(args.output) if args.output else None
        result = scrape_odds_by_date(
            args.kaisai_date, 
            skip=not args.no_skip,
            output_dir=output_dir,
            exclude_trifecta=args.exclude_trifecta
        )
        
        print(f"\n=== 取得完了 ===")
        print(f"開催日: {result['kaisai_date']}")
        print(f"レース数: {result['race_count']}")
        print(f"保存ファイル数: {len(result['saved_files'])}")
        if result['errors']:
            print(f"エラー数: {len(result['errors'])}")
            
    else:
        parser.print_help()


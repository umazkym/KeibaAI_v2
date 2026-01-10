# -*- coding: utf-8 -*-
"""
オッズHTML取得モジュール（Selenium統一方式）

全券種をSeleniumで取得（オッズはJavaScriptで動的ロードされるため）
既存パイプライン同様、HTMLを.binで保存し、後でパース処理を行う。
"""
import time
import random
import logging
import re
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from tqdm import tqdm

from ._scrape_html import prepare_chrome_driver

logger = logging.getLogger(__name__)

# オッズページのURL
ODDS_BASE_URL = "https://race.netkeiba.com/odds/index.html"

# 券種コード（日本語風ディレクトリ名）
BET_TYPES = {
    'tansho_fukusho': 'b1',  # 単勝・複勝
    'umaren': 'b4',          # 馬連
    'wide': 'b5',            # ワイド
    'umatan': 'b6',          # 馬単
    'sanrenpuku': 'b7',      # 三連複
    'sanrentan': 'b8',       # 三連単
}

# スリープ設定 (既存スクレイピングモジュールと統一: 1.5-3.0秒)
MIN_SLEEP = 1.5
MAX_SLEEP = 3.0


def detect_ban(driver) -> Optional[str]:
    """
    BAN状態を検出
    
    Returns:
        None: 正常
        'CAPTCHA': CAPTCHA表示
        'RATE_LIMIT': アクセス制限
        'FORBIDDEN': 403エラー
        'NO_CONTENT': コンテンツなし
    """
    try:
        page_source = driver.page_source.lower()
        
        # CAPTCHA検出
        if 'captcha' in page_source or 'recaptcha' in page_source:
            return 'CAPTCHA'
        
        # アクセス制限メッセージ
        if 'アクセスが集中' in driver.page_source or 'しばらくお待ち' in driver.page_source:
            return 'RATE_LIMIT'
        
        # 403検出（タイトルやボディに含まれる場合）
        if '403' in driver.title or 'forbidden' in page_source:
            return 'FORBIDDEN'
        
        # コンテンツ欠落（オッズテーブルがない）
        if 'odds_table' not in page_source and 'raceodds' not in page_source:
            # 404の可能性もあるのでNoneを返す
            pass
            
    except Exception:
        pass
    
    return None


def log_ban_event(race_id: str, reason: str, output_dir: Path):
    """BAN発生イベントをログに記録"""
    ban_log_path = output_dir.parent / "ban_events.log"
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(ban_log_path, 'a', encoding='utf-8') as f:
        f.write(f"{timestamp},BAN_DETECTED,race_id={race_id},reason={reason}\n")
    logger.error(f"BAN検出: {reason} (race_id={race_id})")


def validate_html_content(html: str, bet_type: str = None) -> bool:
    """
    HTMLコンテンツのバリデーション
    
    プレースホルダー（---.-）ばかりで実データがない場合はFalseを返す
    
    Args:
        html: 検証対象のHTML
        bet_type: 券種（sanrenpuku/sanrentanの場合は専用チェック）
    """
    if bet_type in ['sanrenpuku', 'sanrentan']:
        # 三連複/三連単: odds-7- または odds-8- の後に続く値をチェック
        # 実オッズ: id="odds-8-010203">123.4</span>
        # プレースホルダー: id="odds-8-010203">---.-</span>
        
        # 実オッズ値を検索 (数字のみ)
        real_odds = re.findall(r'id="odds-[78]-\d{6}">(\d+\.?\d*)</span>', html)
        # プレースホルダーを検索
        placeholders = re.findall(r'id="odds-[78]-\d{6}">---\.-</span>', html)
        
        real_count = len(real_odds)
        placeholder_count = len(placeholders)
        total = real_count + placeholder_count
        
        if total == 0:
            # パターンが全くない = ページ構造が異なるかエラー
            return False
        
        # 実データが10%未満はNG
        if (real_count / total) < 0.1:
            logger.warning(f"バリデーション失敗: 実オッズ {real_count}, プレースホルダー {placeholder_count}")
            return False
            
        return True
    else:
        # 他の券種: odds_min_ パターンなど（通常は問題なし）
        # 簡易チェック: ページサイズが1KB未満は異常
        if len(html) < 1000:
            return False
        return True


def scrape_odds_html_selenium(
    driver: webdriver.Chrome,
    race_id: str,
    bet_type: str,
    output_dir: Path,
    skip_existing: bool = True
) -> Optional[Path]:
    """
    Seleniumでオッズページを取得して.binとして保存
    
    Args:
        driver: Selenium WebDriver
        race_id: レースID
        bet_type: 券種
        output_dir: 出力ディレクトリ
        skip_existing: 既存ファイルをスキップ
    
    Returns:
        保存したファイルパス
    """
    output_file = output_dir / f"{race_id}.bin"
    
    if skip_existing and output_file.exists():
        return output_file
    
    type_code = BET_TYPES[bet_type]
    
    # 全券種、オッズ一覧ビューを使用（軸馬ベースのページネーション）
    url = f"{ODDS_BASE_URL}?type={type_code}&race_id={race_id}"
    
    try:
        driver.get(url)
        time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))
        
        # オッズテーブル読み込み待機
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.RaceOdds_HorseList_Table, table.Odds_Table"))
            )
        except TimeoutException:
            logger.warning(f"テーブル読み込みタイムアウト: {race_id} ({bet_type})")
            return None
        
        # オッズ値のロード待機（AJAX完了まで十分な時間を確保）
        time.sleep(3.0)
        
        # 三連複/三連単は軸馬ベースのページネーション処理
        if bet_type in ['sanrenpuku', 'sanrentan']:
            all_html_parts = []
            
            # 軸馬セレクタを取得（list_select_horse）
            try:
                select_elem = driver.find_element(By.CSS_SELECTOR, "#list_select_horse")
                options = select_elem.find_elements(By.TAG_NAME, "option")
                # 重複を除去（valueが同じoptionがある場合）
                page_values = list(dict.fromkeys([opt.get_attribute("value") for opt in options if opt.get_attribute("value")]))
                logger.debug(f"{bet_type} 軸馬数: {len(page_values)}")
            except NoSuchElementException:
                # セレクタがない場合（単一画面の可能性）
                page_values = []
            except Exception as e:
                logger.warning(f"軸馬セレクタ取得エラー: {e}")
                page_values = []
            
            # 各軸馬のHTMLを取得（リトライ機能付き）
            axis_errors = []
            for idx, page_value in enumerate(page_values):
                if idx > 0:  # 最初の軸馬は既に表示されている
                    # リトライ付き軸切替（最大3回）
                    success = False
                    for retry in range(3):
                        try:
                            select = Select(driver.find_element(By.CSS_SELECTOR, "#list_select_horse"))
                            select.select_by_value(page_value)
                            time.sleep(3.0)  # AJAX完了を待つ
                            success = True
                            break
                        except Exception as e:
                            if retry < 2:
                                time.sleep(1.0)
                            else:
                                logger.warning(f"軸馬切替エラー ({page_value}): {e}")
                                axis_errors.append(page_value)
                    
                    if not success:
                        continue
                
                # GraphOdds div内の全Odds_Tableを取得
                page_html = ""
                try:
                    graph_odds = driver.find_element(By.CSS_SELECTOR, ".GraphOdds")
                    page_html = f"<!-- AXIS {page_value} -->\n{graph_odds.get_attribute('outerHTML')}"
                    
                    # === バリデーション ===
                    if not validate_html_content(page_html, bet_type):
                        logger.warning(f"データ未ロード検知 ({race_id} 軸{page_value}): 再待機")
                        time.sleep(2.0)
                        # 再取得
                        graph_odds = driver.find_element(By.CSS_SELECTOR, ".GraphOdds")
                        page_html = f"<!-- AXIS {page_value} -->\n{graph_odds.get_attribute('outerHTML')}"
                        
                        if not validate_html_content(page_html, bet_type):
                            logger.error(f"データ取得失敗 ({race_id} 軸{page_value}): プレースホルダーのみ")
                            axis_errors.append(page_value)
                            # 不完全なデータは保存しない（あるいは欠損扱いにする）
                except:
                    # fallback
                    pass
                
                if page_html:
                    all_html_parts.append(page_html)

            
            # セレクタがない場合でも現在のページを保存
            if not page_values:
                try:
                    graph_odds = driver.find_element(By.CSS_SELECTOR, ".GraphOdds")
                    page_html = graph_odds.get_attribute('outerHTML')
                    if validate_html_content(page_html, bet_type):
                        all_html_parts.append(page_html)
                    else:
                        logger.error(f"データ取得失敗 ({race_id} 単一): プレースホルダーのみ")
                        # 失敗として処理
                        return None
                except:
                    pass
            
            # 結合して保存（データがある場合のみ）
            if all_html_parts:
                combined_html = "\n".join(all_html_parts)
                
                # 最終チェック：全体のサイズ感など（簡易）
                if len(combined_html) < 1000: # 極端に小さい場合
                     logger.warning(f"取得データが小さすぎます ({race_id}): {len(combined_html)} bytes")
                
                output_dir.mkdir(parents=True, exist_ok=True)
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(combined_html)
                
                # 軸エラーがあった場合、エラーレースログに記録
                if axis_errors:
                    error_log_path = output_dir.parent / "error_races.log"
                    with open(error_log_path, 'a', encoding='utf-8') as f:
                        f.write(f"{race_id},{bet_type},{','.join(axis_errors)}\n")
                    logger.warning(f"軸エラー記録: {race_id} ({bet_type}) - 軸{axis_errors}")
                
                return output_file
        else:
            # 単勝/複勝/馬連/ワイド/馬単: ページ全体のHTMLを保存
            html_content = driver.page_source
            
            # バリデーション
            if not validate_html_content(html_content, bet_type):
                 # 再試行（リロード）
                 logger.warning(f"データ未ロード検知 ({race_id}): リロード試行")
                 driver.refresh()
                 time.sleep(3.0)
                 html_content = driver.page_source
                 
                 if not validate_html_content(html_content, bet_type):
                     logger.error(f"データ取得失敗 ({race_id}): プレースホルダーのみ")
                     return None
            
            output_dir.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            return output_file
        
        return None
        
    except Exception as e:
        # ドライバーが死んでいる重大なエラーの場合は再送出する
        error_msg = str(e).lower()
        if "disconnected" in error_msg or "invalid session" in error_msg or "connection refused" in error_msg or "target closed" in error_msg:
             logger.error(f"Critical Selenium Error: {e}")
             raise e
             
        logger.error(f"Selenium取得失敗: {race_id} ({bet_type}): {e}")
        return None


def scrape_odds_html_batch(
    race_ids: List[str],
    output_base_dir: Path,
    skip_existing: bool = True,
    include_trio: bool = True,
    include_trifecta: bool = True
) -> Dict[str, int]:
    """
    複数レースのオッズHTMLを一括取得（Selenium統一）
    
    Args:
        race_ids: レースIDリスト
        output_base_dir: 出力ベースディレクトリ
        skip_existing: 既存ファイルをスキップ
        include_trio: 三連複を含める
        include_trifecta: 三連単を含める
    
    Returns:
        {bet_type: success_count}
    """
    logger.info(f"=== オッズHTML一括取得開始 ===")
    logger.info(f"  レース数: {len(race_ids)}")
    logger.info(f"  出力先: {output_base_dir}")
    logger.info(f"  三連複: {include_trio}, 三連単: {include_trifecta}")
    
    # 取得対象の券種リスト
    target_bet_types = ['tansho_fukusho', 'umaren', 'wide', 'umatan']
    if include_trio:
        target_bet_types.append('sanrenpuku')
    if include_trifecta:
        target_bet_types.append('sanrentan')
    
    stats = {bt: 0 for bt in BET_TYPES.keys()}
    
    # BAN検出用カウンター（レース単位）
    consecutive_race_failures = 0
    MAX_CONSECUTIVE_RACE_FAILURES = 5  # 5レース連続で全券種失敗 = BAN疑い
    
    driver = prepare_chrome_driver(headless=True)
    
    try:
        for race_id in tqdm(race_ids, desc="オッズHTML取得", unit="レース"):
            race_success = False  # このレースで1つでも成功したか
            
            for bet_type in target_bet_types:
                output_dir = output_base_dir / bet_type
                
                # リトライループ（ドライバー再起動用 + エクスポネンシャルバックオフ）
                max_retries = 3
                
                for retry in range(max_retries):
                    try:
                        path = scrape_odds_html_selenium(driver, race_id, bet_type, output_dir, skip_existing)
                        
                        if path:
                            stats[bet_type] += 1
                            race_success = True  # このレースは成功扱い
                        
                        break  # 成功またはNone（スキップ）でループ抜ける
                        
                    except Exception as e:
                        # 重大なエラー（ドライバー切断など）の場合、ドライバーを再起動してリトライ
                        wait_time = 30 * (2 ** retry)  # 30秒, 1分, 2分
                        logger.warning(f"ドライバーエラー発生: {e}")
                        logger.info(f"待機中... ({wait_time}秒) - Retry {retry+1}/{max_retries}")
                        
                        try:
                            driver.quit()
                        except:
                            pass
                        
                        time.sleep(wait_time)
                        driver = prepare_chrome_driver(headless=True)
                        logger.info("ドライバー再起動完了")
                        # 次のretryへ
            
            # レース終了後にカウンター更新
            if race_success:
                consecutive_race_failures = 0  # 1つでも成功ならリセット
            else:
                consecutive_race_failures += 1
                logger.warning(f"レース {race_id}: 全券種失敗 (連続: {consecutive_race_failures})")
                
                # BAN検出チェック
                ban_reason = detect_ban(driver)
                if ban_reason:
                    log_ban_event(race_id, ban_reason, output_base_dir)
                    logger.error(f"BAN検出: {ban_reason} - 5分間待機後に続行")
                    time.sleep(300)
                    consecutive_race_failures = 0  # リセット
            
            # 連続失敗チェック（レース単位）
            if consecutive_race_failures >= MAX_CONSECUTIVE_RACE_FAILURES:
                logger.error(f"連続{MAX_CONSECUTIVE_RACE_FAILURES}レース失敗 - BAN疑い。自動停止します。")
                log_ban_event(race_id, f"CONSECUTIVE_RACE_FAILURES_{consecutive_race_failures}", output_base_dir)
                raise KeyboardInterrupt("BAN検出による自動停止")
                        
    except KeyboardInterrupt:
        logger.warning("ユーザーによる中断または自動停止")
    finally:
        driver.quit()
    
    logger.info(f"=== 取得完了 ===")
    for bt, count in stats.items():
        if count > 0:
            logger.info(f"  {bt}: {count}件")
    
    return stats

"""
HTML スクレイピング関数群（.bin形式で保存）
参考実装に基づく統合版
"""
import os
import re
import time
import random
import logging
from typing import List, Optional, Dict
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from ..constants import UrlPaths, LocalPaths

# ログ設定
logger = logging.getLogger(__name__)

# --- BAN対策設定 ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

MIN_SLEEP_SECONDS = 2.5
MAX_SLEEP_SECONDS = 5.0
REQUESTS_RETRIES = 5
REQUESTS_BACKOFF_FACTOR = 0.5
REQUESTS_STATUS_FORCELIST = (500, 502, 503, 504, 429)
HTTP_400_SLEEP_SECONDS = 60  # IP BAN時の待機時間
SELENIUM_WAIT_TIMEOUT = 30

# --------------------


def get_robust_session() -> requests.Session:
    """堅牢なrequests.Sessionを返す"""
    session = requests.Session()
    retries = Retry(
        total=REQUESTS_RETRIES,
        backoff_factor=REQUESTS_BACKOFF_FACTOR,
        status_forcelist=REQUESTS_STATUS_FORCELIST,
        allowed_methods={"GET", "POST"},
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_html_robust_get(
    url: str, 
    session: Optional[requests.Session] = None,
    additional_headers: Optional[Dict[str, str]] = None
) -> Optional[bytes]:
    """
    堅牢なHTML取得（requests版）
    
    Args:
        url: 取得対象URL
        session: requests.Session（Noneの場合は新規作成）
        additional_headers: 追加ヘッダー（AJAX用など）
        
    Returns:
        HTML content (bytes) or None
    """
    if session is None:
        session = get_robust_session()
        
    try:
        # ランダムな待機
        sleep_time = random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS)
        time.sleep(sleep_time)
        
        # ヘッダー設定
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        if additional_headers:
            headers.update(additional_headers)
            
        response = session.get(url, headers=headers, timeout=30)
        
        # HTTP 400エラーの特別処理（IP BAN対策）
        if response.status_code == 400:
            logger.critical(f"HTTP 400 Error. IP BANの可能性: {url}")
            time.sleep(HTTP_400_SLEEP_SECONDS)
            return None
            
        response.raise_for_status()
        return response.content
        
    except requests.exceptions.RequestException as e:
        logger.warning(f"HTML取得失敗 ({url}): {e}")
        return None


def prepare_chrome_driver(headless: bool = True) -> webdriver.Chrome:
    """
    自動化検出を回避するSelenium WebDriver
    """
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
    except ImportError:
        service = None
        
    options = Options()
    if headless:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1280x800')
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    
    # --- 自動化検出の回避 ---
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    # -------------------------
    
    if service:
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)
        
    # WebDriverフラグの隠蔽
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver


def scrape_kaisai_date(from_: str, to_: str) -> List[str]:
    """
    開催日リストを取得
    
    Args:
        from_: 開始日（YYYY-MM-DD形式）
        to_: 終了日（YYYY-MM-DD形式）
        
    Returns:
        開催日リスト（YYYYMMDD形式）
    """
    logger.info(f'開催日を取得中: {from_} から {to_}')
    
    try:
        import pandas as pd
    except ImportError as e:
        logger.critical(f"pandas のインポートに失敗しました: {e}")
        logger.critical("pandas が正しくインストールされているか確認してください。 pip install pandas")
        return []
        
    try:
        # 月のリストを重複なく作成する堅牢な方法
        start_date = pd.to_datetime(from_)
        end_date = pd.to_datetime(to_)
        months = pd.date_range(start=start_date, end=end_date, freq='D').to_period('M').unique()
        date_range = months.to_timestamp()
    except ValueError as e:
        logger.error(f"日付形式エラー: {e}")
        return []
        
    kaisai_date_list = []
    session = get_robust_session()
    
    for date in date_range:
        year, month = date.year, date.month
        url = f'{UrlPaths.CALENDAR_URL}?year={year}&month={month}'
        logger.debug(f'カレンダーページを取得: {url}')
        
        html_content = fetch_html_robust_get(url, session)
        if html_content:
            try:
                soup = BeautifulSoup(html_content, "lxml", from_encoding='euc-jp')
                calendar_table = soup.find('table', class_='Calendar_Table')
                
                if calendar_table:
                    for a in calendar_table.find_all('a'):
                        href = a.get('href', '')
                        match = re.search(r'kaisai_date=(\d{8})', href)
                        if match:
                            kaisai_date = match.group(1)
                            # 日付範囲チェック
                            if from_.replace('-', '') <= kaisai_date <= to_.replace('-', ''):
                                kaisai_date_list.append(kaisai_date)
                else:
                    logger.warning(f"カレンダーテーブルが見つかりません: {url}")
                                
            except Exception as e:
                logger.error(f"カレンダー解析エラー: {e}")
        else:
            logger.warning(f"HTMLコンテンツの取得に失敗: {url}")
            
    return sorted(list(set(kaisai_date_list)))

def scrape_race_id_list(
    kaisai_date_list: List[str], 
    max_retries: int = 3
) -> List[str]:
    """
    レースIDリストを取得（Selenium使用）
    
    Args:
        kaisai_date_list: 開催日リスト
        max_retries: リトライ回数
        
    Returns:
        レースIDリスト
    """
    logger.info(f'レースIDを取得中（{len(kaisai_date_list)}日分）')
    
    race_id_list = []
    driver = None
    
    try:
        driver = prepare_chrome_driver()
        
        for kaisai_date in kaisai_date_list:
            url = f'{UrlPaths.RACE_LIST_URL}?kaisai_date={kaisai_date}'
            
            for attempt in range(1, max_retries + 1):
                try:
                    # 待機時間
                    time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))
                    
                    driver.get(url)
                    
                    # レース一覧が表示されるまで待機
                    wait = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT)
                    race_list_box = wait.until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'RaceList_Box'))
                    )
                    
                    # リンクを取得
                    links = race_list_box.find_elements(By.TAG_NAME, 'a')
                    
                    for link in links:
                        href = link.get_attribute('href')
                        if href:
                            match = re.search(r'(?:shutuba|result)\.html\?race_id=(\d{12})', href)
                            if match:
                                race_id_list.append(match.group(1))
                                
                    break  # 成功したらループを抜ける
                    
                except (TimeoutException, NoSuchElementException) as e:
                    logger.warning(f'レースリスト取得エラー（試行 {attempt}/{max_retries}）: {e}')
                    if attempt == max_retries:
                        logger.error(f'レースID取得失敗: {kaisai_date}')
                        
    finally:
        if driver:
            driver.quit()
            
    return sorted(list(set(race_id_list)))


def scrape_html_race(race_id_list: List[str], skip: bool = True) -> List[str]:
    """
    レース結果HTMLを取得して.bin形式で保存
    
    Args:
        race_id_list: レースIDリスト
        skip: 既存ファイルをスキップするか
        
    Returns:
        保存したファイルパスのリスト
    """
    logger.info(f'レース結果HTMLを取得中（{len(race_id_list)}件）')
    
    updated_paths = []
    session = get_robust_session()
    os.makedirs(LocalPaths.HTML_RACE_DIR, exist_ok=True)
    
    for race_id in race_id_list:
        filename = os.path.join(LocalPaths.HTML_RACE_DIR, f'{race_id}.bin')
        
        if skip and os.path.exists(filename):
            logger.debug(f'スキップ: {race_id} (既存)')
            continue
            
        url = UrlPaths.RACE_URL + race_id
        html_content = fetch_html_robust_get(url, session)
        
        if html_content:
            try:
                # 有効性チェック
                soup = BeautifulSoup(html_content, "lxml", from_encoding='euc-jp')
                if soup.find("div", attrs={"class": "data_intro"}):
                    with open(filename, 'wb') as f:
                        f.write(html_content)
                    updated_paths.append(filename)
                    logger.info(f'保存: {filename}')
                else:
                    logger.warning(f'無効なページ: {race_id}')
                    
            except Exception as e:
                logger.error(f'保存エラー: {race_id} - {e}')
                
    return updated_paths


def scrape_html_shutuba(race_id_list: List[str], skip: bool = True, force_today_refresh: bool = True) -> List[str]:
    """
    出馬表HTMLを取得して.bin形式で保存
    force_today_refresh=Trueの場合、レース当日であればキャッシュを無視して再取得する
    """
    logger.info(f'出馬表HTMLを取得中（{len(race_id_list)}件）')
    
    updated_paths = []
    session = get_robust_session()
    os.makedirs(LocalPaths.HTML_SHUTUBA_DIR, exist_ok=True)
    
    today_str = datetime.now().strftime('%Y%m%d')

    for race_id in race_id_list:
        filename = os.path.join(LocalPaths.HTML_SHUTUBA_DIR, f'{race_id}.bin')
        
        # スキップ条件の判定
        do_skip = skip
        if force_today_refresh:
            # race_idの先頭8文字が開催日 (YYYYMMDD)
            kaisai_date_str = race_id[:8]
            if kaisai_date_str == today_str:
                logger.info(f"本日開催レースのため、強制的に再取得します: {race_id}")
                do_skip = False

        if do_skip and os.path.exists(filename):
            logger.debug(f'スキップ: {race_id} (既存)')
            continue
            
        url = f'{UrlPaths.SHUTUBA_TABLE}?race_id={race_id}'
        html_content = fetch_html_robust_get(url, session)
        
        if html_content:
            try:
                # 有効性チェック
                soup = BeautifulSoup(html_content, "lxml", from_encoding='euc-jp')
                if soup.find("table", class_="Shutuba_Table"):
                    with open(filename, 'wb') as f:
                        f.write(html_content)
                    updated_paths.append(filename)
                    logger.info(f'保存: {filename}')
                else:
                    logger.warning(f'無効なページ: {race_id}')
                    
            except Exception as e:
                logger.error(f'保存エラー: {race_id} - {e}')
                
    return updated_paths


def _load_horse_cache() -> pd.DataFrame:
    """馬のキャッシュログを読み込む"""
    log_path = LocalPaths.HORSE_CACHE_LOG_PATH
    if os.path.exists(log_path):
        logger.info(f"キャッシュログを読み込みます: {log_path}")
        try:
            df = pd.read_csv(log_path, dtype={'horse_id': str})
            # 'last_updated' 列を datetime 型に変換
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            return df.set_index('horse_id')
        except Exception as e:
            logger.error(f"キャッシュログの読み込みに失敗しました: {e}")
            # 読み込み失敗時は空のDataFrameを返す
            return pd.DataFrame(columns=['last_updated']).set_index('horse_id')
    else:
        return pd.DataFrame(columns=['horse_id', 'last_updated']).set_index('horse_id')

def _save_horse_cache(cache_df: pd.DataFrame):
    """馬のキャッシュログを保存する"""
    log_path = LocalPaths.HORSE_CACHE_LOG_PATH
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    try:
        # インデックスをリセットして 'horse_id' を列に戻す
        cache_df.reset_index().to_csv(log_path, index=False)
        logger.info(f"キャッシュログを保存しました: {log_path}")
    except Exception as e:
        logger.error(f"キャッシュログの保存に失敗しました: {e}")


def scrape_html_horse(horse_id_list: List[str], skip: bool = True, cache_ttl_days: int = 7) -> List[str]:
    """
    馬情報HTMLを取得して.bin形式で保存
    プロフィールと成績を分けて保存し、成績はキャッシュの鮮度を考慮する
    """
    logger.info(f'馬情報HTMLを取得中（{len(horse_id_list)}件, TTL: {cache_ttl_days}日）')
    
    updated_paths = []
    session = get_robust_session()
    os.makedirs(LocalPaths.HTML_HORSE_DIR, exist_ok=True)
    
    # キャッシュログを読み込む
    cache_df = _load_horse_cache()
    
    # 更新があったかどうかのフラグ
    cache_updated = False

    for horse_id in horse_id_list:
        # --- 1. プロフィール (不変データ) ---
        # ファイルが存在しない場合のみ取得
        profile_filename = os.path.join(LocalPaths.HTML_HORSE_DIR, f'{horse_id}_profile.bin')
        if not (skip and os.path.exists(profile_filename)):
            url = UrlPaths.HORSE_URL + horse_id
            html_content = fetch_html_robust_get(url, session)
            
            if html_content:
                try:
                    with open(profile_filename, 'wb') as f:
                        f.write(html_content)
                    updated_paths.append(profile_filename)
                    logger.info(f'保存 (プロフィール): {profile_filename}')
                except Exception as e:
                    logger.error(f'保存エラー: {horse_id}_profile - {e}')
        else:
            logger.debug(f'スキップ (プロフィール既存): {profile_filename}')

        # --- 2. 過去成績 (鮮度管理データ) ---
        should_fetch_perf = False
        if not skip:
            should_fetch_perf = True
        else:
            # キャッシュを確認
            if horse_id in cache_df.index:
                last_updated = cache_df.loc[horse_id, 'last_updated']
                if datetime.now() - last_updated > timedelta(days=cache_ttl_days):
                    logger.info(f"キャッシュ期限切れ: {horse_id} (最終更新: {last_updated})")
                    should_fetch_perf = True
                else:
                    logger.debug(f"キャッシュ有効: {horse_id}")
            else:
                logger.info(f"新規の馬: {horse_id}")
                should_fetch_perf = True

        perf_filename = os.path.join(LocalPaths.HTML_HORSE_DIR, f'{horse_id}_perf.bin')
        if should_fetch_perf:
            ajax_url = 'https://db.netkeiba.com/horse/ajax_horse_results.html'
            headers = {
                "Referer": UrlPaths.HORSE_URL + horse_id,
                "X-Requested-With": "XMLHttpRequest"
            }
            
            ajax_session = get_robust_session()
            try:
                time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))
                response = ajax_session.get(
                    ajax_url, 
                    params={'id': horse_id},
                    headers={**headers, "User-Agent": random.choice(USER_AGENTS)},
                    timeout=30
                )
                
                if response.status_code == 200:
                    try:
                        json_data = response.json()
                        perf_html = json_data['data'].encode('euc-jp', errors='replace')
                    except (ValueError, KeyError):
                        perf_html = response.content
                        
                    with open(perf_filename, 'wb') as f:
                        f.write(perf_html)
                    updated_paths.append(perf_filename)
                    logger.info(f'保存 (過去成績): {perf_filename}')
                    
                    # キャッシュログを更新
                    cache_df.loc[horse_id, 'last_updated'] = datetime.now()
                    cache_updated = True
                else:
                    logger.warning(f"AJAX取得失敗 (Status: {response.status_code}): {horse_id}_perf")
                    
            except Exception as e:
                logger.error(f'AJAX取得エラー: {horse_id}_perf - {e}')
        else:
            logger.debug(f'スキップ (過去成績キャッシュ有効): {perf_filename}')

    # 更新があった場合のみキャッシュを保存
    if cache_updated:
        _save_horse_cache(cache_df)
            
    return updated_paths


def scrape_html_ped(horse_id_list: List[str], skip: bool = True) -> List[str]:
    """
    血統HTMLを取得して.bin形式で保存（Selenium使用）
    """
    logger.info(f'血統HTMLを取得中（{len(horse_id_list)}件）')
    
    updated_paths = []
    os.makedirs(LocalPaths.HTML_PED_DIR, exist_ok=True)
    
    driver = None
    
    try:
        driver = prepare_chrome_driver()
        
        for horse_id in horse_id_list:
            filename = os.path.join(LocalPaths.HTML_PED_DIR, f'{horse_id}.bin')
            
            if skip and os.path.exists(filename):
                logger.debug(f'スキップ: {horse_id} (既存)')
                continue
                
            try:
                # 待機時間
                time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))
                
                url = UrlPaths.PED_URL + horse_id
                driver.get(url)
                
                # 血統表が表示されるまで待機
                wait = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT)
                wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'blood_table'))
                )
                
                # レンダリング完了後のHTMLを取得
                html_content = driver.page_source.encode('euc-jp', errors='replace')
                
                with open(filename, 'wb') as f:
                    f.write(html_content)
                updated_paths.append(filename)
                logger.info(f'保存: {filename}')
                
            except (TimeoutException, NoSuchElementException) as e:
                logger.warning(f'血統ページ取得エラー: {horse_id} - {e}')
            except Exception as e:
                logger.error(f'予期せぬエラー: {horse_id} - {e}')
                
    finally:
        if driver:
            driver.quit()
            
    return updated_paths


def _worker_extract_horse_ids(filepath: str) -> set:
    """単一のHTMLファイルから馬IDを抽出するワーカー関数"""
    horse_ids = set()
    try:
        with open(filepath, 'rb') as f:
            html_content = f.read()
            
        soup = BeautifulSoup(html_content, "lxml", from_encoding='euc-jp')
        
        for a in soup.find_all('a', href=re.compile(r'/horse/\d+')):
            match = re.search(r'/horse/(\d+)', a.get('href', ''))
            if match:
                horse_ids.add(match.group(1))
    except Exception:
        # loggerはマルチプロセスで問題を起こすことがあるため、ここではシンプルに無視
        pass
    return horse_ids

def extract_horse_ids_from_html(html_dir: str) -> set:
    """
    保存済みのレースHTMLから馬IDを並列処理で抽出
    
    Args:
        html_dir: HTMLディレクトリパス
        
    Returns:
        馬IDのセット
    """
    logger.info(f'馬IDを抽出中: {html_dir}')
    
    all_horse_ids = set()
    
    # 処理対象のファイルパスリストを作成
    filepaths = [os.path.join(html_dir, f) for f in os.listdir(html_dir) if f.endswith('.bin')]
    
    if not filepaths:
        logger.warning(f"対象ファイルが見つかりません: {html_dir}")
        return all_horse_ids

    # CPUのコア数に基づいてプロセス数を決定
    num_processes = cpu_count()
    logger.info(f"{num_processes}個のプロセスを使用して並列処理を開始します。")

    # チャンクサイズを計算してオーバーヘッドを削減
    chunksize, extra = divmod(len(filepaths), num_processes * 4)
    if extra:
        chunksize += 1
    logger.info(f"チャンクサイズを {chunksize} に設定しました。")

    with Pool(processes=num_processes) as pool:
        # imap_unordered に chunksize を指定
        results_iterator = pool.imap_unordered(_worker_extract_horse_ids, filepaths, chunksize=chunksize)
        
        for horse_id_set in tqdm(results_iterator, total=len(filepaths), desc=f"馬IDを抽出中 ({os.path.basename(html_dir)})"):
            all_horse_ids.update(horse_id_set)
            
    logger.info(f'抽出完了: {len(all_horse_ids)}頭')
    return all_horse_ids
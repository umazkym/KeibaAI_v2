import requests
import time
import random
import logging
from typing import List, Dict, Optional
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

log = logging.getLogger(__name__)

# --- BAN対策設定 ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# HTTP 400 エラー時の待機時間
HTTP_400_SLEEP_SECONDS = 60


def get_session_with_retries(retries: int = 5, backoff_factor: float = 0.5) -> requests.Session:
    """リトライ機能付きのrequests.Sessionを作成"""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(400, 429, 500, 502, 503, 504),
        allowed_methods={"GET", "POST"}
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def fetch_html(url: str, cfg: Dict) -> requests.Response:
    """
    指定されたURLからHTMLを取得する（.bin形式保存用）
    
    Args:
        url: 取得対象URL
        cfg: 設定辞書
        
    Returns:
        requests.Response オブジェクト
    """
    max_attempts = cfg['scraping']['retry']['max_attempts']
    backoff_factor = cfg['scraping']['retry']['backoff_factor']
    min_delay = cfg['scraping']['delay_seconds']['min']
    max_delay = cfg['scraping']['delay_seconds']['max']
    
    session = get_session_with_retries(max_attempts, backoff_factor)
    
    for attempt in range(max_attempts):
        try:
            # リクエスト前の待機
            delay = random.uniform(min_delay, max_delay)
            time.sleep(delay)
            
            # User-Agentをランダムに選択
            headers = {'User-Agent': random.choice(USER_AGENTS)}
            
            response = session.get(url, headers=headers, timeout=30)
            
            # HTTP 400エラーの特別処理（IP BAN対策）
            if response.status_code == 400:
                log.critical(f"HTTP 400 Bad Request - IPがブロックされた可能性があります: {url}")
                log.critical(f"{HTTP_400_SLEEP_SECONDS} 秒間待機します")
                time.sleep(HTTP_400_SLEEP_SECONDS)
                continue
            
            response.raise_for_status()
            
            log.info(f"Successfully fetched HTML from {url}")
            return response

        except requests.exceptions.RequestException as e:
            log.warning(f"Attempt {attempt + 1}/{max_attempts} failed for {url}: {e}")
            if attempt + 1 == max_attempts:
                log.error(f"All attempts to fetch {url} failed.")
                raise
            
            # 指数バックオフ
            sleep_time = backoff_factor * (2 ** attempt) + random.uniform(0, 1)
            log.info(f"Retrying in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)


def fetch_html_with_requests(url: str, session: Optional[requests.Session] = None) -> Optional[bytes]:
    """
    requestsを使用してHTMLコンテンツを取得する（互換性用）
    
    Args:
        url: 取得対象URL
        session: requests.Session
        
    Returns:
        HTML content (bytes) or None
    """
    if session is None:
        session = get_session_with_retries()
    
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    min_delay = 2.5
    max_delay = 5.0
    
    try:
        # 待機時間
        time.sleep(random.uniform(min_delay, max_delay))
        
        response = session.get(url, headers=headers, timeout=30)
        
        if response.status_code == 400:
            log.warning(f"HTTP 400 Bad Request: {url}")
            time.sleep(HTTP_400_SLEEP_SECONDS)
            return None
        
        response.raise_for_status()
        return response.content
        
    except requests.exceptions.RequestException as e:
        log.error(f"Request failed: {url} - {e}")
        return None


# --- 互換性のための関数（新しい_scrape_html.pyで実装済み） ---

def scrape_kaisai_dates(cfg: Dict) -> List[str]:
    """
    開催日一覧をスクレイピングする（互換性用）
    実際の実装は _scrape_html.scrape_kaisai_date を使用
    """
    log.warning("scrape_kaisai_dates は非推奨です。_scrape_html.scrape_kaisai_date を使用してください。")
    return ["2023/01/05", "2023/01/07", "2023/01/08"]


def scrape_race_id_list(kaisai_dates: List[str], cfg: Dict) -> List[str]:
    """
    開催日からレースID一覧をスクレイピングする（互換性用）
    実際の実装は _scrape_html.scrape_race_id_list を使用
    """
    log.warning("scrape_race_id_list は非推奨です。_scrape_html.scrape_race_id_list を使用してください。")
    return ["202305020811"]
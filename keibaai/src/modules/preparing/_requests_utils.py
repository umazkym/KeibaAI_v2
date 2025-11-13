import requests
import time
import random
import logging
from typing import List, Dict

# from omegaconf import DictConfig

log = logging.getLogger(__name__)

def fetch_html(url: str, cfg: Dict) -> requests.Response:
    """
    指定されたURLからHTMLを取得する。
    設定に基づいて遅延、リトライ、User-Agentローテーションを行う。
    """
    max_attempts = cfg['scraping']['retry']['max_attempts']
    backoff_factor = cfg['scraping']['retry']['backoff_factor']
    
    for attempt in range(max_attempts):
        try:
            # User-Agentをランダムに選択
            user_agent = random.choice(cfg['scraping']['user_agents'])
            headers = {'User-Agent': user_agent}
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # HTTPエラーがあれば例外を発生させる
            
            # リクエスト間の遅延
            delay = random.uniform(cfg['scraping']['delay_seconds']['min'], cfg['scraping']['delay_seconds']['max'])
            time.sleep(delay)
            
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

# --- 以下はプレースホルダー関数 ---

def scrape_kaisai_dates(cfg: Dict) -> List[str]:
    """
    開催日一覧をスクレイピングする（将来実装）
    """
    log.info("開催日一覧のスクレイピングを開始します...")
    # ここに実際のスクレイピングロジックを実装
    # 例: return ["2023/01/01", "2023/01/05", ...]
    log.warning("scrape_kaisai_dates は現在モック実装です。")
    return ["2023/01/05", "2023/01/07", "2023/01/08"]

def scrape_race_id_list(kaisai_dates: List[str], cfg: Dict) -> List[str]:
    """
    開催日からレースID一覧をスクレイピングする（将来実装）
    """
    log.info("レースID一覧のスクレイピングを開始します...")
    # 存在するレースID（2023年日本ダービー）を返すように修正
    log.warning("scrape_race_id_list は現在、単一の存在するレースIDを返すテスト実装です。")
    return ["202305020811"]

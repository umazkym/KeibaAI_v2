#!/usr/bin/env python3
# keibaai/src/modules/preparing/_requests_utils.py
"""
リクエストユーティリティ (ダミー実装)
スクレイピングパイプライン用
"""
import logging
import time
import random
from typing import List, Optional
from dataclasses import dataclass

@dataclass
class Response:
    """レスポンスオブジェクト"""
    content: bytes
    status_code: int

def fetch_html(url: str, config: dict) -> Optional[Response]:
    """
    HTMLを取得する (ダミー実装)
    
    Args:
        url: 取得対象URL
        config: スクレイピング設定
    
    Returns:
        Response または None
    """
    logging.info(f"[DUMMY] fetch_html: {url}")
    
    # ダミーHTMLコンテンツ
    dummy_html = f"""
    <html>
    <head><title>Dummy Page</title></head>
    <body>
    <h1>This is a dummy HTML for {url}</h1>
    <p>Real scraping is not implemented.</p>
    </body>
    </html>
    """.encode('utf-8')
    
    # 遅延をシミュレート
    delay_min = config.get('delay_seconds', {}).get('min', 1.0)
    delay_max = config.get('delay_seconds', {}).get('max', 3.0)
    time.sleep(random.uniform(delay_min, delay_max))
    
    return Response(content=dummy_html, status_code=200)

def scrape_kaisai_dates(config: dict) -> List[str]:
    """
    開催日一覧を取得 (ダミー実装)
    
    Returns:
        開催日のリスト (YYYY-MM-DD形式)
    """
    logging.info("[DUMMY] scrape_kaisai_dates: 2023年5月の開催日を返します")
    
    # 2023年5月の全日を返す (ダミー)
    return [f"2023-05-{day:02d}" for day in range(1, 32)]

def scrape_race_id_list(kaisai_dates: List[str], config: dict) -> List[str]:
    """
    レースID一覧を取得 (ダミー実装)
    
    Args:
        kaisai_dates: 開催日のリスト
        config: スクレイピング設定
    
    Returns:
        レースIDのリスト
    """
    logging.info(f"[DUMMY] scrape_race_id_list: {len(kaisai_dates)}日分のレースIDを生成")
    
    race_ids = []
    for date_str in kaisai_dates:
        # 日付から8桁の数値を生成 (例: 2023-05-01 -> 20230501)
        date_numeric = date_str.replace('-', '')
        
        # 各開催日に3レース分のIDを生成 (ダミー)
        for race_num in range(1, 4):
            race_id = f"{date_numeric}{race_num:02d}01"  # 例: 2023050101, 2023050102, ...
            race_ids.append(race_id)
    
    logging.info(f"合計 {len(race_ids)} 件のレースIDを生成しました")
    return race_ids
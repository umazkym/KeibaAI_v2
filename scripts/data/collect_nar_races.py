#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAR（地方競馬）レース結果一括収集スクリプト

2025年の全地方競馬レース結果をスクレイピングし、parquet形式で保存する。
これにより、基準タイム・平均タイム等の統計計算が可能になる。
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import re
import logging
from typing import List, Dict, Optional
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "keibaai", "data", "parsed", "parquet", "nar_races")

# NAR会場コード
NAR_VENUES = {
    "30": "門別", "35": "盛岡", "36": "水沢",
    "42": "浦和", "43": "船橋", "44": "大井", "45": "川崎",
    "46": "金沢", "47": "笠松", "48": "名古屋",
    "50": "園田", "51": "姫路",
    "54": "高知", "55": "佐賀"
}

class NarRaceCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        self.all_results = []
        
    def get_races_for_date(self, date_str: str) -> List[Dict]:
        """指定日付の全レースIDを取得"""
        url = f"https://nar.netkeiba.com/top/race_list_sub.html?kaisai_date={date_str}"
        
        try:
            resp = self.session.get(url, timeout=15)
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            races = []
            # レースリンクを取得
            race_links = soup.select('a[href*="result.html?race_id="]')
            
            for link in race_links:
                href = link.get('href', '')
                match = re.search(r'race_id=(\d+)', href)
                if match:
                    race_id = match.group(1)
                    # race_id から会場コードを抽出
                    venue_code = race_id[4:6]
                    if venue_code in NAR_VENUES:
                        races.append({
                            'race_id': race_id,
                            'venue_code': venue_code,
                            'venue_name': NAR_VENUES.get(venue_code, 'Unknown')
                        })
            
            # 重複除去
            unique_races = {r['race_id']: r for r in races}
            return list(unique_races.values())
            
        except Exception as e:
            logger.warning(f"Failed to get races for {date_str}: {e}")
            return []
    
    def scrape_race_result(self, race_id: str) -> List[Dict]:
        """レース結果をスクレイピング"""
        url = f"https://nar.netkeiba.com/race/result.html?race_id={race_id}"
        
        try:
            resp = self.session.get(url, timeout=15)
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # レース情報
            race_name_elem = soup.select_one('.RaceName')
            race_name = race_name_elem.text.strip() if race_name_elem else ""
            
            race_data01 = soup.select_one('.RaceData01')
            race_data_text = race_data01.text.strip() if race_data01 else ""
            
            # コース種別・距離
            course_type = "ダート"
            if "芝" in race_data_text:
                course_type = "芝"
            
            distance = ""
            dist_match = re.search(r'(\d+)m', race_data_text)
            if dist_match:
                distance = dist_match.group(1)
            
            # 馬場状態
            condition = ""
            for c in ["良", "稍", "重", "不"]:
                if c in race_data_text:
                    condition = c
                    break
            
            # 天気
            weather = ""
            weather_elem = soup.select_one('.Icon_Weather')
            if weather_elem:
                classes = weather_elem.get('class', [])
                for c in classes:
                    if 'Weather' in c and c != 'Weather':
                        # Weather01=晴, Weather02=曇, etc.
                        weather = c
                        break
            
            # RaceData02から頭数等
            race_data02 = soup.select_one('.RaceData02')
            heads = ""
            if race_data02:
                heads_match = re.search(r'(\d+)頭', race_data02.text)
                if heads_match:
                    heads = heads_match.group(1)
            
            # 会場コード
            venue_code = race_id[4:6]
            venue_name = NAR_VENUES.get(venue_code, "")
            
            # 日付
            race_date = f"{race_id[:4]}/{race_id[6:8]}/{race_id[8:10]}"
            
            # 結果テーブル
            results = []
            result_table = soup.select_one('table.RaceTable01')
            if result_table:
                rows = result_table.find_all('tr')
                for row in rows:
                    # ヘッダー行をスキップ
                    if 'Header' in row.get('class', []):
                        continue
                    
                    tds = row.find_all('td')
                    if len(tds) < 14:
                        continue
                    
                    # 各列のデータを抽出
                    rank = tds[0].get_text(strip=True)
                    waku = tds[1].get_text(strip=True)
                    umaban = tds[2].get_text(strip=True)
                    
                    # 馬名・馬ID
                    horse_name = ""
                    horse_id = ""
                    horse_link = tds[3].select_one('a[href*="/horse/"]')
                    if horse_link:
                        horse_name = horse_link.get_text(strip=True)
                        href = horse_link.get('href', '')
                        hid_match = re.search(r'/horse/(\d+)', href)
                        if hid_match:
                            horse_id = hid_match.group(1)
                    
                    # 性齢
                    seirei = tds[4].get_text(strip=True) if len(tds) > 4 else ""
                    
                    # 斤量
                    kinryou = tds[5].get_text(strip=True) if len(tds) > 5 else ""
                    
                    # 騎手
                    jockey = ""
                    jockey_link = tds[6].select_one('a') if len(tds) > 6 else None
                    if jockey_link:
                        jockey = jockey_link.get_text(strip=True)
                    
                    # タイム
                    time_str = tds[7].get_text(strip=True) if len(tds) > 7 else ""
                    time_sec = self._parse_time(time_str)
                    
                    # 着差
                    margin = tds[8].get_text(strip=True) if len(tds) > 8 else ""
                    
                    # 通過順
                    passing = tds[10].get_text(strip=True) if len(tds) > 10 else ""
                    
                    # 上り
                    l3f_str = tds[11].get_text(strip=True) if len(tds) > 11 else ""
                    l3f_sec = self._parse_time(l3f_str)
                    
                    # 人気
                    popularity = tds[12].get_text(strip=True) if len(tds) > 12 else ""
                    
                    # オッズ
                    odds = tds[13].get_text(strip=True) if len(tds) > 13 else ""
                    
                    # 馬体重
                    weight_raw = tds[14].get_text(strip=True) if len(tds) > 14 else ""
                    weight = ""
                    weight_change = ""
                    if weight_raw:
                        wmatch = re.match(r'(\d+)\(([+-]?\d+)\)', weight_raw)
                        if wmatch:
                            weight = wmatch.group(1)
                            weight_change = wmatch.group(2)
                    
                    result = {
                        'race_id': race_id,
                        'race_date': race_date,
                        'venue_code': venue_code,
                        'venue_name': venue_name,
                        'race_name': race_name,
                        'course_type': course_type,
                        'distance': distance,
                        'condition': condition,
                        'heads': heads,
                        'rank': rank,
                        'waku': waku,
                        'umaban': umaban,
                        'horse_name': horse_name,
                        'horse_id': horse_id,
                        'seirei': seirei,
                        'kinryou': kinryou,
                        'jockey': jockey,
                        'time_str': time_str,
                        'time_sec': time_sec,
                        'margin': margin,
                        'passing': passing,
                        'l3f_str': l3f_str,
                        'l3f_sec': l3f_sec,
                        'popularity': popularity,
                        'odds': odds,
                        'weight': weight,
                        'weight_change': weight_change
                    }
                    results.append(result)
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to scrape {race_id}: {e}")
            return []
    
    def _parse_time(self, time_str: str) -> Optional[float]:
        """タイム文字列を秒に変換"""
        if not time_str:
            return None
        try:
            if ':' in time_str:
                parts = time_str.split(':')
                return float(parts[0]) * 60 + float(parts[1])
            return float(time_str)
        except:
            return None
    
    def get_collected_dates(self) -> set:
        """既に収集済みの日付を取得"""
        collected = set()
        if os.path.exists(OUTPUT_DIR):
            for f in os.listdir(OUTPUT_DIR):
                if f.startswith("nar_races_") and f.endswith(".parquet"):
                    # nar_races_20251225.parquet -> 20251225
                    date_str = f.replace("nar_races_", "").replace(".parquet", "")
                    if len(date_str) == 8:
                        collected.add(date_str)
        return collected
    
    def collect_date_range(self, start_date: str, end_date: str, delay: float = 0.3, force: bool = False):
        """指定期間のレース結果を収集（増分対応）
        
        Args:
            start_date: 開始日 (YYYYMMDD)
            end_date: 終了日 (YYYYMMDD)  
            delay: リクエスト間隔（秒）
            force: Trueの場合、既存データを上書き
        """
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        
        # 既に収集済みの日付
        collected_dates = self.get_collected_dates() if not force else set()
        logger.info(f"Already collected: {len(collected_dates)} dates")
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        current = start
        total_races = 0
        total_results = 0
        skipped_dates = 0
        
        while current <= end:
            date_str = current.strftime("%Y%m%d")
            
            # 既に収集済みならスキップ
            if date_str in collected_dates:
                logger.info(f"Skipping {date_str} (already collected)")
                skipped_dates += 1
                current += timedelta(days=1)
                continue
            
            logger.info(f"Processing {date_str}...")
            
            races = self.get_races_for_date(date_str)
            logger.info(f"  Found {len(races)} races")
            
            if not races:
                # レースがない日はスキップ（休催日など）
                current += timedelta(days=1)
                continue
            
            day_results = []
            for race in races:
                time.sleep(delay)
                results = self.scrape_race_result(race['race_id'])
                day_results.extend(results)
                total_races += 1
            
            # 日付ごとにparquet保存
            if day_results:
                self._save_daily_parquet(date_str, day_results)
                total_results += len(day_results)
                logger.info(f"  Saved {len(day_results)} results for {date_str}")
            
            current += timedelta(days=1)
        
        logger.info(f"Completed: {total_races} races, {total_results} results")
        logger.info(f"Skipped {skipped_dates} already collected dates")
    
    def _save_daily_parquet(self, date_str: str, results: List[Dict]):
        """日付ごとのparquetファイルを保存"""
        df = pd.DataFrame(results)
        
        # 数値型への変換
        numeric_cols = ['distance', 'heads', 'waku', 'umaban', 'time_sec', 'l3f_sec', 
                        'popularity', 'weight', 'weight_change']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        output_path = os.path.join(OUTPUT_DIR, f"nar_races_{date_str}.parquet")
        df.to_parquet(output_path, index=False)
    
    def merge_all_parquets(self, output_name: str = "nar_races_combined.parquet"):
        """全ての日付別parquetを1つに結合"""
        if not os.path.exists(OUTPUT_DIR):
            logger.warning("No parquet directory found")
            return
        
        parquet_files = sorted([
            os.path.join(OUTPUT_DIR, f) 
            for f in os.listdir(OUTPUT_DIR) 
            if f.startswith("nar_races_") and f.endswith(".parquet") and "combined" not in f
        ])
        
        if not parquet_files:
            logger.warning("No parquet files found")
            return
        
        logger.info(f"Merging {len(parquet_files)} parquet files...")
        
        dfs = [pd.read_parquet(f) for f in parquet_files]
        combined = pd.concat(dfs, ignore_index=True)
        
        # 重複除去（同じrace_id + umabanの組み合わせ）
        combined = combined.drop_duplicates(subset=['race_id', 'umaban'])
        
        output_path = os.path.join(OUTPUT_DIR, output_name)
        combined.to_parquet(output_path, index=False)
        
        logger.info(f"Merged {len(combined)} records to {output_path}")
        return output_path
    
    def save_to_parquet(self, output_path: str = None):
        """後方互換性のため残す（mergeを呼び出す）"""
        return self.merge_all_parquets()


def test_single_date():
    """単一日付でテスト"""
    collector = NarRaceCollector()
    
    # 12/25のレースを取得
    races = collector.get_races_for_date("20251225")
    print(f"Found {len(races)} races on 2025/12/25")
    
    if races:
        print("\nFirst 5 races:")
        for r in races[:5]:
            print(f"  {r['race_id']}: {r['venue_name']}")
        
        # 最初のレースの結果を取得
        print(f"\nScraping first race: {races[0]['race_id']}")
        results = collector.scrape_race_result(races[0]['race_id'])
        print(f"Got {len(results)} horse results")
        
        if results:
            print("\nFirst result:")
            for key in ['rank', 'horse_name', 'time_str', 'l3f_str', 'passing']:
                print(f"  {key}: {results[0].get(key)}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="NAR Race Collector")
    parser.add_argument("--test", action="store_true", help="Run test with single date")
    parser.add_argument("--start", type=str, default="20250101", help="Start date (YYYYMMDD)")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYYMMDD)")
    parser.add_argument("--delay", type=float, default=0.3, help="Delay between requests")
    parser.add_argument("--force", action="store_true", help="Force re-collect already collected dates")
    parser.add_argument("--merge-only", action="store_true", help="Only merge existing parquets, no scraping")
    
    args = parser.parse_args()
    
    if args.test:
        test_single_date()
    elif args.merge_only:
        collector = NarRaceCollector()
        collector.merge_all_parquets()
    else:
        if args.end is None:
            args.end = datetime.now().strftime("%Y%m%d")
        
        collector = NarRaceCollector()
        collector.collect_date_range(args.start, args.end, args.delay, args.force)
        collector.merge_all_parquets()

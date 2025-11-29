import pandas as pd
import requests
from bs4 import BeautifulSoup
import argparse
import os
from datetime import datetime, timedelta
import re
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pickle

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constants
DATA_DIR = r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data"
HTML_DIR = os.path.join(DATA_DIR, "raw", "html")
PARQUET_PATH = os.path.join(DATA_DIR, "parsed", "parquet", "races", "races.parquet")

# Venue Mapping (Netkeiba Code -> Name)
VENUE_MAP = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京", 
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉"
}
VENUE_NAME_TO_CODE = {v: k for k, v in VENUE_MAP.items()}

import numpy as np

class MetricCalculator:
    def __init__(self, reference_df: pd.DataFrame):
        self.reference_df = reference_df
        self.avg_time_lookup = {}
        self.avg_cond_time_lookup = {}
        self.std_3f_lookup = {} # Kept for fallback or reference if needed
        self.std_cond_3f_lookup = {} 
        self.grouped_data = {} # (venue, dist, surf) -> DataFrame
        self.std_time_cache = {} # (venue, dist, surf, date_str) -> val
        self.std_cond_time_cache = {} # (venue, dist, surf, cond, date_str) -> val
        
        self.regression_models = {} # (venue, dist, surf) -> (slope, intercept)
        self.regression_cond_models = {} # (venue, dist, surf, cond) -> (slope, intercept)
        
        self._prepare_lookups()

    def _prepare_lookups(self):
        """Pre-calculates static lookups and groups data."""
        logger.info("Preparing metric lookups...")
        
        # Filter for 1-3rd place once
        df_top3 = self.reference_df[self.reference_df['finish_position'].isin([1, 2, 3])].copy()
        
        # 1. Static Averages (Venue, Dist, Surf)
        avg_time_series = df_top3.groupby(['venue', 'distance_m', 'track_surface'])['finish_time_seconds'].mean()
        self.avg_time_lookup = avg_time_series.to_dict()
        
        # 2. Static Condition Averages (Venue, Dist, Surf, Cond)
        avg_cond_time_series = df_top3.groupby(['venue', 'distance_m', 'track_surface', 'track_condition'])['finish_time_seconds'].mean()
        self.avg_cond_time_lookup = avg_cond_time_series.to_dict()
        
        # 3. Group Data for Time-Dependent Queries & Regression
        for name, group in df_top3.groupby(['venue', 'distance_m', 'track_surface']):
            self.grouped_data[name] = group.sort_values('race_date')
            
            # Regression for Standard 3F
            # X = (Distance - 600) / (Time - 3F)
            # Y = 3F
            self._fit_regression(name, group, is_condition_specific=False)
            
            # Regression for Standard Condition 3F
            for cond, sub_group in group.groupby('track_condition'):
                cond_name = name + (cond,)
                self._fit_regression(cond_name, sub_group, is_condition_specific=True)
            
        logger.info("Lookups prepared.")

    def _fit_regression(self, key, group, is_condition_specific):
        """Fits a linear regression model for 3F prediction."""
        if len(group) < 2:
            return

        try:
            # Calculate X: Average speed of the first part
            # (Distance - 600) / (Finish Time - Last 3F)
            # Ensure no division by zero
            dist_minus_600 = group['distance_m'] - 600
            time_minus_3f = group['finish_time_seconds'] - group['last_3f_time']
            
            valid_mask = (time_minus_3f > 0) & (dist_minus_600 > 0)
            if not valid_mask.any():
                return

            X = (dist_minus_600[valid_mask] / time_minus_3f[valid_mask]).values
            Y = group.loc[valid_mask, 'last_3f_time'].values

            if len(X) < 2:
                return

            # Linear Regression: Y = aX + b
            slope, intercept = np.polyfit(X, Y, 1)
            
            if is_condition_specific:
                self.regression_cond_models[key] = (slope, intercept)
            else:
                self.regression_models[key] = (slope, intercept)
        except Exception as e:
            logger.warning(f"Regression failed for {key}: {e}")

    def get_avg_time(self, venue, dist, surf):
        return self.avg_time_lookup.get((venue, dist, surf))

    def get_avg_cond_time(self, venue, dist, surf, cond):
        return self.avg_cond_time_lookup.get((venue, dist, surf, cond))

    def predict_std_3f(self, venue, dist, surf, finish_time, last_3f):
        """Predicts Standard 3F based on regression."""
        key = (venue, dist, surf)
        model = self.regression_models.get(key)
        if not model:
            return None
        
        slope, intercept = model
        try:
            # Calculate X for the target horse
            dist_minus_600 = dist - 600
            time_minus_3f = finish_time - last_3f
            if time_minus_3f <= 0:
                return None
            
            X = dist_minus_600 / time_minus_3f
            predicted_3f = slope * X + intercept
            return predicted_3f
        except:
            return None

    def predict_std_cond_3f(self, venue, dist, surf, cond, finish_time, last_3f):
        """Predicts Standard Condition 3F based on regression."""
        key = (venue, dist, surf, cond)
        model = self.regression_cond_models.get(key)
        if not model:
            return None
        
        slope, intercept = model
        try:
            dist_minus_600 = dist - 600
            time_minus_3f = finish_time - last_3f
            if time_minus_3f <= 0:
                return None
            
            X = dist_minus_600 / time_minus_3f
            predicted_3f = slope * X + intercept
            return predicted_3f
        except:
            return None

    def get_std_time(self, venue, dist, surf, date_obj):
        """Calculates Standard Time (Avg of +/- 3 days) with caching."""
        date_str = date_obj.strftime('%Y%m%d')
        key = (venue, dist, surf, date_str)
        if key in self.std_time_cache:
            return self.std_time_cache[key]
        
        group = self.grouped_data.get((venue, dist, surf))
        if group is None or group.empty:
            self.std_time_cache[key] = None
            return None
            
        # Filter by date range
        start_date = date_obj - timedelta(days=3)
        end_date = date_obj + timedelta(days=3)
        
        # Since group is sorted, we could use searchsorted, but boolean mask is fast enough on small groups
        mask = (group['race_date'] >= start_date) & (group['race_date'] <= end_date)
        sub_group = group[mask]
        
        if sub_group.empty:
            val = None
        else:
            val = sub_group['finish_time_seconds'].mean()
        
        self.std_time_cache[key] = val
        return val

    def get_std_cond_time(self, venue, dist, surf, cond, date_obj):
        """Calculates Standard Condition Time with caching."""
        date_str = date_obj.strftime('%Y%m%d')
        key = (venue, dist, surf, cond, date_str)
        if key in self.std_cond_time_cache:
            return self.std_cond_time_cache[key]
        
        # We can reuse the group from get_std_time logic, but we need to filter by condition too.
        # It's cleaner to just do the filter here.
        group = self.grouped_data.get((venue, dist, surf))
        if group is None or group.empty:
            self.std_cond_time_cache[key] = None
            return None
            
        start_date = date_obj - timedelta(days=3)
        end_date = date_obj + timedelta(days=3)
        
        mask = (group['race_date'] >= start_date) & (group['race_date'] <= end_date) & (group['track_condition'] == cond)
        sub_group = group[mask]
        
        if sub_group.empty:
            val = None
        else:
            val = sub_group['finish_time_seconds'].mean()
            
        self.std_cond_time_cache[key] = val
        return val

class NetkeibaAnalyzer:
    def __init__(self, target_date: str, venue_name: str):
        self.target_date = target_date  # YYYYMMDD
        self.venue_name = venue_name
        self.venue_code = VENUE_NAME_TO_CODE.get(venue_name)
        if not self.venue_code:
            raise ValueError(f"Unknown venue name: {venue_name}")
        
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        
        self.driver = None
        self.reference_df = None
        self.metric_calculator = None
        self.load_reference_data()
        self.load_stats_lookup()

    def init_driver(self):
        """Initializes Selenium WebDriver."""
        if self.driver:
            return
        logger.info("Initializing Selenium Driver...")
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--log-level=3")
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(30)

    def close_driver(self):
        if self.driver:
            self.driver.quit()
            self.driver = None

    def load_reference_data(self):
        """Loads races.parquet for metric calculations."""
        logger.info(f"Loading reference data from {PARQUET_PATH}...")
        if not os.path.exists(PARQUET_PATH):
            logger.warning(f"Reference data not found at {PARQUET_PATH}. Metrics requiring it will be NaN.")
            return

        try:
            # Optimization: Load only required columns
            required_cols = [
                'race_id', 'horse_id', 'trainer_name', # Added for Stable lookup
                'race_date', 'venue', 'distance_m', 'track_surface', 
                'track_condition', 'finish_position', 'finish_time_seconds', 'last_3f_time'
            ]
            
            df = pd.read_parquet(PARQUET_PATH, columns=required_cols)
            df['race_date'] = pd.to_datetime(df['race_date'])
            # Ensure IDs are strings for lookup
            df['race_id'] = df['race_id'].astype(str)
            df['horse_id'] = df['horse_id'].astype(str)
            
            self.reference_df = df
            logger.info(f"Loaded {len(df)} rows of reference data.")
            
            # Create Trainer Lookup: (race_id, horse_id) -> trainer_name
            self.trainer_lookup = df.set_index(['race_id', 'horse_id'])['trainer_name'].to_dict()
            
            # Initialize Metric Calculator
            self.metric_calculator = MetricCalculator(df)
            
        except Exception as e:
            logger.error(f"Failed to load reference data: {e}")

    def scrape_race_list(self) -> List[str]:
        """Scrapes the race list for the target date and venue to get race_ids using Selenium."""
        self.init_driver()
        url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={self.target_date}"
        logger.info(f"Scraping race list from {url}")
        
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "RaceList_Data"))
            )
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            race_ids = []
            prefix = f"{self.target_date[:4]}{self.venue_code}"
            
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                rid = None
                if 'race_id=' in href:
                    rid = href.split('race_id=')[1].split('&')[0]
                elif '/race/' in href:
                    parts = href.split('/')
                    for p in parts:
                        if p.isdigit() and len(p) == 12:
                            rid = p
                            break
                
                if rid and rid.startswith(prefix):
                    # Extract Race Name
                    name = link.get_text(strip=True)
                    
                    # Check if already added
                    if not any(r['id'] == rid for r in race_ids):
                        race_ids.append({'id': rid, 'name': name})
            
            race_ids.sort(key=lambda x: x['id'])
            logger.info(f"Found {len(race_ids)} races for {self.venue_name} ({self.venue_code}) on {self.target_date}")
            return race_ids
            
        except Exception as e:
            logger.error(f"Error scraping race list: {e}")
            return []

    def restart_driver(self):
        """Restarts the Selenium driver."""
        logger.info("Restarting Selenium Driver...")
        self.close_driver()
        self.init_driver()

    def scrape_shutuba(self, race_id: str) -> List[Dict]:
        """Scrapes the Shutuba table for a given race_id using Selenium."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.init_driver()
                url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
                logger.info(f"Scraping shutuba for {race_id} (Attempt {attempt+1}/{max_retries})")
                
                try:
                    self.driver.get(url)
                except Exception:
                    # If page load times out, stop loading and try to parse anyway
                    logger.warning(f"Page load timed out for {race_id}, stopping load and proceeding...")
                    self.driver.execute_script("window.stop();")
                
                # Wait for RaceName first as it should be present
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "RaceName"))
                )
                
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                race_name_elem = soup.select_one('.RaceName')
                race_name = race_name_elem.text.strip() if race_name_elem else "Unknown Race"
                
                # Check for Shinba (New Horse) immediately
                if '新馬' in race_name:
                    logger.info(f"Skipping Race {race_id} ({race_name}) - New Horse Race")
                    return []

                # Now wait for HorseList
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "HorseList"))
                )
                
                horses = []
                rows = soup.select('tr.HorseList')
                
                race_meta = soup.select_one('.RaceData01')
                race_meta_text = race_meta.text.strip() if race_meta else ""
                course_info = ""
                if race_meta_text:
                    course_info = race_meta_text.split('/')[0].strip()

                for row in rows:
                    horse_data = {
                        'race_id': race_id,
                        'race_name': race_name,
                        'course_info': course_info,
                        'date': self.target_date
                    }
                    
                    waku_elem = row.select_one('td[class^="Waku"]')
                    horse_data['枠番'] = waku_elem.text.strip() if waku_elem else ""
                    
                    umaban_elem = row.select_one('td[class^="Umaban"]')
                    horse_data['馬番'] = umaban_elem.text.strip() if umaban_elem else ""
                    
                    name_elem = row.select_one('.HorseName a')
                    if name_elem:
                        horse_data['馬名'] = name_elem.text.strip()
                        href = name_elem['href']
                        if '/horse/' in href:
                             horse_data['horse_id'] = href.split('/horse/')[-1].replace('/', '')
                    
                    jockey_elem = row.select_one('.Jockey a')
                    horse_data['騎手'] = jockey_elem.text.strip() if jockey_elem else ""
                    
                    trainer_elem = row.select_one('.Trainer a')
                    horse_data['厩舎'] = trainer_elem.text.strip() if trainer_elem else ""
                    
                    tds = row.find_all('td')
                    if len(tds) >= 6:
                        horse_data['性齢'] = tds[4].text.strip()
                        horse_data['斤量'] = tds[5].text.strip()

                    horses.append(horse_data)
                return horses

            except Exception as e:
                logger.error(f"Error scraping shutuba for {race_id} (Attempt {attempt+1}): {e}")
                self.restart_driver()
                time.sleep(2)
        
        return []

    def get_horse_history(self, horse_id: str) -> List[Dict]:
        """Gets past performance for a horse."""
        local_path = os.path.join(HTML_DIR, "horse", f"{horse_id}_perf.bin")
        html_content = None
        
        if os.path.exists(local_path):
            try:
                with open(local_path, 'rb') as f:
                    html_content = f.read()
            except Exception as e:
                logger.error(f"Error reading local file {local_path}: {e}")
        
        if not html_content:
            url = f"https://db.netkeiba.com/horse/result/{horse_id}/"
            try:
                resp = self.session.get(url)
                resp.encoding = 'euc-jp'
                html_content = resp.content
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Error scraping history for {horse_id}: {e}")
                return []

        return self.parse_horse_history(html_content, horse_id)

    def parse_horse_history(self, html_content: bytes, horse_id: str) -> List[Dict]:
        """Parses the horse result table with advanced column extraction."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find('table', class_='db_h_race_results')
            if not table:
                return []
            
            history = []
            rows = table.find('tbody').find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 28: # Ensure enough columns
                    continue
                
                data = [c.text.strip() for c in cols]
                
                race_id_full = ""
                race_link = cols[4].find('a')
                if race_link and 'race/' in race_link['href']:
                     race_id_full = race_link['href'].split('/race/')[-1].replace('/', '')

                kai = ""
                day = ""
                race_no = data[3] # R
                if race_id_full and len(race_id_full) == 12:
                    kai = str(int(race_id_full[6:8]))
                    day = str(int(race_id_full[8:10]))
                
                # Corrected Indices based on debug output
                # 16: 馬場, 18: タイム, 19: 着差, 21: 通過, 22: ペース, 23: 上り, 24: 馬体重
                
                passing = data[21]
                p1, p2, p3, p4 = "", "", "", ""
                if passing:
                    parts = passing.split('-')
                    if len(parts) >= 1: p1 = parts[0]
                    if len(parts) >= 2: p2 = parts[1]
                    if len(parts) >= 3: p3 = parts[2]
                    if len(parts) >= 4: p4 = parts[3]
                
                pace = data[22]
                pace1, pace2 = "", ""
                if pace and '-' in pace:
                    pp = pace.split('-')
                    if len(pp) == 2:
                        pace1, pace2 = pp[0], pp[1]
                
                weight_raw = data[24]
                weight = ""
                change = ""
                if weight_raw:
                    match = re.match(r'(\d+)\((.+)\)', weight_raw)
                    if match:
                        weight = match.group(1)
                        change = match.group(2)
                    else:
                        weight = weight_raw

                # Lookup Trainer
                trainer = ""
                if hasattr(self, 'trainer_lookup') and race_id_full:
                     key = (str(race_id_full), str(horse_id))
                     trainer = self.trainer_lookup.get(key, "")
                     # Debug logging for first few lookups
                     if not trainer and len(history) < 3:
                         logger.info(f"Lookup failed for {key}. Sample keys: {list(self.trainer_lookup.keys())[:3]}")

                race_data = {
                    '日付': data[0],
                    '場所': "".join([c for c in data[1] if not c.isdigit()]),
                    '回': kai,
                    '日': day,
                    'ｺｰｽ': "芝" if "芝" in data[14] else "ダート" if "ダ" in data[14] else "障害" if "障" in data[14] else "",
                    '距離': re.search(r'\d+', data[14]).group() if re.search(r'\d+', data[14]) else "",
                    'R': race_no,
                    '馬場': data[16],
                    '天気': data[2],
                    '頭数': data[6],
                    '枠番': data[7],
                    '馬番': data[8],
                    '馬名': "",
                    '斤量': data[13],
                    '騎手': data[12],
                    '厩舎': trainer,
                    'ﾀｲﾑ': data[18],
                    '着差': data[19],
                    '人気': data[10],
                    'ｵｯｽﾞ': data[9],
                    '上り': data[23],
                    'ﾍﾟｰｽ1': pace1,
                    'ﾍﾟｰｽ2': pace2,
                    '通過': passing,
                    '1C': p1, '2C': p2, '3C': p3, '4C': p4,
                    '着順': data[11],
                    '馬体重': weight,
                    '増減': change,
                    'レース名': data[4],
                    '性': "",
                    '年齢': "",
                    'horse_id': "",
                    'race_id': race_id_full
                }
                history.append(race_data)
                
            return history
        except Exception as e:
            logger.error(f"Error parsing history: {e}")
            return []

    def load_stats_lookup(self):
        """Load pre-calculated statistics lookup table."""
        path = r"keibaai\data\processed\stats_lookup.pkl"
        if os.path.exists(path):
            try:
                with open(path, 'rb') as f:
                    self.stats_lookup = pickle.load(f)
                logger.info(f"Loaded stats lookup with {len(self.stats_lookup)} conditions.")
            except Exception as e:
                logger.error(f"Failed to load stats lookup: {e}")
                self.stats_lookup = {}
        else:
            logger.warning("stats_lookup.pkl not found. Statistical metrics will be empty.")
            self.stats_lookup = {}

    def calculate_metrics(self, history: List[Dict]) -> List[Dict]:
        """Calculates statistical metrics for each race in history."""
        if not hasattr(self, 'stats_lookup'):
            self.load_stats_lookup()

        for race in history:
            venue = race.get('場所', '')
            dist_str = race.get('距離', '')
            cond = race.get('馬場', '')
            time_str = race.get('ﾀｲﾑ', '')
            l3f_str = race.get('上り', '')
            pace_str = race.get('ﾍﾟｰｽ1', '')
            
            # Parse Distance and Surface
            surface = race.get('ｺｰｽ', '')
            
            try:
                distance = int(dist_str) if dist_str.isdigit() else 0
            except:
                distance = 0
            
            # Parse Time
            def parse_time(t_str):
                try:
                    if not t_str: return None
                    parts = t_str.split(':')
                    if len(parts) == 2:
                        return float(parts[0]) * 60 + float(parts[1])
                    return float(t_str)
                except:
                    return None
            
            time_sec = parse_time(time_str)
            l3f_sec = parse_time(l3f_str)
            
            # Initialize metrics with None
            for col in ['タイム指数', '上り指数', '馬場差', 'RPCI',
                       '平均t', '平均場t', '基準t', '基準場t', '基準3F', '基準場3F', 
                       '平t差', '平場t差', '基t差', '基場t差', '基3F差', '基場3F差']:
                race[col] = None
            
            # Parse Date
            try:
                date_obj = pd.to_datetime(race.get('日付', ''))
            except:
                date_obj = None

            # Map abbreviated condition to full string
            cond_map = {"良": "良", "稍": "稍重", "重": "重", "不": "不良"}
            full_cond = cond_map.get(cond, cond)

            # --- Calculate Standard/Average Metrics using MetricCalculator ---
            if self.metric_calculator and date_obj and distance > 0 and time_sec:
                # 1. Average Time (Static)
                avg_t = self.metric_calculator.get_avg_time(venue, distance, surface)
                if avg_t:
                    race['平均t'] = round(avg_t, 1)
                    race['平t差'] = round(time_sec - avg_t, 1)

                # 2. Average Condition Time (Static)
                avg_cond_t = self.metric_calculator.get_avg_cond_time(venue, distance, surface, full_cond)
                if avg_cond_t:
                    race['平均場t'] = round(avg_cond_t, 1)
                    race['平場t差'] = round(time_sec - avg_cond_t, 1)

                # 3. Standard Time (Dynamic - +/- 3 days)
                std_t = self.metric_calculator.get_std_time(venue, distance, surface, date_obj)
                if std_t:
                    race['基準t'] = round(std_t, 1)
                    race['基t差'] = round(time_sec - std_t, 1)
                
                # Calculate Track Bias (馬場差) = Standard Time - Average Time
                if avg_t and std_t:
                    race['馬場差'] = round(std_t - avg_t, 1)

                # 4. Standard Condition Time (Dynamic)
                std_cond_t = self.metric_calculator.get_std_cond_time(venue, distance, surface, full_cond, date_obj)
                if std_cond_t:
                    race['基準場t'] = round(std_cond_t, 1)
                    race['基場t差'] = round(time_sec - std_cond_t, 1)
                
                # 5. Standard 3F (Regression)
                if l3f_sec:
                    std_3f = self.metric_calculator.predict_std_3f(venue, distance, surface, time_sec, l3f_sec)
                    if std_3f:
                        race['基準3F'] = round(std_3f, 1)
                        race['基3F差'] = round(l3f_sec - std_3f, 1)
                        
                    std_cond_3f = self.metric_calculator.predict_std_cond_3f(venue, distance, surface, full_cond, time_sec, l3f_sec)
                    if std_cond_3f:
                        race['基準場3F'] = round(std_cond_3f, 1)
                        race['基場3F差'] = round(l3f_sec - std_cond_3f, 1)

            # Lookup Stats (Time Index, L3F Index, RPCI)
            key = (venue, distance, surface, full_cond)
            stats = self.stats_lookup.get(key)
            
            if stats and time_sec:
                # Time Index: 50 + 10 * (Mean - Time) / Std
                mean = stats['time_mean']
                std = stats['time_std']
                if std > 0:
                    idx = 50 + 10 * (mean - time_sec) / std
                    race['タイム指数'] = round(idx, 1)
                    
            if stats and l3f_sec:
                # L3F Index
                mean_3f = stats['l3f_mean']
                std_3f = stats['l3f_std']
                if std_3f > 0:
                    idx_3f = 50 + 10 * (mean_3f - l3f_sec) / std_3f
                    race['上り指数'] = round(idx_3f, 1)

            # RPCI: (First 3F / Last 3F) * 50
            if pace_str and l3f_sec:
                try:
                    first_3f = float(pace_str)
                    if first_3f > 0 and l3f_sec > 0:
                        rpci = (first_3f / l3f_sec) * 50
                        race['RPCI'] = round(rpci, 1)
                except:
                    pass
                    
        return history

    def run(self):
        try:
            races = self.scrape_race_list()
            if not races:
                logger.error("No races found.")
                return

            all_races_data = {}

            for race_info in races:
                race_id = race_info['id']
                race_name = race_info['name']
                
                # Skip Shinba (New Horse) races EARLY
                if '新馬' in race_name:
                    logger.info(f"Skipping Race {race_id} ({race_name}) - New Horse Race (Skipped before scraping)")
                    continue
                
                logger.info(f"Processing Race {race_id} ({race_name})...")
                shutuba = self.scrape_shutuba(race_id)
                
                if not shutuba:
                    continue
                
                # --- 1. Collect all history first for Grouping ---
                race_horses_data = [] 
                all_history_entries = [] 
                
                for horse in shutuba:
                    horse_id = horse.get('horse_id')
                    if not horse_id:
                        continue
                        
                    history = self.get_horse_history(horse_id)
                    history = self.calculate_metrics(history)
                    
                    race_horses_data.append({
                        'horse_info': horse,
                        'history': history
                    })
                    
                    for i, h_row in enumerate(history):
                        all_history_entries.append({
                            'horse_id': horse_id,
                            'row': h_row,
                            'date': pd.to_datetime(h_row['日付']),
                            'venue': h_row['場所'],
                            'dist': h_row['距離'],
                            'surf': h_row['馬場']
                        })

                # --- 2. Calculate GroupNo (Clustering) ---
                grouped_entries = {}
                for entry in all_history_entries:
                    key = (entry['venue'], entry['dist'])
                    if key not in grouped_entries:
                        grouped_entries[key] = []
                    grouped_entries[key].append(entry)
                
                next_group_id = 1
                
                for key, entries in grouped_entries.items():
                    entries.sort(key=lambda x: x['date'])
                    
                    if not entries:
                        continue
                        
                    current_cluster = [entries[0]]
                    
                    for i in range(1, len(entries)):
                        prev = entries[i-1]
                        curr = entries[i]
                        diff = (curr['date'] - prev['date']).days
                        
                        if diff <= 3:
                            current_cluster.append(curr)
                        else:
                            unique_horses = set(e['horse_id'] for e in current_cluster)
                            if len(unique_horses) >= 2:
                                for e in current_cluster:
                                    e['row']['グループNo'] = next_group_id
                                next_group_id += 1
                            current_cluster = [curr]
                    
                    unique_horses = set(e['horse_id'] for e in current_cluster)
                    if len(unique_horses) >= 2:
                        for e in current_cluster:
                            e['row']['グループNo'] = next_group_id
                        next_group_id += 1

                # --- 3. Finalize Data & Jockey Change ---
                race_data_list = []
                target_date_obj = pd.to_datetime(self.target_date)
                
                for item in race_horses_data:
                    horse = item['horse_info']
                    history = item['history']
                    horse_id = horse.get('horse_id')
                    horse_name = horse.get('馬名', 'Unknown')
                    current_jockey = horse.get('騎手', '')
                    
                    prev_jockey = ""
                    for h_row in history:
                        h_date = pd.to_datetime(h_row['日付'])
                        if h_date < target_date_obj:
                            prev_jockey = h_row.get('騎手', '')
                            break
                    
                    jockey_change = "〇" if prev_jockey and current_jockey != prev_jockey else "-"
                    
                    for h_row in history:
                        h_row['馬名'] = horse_name
                        h_row['horse_id'] = horse_id
                        h_row['出走枠番'] = horse.get('枠番', '')
                        h_row['出走馬番'] = horse.get('馬番', '')
                        h_row['乗り替わり'] = jockey_change
                        if 'グループNo' not in h_row:
                            h_row['グループNo'] = ""
                        
                        current_sex_age = horse.get('性齢', '')
                        if current_sex_age and len(current_sex_age) >= 2:
                            sex = current_sex_age[0]
                            try:
                                current_age = int(re.search(r'\d+', current_sex_age).group())
                                race_date = pd.to_datetime(h_row['日付'])
                                year_diff = target_date_obj.year - race_date.year
                                hist_age = current_age - year_diff
                                h_row['性'] = sex
                                h_row['年齢'] = str(hist_age) if hist_age > 0 else ""
                            except:
                                pass

                    race_data_list.append(item)
                
                all_races_data[race_id] = race_data_list
            
            self.generate_excel(all_races_data)
        finally:
            self.close_driver()

    def generate_excel(self, all_data: Dict):
        output_file = f"race_analysis_{self.target_date}_{self.venue_name}.xlsx"
        logger.info(f"Generating Excel: {output_file}")
        
        columns = [
            '出走枠番', '出走馬番', '乗り替わり', 'グループNo',
            '日付', '場所', '回', '日', 'ｺｰｽ', '距離', 'R', '馬場', '天気', '頭数', 
            '枠番', '馬番', '馬名', '斤量', '騎手', '厩舎', 'ﾀｲﾑ', '着差', '人気', 'ｵｯｽﾞ', 
            '上り', 'ﾍﾟｰｽ1', 'ﾍﾟｰｽ2', '通過', '1C', '2C', '3C', '4C', '着順', '馬体重', '増減', 
            'レース名', '性', '年齢', 
            '平均t', '平均場t', '基準t', '基準場t', '基準3F', '基準場3F', 
            '平t差', '平場t差', '基t差', '基場t差', '基3F差', '基場3F差', 
            'タイム指数', '上り指数', '馬場差', 'RPCI',
            'horse_id', 'race_id'
        ]
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for race_id, horses_data in all_data.items():
                sheet_name = f"Race_{race_id[-2:]}"
                rows = []
                
                # Header Row
                rows.append(columns)
                
                for h_data in horses_data:
                    for hist in h_data['history']:
                        row_vals = [hist.get(col, '') for col in columns]
                        rows.append(row_vals)
                
                df_out = pd.DataFrame(rows[1:], columns=rows[0])
                df_out.to_excel(writer, sheet_name=sheet_name, index=False)
        
        logger.info(f"Excel report saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Generate Netkeiba Race Analysis Excel")
    parser.add_argument("--date", required=True, help="Target Date (YYYYMMDD), e.g., 20251130")
    parser.add_argument("--venue", required=True, help="Venue Name, e.g., 東京")
    
    args = parser.parse_args()
    
    analyzer = NetkeibaAnalyzer(args.date, args.venue)
    analyzer.run()

if __name__ == "__main__":
    main()

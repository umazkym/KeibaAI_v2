#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAR スクレイピングデバッグスクリプト

nar.netkeiba.com のHTML構造を詳細に分析する
"""

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json

def init_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument("--log-level=3")
    options.page_load_strategy = 'eager'
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver

def debug_shutuba_page():
    """出馬表ページの構造を分析"""
    race_id = "202544122601"  # 大井 12/26 1R
    url = f"https://nar.netkeiba.com/race/shutuba.html?race_id={race_id}"
    
    print(f"=== Analyzing: {url} ===\n")
    
    driver = init_driver()
    try:
        driver.get(url)
        time.sleep(3)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # 1. レース情報の取得
        print("=== RACE INFO ===")
        race_name = soup.select_one('.RaceName')
        print(f"RaceName: {race_name.text.strip() if race_name else 'NOT FOUND'}")
        
        race_data01 = soup.select_one('.RaceData01')
        print(f"RaceData01: {race_data01.text.strip() if race_data01 else 'NOT FOUND'}")
        
        race_data02 = soup.select_one('.RaceData02')
        if race_data02:
            spans = race_data02.find_all('span')
            print(f"RaceData02 spans: {[s.text.strip() for s in spans]}")
        
        # 2. 馬リストの取得
        print("\n=== HORSE LIST ===")
        rows = soup.select('tr.HorseList')
        print(f"Found {len(rows)} horses\n")
        
        if rows:
            first_row = rows[0]
            print("First row HTML structure:")
            print(first_row.prettify()[:2000])
            
            print("\n=== Parsing First Horse ===")
            
            # 枠番
            waku = first_row.select_one('td[class^="Waku"]')
            print(f"Waku: {waku.text.strip() if waku else 'NOT FOUND'}")
            
            # 馬番
            umaban = first_row.select_one('td[class^="Umaban"]')
            print(f"Umaban: {umaban.text.strip() if umaban else 'NOT FOUND'}")
            
            # 馬名
            name_link = first_row.select_one('.HorseName a')
            print(f"Horse Name: {name_link.text.strip() if name_link else 'NOT FOUND'}")
            if name_link and 'href' in name_link.attrs:
                horse_id = name_link['href'].split('/horse/')[-1].replace('/', '')
                print(f"Horse ID: {horse_id}")
            
            # 騎手
            jockey = first_row.select_one('.Jockey a')
            print(f"Jockey: {jockey.text.strip() if jockey else 'NOT FOUND'}")
            
            # 調教師
            trainer = first_row.select_one('.Trainer a')
            print(f"Trainer: {trainer.text.strip() if trainer else 'NOT FOUND'}")
            
            # 全セル
            tds = first_row.find_all('td')
            print(f"\nAll TD values ({len(tds)} cells):")
            for i, td in enumerate(tds):
                text = td.get_text(strip=True)[:30]
                classes = td.get('class', [])
                print(f"  [{i}] class={classes}: '{text}'")
            
            # オッズ・人気
            print("\n=== ODDS & POPULARITY ===")
            odds_span = first_row.select_one('span[id^="odds-"]')
            print(f"Odds span: {odds_span.text if odds_span else 'NOT FOUND'}")
            
            ninki_span = first_row.select_one('span[id^="ninki-"]')
            print(f"Ninki span: {ninki_span.text if ninki_span else 'NOT FOUND'}")
            
            # Popular class elements
            pop_divs = first_row.select('.Popular')
            print(f"Popular divs: {[p.get('class') for p in pop_divs]}")
            
    finally:
        driver.quit()

def debug_horse_history():
    """馬の過去成績ページの構造を分析"""
    horse_id = "2023104929"  # サンプルの馬ID
    url = f"https://db.netkeiba.com/horse/result/{horse_id}/"
    
    print(f"\n\n=== Analyzing Horse History: {url} ===\n")
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    
    resp = session.get(url, timeout=10)
    resp.encoding = 'euc-jp'
    
    soup = BeautifulSoup(resp.content, 'html.parser')
    
    table = soup.find('table', class_='db_h_race_results')
    if not table:
        print("Table NOT FOUND!")
        return
    
    print("=== TABLE HEADERS ===")
    headers = table.find('thead')
    if headers:
        ths = headers.find_all('th')
        for i, th in enumerate(ths):
            print(f"  [{i}] {th.text.strip()}")
    
    print("\n=== FIRST ROW DATA ===")
    tbody = table.find('tbody')
    if tbody:
        first_row = tbody.find('tr')
        if first_row:
            tds = first_row.find_all('td')
            for i, td in enumerate(tds):
                text = td.get_text(strip=True)[:40]
                link = td.find('a')
                if link and 'href' in link.attrs:
                    href = link['href']
                    print(f"  [{i}] '{text}' (link: {href[:50]}...)")
                else:
                    print(f"  [{i}] '{text}'")

if __name__ == "__main__":
    debug_shutuba_page()
    debug_horse_history()

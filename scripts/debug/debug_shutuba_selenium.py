#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Seleniumでオッズを取得（動的読み込み対応）"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

url = 'https://race.netkeiba.com/race/shutuba.html?race_id=202506050401'

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')
options.page_load_strategy = 'eager'

driver = webdriver.Chrome(options=options)
driver.set_page_load_timeout(30)

try:
    driver.get(url)
    
    # オッズが読み込まれるまで待機
    time.sleep(3)
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    rows = soup.select('tr.HorseList')
    print(f'Found {len(rows)} horses')
    
    for i, row in enumerate(rows[:5]):
        name_elem = row.select_one('.HorseName a')
        odds_elem = row.select_one('span[id^="odds-"]')
        ninki_elem = row.select_one('span[id^="ninki-"]')
        
        name = name_elem.text.strip() if name_elem else "N/A"
        odds = odds_elem.text.strip() if odds_elem else "N/A"
        ninki = ninki_elem.text.strip() if ninki_elem else "N/A"
        
        print(f'{i+1}. {name}: オッズ={odds}, 人気={ninki}')

finally:
    driver.quit()

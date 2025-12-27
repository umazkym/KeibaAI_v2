#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAR レース結果スクレイピング可能性調査

1. レース一覧ページの構造確認
2. レース結果ページの構造確認
3. 必要なデータが取得可能か検証
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time

def get_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    return session

def check_race_list_page():
    """日付別レース一覧ページの確認"""
    session = get_session()
    
    # 2025年12月25日のレース一覧
    date = "20251225"
    url = f"https://nar.netkeiba.com/top/race_list.html?kaisai_date={date}"
    
    print(f"=== Checking Race List Page ===")
    print(f"URL: {url}\n")
    
    try:
        resp = session.get(url, timeout=15)
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # 開催一覧を取得
        race_lists = soup.select('.RaceList_DataList')
        print(f"Found {len(race_lists)} venue sections")
        
        all_races = []
        for venue_section in race_lists:
            # 各レースへのリンクを取得
            links = venue_section.select('a[href*="race_id="]')
            for link in links:
                href = link.get('href', '')
                if 'race_id=' in href:
                    race_id = href.split('race_id=')[1].split('&')[0]
                    race_name = link.get_text(strip=True)
                    all_races.append({'id': race_id, 'name': race_name})
        
        print(f"Found {len(all_races)} races total")
        if all_races[:5]:
            print("Sample races:")
            for r in all_races[:5]:
                print(f"  {r['id']}: {r['name']}")
        
        return all_races
        
    except Exception as e:
        print(f"Error: {e}")
        return []

def check_race_result_page(race_id: str):
    """レース結果ページの構造確認"""
    session = get_session()
    
    # レース結果ページ
    url = f"https://nar.netkeiba.com/race/result.html?race_id={race_id}"
    
    print(f"\n=== Checking Race Result Page ===")
    print(f"URL: {url}\n")
    
    try:
        resp = session.get(url, timeout=15)
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # レース情報
        race_name = soup.select_one('.RaceName')
        print(f"Race Name: {race_name.text.strip() if race_name else 'NOT FOUND'}")
        
        race_data01 = soup.select_one('.RaceData01')
        print(f"RaceData01: {race_data01.text.strip()[:100] if race_data01 else 'NOT FOUND'}")
        
        race_data02 = soup.select_one('.RaceData02')
        if race_data02:
            spans = [s.text.strip() for s in race_data02.find_all('span')]
            print(f"RaceData02: {spans}")
        
        # 結果テーブル
        result_table = soup.select_one('table.RaceTable01')
        if result_table:
            rows = result_table.select('tr')
            print(f"\nResult table rows: {len(rows)}")
            
            # ヘッダー確認
            headers = result_table.select('th')
            if headers:
                print(f"Headers: {[h.text.strip() for h in headers]}")
            
            # 最初のデータ行
            data_rows = result_table.select('tr.HorseList')
            if data_rows:
                first_row = data_rows[0]
                tds = first_row.find_all('td')
                print(f"\nFirst row ({len(tds)} columns):")
                for i, td in enumerate(tds[:15]):  # 最初の15列のみ
                    text = td.get_text(strip=True)[:30]
                    print(f"  [{i}] {text}")
                
                # タイム、上りなどの重要データ確認
                print(f"\nImportant data check:")
                print(f"  着順: {tds[0].text.strip() if len(tds) > 0 else 'N/A'}")
                print(f"  馬番: {tds[2].text.strip() if len(tds) > 2 else 'N/A'}")
                
                # タイムを探す
                for i, td in enumerate(tds):
                    text = td.get_text(strip=True)
                    if ':' in text and '.' in text:  # タイム形式
                        print(f"  タイム (col {i}): {text}")
                        break
                
                return True
        else:
            print("Result table NOT FOUND")
            
            # 別のテーブル形式を探す
            tables = soup.find_all('table')
            print(f"Found {len(tables)} tables on page")
            
        return False
        
    except Exception as e:
        print(f"Error: {e}")
        return False

def check_db_result_page(race_id: str):
    """db.netkeiba.comのレース結果ページ確認（より詳細なデータ）"""
    session = get_session()
    
    url = f"https://db.netkeiba.com/race/{race_id}/"
    
    print(f"\n=== Checking DB Race Result Page ===")
    print(f"URL: {url}\n")
    
    try:
        resp = session.get(url, timeout=15)
        resp.encoding = 'euc-jp'
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # レース情報
        race_name = soup.select_one('.racedata fc h1')
        if not race_name:
            race_name = soup.find('h1')
        print(f"Race Name: {race_name.text.strip() if race_name else 'NOT FOUND'}")
        
        # 結果テーブル
        result_table = soup.select_one('table.race_table_01')
        if result_table:
            rows = result_table.select('tr')
            print(f"Result table rows: {len(rows)}")
            
            # ヘッダー確認
            header_row = result_table.find('tr')
            if header_row:
                headers = header_row.find_all('th')
                print(f"Headers ({len(headers)}): {[h.text.strip() for h in headers]}")
            
            # データ行
            data_rows = result_table.find_all('tr')[1:]  # ヘッダー除く
            if data_rows:
                first_row = data_rows[0]
                tds = first_row.find_all('td')
                print(f"\nFirst data row ({len(tds)} columns):")
                for i, td in enumerate(tds):
                    text = td.get_text(strip=True)[:30]
                    print(f"  [{i}] {text}")
                    
                return True
        else:
            print("race_table_01 NOT FOUND")
            
        return False
        
    except Exception as e:
        print(f"Error: {e}")
        return False

def estimate_data_volume():
    """データ量の見積もり"""
    print("\n=== Data Volume Estimation ===")
    
    # 2025年1月から現在までの開催日数を概算
    # 地方競馬は基本毎日どこかで開催
    days = (datetime.now() - datetime(2025, 1, 1)).days
    
    # 1日あたりの平均レース数（複数会場の合計）
    avg_races_per_day = 60  # 推定
    
    total_races = days * avg_races_per_day
    
    print(f"Days since Jan 1, 2025: {days}")
    print(f"Avg races per day: {avg_races_per_day}")
    print(f"Estimated total races: {total_races:,}")
    
    # スクレイピング時間見積もり
    time_per_race = 0.5  # 秒
    total_time_sec = total_races * time_per_race
    total_time_min = total_time_sec / 60
    total_time_hour = total_time_min / 60
    
    print(f"\nScraping time estimate:")
    print(f"  Per race: {time_per_race}s")
    print(f"  Total: {total_time_min:.0f} min ({total_time_hour:.1f} hours)")

if __name__ == "__main__":
    # 1. レース一覧ページの確認
    races = check_race_list_page()
    
    # 2. レース結果ページの確認（最初のレースで）
    if races:
        time.sleep(1)
        check_race_result_page(races[0]['id'])
        
        time.sleep(1)
        check_db_result_page(races[0]['id'])
    
    # 3. データ量見積もり
    estimate_data_volume()

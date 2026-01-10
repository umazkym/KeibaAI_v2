# -*- coding: utf-8 -*-
"""
Netkeibaオッズ取得検証スクリプト (PoC)

指定されたレースIDに対して、以下の券種のオッズ取得を試みる。
1. 単勝 (Win)
2. 馬連 (Umaren)
3. 三連複 (Trio) - 人気順 (housiki=c99)
4. 三連単 (Trifecta) - 人気順 (housiki=c99)

Dependencies: requests, beautifulsoup4, lxml
"""
import requests
from bs4 import BeautifulSoup
import time
import re

RACE_ID = "202506050810"
BASE_URL = "https://race.netkeiba.com/odds/index.html"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def get_soup(url):
    print(f"Fetching: {url}")
    try:
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
        # encode check
        resp.encoding = resp.apparent_encoding
        return BeautifulSoup(resp.text, 'lxml')
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def verify_win_place(race_id):
    """単勝・複勝オッズの検証"""
    url = f"{BASE_URL}?type=b1&race_id={race_id}&rf=shutuba_submenu"
    soup = get_soup(url)
    if not soup: return

    print("\n--- 単勝・複勝 (Win/Place) ---")
    
    # ID: odds_tan_block (単勝), odds_fuku_block (複勝)
    # 構造: tr class="HorseList" -> td class="Odds"
    
    rows = soup.select("tr.HorseList")
    print(f"Found {len(rows)} horses.")
    
    for i, row in enumerate(rows[:5]): # Show top 5
        horse_name_tag = row.select_one(".Horse_Name")
        horse_name = horse_name_tag.get_text(strip=True) if horse_name_tag else "Unknown"
        
        odds_tag = row.select_one("td.Odds")
        odds = odds_tag.get_text(strip=True) if odds_tag else "-"
        
        print(f"  馬番{i+1}: {horse_name} -> {odds}倍")

def verify_umaren(race_id):
    """馬連オッズの検証（マトリックス）"""
    url = f"{BASE_URL}?type=b4&race_id={race_id}&housiki=c0&rf=shutuba_submenu"
    soup = get_soup(url)
    if not soup: return

    print("\n--- 馬連 (Umaren) ---")
    # 構造: #odds_view_form > table
    # マトリックスは解析が複雑だが、データが含まれているかチェックする
    
    table = soup.select_one("table.Odds_Table")
    if table:
        print("Odds Matrix Found.")
        # サンプルとして数個のオッズを抽出
        odds_cells = table.select("td.Odds")
        print(f"Total Odds Cells: {len(odds_cells)}")
        
        valid_odds = [c.get_text(strip=True) for c in odds_cells if c.get_text(strip=True)]
        print(f"Sample Odds: {valid_odds[:5]} ...")
    else:
        print("Odds Matrix NOT Found.")

def verify_trio_popularity(race_id):
    """三連複（人気順）の検証"""
    # housiki=c99 で人気順ページに直接アクセス試行
    url = f"{BASE_URL}?type=b7&race_id={race_id}&housiki=c99&rf=shutuba_submenu"
    soup = get_soup(url)
    if not soup: return

    print("\n--- 三連複 (Trio) - 人気順 ---")
    
    # 構造: #odds_view_form > table > tr
    rows = soup.select("table.Odds_Table tr")
    # Headerを除外
    data_rows = [r for r in rows if not r.select("th")]
    
    print(f"Found {len(data_rows)} combinations (Page 1).")
    
    for i, row in enumerate(data_rows[:5]):
        # 組み合わせ
        target = row.select_one(".Odds_Ninki")
        rank = target.get_text(strip=True) if target else f"{i+1}人気"
        
        comb_tag = row.select_one(".Uma_No_List")
        if comb_tag:
            # spam class="UmaNo"
            umas = [u.get_text(strip=True) for u in comb_tag.select(".UmaNo")]
            comb_str = "-".join(umas)
        else:
            comb_str = "Unknown"
            
        odds_tag = row.select_one(".Odds")
        odds = odds_tag.get_text(strip=True) if odds_tag else "-"
        
        print(f"  {rank}: {comb_str} -> {odds}倍")
    
    # ドロップダウンの確認
    select = soup.select_one("#ninki_select")
    if select:
        options = select.select("option")
        print(f"Pager Found: {len(options)} pages.")
    else:
        print("Pager NOT Found (Single page or Error).")

def verify_trifecta_popularity(race_id):
    """三連単（人気順）の検証"""
    url = f"{BASE_URL}?type=b8&race_id={race_id}&housiki=c99&rf=shutuba_submenu"
    soup = get_soup(url)
    if not soup: return

    print("\n--- 三連単 (Trifecta) - 人気順 ---")
    
    rows = soup.select("table.Odds_Table tr")
    data_rows = [r for r in rows if not r.select("th")]
    
    print(f"Found {len(data_rows)} combinations (Page 1).")
    
    for i, row in enumerate(data_rows[:5]):
        comb_tag = row.select_one(".Uma_No_List")
        if comb_tag:
            umas = [u.get_text(strip=True) for u in comb_tag.select(".UmaNo")]
            comb_str = "->".join(umas)
        else:
            comb_str = "Unknown"
            
        odds_tag = row.select_one(".Odds")
        odds = odds_tag.get_text(strip=True) if odds_tag else "-"
        
        print(f"  {i+1}人気: {comb_str} -> {odds}倍")

def main():
    print(f"Verifying Odds Fetch for Race ID: {RACE_ID}")
    
    verify_win_place(RACE_ID)
    time.sleep(1)
    
    verify_umaren(RACE_ID)
    time.sleep(1)
    
    verify_trio_popularity(RACE_ID)
    time.sleep(1)

    verify_trifecta_popularity(RACE_ID)

if __name__ == "__main__":
    main()

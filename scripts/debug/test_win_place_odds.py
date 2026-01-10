# -*- coding: utf-8 -*-
"""単勝/複勝オッズ取得のテスト"""
from keibaai.src.preparing._scrape_odds import scrape_odds_win_place
from keibaai.src.preparing._scrape_html import prepare_chrome_driver

print("=== 単勝/複勝オッズ取得テスト ===")
driver = prepare_chrome_driver()
try:
    result = scrape_odds_win_place(driver, '202506050810')
    print(f"取得件数: {len(result['data'])}件")
    print("サンプルデータ (先頭3件):")
    for item in result['data'][:3]:
        print(f"  馬番{item['horse_number']}: 単勝{item['win_odds']}倍, 複勝{item['place_odds_min']}-{item['place_odds_max']}倍")
finally:
    driver.quit()

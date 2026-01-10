# -*- coding: utf-8 -*-
"""
全レース・全券種の取得可能性検証スクリプト
"""
import sys
from pathlib import Path
import time

PROJECT_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from keibaai.src.preparing._scrape_html import prepare_chrome_driver
from keibaai.src.preparing._scrape_odds_html import scrape_odds_html_selenium

RACE_IDS = [
    '202006050501',
    '202106050501', 
    '202206050501',
    '202306050501',
    '202406050501',
]

BET_TYPES = [
    'tansho_fukusho',
    'umaren',
    'wide',
    'umatan',
    'sanrenpuku',
    'sanrentan',
]

RAW_ODDS_DIR = PROJECT_ROOT / 'keibaai' / 'data' / 'raw' / 'html' / 'odds'

def main():
    results = {}
    
    driver = prepare_chrome_driver(headless=True)
    
    try:
        for race_id in RACE_IDS:
            results[race_id] = {}
            print(f'\n=== {race_id} ===')
            
            for bet_type in BET_TYPES:
                output_dir = RAW_ODDS_DIR / bet_type
                
                try:
                    result = scrape_odds_html_selenium(
                        driver, race_id, bet_type, output_dir, skip_existing=False
                    )
                    
                    if result and result.exists():
                        size = result.stat().st_size
                        status = f'OK ({size/1024:.0f}KB)'
                    else:
                        status = 'FAILED'
                        
                except Exception as e:
                    status = f'ERROR: {str(e)[:50]}'
                
                results[race_id][bet_type] = status
                print(f'  {bet_type}: {status}')
                
    finally:
        driver.quit()
    
    # サマリー表示
    print('\n' + '=' * 80)
    print('SUMMARY')
    print('=' * 80)
    
    # ヘッダー
    header = 'race_id'.ljust(15) + ''.join([bt[:8].ljust(10) for bt in BET_TYPES])
    print(header)
    print('-' * 80)
    
    for race_id in RACE_IDS:
        row = race_id.ljust(15)
        for bet_type in BET_TYPES:
            status = results[race_id][bet_type]
            if 'OK' in status:
                row += '✓'.ljust(10)
            else:
                row += '✗'.ljust(10)
        print(row)

if __name__ == '__main__':
    main()

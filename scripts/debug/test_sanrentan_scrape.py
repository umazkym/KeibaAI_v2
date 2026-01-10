import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from keibaai.src.preparing._scrape_html import prepare_chrome_driver
from keibaai.src.preparing._scrape_odds_html import scrape_odds_html_selenium

driver = prepare_chrome_driver(headless=False)  # ヘッドレスじゃなくして確認
try:
    race_id = '202506050501'
    output_dir = PROJECT_ROOT / 'keibaai' / 'data' / 'raw' / 'html' / 'odds' / 'sanrentan'
    
    result = scrape_odds_html_selenium(driver, race_id, 'sanrentan', output_dir, skip_existing=False)
    print(f'Result: {result}')
finally:
    driver.quit()

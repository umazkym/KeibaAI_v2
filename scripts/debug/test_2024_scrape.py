from pathlib import Path
import sys
import logging

PROJECT_ROOT = Path("C:/Users/zk-ht/Keiba/Keiba_AI_v2")
sys.path.append(str(PROJECT_ROOT))

from keibaai.src.preparing._scrape_html import prepare_chrome_driver
from keibaai.src.preparing._scrape_odds_html import scrape_odds_html_selenium

logging.basicConfig(level=logging.INFO)

driver = prepare_chrome_driver(headless=True)
try:
    race_id = "202408010101" # 2024年京都1R（金杯当日）
    bet_type = "sanrentan"
    output_dir = PROJECT_ROOT / "keibaai/data/raw/html/odds" / bet_type
    
    print(f"Scraping {race_id} {bet_type}...")
    result = scrape_odds_html_selenium(driver, race_id, bet_type, output_dir, skip_existing=False)
    print(f"Result: {result}")
    
    if result and result.exists():
        print(f"File created: {result} ({result.stat().st_size} bytes)")
    else:
        print("File not created")
        
except Exception as e:
    print(f"Error: {e}")
finally:
    driver.quit()

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import sys

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

VENUE_MAP = {"05": "東京"}
VENUE_CODE = "05"
TARGET_DATE = "20251130"

def init_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # Anti-detection
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=options)
    return driver

def scrape_race_list():
    driver = init_driver()
    url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={TARGET_DATE}"
    logger.info(f"Scraping race list from {url}")
    
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "RaceList_Data"))
        )
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        race_list = []
        prefix = f"{TARGET_DATE[:4]}{VENUE_CODE}"
        
        # Find the specific venue block if possible, but for now just find all links
        # Netkeiba race list structure usually groups by venue.
        # We look for links that contain the race_id
        
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
                # The name is usually inside the link or in a child element
                # Let's try to get all text from the link
                name = link.get_text(strip=True)
                
                # Sometimes the name is in a sibling div or span.
                # Let's inspect the parent if the link text is empty or just a number
                if not name or name.isdigit():
                    # Try finding a sibling with class 'ItemTitle' or similar
                    # This is a guess, we will see the output
                    pass
                
                # Store unique races
                if not any(r['id'] == rid for r in race_list):
                    race_list.append({'id': rid, 'name': name, 'href': href})
        
        race_list.sort(key=lambda x: x['id'])
        
        logger.info(f"Found {len(race_list)} races.")
        for r in race_list:
            logger.info(f"ID: {r['id']}, Name: {r['name']}")
            
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_race_list()

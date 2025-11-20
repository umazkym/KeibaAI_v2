import requests
from bs4 import BeautifulSoup
import time
import random

def test_race_list_fetch():
    print("Testing Race List Fetch with Selenium...")
    url = "https://race.netkeiba.com/top/race_list.html?kaisai_date=20231029"
    
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        driver.get(url)
        
        # Wait for RaceList_Box
        wait = WebDriverWait(driver, 10)
        race_list_box = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, 'RaceList_Box'))
        )
        
        if race_list_box:
            print("SUCCESS: RaceList_Box found with Selenium!")
            links = race_list_box.find_elements(By.TAG_NAME, 'a')
            print(f"Found {len(links)} links.")
            
            race_links = []
            for link in links:
                href = link.get_attribute('href')
                if href and 'race_id=' in href:
                    race_links.append(href)
            
            print(f"Found {len(race_links)} race links.")
            if len(race_links) > 0:
                 print("Sample link:", race_links[0])
        else:
            print("FAILURE: RaceList_Box NOT found even with Selenium.")
            
    except Exception as e:
        print(f"ERROR: {e}")
        with open("debug_race_list_selenium_error.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
    finally:
        driver.quit()

def test_pedigree_fetch(horse_id):
    print(f"Testing Pedigree Fetch for {horse_id}...")
    url = f"https://db.netkeiba.com/horse/ped/{horse_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        # response.encoding = 'euc-jp' # netkeiba usually uses euc-jp
        soup = BeautifulSoup(response.content, "lxml", from_encoding='euc-jp')
        blood_table = soup.find(class_='blood_table')
        if blood_table:
            print("SUCCESS: blood_table found!")
        else:
            print("FAILURE: blood_table NOT found.")
            with open(f"debug_ped_{horse_id}.html", "wb") as f:
                f.write(response.content)
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    test_race_list_fetch()
    test_pedigree_fetch("2022105820")

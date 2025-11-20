import requests
from bs4 import BeautifulSoup
import time
import random

def test_race_list_fetch():
    print("Testing Race List Fetch with requests...")
    url = "https://race.netkeiba.com/top/race_list.html?kaisai_date=20251116"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'euc-jp'
        soup = BeautifulSoup(response.content, "lxml")
        race_list_box = soup.find(class_='RaceList_Box')
        if race_list_box:
            print("SUCCESS: RaceList_Box found with requests!")
            links = race_list_box.find_all('a')
            print(f"Found {len(links)} links.")
        else:
            print("FAILURE: RaceList_Box NOT found with requests.")
            # Save html for inspection
            with open("debug_race_list_requests.html", "wb") as f:
                f.write(response.content)
    except Exception as e:
        print(f"ERROR: {e}")

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

import requests
from bs4 import BeautifulSoup

def debug_race_list(date):
    url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={date}"
    print(f"Fetching {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    resp = requests.get(url, headers=headers)
    print(f"Status Code: {resp.status_code}")
    print(f"Encoding: {resp.encoding}")
    print(f"Content Snippet: {resp.content[:500]}")
    
    resp.encoding = 'euc-jp' 
    soup = BeautifulSoup(resp.content, 'html.parser')
    
    print(f"Page Title: {soup.title.string if soup.title else 'No Title'}")
    
    links = soup.find_all('a', href=True)
    print(f"Found {len(links)} links.")
    
    count = 0
    for link in links:
        href = link.get('href', '')
        text = link.text.strip()
        if 'result' in href or 'shutuba' in href or 'race_id' in href:
            print(f"Link: {text} -> {href}")
            count += 1
            if count > 100: break
    # Inspect RaceList_Area
    area = soup.find('div', class_='RaceList_Area')
    if area:
        print("Found RaceList_Area")
        print(area.prettify()[:1000])
        # Print links INSIDE RaceList_Area
        links = area.find_all('a', href=True)
        for link in links:
            print(f"Area Link: {link.text.strip()} -> {link['href']}")
    else:
        print("RaceList_Area not found")

if __name__ == "__main__":
    # Test race list without date (should show today/tomorrow)
    url = "https://race.netkeiba.com/top/race_list.html"
    print(f"Fetching {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    resp = requests.get(url, headers=headers)
    resp.encoding = 'euc-jp'
    soup = BeautifulSoup(resp.content, 'html.parser')
    
    links = soup.find_all('a', href=True)
    count = 0
    for link in links:
        href = link['href']
        if 'race/' in href and href.count('/') > 2: # heuristic for race link
             print(f"DB Link: {link.text.strip()} -> {href}")
             count += 1
             if count > 20: break

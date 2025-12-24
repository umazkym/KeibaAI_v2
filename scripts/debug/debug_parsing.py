import os
from bs4 import BeautifulSoup
import pandas as pd

# Path to a sample bin file
bin_path = r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\horse\2002100816_perf.bin"

if not os.path.exists(bin_path):
    # Try to find another one if this specific one doesn't exist
    dir_path = r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\horse"
    files = os.listdir(dir_path)
    if files:
        bin_path = os.path.join(dir_path, files[0])
    else:
        print("No bin files found.")
        exit()

print(f"Inspecting: {bin_path}")

with open(bin_path, 'rb') as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, 'html.parser')
table = soup.find('table', class_='db_h_race_results')

if not table:
    print("No table found.")
    exit()

# Print Headers
thead = table.find('thead')
if thead:
    headers = [th.text.strip() for th in thead.find_all('th')]
    print("Headers:")
    for i, h in enumerate(headers):
        print(f"{i}: {h}")

# Print First Row Data
tbody = table.find('tbody')
if tbody:
    rows = tbody.find_all('tr')
    if rows:
        first_row = rows[0]
        cols = first_row.find_all('td')
        print("\nFirst Row Data:")
        for i, c in enumerate(cols):
            print(f"{i}: {c.text.strip()}")

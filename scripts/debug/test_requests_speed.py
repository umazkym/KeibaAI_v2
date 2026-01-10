import requests
import re

race_id = "202406050810"
base_url = "https://race.netkeiba.com/odds/index.html"
headers = { 'User-Agent': 'Mozilla/5.0' }

r = requests.get(base_url, params={'type': 'b8', 'race_id': race_id}, headers=headers)
r.encoding = 'EUC-JP'

print("Searching for API/JSON...")
matches = re.findall(r'https?://[^\s"\']*api[^\s"\']*', r.text)
for m in matches[:10]:
    print(m)

matches2 = re.findall(r'/[^\s"\']*api[^\s"\']*', r.text)
for m in matches2[:10]:
    print(m)

"""netkeibaのレスポンスを調査"""
import requests

url = 'https://db.netkeiba.com/race/201405010404/'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}
response = requests.get(url, headers=headers, timeout=30)
print(f'Status: {response.status_code}')
print(f'Content-Type: {response.headers.get("Content-Type", "N/A")}')
print(f'Length: {len(response.content)} bytes')
print(f'First 1000 chars:')
print(response.content[:1000].decode('utf-8', errors='replace'))

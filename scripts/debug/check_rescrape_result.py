"""再スクレイピング後のHTMLを確認"""
from pathlib import Path
from bs4 import BeautifulSoup

html_dir = Path('keibaai/data/raw/html/race')

missing_race_ids = ['201805010304', '201408020304', '201405010404', '201405010508']

print("=" * 80)
print("再スクレイピング後のHTML内容確認")
print("=" * 80)

for race_id in missing_race_ids:
    race_html = html_dir / f'{race_id}.bin'
    
    print(f"\n--- {race_id} ---")
    
    with open(race_html, 'rb') as f:
        html_bytes = f.read()
    
    # エンコーディングを試行
    for enc in ['utf-8', 'euc_jp', 'shift_jis']:
        try:
            html_text = html_bytes.decode(enc, errors='replace')
            break
        except:
            continue
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # タイトルを確認
    title = soup.find('title')
    print(f"title: {title.get_text()[:100] if title else 'なし'}")
    
    # レース名を探す
    race_name = soup.find('h1', class_='RaceName')
    if race_name:
        print(f"レース名: {race_name.get_text(strip=True)[:50]}")
    
    # 中止関連のメッセージを探す
    page_text = soup.get_text()
    keywords = ['中止', '不成立', '発売中止', '取消', '開催中止', '競走中止']
    found_keywords = [kw for kw in keywords if kw in page_text]
    if found_keywords:
        print(f"⚠️ 検出キーワード: {found_keywords}")
    
    # RaceDataを確認
    race_data = soup.find('div', class_='RaceData01')
    if race_data:
        print(f"RaceData: {race_data.get_text(strip=True)[:80]}")

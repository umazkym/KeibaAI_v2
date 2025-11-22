from bs4 import BeautifulSoup
import re

html_path = 'keibaai/data/raw/html/race/202001010101.bin'

with open(html_path, 'rb') as f:
    html_bytes = f.read()

try:
    html_text = html_bytes.decode('euc_jp', errors='replace')
except:
    html_text = html_bytes.decode('utf-8', errors='replace')

soup = BeautifulSoup(html_text, 'html.parser')
prize_elements = soup.find_all(string=re.compile('賞金|本賞金'))
race_data02 = soup.find('div', class_='RaceData02')
racedata_dl = soup.find('dl', class_='racedata')

# ファイルに保存
with open('prize_html_analysis.txt', 'w', encoding='utf-8') as f:
    f.write("=== 賞金関連のテキスト検索 ===\n\n")
    f.write(f"'賞金'を含む要素: {len(prize_elements)}個\n\n")
    for i, elem in enumerate(prize_elements):
        f.write(f"要素 {i+1}:\n")
        f.write(f"  テキスト: {elem.strip()}\n")
        if elem.parent:
            f.write(f"  親タグ: {elem.parent.name}\n")
            f.write(f"  親のclass: {elem.parent.get('class')}\n")
            f.write(f"  親のHTML:\n{str(elem.parent)}\n\n")
    
    f.write("\n=== RaceData02 ===\n")
    if race_data02:
        f.write("見つかりました\n")
        f.write(f"HTML:\n{str(race_data02)}\n\n")
    else:
        f.write("見つかりません\n\n")
    
    f.write("\n=== dl.racedata ===\n")
    if racedata_dl:
        f.write(f"HTML:\n{str(racedata_dl)}\n\n")
        dds = racedata_dl.find_all('dd')
        for i, dd in enumerate(dds):
            f.write(f"dd[{i}]: {dd.get_text(strip=True)[:200]}\n")
    else:
        f.write("見つかりません\n")

print("prize_html_analysis.txt に保存しました")

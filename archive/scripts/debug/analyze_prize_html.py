"""
実際のHTMLから賞金パターンを抽出
"""
from bs4 import BeautifulSoup
import re

# HTMLファイルを読み込み
html_path = 'keibaai/data/raw/html/race/202001010101.bin'

with open(html_path, 'rb') as f:
    html_bytes = f.read()

# デコード試行
try:
    html_text = html_bytes.decode('euc_jp', errors='replace')
    print("Decoded as EUC-JP")
except:
    html_text = html_bytes.decode('utf-8', errors='replace')
    print("Decoded as UTF-8")

soup = BeautifulSoup(html_text, 'html.parser')

# 賞金関連のパターンを探す
print("\n=== 賞金関連のテキスト検索 ===")

# パターン1: '賞金'を含むすべての要素
prize_elements = soup.find_all(string=re.compile('賞金|本賞金'))
print(f"\n'賞金'を含む要素: {len(prize_elements)}個")
for i, elem in enumerate(prize_elements[:5]):
    print(f"\n{i+1}. テキスト: {elem.strip()}")
    if elem.parent:
        print(f"   親タグ: {elem.parent.name}")
        print(f"   親のclass: {elem.parent.get('class')}")
        print(f"   親のHTML: {str(elem.parent)[:200]}")

# パターン2: 'RaceData02'クラス
race_data02 = soup.find('div', class_='RaceData02')
if race_data02:
    print(f"\n\n=== RaceData02が見つかりました ===")
    print(f"HTML: {str(race_data02)[:500]}")
    print(f"テキスト: {race_data02.get_text()[:300]}")
else:
    print("\n\n=== RaceData02が見つかりません ===")

# パターン3: すべてのdivのclass属性を確認
print("\n\n=== 'Data'または'Race'を含むクラス名 ===")
all_divs = soup.find_all('div')
race_related = set()
for div in all_divs:
    classes = div.get('class', [])
    for cls in classes:
        if 'Data' in cls or 'Race' in cls or 'race' in cls:
            race_related.add(cls)

for cls in sorted(race_related):
    print(f"  {cls}")

# パターン4: dl.racedataを詳しく調査
racedata_dl = soup.find('dl', class_='racedata')
if racedata_dl:
    print(f"\n\n=== dl.racedata の内容 ===")
    print(f"HTML: {str(racedata_dl)[:500]}")
    
    # すべてのddタグを確認
"""
実際のHTMLから賞金パターンを抽出
"""
from bs4 import BeautifulSoup
import re

# HTMLファイルを読み込み
html_path = 'keibaai/data/raw/html/race/202001010101.bin'

with open(html_path, 'rb') as f:
    html_bytes = f.read()

# デコード試行
try:
    html_text = html_bytes.decode('euc_jp', errors='replace')
    print("Decoded as EUC-JP")
except:
    html_text = html_bytes.decode('utf-8', errors='replace')
    print("Decoded as UTF-8")

soup = BeautifulSoup(html_text, 'html.parser')

# 賞金関連のパターンを探す
print("\n=== 賞金関連のテキスト検索 ===")

# パターン1: '賞金'を含むすべての要素
prize_elements = soup.find_all(string=re.compile('賞金|本賞金'))
print(f"\n'賞金'を含む要素: {len(prize_elements)}個")
for i, elem in enumerate(prize_elements[:5]):
    print(f"\n{i+1}. テキスト: {elem.strip()}")
    if elem.parent:
        print(f"   親タグ: {elem.parent.name}")
        print(f"   親のclass: {elem.parent.get('class')}")
        print(f"   親のHTML: {str(elem.parent)[:200]}")

# パターン2: 'RaceData02'クラス
race_data02 = soup.find('div', class_='RaceData02')
if race_data02:
    print(f"\n\n=== RaceData02が見つかりました ===")
    print(f"HTML: {str(race_data02)[:500]}")
    print(f"テキスト: {race_data02.get_text()[:300]}")
else:
    print("\n\n=== RaceData02が見つかりません ===")

# パターン3: すべてのdivのclass属性を確認
print("\n\n=== 'Data'または'Race'を含むクラス名 ===")
all_divs = soup.find_all('div')
race_related = set()
for div in all_divs:
    classes = div.get('class', [])
    for cls in classes:
        if 'Data' in cls or 'Race' in cls or 'race' in cls:
            race_related.add(cls)

for cls in sorted(race_related):
    print(f"  {cls}")

# パターン4: dl.racedataを詳しく調査
racedata_dl = soup.find('dl', class_='racedata')
if racedata_dl:
    print(f"\n\n=== dl.racedata の内容 ===")
    print(f"HTML: {str(racedata_dl)[:500]}")
    
    # すべてのddタグを確認
    dds = racedata_dl.find_all('dd')
    for i, dd in enumerate(dds):
        text = dd.get_text(strip=True)
        if '賞金' in text or '万円' in text or len(text) > 10:
            print(f"\n  dd[{i}]: {text[:100]}")

# パターン5: spanタグで賞金を探す
print("\n\n=== spanタグで'万円'を含むもの ===")
span_with_yen = soup.find_all('span', string=re.compile('万円'))
for i, span in enumerate(span_with_yen[:3]):
    print(f"\n{i+1}. {span.get_text()}")
    print(f"   親: {span.parent.name}, class={span.parent.get('class')}")

# 結果をファイルに保存
with open('prize_html_analysis.txt', 'w', encoding='utf-8') as f:
    f.write("=== 賞金関連のテキスト検索 ===\n\n")
    f.write(f"'賞金'を含む要素: {len(prize_elements)}個\n")
    for i, elem in enumerate(prize_elements):
        f.write(f"\n{i+1}. テキスト: {elem.strip()}\n")
        if elem.parent:
            f.write(f"   親タグ: {elem.parent.name}\n")
            f.write(f"   親のclass: {elem.parent.get('class')}\n")
            f.write(f"   親のHTML: {str(elem.parent)[:500]}\n")
    
    f.write("\n\n=== RaceData02 ===\n")
    if race_data02:
        f.write(f"見つかりました\n")
        f.write(f"HTML: {str(race_data02)}\n")
    else:
        f.write("見つかりません\n")
    
    f.write("\n\n=== dl.racedata ===\n")
    if racedata_dl:
        f.write(f"HTML: {str(racedata_dl)}\n")

print("\n結果を prize_html_analysis.txt に保存しました")

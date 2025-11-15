# 検証スクリプト
from pathlib import Path
from bs4 import BeautifulSoup

html_file = Path("keibaai/data/raw/html/horse/horse_2017110144_20251114T103150+0900_sha256=f819937a.html")

with open(html_file, 'rb') as f:
    html = f.read()

try:
    text = html.decode('euc_jp')
except:
    text = html.decode('utf-8')

soup = BeautifulSoup(text, 'html.parser')

# プロフィール情報の有無
has_profile = soup.find('table', class_='db_prof_table') is not None
print(f"プロフィール情報: {'あり' if has_profile else 'なし'}")

# 成績情報の有無
has_performance = soup.find('table', class_='db_h_race_results') is not None
print(f"成績情報: {'あり' if has_performance else 'なし'}")
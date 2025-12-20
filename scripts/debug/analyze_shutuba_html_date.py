"""
2014年のshutubaHTMLを分析して日付情報を探す
"""
from pathlib import Path
from bs4 import BeautifulSoup
import re

html_dir = Path('keibaai/data/raw/html/shutuba')

# 2014年と2020年のファイルを比較
files = {
    '2014': list(html_dir.glob('2014*.bin'))[:3],
    '2020': list(html_dir.glob('2020*.bin'))[:3],
}

for year, file_list in files.items():
    print(f"\n{'='*60}")
    print(f"{year}年のHTMLファイル分析")
    print('='*60)
    
    for f in file_list:
        print(f"\n--- {f.name} ---")
        
        with open(f, 'rb') as fp:
            html_bytes = fp.read()
        
        try:
            html_text = html_bytes.decode('euc_jp', errors='replace')
        except:
            html_text = html_bytes.decode('utf-8', errors='replace')
        
        soup = BeautifulSoup(html_text, 'html.parser')
        
        # 1. smalltxt を探す
        smalltxt = soup.find('p', class_='smalltxt')
        print(f"  smalltxt: {smalltxt.get_text()[:100] if smalltxt else 'なし'}")
        
        # 2. RaceData01 を探す
        rd01 = soup.find('p', class_='RaceData01')
        print(f"  RaceData01: {rd01.get_text()[:100] if rd01 else 'なし'}")
        
        # 3. Active dd を探す
        active = soup.find('dd', class_='Active')
        print(f"  dd.Active: {active.get_text()[:100] if active else 'なし'}")
        
        # 4. title を探す
        title = soup.find('title')
        print(f"  title: {title.get_text()[:100] if title else 'なし'}")
        
        # 5. data_intro を探す
        data_intro = soup.find('div', class_='data_intro')
        print(f"  data_intro: {data_intro.get_text()[:100] if data_intro else 'なし'}")
        
        # 6. 日付パターン(YYYY年M月D日)を全文から検索
        date_pattern = re.findall(r'\d{4}年\d{1,2}月\d{1,2}日', html_text)
        print(f"  全文から日付パターン: {date_pattern[:3] if date_pattern else 'なし'}")
        
        break  # 各年1ファイルのみ

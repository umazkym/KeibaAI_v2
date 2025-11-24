import logging
from pathlib import Path
from bs4 import BeautifulSoup

# サンプルHTMLファイルを探す
raw_data_path = Path('keibaai/data/raw/html')

# 最初のraceファイルを取得
race_files = list(raw_data_path.glob('race/*.bin'))

if race_files:
    sample_file = race_files[0]
    print(f"サンプルファイル: {sample_file}")
    
    # HTMLを読み込む
    with open(sample_file, 'rb') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 結果テーブルを探す
    result_table = soup.find('table', class_='race_table_01')
    
    if result_table:
        # 最初のデータ行を取得
        rows = result_table.find_all('tr')
        for i, row in enumerate(rows):
            cells = row.find_all('td')
            if len(cells) >= 15:  # データ行
                print(f"\n=== 行{i}のセル情報（最初のデータ行） ===")
                print(f"セル総数: {len(cells)}")
                for j, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    print(f"セル[{j}]: {text[:30] if len(text) > 30 else text}")
                
                # オッズがあると思われる位置（セル12）を確認
                print(f"\n--- オッズ抽出テスト ---")
                print(f"セル[12]の値: '{cells[12].get_text(strip=True)}'")
                
                # 他の可能性のあるセルも確認
                for idx in range(10, min(15, len(cells))):
                    val = cells[idx].get_text(strip=True)
                    # オッズっぽい値（小数点を含む数値）を探す
                    if '.' in val and val.replace('.', '').replace(',', '').isdigit():
                        print(f"  可能性: セル[{idx}] = '{val}'")
                
                break  # 最初のデータ行だけでOK
    else:
        print("race_table_01が見つかりません")
        # 他のテーブルクラスを探す
        tables = soup.find_all('table')
        print(f"見つかったテーブル数: {len(tables)}")
        for i, table in enumerate(tables[:3]):
            class_name = table.get('class')
            print(f"  テーブル{i}: class={class_name}")
else:
    print("サンプルHTMLファイルが見つかりません")

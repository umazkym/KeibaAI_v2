"""
欠損馬2頭の対応スクリプト

問題: 以下の馬がhorses.parquetに登録されていない
- 2013105497 (アンジェリカス): プロフィールファイルなし
- 2008100563 (クリーンエコロジー): _perf.binのみあり

対応: races.parquetから基本情報を抽出し、horses.parquetに追加
"""

import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import re

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"
RAW_DIR = PROJECT_ROOT / "keibaai" / "data" / "raw" / "html"

def read_html_file(file_path):
    """HTMLファイルを読み込む"""
    with open(file_path, 'rb') as f:
        html_bytes = f.read()
    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')
    return html_text

def extract_horse_info_from_perf(horse_id):
    """_perf.binから馬情報を抽出"""
    perf_path = RAW_DIR / "horse" / f"{horse_id}_perf.bin"
    
    if not perf_path.exists():
        return None
    
    html_text = read_html_file(perf_path)
    soup = BeautifulSoup(html_text, 'html.parser')
    
    info = {'horse_id': horse_id}
    
    # 馬名
    horse_title = soup.find('div', class_='horse_title')
    if horse_title:
        h1 = horse_title.find('h1')
        if h1:
            info['horse_name'] = h1.get_text(strip=True)
    
    # プロフィール情報
    profile = soup.find('div', class_='db_prof_area_02')
    if profile:
        for dl in profile.find_all('dl'):
            dt = dl.find('dt')
            dd = dl.find('dd')
            if dt and dd:
                label = dt.get_text(strip=True)
                value = dd.get_text(strip=True)
                
                if '生年月日' in label:
                    info['birth_date'] = value
                elif '調教師' in label:
                    info['trainer_name'] = value
                elif '馬主' in label:
                    info['owner_name'] = value
                elif '生産者' in label:
                    info['breeder_name'] = value
    
    return info if len(info) > 1 else None

def extract_horse_info_from_races(horse_id, races_df):
    """races.parquetから馬情報を抽出"""
    horse_races = races_df[races_df['horse_id'] == horse_id]
    
    if horse_races.empty:
        return None
    
    # 最新のレース情報を使用
    latest = horse_races.sort_values('race_date', ascending=False).iloc[0]
    
    info = {
        'horse_id': horse_id,
        'horse_name': latest.get('horse_name', None),
        'sex': latest.get('sex', None),
        'trainer_id': latest.get('trainer_id', None),
        'trainer_name': latest.get('trainer_name', None),
        'owner_name': latest.get('owner_name', None),
    }
    
    # 誕生年を推定（horse_idの最初の4桁）
    if len(str(horse_id)) >= 4:
        info['birth_year'] = str(horse_id)[:4]
    
    return info

def add_missing_horses():
    """欠損馬をhorses.parquetに追加"""
    print("=" * 80)
    print("  欠損馬の追加処理")
    print("=" * 80)
    
    # データ読み込み
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    horses = pd.read_parquet(DATA_DIR / "horses" / "horses.parquet")
    
    racing_horses = set(races['horse_id'].dropna().unique())
    registered_horses = set(horses['horse_id'].unique())
    
    missing_horses = racing_horses - registered_horses
    
    print(f"\n  欠損馬: {len(missing_horses)}頭")
    
    new_records = []
    
    for horse_id in missing_horses:
        print(f"\n  【馬ID: {horse_id}】")
        
        # まず_perf.binから情報を取得
        info = extract_horse_info_from_perf(horse_id)
        
        if info:
            print(f"    _perf.binから情報取得: {info.get('horse_name', 'N/A')}")
        else:
            # races.parquetから情報を取得
            info = extract_horse_info_from_races(horse_id, races)
            if info:
                print(f"    races.parquetから情報取得: {info.get('horse_name', 'N/A')}")
        
        if info:
            new_records.append(info)
            print(f"    ✓ 追加対象")
        else:
            print(f"    ✗ 情報取得失敗")
    
    if new_records:
        # 新規レコードをDataFrameに変換
        new_df = pd.DataFrame(new_records)
        
        # 既存horsesと結合
        updated_horses = pd.concat([horses, new_df], ignore_index=True)
        
        # 重複排除（念のため）
        updated_horses = updated_horses.drop_duplicates(subset='horse_id', keep='first')
        
        # 保存
        output_path = DATA_DIR / "horses" / "horses.parquet"
        updated_horses.to_parquet(output_path, index=False)
        
        print(f"\n  ✓ horses.parquetを更新しました")
        print(f"    追加: {len(new_records)}頭")
        print(f"    総数: {len(updated_horses)}頭")
    else:
        print(f"\n  追加対象なし")

def main():
    add_missing_horses()
    
    print("\n" + "=" * 80)
    print("  完了")
    print("=" * 80)

if __name__ == "__main__":
    main()

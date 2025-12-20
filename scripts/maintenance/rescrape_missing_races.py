"""
不完全な4レースのHTMLを再スクレイピング
"""
import requests
import time
from pathlib import Path

# 問題のrace_id
missing_race_ids = ['201805010304', '201408020304', '201405010404', '201405010508']

html_dir = Path('keibaai/data/raw/html/race')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

print("=" * 80)
print("4レースの再スクレイピング")
print("=" * 80)

for race_id in missing_race_ids:
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    output_path = html_dir / f"{race_id}.bin"
    
    print(f"\n--- {race_id} ---")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # 保存
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        print(f"保存: {output_path} ({len(response.content):,} bytes)")
        
        # エラーチェック
        if b'race_table_01' in response.content:
            print("✅ race_table_01 あり")
        else:
            print("⚠️ race_table_01 なし（レース中止の可能性）")
        
    except Exception as e:
        print(f"❌ エラー: {e}")
    
    time.sleep(2)  # 礼儀正しく待機

print("\n完了")

"""
出馬表HTMLに朝オッズが含まれているか確認
"""
import os
from pathlib import Path

# 出馬表HTMLのサンプルを確認
shutuba_dir = Path('keibaai/data/raw/html/shutuba')

if shutuba_dir.exists():
    files = list(shutuba_dir.glob('*.bin'))
    print(f'出馬表HTMLファイル: {len(files)}件')
    
    if files:
        # 最初のファイルの内容を確認
        sample_file = files[0]
        print(f'サンプル: {sample_file.name}')
        
        with open(sample_file, 'rb') as f:
            content = f.read()
            text = content.decode('euc_jp', errors='replace')
        
        # オッズ関連のキーワードを検索
        print('\nオッズ関連キーワード検索:')
        keywords = ['オッズ', 'Odds', '単勝', '倍', 'Popular']
        for kw in keywords:
            if kw in text:
                idx = text.find(kw)
                snippet = text[max(0, idx-50):idx+100].replace('\n', ' ').replace('\r', '')
                print(f'  "{kw}" found: ...{snippet[:100]}...')
            else:
                print(f'  "{kw}" not found')
        
        # テーブル構造を確認
        print('\nテーブルクラス検索:')
        if 'Shutuba_Table' in text:
            print('  Shutuba_Table found')
        if 'OddsList' in text:
            print('  OddsList found')
        if 'Odds' in text.lower():
            print('  "odds" (case insensitive) found')
else:
    print('出馬表ディレクトリが存在しません')

#!/usr/bin/env python3
"""阪神レポートの生データ(上り/L指数関連)を完全ダンプ"""
import json, re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

for name, html_path in [
    ("中山0228", PROJECT_ROOT / "outputs/reports/race_analysis_20260228_中山_interactive.html"),
    ("阪神0329", PROJECT_ROOT / "outputs/reports/race_analysis_20260329_阪神_interactive.html"),
]:
    if not html_path.exists():
        continue
    
    content = html_path.read_text(encoding='utf-8')
    match = re.search(r'const DATA = ({.*?});', content, re.DOTALL)
    if not match:
        continue
    
    data = json.loads(match.group(1))
    rows = data.get('races', {}).get('11', {}).get('rows', [])
    
    print(f"\n{'='*80}")
    print(f"  {name} 11R")
    print(f"{'='*80}")
    
    # 馬番1の全レコードの上り/L指数関連カラムをダンプ
    horse1_rows = [r for r in rows if r.get('番') == '1']
    
    # 上り関連のキーを全探索
    l3f_keys = [k for k in rows[0].keys() if any(x in k for x in ['上り', '上', 'L指数', 'l3f', '3F', '基場', '基準'])]
    print(f"  上り関連キー: {l3f_keys}")
    
    print(f"\n  馬番1 ({horse1_rows[0].get('馬名', '?')}) - {len(horse1_rows)}走分:")
    for i, r in enumerate(horse1_rows[:5]):
        print(f"\n    走{i+1} ({r.get('日付','?')} {r.get('場所','?')} {r.get('ｺｰｽ','?')}{r.get('距離','?')}m {r.get('馬場','?')}):")
        for k in l3f_keys:
            v = r.get(k, 'KEY_MISSING')
            print(f"      {k}: '{v}'")
        # タイムも確認
        print(f"      ﾀｲﾑ: '{r.get('ﾀｲﾑ', 'KEY_MISSING')}'")
        print(f"      T指数: '{r.get('T指数', 'KEY_MISSING')}'")

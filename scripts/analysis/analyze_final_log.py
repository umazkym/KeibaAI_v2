import re

with open('dry_run_final_fixed_v2.txt', 'r', encoding='utf-16-le') as f:
    lines = f.readlines()

print("=== 血統ID抽出結果 ===")
bloodline_logs = [l.strip() for l in lines if '血統ID' in l]
for log in bloodline_logs:
    print(log)

print("\n=== パターン数の確認 ===")
patterns = {
    '競馬場別成績': r'競馬場別成績を計算完了: ([\d,]+)パターン',
    '距離別成績': r'距離別成績を計算完了: ([\d,]+)パターン',
    '馬場別成績': r'馬場別成績を計算完了: ([\d,]+)パターン',
}

full_text = ''.join(lines)
for name, pattern in patterns.items():
    match = re.search(pattern, full_text)
    if match:
        count = match.group(1)
        print(f'{name}: {count}パターン')
    else:
        print(f'{name}: 0パターン')

print("\n=== sire/damsire関連の警告 ===")
sire_warnings = [l.strip() for l in lines if ('sire' in l.lower() or 'damsire' in l.lower()) and ('WARNING' in l or 'ERROR' in l)]
print(f'警告数: {len(sire_warnings)}件')
for i, warn in enumerate(sire_warnings[:5]):
    print(f"{i+1}. {warn}")

if len(sire_warnings) > 5:
    print(f"... 他 {len(sire_warnings)-5}件")

print("\n=== 重要なINFOメッセージ ===")
info_msgs = [l.strip() for l in lines if 'INFO' in l and ('生成完了' in l or '特徴量' in l)]
for i, msg in enumerate(info_msgs[:15]):
    print(f"{i+1}. {msg}")

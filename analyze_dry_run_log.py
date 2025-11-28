import re

with open('dry_run_success.txt', 'r', encoding='utf-16-le') as f:
    lines = f.readlines()

print("=== 特徴量生成統計（ドライランログより）===\n")

# パターン数の抽出
patterns = {
    '競馬場別成績': r'競馬場別成績を計算完了: ([\d,]+)パターン',
    '距離別成績': r'距離別成績を計算完了: ([\d,]+)パターン',
    '馬場別成績': r'馬場別成績を計算完了: ([\d,]+)パターン',
}

for name, pattern in patterns.items():
    matches = [re.search(pattern, l) for l in lines if re.search(pattern, l)]
    if matches:
        count = matches[0].group(1)
        print(f'{name}: {count}パターン')

print("\n=== 重要な生成完了メッセージ ===")
complete_msgs = [l.strip() for l in lines if '生成完了' in l and 'INFO' in l]
for i, msg in enumerate(complete_msgs[:20]):
    print(f"{i+1}. {msg}")

if len(complete_msgs) > 20:
    print(f"\n... 他 {len(complete_msgs)-20}件")

print("\n=== 警告メッセージ（抜粋）===")
warning_msgs = [l.strip() for l in lines if 'WARNING' in l and ('sire' in l or 'combo' in l or 'damsire' in l or 'nicks' in l)]
for i, msg in enumerate(warning_msgs[:10]):
    print(f"{i+1}. {msg}")

if len(warning_msgs) > 10:
    print(f"\n... 他 {len(warning_msgs)-10}件")

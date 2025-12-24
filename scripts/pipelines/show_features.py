import json

with open('feature_importance_v35.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print("=" * 70)
print("v3.5モデル 特徴量重要度ランキング（全165特徴量）")
print("=" * 70)

for i, item in enumerate(data, 1):
    print(f"{i:3}. {item['feature']:45} {item['importance']:>6}")

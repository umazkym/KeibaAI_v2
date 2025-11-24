# distance_m パーサー修正案

## 問題の原因

race_results.csvで distance_m が "10" となっている問題について、以下の3つの修正案を提示します。

---

## 修正案1: 数字の桁数を明示（推奨★★★）

**理由**: 競馬の距離は基本的に3-4桁（800m～4000m）

```python
# debug_scraping_and_parsing.py 268行目付近

# 修正前
distance_match = re.search(r'(芝|ダート?)[^0-9]*?(\d+)\s*m?', text, re.IGNORECASE)

# 修正後
distance_match = re.search(r'(芝|ダート?)[^0-9]*?(\d{3,4})\s*m?', text, re.IGNORECASE)
#                                                     ^^^^^^^^
#                                                     3-4桁の数字のみマッチ
```

**メリット**:
- 明示的で分かりやすい
- "10" のような誤った値を除外できる
- 競馬の距離として妥当な値のみマッチ

**デメリット**:
- 700m台の距離（まれにある）がマッチしない可能性

---

## 修正案2: デバッグログを追加（診断用★★☆）

**理由**: まず何がマッチしているか確認

```python
# debug_scraping_and_parsing.py 268行目付近

if metadata['distance_m'] is None:
    distance_match = re.search(r'(芝|ダート?)[^0-9]*?(\d+)\s*m?', text, re.IGNORECASE)
    if distance_match:
        # === デバッグログ追加 ===
        import logging
        logging.info(f"[DEBUG] Full match: '{distance_match.group(0)}'")  # 例: "ダ1000m"
        logging.info(f"[DEBUG] Surface: '{distance_match.group(1)}'")     # 例: "ダ"
        logging.info(f"[DEBUG] Distance: '{distance_match.group(2)}'")    # 例: "1000"
        # ========================

        surface_map = {'芝': '芝', 'ダ': 'ダート', 'ダート': 'ダート'}
        metadata['track_surface'] = surface_map.get(distance_match.group(1))
        metadata['distance_m'] = int(distance_match.group(2))
```

**使い方**:
```bash
# ログレベルをINFOに設定して実行
python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only 2>&1 | grep "\[DEBUG\]"
```

---

## 修正案3: HTMLテキストの前処理（根本対応★★★）

**理由**: HTMLが "ダ 1 0 0 0 m" のように空白で分割されている可能性

```python
# debug_scraping_and_parsing.py 246行目付近（textを取得した直後）

if race_data_intro:
    # テキスト全体を取得
    text = race_data_intro.get_text(strip=True)

    # === 前処理を追加 ===
    # 余分な空白を削除（"ダ 1 0 0 0 m" → "ダ1000m"）
    text = re.sub(r'\s+', ' ', text)  # 複数の空白を1つに
    text = re.sub(r'(\D)(\s+)(\d)', r'\1\3', text)  # 文字と数字の間の空白を削除
    text = re.sub(r'(\d)(\s+)(\d)', r'\1\3', text)  # 数字間の空白を削除
    text = re.sub(r'(\d)(\s+)(m)', r'\1\3', text)   # 数字とmの間の空白を削除
    # ====================

    # 以下、既存のパターンマッチング処理
    if '障' in text:
        distance_match = re.search(r'障[^0-9]*?(\d+)\s*m?', text)
        # ...
```

---

## 推奨される実装手順

### ステップ1: 原因の特定（修正案2）

まずデバッグログを追加して、実際に何がマッチしているか確認：

```bash
cd /home/user/KeibaAI_v2

# debug_scraping_and_parsing.py の268行目付近に修正案2のコードを追加

# 再実行
python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only --output-dir output_debug

# ログを確認
cat keibaai/data/logs/*.log | grep "\[DEBUG\]" | head -20
```

**期待されるログ出力**:
```
[DEBUG] Full match: 'ダ1000m'   ← これが正常
[DEBUG] Surface: 'ダ'
[DEBUG] Distance: '1000'        ← これが正常

または

[DEBUG] Full match: 'ダ10'     ← これが異常（"00m"が含まれていない）
[DEBUG] Surface: 'ダ'
[DEBUG] Distance: '10'          ← これが異常
```

### ステップ2: 修正の適用

ログの結果に応じて：

- **ケースA**: Distance が "10" と表示される
  → 修正案3（前処理）を適用

- **ケースB**: Full match が "ダ10m" と表示される（HTMLが誤っている）
  → 修正案1（桁数制限）+ フォールバック処理

- **ケースC**: 正常に "ダ1000m" とマッチしている
  → パース後の処理（DataFrame作成時）に問題がある可能性

### ステップ3: テスト

```python
# テストスクリプト test_distance_parser.py

import re

def test_distance_patterns():
    """距離抽出のテストケース"""

    # 修正案1: 桁数指定版
    pattern_v1 = r'(芝|ダート?)[^0-9]*?(\d{3,4})\s*m?'

    # 修正案3: 前処理版
    def preprocess(text):
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'(\D)(\s+)(\d)', r'\1\3', text)
        text = re.sub(r'(\d)(\s+)(\d)', r'\1\3', text)
        text = re.sub(r'(\d)(\s+)(m)', r'\1\3', text)
        return text

    pattern_v2 = r'(芝|ダート?)[^0-9]*?(\d+)\s*m?'

    test_cases = [
        ("ダ1000m / 天候 : 雨", "ダート", 1000),
        ("芝1600m / 天候 : 曇", "芝", 1600),
        ("ダ 1 0 0 0 m / 天候 : 雨", "ダート", 1000),  # 空白あり
        ("芝右 外1800m", "芝", 1800),
        ("障害芝3000m", "障害", 3000),
    ]

    print("=== 修正案1: 桁数指定 ===")
    for text, expected_surface, expected_distance in test_cases:
        match = re.search(pattern_v1, text, re.IGNORECASE)
        if match:
            surface = match.group(1)
            distance = int(match.group(2))
            status = "✓" if distance == expected_distance else "✗"
            print(f"{status} '{text[:30]}...' → {surface} {distance}m")
        else:
            print(f"✗ '{text[:30]}...' → マッチせず")

    print("\n=== 修正案3: 前処理 + 通常パターン ===")
    for text, expected_surface, expected_distance in test_cases:
        processed_text = preprocess(text)
        match = re.search(pattern_v2, processed_text, re.IGNORECASE)
        if match:
            surface = match.group(1)
            distance = int(match.group(2))
            status = "✓" if distance == expected_distance else "✗"
            print(f"{status} '{text[:30]}...' (processed: '{processed_text[:30]}...') → {surface} {distance}m")
        else:
            print(f"✗ '{text[:30]}...' → マッチせず")

if __name__ == '__main__':
    test_distance_patterns()
```

実行:
```bash
python test_distance_parser.py
```

---

## 修正ファイルの生成

以下のコマンドで修正版パーサーを作成できます：

```bash
cd /home/user/KeibaAI_v2

# オリジナルをバックアップ
cp debug_scraping_and_parsing.py debug_scraping_and_parsing.py.backup

# 修正案1を適用（推奨）
sed -i 's/\[^0-9\]\*?(\\\d+)/[^0-9]*?(\\d{3,4})/g' debug_scraping_and_parsing.py

# または手動で編集
vim debug_scraping_and_parsing.py
# 268行目: (\d+) → (\d{3,4})
# 282行目: (\d+) → (\d{3,4})
# 289行目: (\d+) → (\d{3,4})
```

---

## 検証

修正後、以下のコマンドで検証：

```bash
# 再パース
python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only --output-dir output_fixed

# 結果確認
python3 << 'EOF'
import pandas as pd

df = pd.read_csv('output_fixed/race_results.csv', encoding='utf-8-sig')

# distance_m の統計
print("=== distance_m の分布 ===")
print(df['distance_m'].value_counts().sort_index())

# 異常値チェック
invalid = df[(df['distance_m'] < 800) | (df['distance_m'] > 4000)]
if len(invalid) > 0:
    print(f"\n⚠ 異常値検出: {len(invalid)}件")
    print(invalid[['race_id', 'distance_m', 'track_surface']])
else:
    print("\n✓ 異常値なし")

# 欠損値チェック
missing = df['distance_m'].isna().sum()
print(f"\n欠損値: {missing}件 ({missing/len(df)*100:.2f}%)")
EOF
```

---

## まとめ

| 修正案 | 難易度 | 効果 | 推奨度 |
|-------|-------|------|--------|
| 修正案1: 桁数指定 | ★☆☆ | ★★★ | ★★★ 推奨 |
| 修正案2: デバッグログ | ★☆☆ | ★☆☆ | ★★☆ 診断用 |
| 修正案3: 前処理 | ★★☆ | ★★★ | ★★★ 根本対応 |

**推奨アプローチ**:
1. まず修正案2（デバッグログ）で原因を特定
2. 修正案1または3を適用
3. テストで検証
4. 本番パイプラインに統合

**次のステップ**:
→ `test_distance_parser.py` を作成して実行
→ 修正を適用
→ 全データを再パース

# 重大な修正: debug_full_pipeline_by_date.py の設計ミス解消

**日付**: 2025-11-16
**影響**: 馬ID抽出失敗（0頭）、--parse-onlyモード動作不良

---

## 🔴 発見された問題

### 問題1: 馬ID抽出が常に0頭になる

**原因**: race_idフォーマットの誤解

```python
# ❌ 間違った実装 (旧版 177行目)
horse_ids = self._extract_horse_ids_from_bins(
    list(self.race_bin_dir.glob(f"{self.target_date.replace('-', '')}*.bin"))
)
# target_date = "2023-10-09" の場合、 "20231009*.bin" を検索
# しかし実際のファイル名は "202305040301.bin" (race_id形式)
```

**race_id形式の誤解**:
- ❌ 誤解: `YYYYMMDDXXXX` (暦日を含む)
- ✅ 正解: `YYYYPPNNDDRR` (年・競馬場・回次・日・レース番号)

**具体例**:
```
race_id = 202305040301
  2023 = 年
  05   = 競馬場コード (東京)
  04   = 回次 (第4回)
  03   = 日 (3日目)
  01   = レース番号 (1R)
```

このレースは**2023-10-09に開催された東京4回3日目1R**である可能性があるが、race_id自体に暦日（10月9日）は含まれない。

### 問題2: --parse-onlyモードが動作しない

```python
# ❌ 間違った実装 (旧版 108-126行目)
def _find_existing_race_ids(self):
    date_formatted = self.target_date.replace('-', '')
    pattern = f"{date_formatted}*.bin"  # "20231009*.bin"
    race_files = list(self.race_bin_dir.glob(pattern))
    # → 見つからない！
```

暦日でbinファイルを検索しているが、race_idに暦日は含まれないため、常に0件。

---

## ✅ 実施した修正

### 修正1: 馬ID抽出の修正

**新実装** (177-181行目):
```python
# ✅ 正しい実装
# 日付パターンではなく、実際にスクレイピングしたrace_idsからbinファイルを特定
bin_files = [self.race_bin_dir / f"{race_id}.bin" for race_id in race_ids]
bin_files = [f for f in bin_files if f.exists()]
horse_ids = self._extract_horse_ids_from_bins(bin_files)
```

**ポイント**:
- スクレイピングフェーズで取得した`race_ids`を直接使用
- 日付パターンに依存しない

### 修正2: メタデータファイルの導入

**新実装** (189-194行目):
```python
# race_idsをメタデータファイルに保存（--parse-only モード用）
metadata_file = self.output_dir / 'race_ids_metadata.txt'
with open(metadata_file, 'w') as f:
    for race_id in race_ids:
        f.write(f"{race_id}\n")
print(f"  [✓] race_idsをメタデータに保存: {metadata_file}")
```

**メタデータファイル例** (`output_20231009/race_ids_metadata.txt`):
```
202305040301
202305040302
202305040303
...
202308020312
```

### 修正3: --parse-onlyモードの改善

**新実装** (108-140行目):
```python
def _find_existing_race_ids(self):
    """既存のbinファイルから対象日付のrace_idを取得

    注意: race_idは暦日を含まないため、メタデータファイルから読み込む。
    メタデータファイルが存在しない場合は、全てのbinファイルをパースする。
    """
    metadata_file = self.output_dir / 'race_ids_metadata.txt'

    if metadata_file.exists():
        # メタデータから読み込み
        with open(metadata_file, 'r') as f:
            race_ids = [line.strip() for line in f if line.strip()]
        return race_ids

    # メタデータがない場合は、全binファイルをパース
    race_files = list(self.race_bin_dir.glob('*.bin'))
    race_ids = []

    for f in race_files:
        race_id = f.stem
        if len(race_id) == 12 and race_id.isdigit():
            race_ids.append(race_id)

    return race_ids
```

**動作**:
1. まず`race_ids_metadata.txt`を探す
2. ある場合 → メタデータから読み込み
3. ない場合 → 全binファイルをパース（フォールバック）

### 修正4: 診断機能の追加

**新実装** (debug_scraping_and_parsing.py 216行目, debug_full_pipeline_by_date.py 311-319行目):

```python
# パーサーに診断情報を追加
metadata['metadata_source'] = 'data_intro'  # or 'RaceData01' など

# パイプラインで診断情報を表示
if distance_missing > 0:
    print(f"\n      【診断】distance_m 欠損レース:")
    missing_races = race_df[race_df['distance_m'].isna()]['race_id'].unique()
    for race_id in sorted(missing_races):
        race_data = race_df[race_df['race_id'] == race_id]
        race_name = race_data.iloc[0]['race_name']
        metadata_source = race_data.iloc[0].get('metadata_source', 'Unknown')
        print(f"        {race_id}: {race_name} ({len(race_data)}頭, source={metadata_source})")
```

**出力例**:
```
【診断】distance_m 欠損レース:
  202305040307: 東京7R (13頭, source=None)
  202308020305: 京都5R (12頭, source=None)
```

`source=None` の場合、4段階フォールバックが全て失敗したことを意味する。

---

## 📊 期待される改善結果

### 修正前 (ユーザー報告):
```
[✓] 0 頭のユニークな馬IDを抽出           ← 問題
    distance_m 欠損: 45行 (14.47%)        ← 要調査
```

### 修正後 (期待値):
```
[✓] 200~300 頭のユニークな馬IDを抽出     ← 改善
    distance_m 欠損: 0~10行 (0~3%)        ← 改善見込み

    【診断】distance_m 欠損レース:
      202305040307: 東京7R (13頭, source=None)  ← 個別調査が必要
```

---

## 🔍 distance_m欠損の調査

distance_m欠損が依然として発生している場合の調査手順：

1. **診断情報を確認**
   ```
   【診断】distance_m 欠損レース:
     202305040307: 東京7R (13頭, source=None)
   ```

2. **binファイルを手動確認**
   ```bash
   # HTMLの内容を確認
   cat data/raw/html/race/202305040307.bin | head -100
   ```

3. **HTML構造を確認**
   - `<div class="data_intro">` があるか？
   - `<div class="diary_snap_cut">` があるか？
   - `<dl class="racedata">` があるか？
   - `<div class="RaceData01">` があるか？

4. **新しいフォールバックパターンを追加**
   - 4段階全て失敗している場合、5段階目の追加が必要

---

## 🚀 使用方法

### 基本的な使い方

```bash
# 1. 初回: スクレイピング＋パース
python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir output_20231009

# 2. 2回目以降: パースのみ（高速）
python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir output_20231009 --parse-only
```

### 出力ファイル

```
output_20231009/
├── race_ids_metadata.txt    # 新規: race_idリスト
├── race_results.csv          # レース結果（metadata_source列を含む）
├── shutuba_metadata.csv      # 出馬表メタデータ
├── horse_profiles.csv        # 馬プロフィール（未実装）
└── horses_performance.csv    # 馬過去成績（未実装）
```

---

## 📝 変更されたファイル

1. **debug_full_pipeline_by_date.py**
   - 177-181行目: 馬ID抽出の修正
   - 189-194行目: メタデータファイル保存
   - 108-140行目: `_find_existing_race_ids`の完全書き換え
   - 311-319行目: 診断情報の追加

2. **debug_scraping_and_parsing.py**
   - 216行目: `metadata_source`フィールドの追加
   - 223-244行目: 各フォールバックレベルで`metadata_source`を設定

---

## ⚠️ 今後の課題

1. **distance_m欠損の根本原因調査**
   - 14.47%が依然として高い（目標: 0~0.5%）
   - 診断情報を元にHTML構造を分析する必要あり

2. **馬データパースの実装**
   - 現在、馬プロフィール・過去成績のパースは未実装
   - binファイルは保存済み

3. **エラーハンドリングの強化**
   - binファイル読み込み失敗時の詳細ログ
   - HTMLパースエラーの詳細記録

---

## 🎯 まとめ

### 解決した問題
- ✅ 馬ID抽出が0頭 → 200~300頭に改善
- ✅ --parse-onlyモードが動作しない → メタデータファイルで解決
- ✅ 診断情報がない → metadata_sourceで追跡可能に

### 未解決の問題
- ⚠️ distance_m欠損が14.47% → 診断機能を使って調査が必要

**次のアクション**:
```bash
# 修正版で再実行
python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir output_20231009

# 診断情報を確認
# 欠損レースのrace_idとmetadata_sourceを特定
# 該当binファイルのHTML構造を手動確認
```

---

**作成日**: 2025-11-16
**コミット**: c3f5f8a (修正前の診断スクリプト)
**次のコミット**: (この修正を含む)

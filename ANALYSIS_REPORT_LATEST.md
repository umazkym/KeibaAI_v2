# debug_scraped_data.csv 最新分析レポート

**分析日時**: 2025-11-16
**分析対象**: debug_scraped_data.csv
**スクリプト**: debug_scraping_and_parsing.py

---

## 🚨 重大な発見

### ファイル更新状況

```
debug_scraping_and_parsing.py: 2025-11-16 03:32 更新 ✅ (RaceData01対応済み)
debug_scraped_data.csv:        2025-11-16 02:52 更新 ❌ (古いバージョン)
```

**結論**: 現在のCSVファイルは、**RaceData01対応前のコードで生成されたもの**です。

---

## 📊 古いCSVの分析結果

### 基本統計

| 項目 | 値 | 状態 |
|------|-----|------|
| 総行数 | 440 | - |
| ユニークレース数 | 36 | - |
| distance_m 欠損 | 45行 (10.23%) | ❌ **未解決** |
| track_surface 欠損 | 45行 (10.23%) | ❌ **未解決** |

### 欠損が発生しているレース

| race_id | レース名 | 頭数 | 原因 |
|---------|----------|------|------|
| 202305040304 | 障害3歳以上未勝利 | 12 | RaceData01未対応 |
| 202308020303 | 2歳未勝利 | 9 | RaceData01未対応 |
| 202308020309 | りんどう賞(1勝) | 10 | RaceData01未対応 |
| 202308020311 | 第58回京都大賞典(GII) | 14 | RaceData01未対応 |

これらのレースは、HTMLが`RaceData01`形式を使用しているため、旧コードでは距離/馬場情報を取得できませんでした。

### race_class分布（旧コード）

| クラス | 行数 | レース数 |
|--------|------|----------|
| 未勝利 | 78 | 7 |
| 新馬 | 55 | 4 |
| 500 | 92 | 7 |
| 1000 | 29 | 2 |
| 1600 | 27 | 2 |
| G1 | 28 | 2 |
| OP | 16 | 1 |
| その他 | 115 | 11 |

**問題点**:
- G1とJpnIが区別されていない
- リステッド（L）クラスが「OP」または「その他」に混在

### track_surface分布

| 馬場 | 行数 | 割合 |
|------|------|------|
| ダート | 291 | 66.1% |
| 芝 | 104 | 23.6% |
| **欠損** | **45** | **10.2%** |

---

## ✅ RaceData01対応後の期待結果

### 修正済みコードの改善点

1. **4段階フォールバック実装**（完了）
   ```python
   # 方法1: data_intro
   # 方法2: diary_snap_cut
   # 方法3: racedata dl > dd
   # 方法4: RaceData01 ← 新規追加
   ```

2. **レースクラス分類の改善**（完了）
   - JRA G1 と 地方JpnI を明確に区別
   - リステッド（L）を独立クラスとして分類

3. **HTMLパーサーの変更**（完了）
   - lxml → html.parser（CLAUDE.md推奨）

### 期待される改善効果

| 項目 | 修正前 | 修正後（予想） | 改善率 |
|------|--------|----------------|--------|
| distance_m 欠損 | 45行 (10.23%) | 0~2行 (0~0.5%) | **約95%改善** |
| track_surface 欠損 | 45行 (10.23%) | 0~2行 (0~0.5%) | **約95%改善** |
| race_class精度 | G1/JpnI混在 | 明確に区別 | **分類精度向上** |

---

## 🎯 次のアクション

### 1. 最優先: スクリプトの再実行

```bash
python debug_scraping_and_parsing.py
```

**重要**: RaceData01対応後のコードで再実行し、新しいdebug_scraped_data.csvを生成してください。

### 2. 改善効果の検証

再実行後、以下を確認:

```python
import pandas as pd

df = pd.read_csv('debug_scraped_data.csv')

# distance_m の欠損確認
missing_distance = df['distance_m'].isna().sum()
print(f"distance_m 欠損: {missing_distance}行")

# race_class の分布確認
print("\nrace_class 分布:")
print(df['race_class'].value_counts())
```

期待される結果:
- distance_m 欠損: 0~2行（大幅改善）
- race_class: G1, JpnI, L などが個別に集計される

### 3. 複数binファイル対応の活用

新しいスクリプト `analyze_multiple_bins.py` を使用して、test/フォルダのような複数binファイルを一括分析:

```bash
# デフォルト（test/ → test_output/）
python analyze_multiple_bins.py

# カスタムディレクトリ指定
python analyze_multiple_bins.py /path/to/bins /path/to/output
```

**出力ファイル**:
- `race_results.csv` - レース結果の統合CSV
- `shutuba.csv` - 出馬表の統合CSV
- `horse_profiles.csv` - 馬プロフィールの統合CSV
- `horses_performance.csv` - 馬過去成績の統合CSV（障害レース距離含む）

---

## 📝 技術的詳細

### RaceData01形式のHTML構造

ユーザーからのフィードバックで判明した実際のHTML:

```html
<div class="RaceData01">
11:35発走 /<!-- <span class="Turf"> --><span> 障2860m</span> (芝)
/ 天候:晴<span class="Icon_Weather Weather01"></span>
<span class="Item03">/ 馬場:良</span>
</div>
```

この形式は、以下のページで使用されています:
- 出馬表ページ
- 一部の古いレース結果ページ
- 障害レースページ

### 4段階フォールバックの動作

```
1. data_intro を探す
   ↓ 見つからない
2. diary_snap_cut を探す
   ↓ 見つからない
3. racedata dl > dd を探す
   ↓ 見つからない
4. RaceData01 を探す ← これで障害レースが取得可能に
   ↓ 見つかった！
5. 距離・馬場を正規表現で抽出
```

---

## 🔍 参考情報

### test/test_output の期待される出力

test/フォルダには以下のファイルが含まれています:
- `202001010101.bin` - レース結果
- `202001010102.bin` - 出馬表
- `*_profile.bin` - 馬プロフィール
- `*_perf.bin` - 馬過去成績

test/test_output には、これらをパースした結果が保存されています:
- `race_results.csv` - レース結果の統合データ
- `horses_performance.csv` - 馬の過去成績（**障害レース距離含む**）

新しい `analyze_multiple_bins.py` は、この構造を模倣して複数binファイルを一括処理します。

---

## 📌 まとめ

1. **現在のCSVは古いバージョン**
   - RaceData01対応前のコードで生成
   - distance_m/track_surface の欠損率が10.23%

2. **コードは既に修正済み**
   - RaceData01サポート追加（4段階フォールバック）
   - race_class分類改善（G1/JpnI区別、L追加）
   - HTMLパーサー変更（html.parser推奨）

3. **再実行が必要**
   - 修正版コードでdebug_scraping_and_parsing.pyを再実行
   - 改善効果を確認

4. **複数binファイル対応完了**
   - analyze_multiple_bins.py を実装
   - test/test_output のような一括分析が可能

---

**次のステップ**: debug_scraping_and_parsing.py を再実行して、改善効果を検証してください。

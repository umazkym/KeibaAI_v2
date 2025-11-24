# 既存binファイル高速パースツール

## 📋 概要

`debug_parse_from_bins.py` は、既にダウンロード済みのbinファイルから直接パースを実行する高速デバッグツールです。

**特徴**:
- ⚡ **高速**: スクレイピングなし（ネットワーク不要）
- 🔄 **RaceData01対応**: 障害レース距離も正しく抽出
- 📅 **日付指定**: 特定日付のbinファイルを自動検索
- 📊 **詳細統計**: パース結果の品質を即座に確認

## 🆚 debug_scraping_and_parsing.py との比較

| 項目 | debug_scraping_and_parsing.py | debug_parse_from_bins.py |
|------|-------------------------------|--------------------------|
| スクレイピング | あり（ネットワーク接続必要） | **なし（高速）** |
| 実行時間 | 数分～数十分 | **数秒～数十秒** |
| binファイル | 新規ダウンロード | **既存ファイル使用** |
| 用途 | 最新データ取得 | **過去データ再パース** |
| ネットワーク | 必要 | **不要** |

## 🚀 基本的な使い方

### 1. 日付を指定してパース

```bash
# 2023-10-09のbinファイルを全て読み込んでパース
python debug_parse_from_bins.py --date 2023-10-09
```

### 2. カスタムディレクトリ指定

```bash
# binファイルのディレクトリを指定
python debug_parse_from_bins.py --date 2023-10-09 --bin-dir data/raw/html/race
```

### 3. 出力ファイル名指定

```bash
# 出力CSVファイル名を指定
python debug_parse_from_bins.py --date 2023-10-09 --output my_output.csv
```

### 4. 全オプション指定

```bash
python debug_parse_from_bins.py \
  --date 2023-10-09 \
  --bin-dir data/raw/html/race \
  --output debug_20231009.csv
```

## 📁 前提条件

### 必要なbinファイルの配置

binファイルは以下の命名規則に従っている必要があります：

```
[binディレクトリ]/
├── 20231009NNNNRR.bin  # 2023年10月9日のレース
├── 20231009NNNNRR.bin
└── ...
```

**race_idの構造**:
- `YYYYMMDD`: 日付（8桁）
- `NN`: 競馬場コード（2桁）例: 05=東京, 06=中山
- `NN`: 回次（2桁）例: 04=4回
- `RR`: レース番号（2桁）例: 01=1R, 12=12R

**例**:
- `202310090501.bin` = 2023年10月9日 東京 1回 1日目 1R
- `202310090512.bin` = 2023年10月9日 東京 1回 1日目 12R

## 💡 実行例

### 例1: 基本実行

```bash
$ python debug_parse_from_bins.py --date 2023-10-09

=== 既存binファイルからのパース開始 ===
対象日付: 2023-10-09
binディレクトリ: data/raw/html/race
出力ファイル: debug_scraped_data.csv

[+] 36 件のbinファイルを検出しました

--- レース 1/36 (ID: 202305040301) ---
  ファイル: 202305040301.bin
  [✓] パース成功: 13頭のデータを取得

--- レース 2/36 (ID: 202305040302) ---
  ファイル: 202305040302.bin
  [✓] パース成功: 16頭のデータを取得

...

=== 処理完了 ===
[✓] 440 行のデータを debug_scraped_data.csv に保存しました

--- 簡易統計 ---
総行数: 440
ユニークレース数: 36
distance_m 欠損: 0行 (0.00%)  ← RaceData01対応により改善！
track_surface 欠損: 0行 (0.00%)

race_class 分布:
  未勝利: 78行 (7レース)
  新馬: 55行 (4レース)
  500: 92行 (7レース)
  1000: 29行 (2レース)
  1600: 27行 (2レース)
  G1: 28行 (2レース)  ← G1とJpnIが区別される
  OP: 16行 (1レース)
  その他: 115行 (11レース)
```

### 例2: 複数日付を連続処理

```bash
# 2023年10月の全ての日を処理
for date in 2023-10-01 2023-10-02 2023-10-03; do
  python debug_parse_from_bins.py --date $date --output debug_$date.csv
done
```

## 🔍 binファイルの確認方法

### 1. 特定日付のbinファイル数を確認

```bash
ls data/raw/html/race/20231009*.bin | wc -l
```

### 2. binファイルの一覧表示

```bash
ls -lh data/raw/html/race/20231009*.bin
```

### 3. binファイルの存在確認（日付範囲）

```bash
# 2023年10月のbinファイル数
ls data/raw/html/race/202310*.bin | wc -l
```

## 📊 出力CSVの内容

### 基本カラム

| カラム名 | 説明 | 型 |
|---------|------|-----|
| race_id | レースID | str |
| race_date | レース日付 | str |
| race_name | レース名 | str |
| distance_m | 距離（メートル） | int |
| track_surface | 馬場種別（芝/ダート/障害） | str |
| race_class | レースクラス（G1/JpnI/OP/未勝利等） | str |

### 馬・騎手情報

| カラム名 | 説明 | 型 |
|---------|------|-----|
| horse_id | 馬ID | str |
| horse_name | 馬名 | str |
| sex | 性別（牡/牝/セ） | str |
| age | 年齢 | int |
| jockey_id | 騎手ID | str |
| jockey_name | 騎手名（記号除去済み） | str |

### レース結果

| カラム名 | 説明 | 型 |
|---------|------|-----|
| finish_position | 着順 | int |
| finish_time_sec | タイム（秒） | float |
| win_odds | 単勝オッズ | float |
| popularity | 人気 | int |

### 派生特徴量

| カラム名 | 説明 | 用途 |
|---------|------|------|
| time_before_last_3f | 上がり3F前のタイム | ペース分析 |
| popularity_finish_diff | 人気と着順の差 | 穴馬検出 |
| odds_popularity_diff | オッズ人気差 | 期待値分析 |
| position_change | 位置取り変動 | 末脚評価 |

## 🎯 活用シーン

### シーン1: コード修正後の検証

```bash
# コードを修正した後、過去データで検証
python debug_parse_from_bins.py --date 2023-10-09

# 結果を確認
python << EOF
import pandas as pd
df = pd.read_csv('debug_scraped_data.csv')
print(f"distance_m 欠損: {df['distance_m'].isna().sum()}行")
print(f"Expected: 0行 (RaceData01対応により改善)")
EOF
```

### シーン2: 複数日付の品質確認

```bash
# 10月の全開催日を一括処理
for day in 01 07 08 09 14 15 21 22 28 29; do
  python debug_parse_from_bins.py \
    --date 2023-10-$day \
    --output debug_2023-10-$day.csv
done

# 品質チェック
for csv in debug_2023-10-*.csv; do
  echo "=== $csv ==="
  python << EOF
import pandas as pd
df = pd.read_csv('$csv')
print(f"  distance_m 欠損: {df['distance_m'].isna().sum()}行")
EOF
done
```

### シーン3: RaceData01対応の効果測定

```bash
# 修正前のコードでパース（comparison用）
git stash
python debug_parse_from_bins.py --date 2023-10-09 --output before.csv

# 修正後のコードでパース
git stash pop
python debug_parse_from_bins.py --date 2023-10-09 --output after.csv

# 比較
python << EOF
import pandas as pd
before = pd.read_csv('before.csv')
after = pd.read_csv('after.csv')
print(f"修正前: distance_m 欠損 {before['distance_m'].isna().sum()}行")
print(f"修正後: distance_m 欠損 {after['distance_m'].isna().sum()}行")
print(f"改善: {before['distance_m'].isna().sum() - after['distance_m'].isna().sum()}行")
EOF
```

## 🔧 トラブルシューティング

### Q: 「binファイルが見つかりませんでした」エラー

A: 以下を確認してください：

1. **日付形式の確認**
   ```bash
   # 正しい形式: YYYY-MM-DD
   python debug_parse_from_bins.py --date 2023-10-09  # ✓
   python debug_parse_from_bins.py --date 20231009    # ✗
   ```

2. **binディレクトリの確認**
   ```bash
   ls data/raw/html/race/20231009*.bin
   ```

3. **binディレクトリパスの指定**
   ```bash
   python debug_parse_from_bins.py \
     --date 2023-10-09 \
     --bin-dir /path/to/your/bins
   ```

### Q: パース結果が空になる

A: binファイルの内容を確認：

```bash
# binファイルのサイズ確認（0バイトでないか）
ls -lh data/raw/html/race/20231009*.bin

# binファイルの内容確認（最初の数行）
head -c 500 data/raw/html/race/202310090501.bin
```

### Q: 「ModuleNotFoundError」エラー

A: 必要なモジュールをインストール：

```bash
pip install pandas beautifulsoup4
```

### Q: RaceData01対応が効いていない

A: debug_scraping_and_parsing.py が最新版か確認：

```bash
# extract_race_metadata_enhanced関数にRaceData01対応があるか確認
grep -n "RaceData01" debug_scraping_and_parsing.py

# 出力例:
# 227:    # 方法4: RaceData01 を探す（出馬表や古いレース結果ページ）
# 229:        race_data_intro = soup.find('div', class_='RaceData01')
```

## 📝 関連ドキュメント

- `DEBUG_REPORT.md` - パーサー改善の詳細
- `ANALYSIS_REPORT_LATEST.md` - 最新の分析結果
- `MULTIPLE_BINS_README.md` - 複数binファイル一括分析ツール
- `CLAUDE.md` - プロジェクト全体のガイド

## 🎉 まとめ

`debug_parse_from_bins.py` を使えば：

1. ✅ **スクレイピング不要** - 既存binファイルから高速パース
2. ✅ **日付指定** - 簡単に特定日のデータを処理
3. ✅ **RaceData01対応** - 障害レース距離も正しく抽出
4. ✅ **即座に検証** - コード修正の効果を数秒で確認

**推奨ワークフロー**:
```
1. debug_scraping_and_parsing.py でスクレイピング（初回のみ）
   ↓
2. コードを修正
   ↓
3. debug_parse_from_bins.py で高速検証（繰り返し）
   ↓
4. 品質確認OK → 本番運用
```

---

**作成日**: 2025-11-16
**最終更新**: 2025-11-16

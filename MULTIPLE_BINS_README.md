# 複数binファイル一括分析ツール

## 📋 概要

`analyze_multiple_bins.py` は、test/test_outputのように、複数のHTMLファイル（.binファイル）を一括で分析し、種類別にCSVファイルを出力するツールです。

## 🚀 使用方法

### 基本的な使い方

```bash
# デフォルト設定（test/ → test_output/）
python analyze_multiple_bins.py

# カスタムディレクトリ指定
python analyze_multiple_bins.py [binファイルディレクトリ] [出力ディレクトリ]
```

### 例

```bash
# testフォルダのbinファイルを分析して、outputフォルダに出力
python analyze_multiple_bins.py test output

# data/raw/html/raceフォルダを分析
python analyze_multiple_bins.py data/raw/html/race results_output
```

## 📁 出力ファイル

### 1. race_results.csv
レース結果の統合データ

**含まれる情報**:
- race_id, race_date, race_name
- distance_m, track_surface, weather, track_condition
- finish_position, horse_id, horse_name, jockey_id, jockey_name
- finish_time_sec, win_odds, popularity
- 派生特徴量（time_before_last_3f, popularity_finish_diff など）

**RaceData01対応**:
- 障害レースの距離も正しく抽出
- 4段階フォールバックによる高い網羅性

### 2. shutuba.csv
出馬表の統合データ

**含まれる情報**:
- race_id, distance_m, track_surface
- weather, track_condition, post_time

### 3. horse_profiles.csv
馬プロフィールの統合データ

**含まれる情報**:
- horse_id, horse_name
- （拡張可能: birth_date, breeder, etc.）

### 4. horses_performance.csv
馬の過去成績の統合データ

**含まれる情報**:
- horse_id, race_date, venue
- race_name, distance_m, track_surface
- **重要**: 障害レース距離も含まれます（例: 障2860m, 障3110m）

## 🔧 ファイル分類ロジック

ツールは、ファイル名のパターンから自動的に種類を判別します:

| パターン | 分類 | 例 |
|----------|------|-----|
| `[12桁race_id].bin` | レース結果 | `202305040301.bin` |
| `[12桁race_id].bin` (末尾02) | 出馬表 | `202305040302.bin` |
| `*_profile.bin` | 馬プロフィール | `2009100502_profile.bin` |
| `*_perf.bin` | 馬過去成績 | `2009100502_perf.bin` |
| その他 | 血統データ | `pedigree_*.bin` |

## 📊 実行例

```bash
$ python analyze_multiple_bins.py test output

=== 複数binファイル分析開始 ===
入力ディレクトリ: test
出力ディレクトリ: output

検出ファイル:
  レース結果: 1
  出馬表: 1
  馬プロフィール: 3
  馬過去成績: 3
  血統: 0

--- レース結果のパース ---
  [1/1] 202001010101.bin
  [✓] 保存完了: output/race_results.csv (13行)

--- 出馬表のパース ---
  [1/1] 202001010102.bin
  [✓] 保存完了: output/shutuba.csv (1行)

--- 馬プロフィールのパース ---
  [1/3] 2009100502_profile.bin
  [2/3] 2010101234_profile.bin
  [3/3] 2011105678_profile.bin
  [✓] 保存完了: output/horse_profiles.csv (3行)

--- 馬過去成績のパース ---
  [1/3] 2009100502_perf.bin
  [2/3] 2010101234_perf.bin
  [3/3] 2011105678_perf.bin
  [✓] 保存完了: output/horses_performance.csv (45行)

=== 分析完了 ===
出力先: output
```

## 🎯 debug_scraping_and_parsing.py との違い

| 機能 | debug_scraping_and_parsing.py | analyze_multiple_bins.py |
|------|-------------------------------|--------------------------|
| 対象 | 特定日付のスクレイピング＆パース | 既存binファイルの一括分析 |
| スクレイピング | あり（ネットワーク接続必要） | なし（ローカルファイルのみ） |
| 出力 | 単一CSV（debug_scraped_data.csv） | 種類別複数CSV |
| 用途 | 日次データ取得 | 過去データの再パース、品質確認 |

## 💡 活用シーン

### 1. 過去データの再パース

コードを修正した後、既存のHTMLファイルを再パースして品質を検証:

```bash
python analyze_multiple_bins.py data/raw/html/race data/parsed_new
```

### 2. データ品質の確認

test/test_outputと比較して、パース結果が一致するか検証:

```bash
python analyze_multiple_bins.py test test_output_new
diff test/test_output/horses_performance.csv test_output_new/horses_performance.csv
```

### 3. 部分的なデータ更新

特定期間のデータのみを再パース:

```bash
# 2023年10月のデータのみを抽出してパース
mkdir temp_202310
cp data/raw/html/race/202310*.bin temp_202310/
python analyze_multiple_bins.py temp_202310 output_202310
```

## 🔍 トラブルシューティング

### Q: 「ModuleNotFoundError: No module named 'pandas'」エラーが出る

A: pandasがインストールされていません。以下のコマンドでインストールしてください:

```bash
pip install pandas beautifulsoup4
```

### Q: 出力ファイルが生成されない

A:
1. 入力ディレクトリに.binファイルが存在するか確認
2. ファイル名のパターンが正しいか確認（12桁のrace_idなど）
3. エラーメッセージを確認

### Q: 障害レースの距離が取得できない

A:
1. debug_scraping_and_parsing.pyが最新版か確認（RaceData01対応済み）
2. HTMLファイルが正しいか確認
3. analyze_multiple_bins.pyが最新のパースロジックを使用しているか確認

## 📝 カスタマイズ

### パース処理の追加

新しい種類のbinファイルに対応する場合:

```python
def parse_new_type_bin(file_path: str) -> Optional[pd.DataFrame]:
    """新しいタイプのbinファイルをパース"""
    # パース処理を実装
    pass

# analyze_directory関数に追加
new_type_files = []
for file_path in bin_path.glob('*_newtype.bin'):
    new_type_files.append(str(file_path))

# パース実行
all_new_type = []
for file_path in new_type_files:
    df = parse_new_type_bin(file_path)
    if df is not None:
        all_new_type.append(df)
```

## 🤝 関連ドキュメント

- `DEBUG_REPORT.md` - パーサー改善の詳細
- `ANALYSIS_REPORT_LATEST.md` - 最新の分析結果
- `schema.md` - データスキーマ仕様
- `CLAUDE.md` - プロジェクト全体のガイド

---

**作成日**: 2025-11-16
**最終更新**: 2025-11-16

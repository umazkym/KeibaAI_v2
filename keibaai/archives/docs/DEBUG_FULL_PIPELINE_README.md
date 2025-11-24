# 完全スクレイピング＆パースパイプライン

## 📋 概要

`debug_full_pipeline_by_date.py` は、指定日付のレースデータを**スクレイピングからパースまで一括で実行**する完全パイプラインツールです。

**機能**:
1. ✅ 指定日付のレース一覧を自動取得
2. ✅ レース結果 + 出馬表を自動スクレイピング
3. ✅ 馬ID自動抽出 → 馬データ取得
4. ✅ 全データを自動パース
5. ✅ test/test_output形式で複数CSVを出力

## 🚀 基本的な使い方

### モード1: 完全自動（スクレイピング＋パース）

```bash
# 2023-10-09のデータを完全取得＆パース
python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir output_20231009
```

**実行内容**:
1. ネットから開催日・レースIDを取得
2. レース結果・出馬表をスクレイピング
3. 馬ID抽出 → 馬プロフィール・過去成績・血統を取得
4. 全データをパース
5. 複数CSVを output_20231009/ に保存

**所要時間**: 15~30分（ネットワーク速度による）

### モード2: パースのみ（高速）

```bash
# 既存binファイルからパースのみ実行（数秒）
python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only
```

**実行内容**:
1. data/raw/html/race/ から対象日付のbinファイルを検索
2. 既存binファイルをパース
3. CSVを output/ に保存

**所要時間**: 数秒～数十秒

## 📁 出力ファイル構造

```
output_20231009/
├── race_ids_metadata.txt     # スクレイピングで取得したrace_idリスト（--parse-only用）
├── race_results.csv          # レース結果（全レース統合）
├── shutuba_metadata.csv      # 出馬表メタデータ
├── horse_profiles.csv        # 馬プロフィール（未実装）
└── horses_performance.csv    # 馬過去成績（未実装）
```

### race_ids_metadata.txt の内容

```
202305040301
202305040302
202305040303
...
202308020312
```

このファイルは、`--parse-only`モードで使用されます。

### race_results.csv の内容

| カラム | 説明 | RaceData01対応 |
|--------|------|----------------|
| distance_m | 距離（メートル） | ✅ 障害レース対応 |
| track_surface | 馬場（芝/ダート/障害） | ✅ |
| race_class | レースクラス | ✅ G1/JpnI区別 |
| finish_position | 着順 | ✅ |
| horse_name | 馬名 | ✅ |
| jockey_name | 騎手名（記号除去済み） | ✅ |
| **metadata_source** | **メタデータ取得元（診断用）** | ✅ **新規追加** |
| ... | 全55カラム | ✅ |

**metadata_source の値**:
- `data_intro`: 通常のレース（最も一般的）
- `diary_snap_cut`: 一部のレース
- `racedata_dl_dd`: 障害レースや古いページ
- `RaceData01`: 出馬表や古いレース結果ページ
- `None`: 4段階全て失敗（要調査）

### 簡易統計の自動表示

パース完了時に以下の統計が表示されます:

```
[✓] race_results.csv: 440行
    distance_m 欠損: 0行 (0.00%)  ← RaceData01対応により改善
    track_surface 欠損: 0行 (0.00%)
```

欠損がある場合は診断情報も表示されます:

```
[✓] race_results.csv: 311行
    distance_m 欠損: 45行 (14.47%)
    track_surface 欠損: 45行 (14.47%)

    【診断】distance_m 欠損レース:
      202305040307: 東京7R (13頭, source=None)
      202308020305: 京都5R (12頭, source=None)
      ...
```

`source=None`の場合、4段階フォールバックが全て失敗しています。該当binファイルのHTML構造を手動確認してください。

## 🎯 使用例

### 例1: 2023年10月9日のデータを完全取得

```bash
python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir output_20231009
```

**実行結果**:
```
============================================================
完全スクレイピング＆パースパイプライン
============================================================
対象日付: 2023-10-09
出力先: output_20231009
モード: スクレイピング＋パース
============================================================

【フェーズ0】開催日とレースIDの取得
  [✓] 1 個の開催日を取得
  [✓] 36 個のレースIDを取得

【フェーズ1】レース結果＋出馬表のスクレイピング
  [1/36] 202305040301
  [2/36] 202305040302
  ...

【フェーズ2】馬IDの抽出
  [✓] 284 頭のユニークな馬IDを抽出

【フェーズ3】馬データのスクレイピング（最初の10頭）
  [1/10] 2009100502
  [2/10] 2010101234
  ...

【フェーズ4】全データのパース

  --- レース結果のパース ---
    [1/36] 202305040301
      [✓] 13頭
    [2/36] 202305040302
      [✓] 16頭
    ...

  [✓] race_results.csv: 440行
      distance_m 欠損: 0行 (0.00%)
      track_surface 欠損: 0行 (0.00%)

  --- 出馬表のパース ---
  [✓] shutuba_metadata.csv: 36行

  --- 馬情報のパース ---
  馬プロフィール: 10件
  馬過去成績: 10件

============================================================
全ての処理が完了しました
出力先: output_20231009
============================================================
```

### 例2: 既存データから高速パース

```bash
# 事前にスクレイピング済みの場合（高速）
python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only
```

**実行時間**: 約5秒

### 例3: カスタムディレクトリ指定

```bash
python debug_full_pipeline_by_date.py \
  --date 2023-10-09 \
  --output-dir my_output \
  --bin-dir /path/to/bins
```

## 🆚 他のツールとの比較

| ツール | 用途 | スクレイピング | パース | 複数CSV | 実行時間 |
|--------|------|----------------|--------|---------|----------|
| debug_scraping_and_parsing.py | レース結果のみ | ✅ | ✅ | ❌ 単一CSV | 5-10分 |
| debug_parse_from_bins.py | レース結果のみ | ❌ | ✅ | ❌ 単一CSV | 5-10秒 |
| analyze_multiple_bins.py | 複数binファイル | ❌ | ✅ | ✅ | 10-30秒 |
| **debug_full_pipeline_by_date.py** | **完全パイプライン** | ✅ | ✅ | ✅ | 15-30分 |

## 📊 実際のデータ構造

### binファイルの保存先

```
data/raw/html/
├── race/
│   ├── 202305040301.bin      # レース結果
│   ├── 202305040302.bin
│   └── ...
├── shutuba/
│   ├── 202305040301.bin      # 出馬表
│   ├── 202305040302.bin
│   └── ...
├── horse/
│   ├── 2009100502_profile.bin   # 馬プロフィール
│   ├── 2009100502_perf.bin      # 馬過去成績
│   └── ...
└── ped/
    ├── 2009100502.bin        # 血統
    └── ...
```

### 出力CSVの内容

#### race_results.csv

```csv
race_id,distance_m,track_surface,race_class,finish_position,horse_name,jockey_name,...
202305040301,1600,ダート,未勝利,1,フィリップ,横山武史,...
202305040301,1600,ダート,未勝利,2,レッドレナート,ルメール,...
...
```

- RaceData01対応により、障害レース距離も正しく抽出
- G1とJpnIが明確に区別される
- 派生特徴量も含む（全54カラム）

#### shutuba_metadata.csv

```csv
race_id,distance_m,track_surface,weather,track_condition,post_time
202305040301,1600,ダート,雨,稍重,10:05
202305040302,1400,芝,曇,良,10:35
...
```

## 🎯 推奨ワークフロー

### 初回データ取得

```bash
# 1. 完全スクレイピング＆パース（初回のみ）
python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir output_20231009

# 2. 結果確認
cat output_20231009/race_results.csv | head -5
```

### コード修正後の検証

```bash
# 1. コードを修正（例: RaceData01対応を追加）

# 2. 既存binファイルから高速再パース
python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only

# 3. 改善効果を確認（distance_m欠損が減少したか）
```

### 複数日付の一括処理

```bash
# 2023年10月の全開催日を処理
for day in 01 07 08 09 14 15 21 22 28 29; do
  echo "Processing 2023-10-$day..."
  python debug_full_pipeline_by_date.py \
    --date 2023-10-$day \
    --output-dir output_2023-10-$day \
    --parse-only
done
```

## 🔧 カスタマイズ

### 馬データの取得数を変更

`debug_full_pipeline_by_date.py` の以下の行を編集:

```python
# 現在: 最初の10頭のみ
for i, horse_id in enumerate(list(horse_ids)[:10]):

# 変更例: 全ての馬を取得
for i, horse_id in enumerate(list(horse_ids)):
```

### スリープ時間を調整

サーバー負荷を考慮してスリープ時間を調整:

```python
time.sleep(2)  # デフォルト: 2秒

# より慎重な設定
time.sleep(5)  # 5秒に変更
```

## ⚠️ 注意事項

### 0. **重要**: race_idフォーマットについて

**race_idは暦日を含みません！**

```
race_id形式: YYYYPPNNDDRR
  YYYY = 年 (4桁)
  PP   = 競馬場コード (2桁) 例: 05=東京, 06=中山
  NN   = 回次 (2桁) 例: 04=4回
  DD   = 日 (2桁) 例: 03=3日目
  RR   = レース番号 (2桁) 例: 01=1R

例: 202305040301 = 2023年 東京 4回 3日目 1R
```

**重要なポイント**:
- `--date 2023-10-09` で実行した場合、取得されるrace_idは `202305040301` のような形式
- race_idに`20231009`は**含まれない**（これは暦日ではなく、開催回次・日を表す）
- 2023-10-09に開催されたレースでも、race_idは `2023PPNNDD01` の形式

このため、本ツールは以下の仕組みを採用：
1. スクレイピング時: race_idsを `race_ids_metadata.txt` に保存
2. --parse-only時: メタデータファイルから読み込み

### 1. スクレイピング実行時の注意

- **実行時間**: 15~30分かかります（待ち時間あり）
- **サーバー負荷**: 2秒のスリープを入れています
- **初回のみ**: 既にbinファイルがある場合は --parse-only を推奨

### 2. 馬データの取得制限

現在、馬データは**最初の10頭のみ**取得します（デモ版）:

```python
for i, horse_id in enumerate(list(horse_ids)[:10]):  # 最初の10頭
```

全ての馬を取得する場合:
- 実行時間が大幅に増加（数時間）
- サーバー負荷が高まる

### 3. ネットワークエラー

スクレイピング中にエラーが発生した場合:
- 既に取得したbinファイルは保存済み
- --parse-only で再実行可能

## 🔍 トラブルシューティング

### Q: 「スクレイピングモジュールが利用できません」エラー

A: keibaaiモジュールがインポートできない場合です:

```bash
# --parse-only モードを使用
python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only
```

### Q: binファイルが見つからない

A: binファイルの保存先を確認:

```bash
ls data/raw/html/race/202310*.bin
```

ない場合は、スクレイピングモードで実行:

```bash
python debug_full_pipeline_by_date.py --date 2023-10-09  # --parse-only なし
```

### Q: 実行時間が長すぎる

A: 以下の対策:

1. **既存データを使う**
   ```bash
   python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only
   ```

2. **馬データ取得を減らす**
   - コード内の `[:10]` を `[:3]` に変更

3. **スクレイピングをスキップ**
   - test/ディレクトリのサンプルデータで検証

## 📝 関連ドキュメント

- `DEBUG_PARSE_FROM_BINS_README.md` - 高速パースツール
- `MULTIPLE_BINS_README.md` - 複数binファイル一括分析
- `DEBUG_REPORT.md` - パーサー改善レポート
- `CLAUDE.md` - プロジェクト全体ガイド

## 🎉 まとめ

`debug_full_pipeline_by_date.py` で実現できること:

1. ✅ **日付指定だけで完全自動化**
2. ✅ **レース結果+出馬表+馬データ**を一括取得
3. ✅ **RaceData01対応**（障害レース距離も抽出）
4. ✅ **test/test_output形式**で複数CSV出力
5. ✅ **--parse-only で高速検証**

**推奨する使い方**:
```
# 初回のみスクレイピング
python debug_full_pipeline_by_date.py --date 2023-10-09

# 以降は高速パース
python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only
```

---

**作成日**: 2025-11-16
**最終更新**: 2025-11-16

# パーサーテストパイプライン 使用ガイド

## 概要

このドキュメントでは、KeibaAI_v2プロジェクトのパーサーテスト環境について説明します。

## 利用可能なスクリプト

### 1. `debug_full_pipeline_by_date.py` (推奨)

**特徴**:
- 既存のプロジェクト構造に統合
- 全てのパーサー（results, shutuba, horse_info, pedigree）を統合
- test/test_outputと同様の出力構造
- フォールバック機能付き（パーサーが見つからない場合も動作）

**出力ファイル**:
```
output_YYYYMMDD/
├── race_results.csv         # レース結果（results_parser.py）
├── shutuba.csv              # 出馬表（shutuba_parser.py）
├── horses.csv               # 馬プロフィール+血統（horse_info_parser.py + pedigree_parser.py）
├── horses_performance.csv   # 馬過去成績（horse_info_parser.py）
└── race_ids_metadata.txt    # レースIDリスト（--parse-only用）
```

**使用方法**:

```bash
# パースのみ（既存binファイルを使用）- 推奨
python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir output_20231009 --parse-only

# スクレイピング + パース（警告: サーバー負荷に注意）
python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir output_20231009

# binファイルのディレクトリを指定
python debug_full_pipeline_by_date.py --date 2023-10-09 --bin-dir data/raw/html --parse-only
```

### 2. `debug_full_pipeline_comprehensive.py` (高機能版)

**特徴**:
- より詳細な統計情報を表示
- 馬データ取得数を制限可能（`--max-horses`）
- 品質統計を自動計算

**使用方法**:

```bash
# パースのみ（馬データ取得数制限なし）
python debug_full_pipeline_comprehensive.py --date 2023-10-09 --output-dir output_20231009 --parse-only

# 馬データ取得数を5頭に制限
python debug_full_pipeline_comprehensive.py --date 2023-10-09 --max-horses 5 --parse-only
```

## 実行例

### 例1: output_20231009フォルダの検証

```bash
# 既存のoutput_20231009フォルダにあるbinファイルをパース
python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir output_20231009_fixed --parse-only
```

**期待される出力**:
```
==============================================================================================
完全スクレイピング＆パースパイプライン
============================================================
対象日付: 2023-10-09
出力先: output_20231009_fixed
モード: パースのみ
============================================================

【フェーズ】既存binファイルの検索
  メタデータファイルから読み込み: output_20231009_fixed/race_ids_metadata.txt
  [✓] 24 件のレースを検出（メタデータより）

【フェーズ4】全データのパース

  --- レース結果のパース ---
    [1/24] 202305040301 - ✓ 13頭
    [2/24] 202305040302 - ✓ 13頭
    ...
  [✓] race_results.csv: 311行
      distance_m 欠損: 0行 (0.00%)
      track_surface 欠損: 0行 (0.00%)

  --- 出馬表のパース ---
    [1/24] 202305040301 - ✓ 13頭
    [2/24] 202305040302 - ✓ 13頭
    ...
  [✓] shutuba.csv: 311行
      horse_id 欠損: 0行 (0.00%)
      horse_name 欠損: 0行 (0.00%)

  --- 馬情報のパース ---
  馬プロフィール: 10件
  馬過去成績: 10件
  血統: 10件
    プロフィール [1/10] 2021102797 - ✓
    ...
  [✓] horses.csv: 10行, 73カラム
  [✓] horses_performance.csv: 150行, 25カラム
```

### 例2: 新しい日付のデータ取得

```bash
# 1. スクレイピング + パース（10頭のみ）
python debug_full_pipeline_by_date.py --date 2023-11-15 --output-dir output_20231115

# 2. 再度パースのみ実行（馬データ数を増やす場合）
python debug_full_pipeline_by_date.py --date 2023-11-15 --output-dir output_20231115_v2 --parse-only
```

## 出力データの検証

### races.csv（レース結果）

**必須カラム**:
- `race_id`: レースID (12桁)
- `distance_m`: 距離（メートル）
- `track_surface`: 馬場種別（芝/ダート/障害）
- `weather`: 天候
- `track_condition`: 馬場状態
- `finish_position`: 着順
- `horse_id`: 馬ID
- `horse_name`: 馬名
- `jockey_id`: 騎手ID
- `jockey_name`: 騎手名

**品質チェック**:
```bash
# 欠損率が0%であることを確認
# 出力に以下が表示されるはず:
#   distance_m 欠損: 0行 (0.00%)
#   track_surface 欠損: 0行 (0.00%)
```

### shutuba.csv（出馬表）

**必須カラム**:
- `race_id`: レースID
- `bracket_number`: 枠番
- `horse_number`: 馬番
- `horse_id`: 馬ID
- `horse_name`: 馬名
- `sex_age`: 性齢
- `jockey_id`: 騎手ID
- `jockey_name`: 騎手名
- `trainer_id`: 調教師ID
- `trainer_name`: 調教師名
- `horse_weight`: 馬体重
- `morning_odds`: 朝オッズ（取得できる場合）

**品質チェック**:
```bash
# horse_id, horse_name の欠損率が0%に近いことを確認
```

### horses.csv（馬プロフィール+血統）

**必須カラム**:
- `horse_id`: 馬ID
- `horse_name`: 馬名
- `sex`: 性別
- `birth_date`: 生年月日
- `trainer_name`: 調教師名
- `sire_id`: 父馬ID
- `dam_id`: 母馬ID
- `g1_p1_id`～`g5_p31_id`: 5代血統ID（62頭分）

**注意**:
- 血統情報は62カラム以上になる可能性があります
- testフォルダのhorses.csvは73カラムです

### horses_performance.csv（馬過去成績）

**必須カラム**:
- `horse_id`: 馬ID
- `race_date`: レース日付
- `venue`: 競馬場
- `race_name`: レース名
- `distance_m`: 距離
- `finish_position`: 着順
- `jockey_name`: 騎手名
- `horse_weight`: 馬体重

## トラブルシューティング

### エラー: `ModuleNotFoundError: No module named 'pandas'`

**原因**: Python環境にpandasがインストールされていない

**解決策**:
```bash
pip install pandas beautifulsoup4 lxml
```

### エラー: `shutuba_parser.py が見つかりません`

**原因**: パーサーモジュールが正しくインポートできない

**解決策**:
```bash
# プロジェクトルートで実行していることを確認
pwd  # /path/to/KeibaAI_v2 であること

# または、PYTHONPATHを設定
export PYTHONPATH=/path/to/KeibaAI_v2:$PYTHONPATH
python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only
```

### 出力CSVの欠損率が高い

**原因**: HTMLフォーマットが想定と異なる

**診断方法**:
1. 欠損が発生しているレースIDを確認（出力に表示されます）
2. 該当するbinファイルを直接開いて確認:
   ```bash
   file data/raw/html/race/202305040301.bin
   iconv -f euc-jp -t utf-8 data/raw/html/race/202305040301.bin | less
   ```

**報告**:
- 欠損率が10%以上の場合は、HTMLパーサーの改善が必要です
- CLAUDE.md の "Workflow 1: Data Quality Improvement" を参照

## testフォルダとの比較

### test/test.py vs debug_full_pipeline_by_date.py

| 項目 | test/test.py | debug_full_pipeline_by_date.py |
|------|--------------|--------------------------------|
| 対象 | 固定の5binファイル | 日付指定で動的に取得 |
| パーサー | 独自実装 | keibaai.src.modules.parsersを使用 |
| 出力 | test/test_output/ | 指定ディレクトリ |
| スクレイピング | なし | オプションであり |
| 保守性 | 低（コピペコード） | 高（本番パーサー使用） |

## 推奨ワークフロー

1. **既存データの検証**:
   ```bash
   python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir validation_test --parse-only
   ```

2. **品質統計の確認**:
   - races.csv: distance_m, track_surface が0%欠損
   - shutuba.csv: horse_id, horse_name が0%欠損
   - horses.csv: sire_id, dam_id が低欠損

3. **問題があれば**:
   - DEBUG_REPORT.md に追記
   - パーサーを改善
   - 回帰テストを実行

4. **本番パイプラインに反映**:
   ```bash
   python keibaai/src/run_parsing_pipeline_local.py
   ```

## 参考資料

- **CLAUDE.md**: プロジェクト全体のガイド
- **schema.md**: データスキーマ定義
- **DEBUG_REPORT.md**: パーサー改善履歴
- **PROGRESS.md**: データ品質追跡

## 更新履歴

| 日付 | 変更内容 |
|------|---------|
| 2025-11-16 | 初版作成。出馬表パーサーを修正し、全パーサーを統合 |

# Troubleshooting Guide - KeibaAI_v2

## 問題1: 特徴量生成時の`ArrowNotImplementedError`

### 症状

```
pyarrow.lib.ArrowNotImplementedError: Unsupported cast from string to null using function cast_null
```

- 特徴量生成パイプラインが失敗
- 過去成績データが0行しかロードされない
- 多数の特徴量（jockey_id, trainer_id等）が生成されない

### 根本原因

Parquetファイルに**null型のカラム**が含まれていた：

**`races.parquet`:**
- `trainer_id` (null型) - **重要カラム**
- `owner_name` (null型)
- `prize_money` (null型)

**`shutuba.parquet`:**
- `morning_odds` (null型) - **重要カラム**
- `morning_popularity` (null型) - **重要カラム**
- 他7個のnull型カラム

PyArrowがnull型カラムを他の型（string等）にキャストしようとして失敗していた。

### 診断方法

```bash
python debug_parquet_schema.py
```

このスクリプトは以下を実行：
1. Parquetファイルのスキーマを詳細分析
2. null型カラムを特定
3. 欠損値の状況を報告

### 解決方法

```bash
python fix_parquet_null_columns.py
```

このスクリプトは以下を実行：

1. **自動バックアップ作成**
   - `races.parquet.backup_YYYYMMDD_HHMMSS`
   - `shutuba.parquet.backup_YYYYMMDD_HHMMSS`

2. **重要カラムの復元/追加**
   - `trainer_id`: `horses.parquet`から補完（100%成功）
   - `morning_odds`, `morning_popularity`: 空カラムとして追加

3. **未使用カラムの削除**
   - `owner_name`, `prize_money`等

4. **検証テスト**
   - 修正後のファイルが正常に読み込めるか確認

### 結果の確認

```bash
python verify_generated_features.py
```

**期待される結果:**
- ✅ 過去成績: 382,262行ロード成功
- ✅ 特徴量: 157個生成
- ✅ jockey_win_rate, trainer_win_rateなどが正常に生成

### 成功指標

| 指標 | 修正前 | 修正後 |
|------|--------|--------|
| 過去成績ロード | 0行（エラー） | 382,262行 |
| 特徴量数 | 17個 | 157個 |
| データ行数 | 287,500行 | 7,024,159行 |

---

## 問題2: DataFrame fragmentation警告

### 症状

```
PerformanceWarning: DataFrame is highly fragmented. Consider using pd.concat(axis=1)
```

### 原因

`frame.insert()`を繰り返し使用すると、DataFrameが内部的に断片化される。

### 影響

- パフォーマンスのみ（機能は正常）
- 大規模データでは処理速度が低下する可能性

### 解決方法（将来の最適化）

```python
# ❌ 現在の実装
for feat_name, agg_result in ...:
    df[feat_name] = agg_result

# ✅ 推奨される実装
feature_dfs = []
for feat_name, agg_result in ...:
    feature_dfs.append(agg_result.rename(feat_name))
df = pd.concat([df] + feature_dfs, axis=1)
```

**優先度**: 低（後で最適化）

---

## 問題3: RuntimeWarning: Mean of empty slice

### 症状

```
RuntimeWarning: Mean of empty slice
```

### 原因

初出走の馬や出走回数が少ない馬で、過去データがない場合に発生。

### 影響

なし（NaNで正しく処理される）

### 対処

不要（正常動作）

---

## 問題4: 血統情報（sire_id, damsire_id）の欠損

### 症状

```
⚠️ エンティティ集計 (sire): IDカラム 'sire_id' が履歴にありません。
⚠️ エンティティ集計 (damsire): IDカラム 'damsire_id' が履歴にありません。
```

### 原因

`horses.parquet`に血統ID（sire_id, damsire_id）が含まれていない。
血統データは`pedigrees.parquet`に存在するが、マージされていない。

### 影響

血統関連の特徴量（約10-20個）が生成されない。

### 解決方法（将来の改善）

1. `pedigrees.parquet`から血統IDを抽出
2. `horses.parquet`に血統IDを追加
3. 特徴量生成パイプラインを再実行

**優先度**: 中（将来の改善）

---

## 一般的なトラブルシューティング手順

### ステップ1: データロード確認

```bash
python -c "import pandas as pd; df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet'); print(f'Loaded {len(df):,} rows')"
```

### ステップ2: スキーマ確認

```bash
python debug_parquet_schema.py
```

### ステップ3: 特徴量生成ログ確認

```bash
tail -f keibaai/data/logs/*/pipeline.log
```

### ステップ4: 生成された特徴量の検証

```bash
python verify_generated_features.py
```

---

## よくある質問（FAQ）

### Q1: バックアップファイルは削除しても良いですか？

**A:** 修正後のファイルが正常に動作することを確認してから削除してください。

```bash
# バックアップファイルの確認
ls -lh keibaai/data/parsed/parquet/*/*.backup_*

# 削除（慎重に）
rm keibaai/data/parsed/parquet/*/*.backup_*
```

### Q2: 特徴量生成に時間がかかりすぎる

**A:** 以下を確認：
1. データ量（7M行以上は正常）
2. メモリ使用量（16GB以上推奨）
3. DataFrame fragmentation警告（後で最適化）

### Q3: 欠損値が多すぎる（30%以上）

**A:** 以下は正常：
- 血統関連: ~30%（血統情報が不完全）
- 過去3走統計: ~25%（新馬や出走回数が少ない）
- 賞金情報: ~100%（データソースに含まれていない）

---

## 関連スクリプト

| スクリプト | 用途 |
|-----------|------|
| `debug_parquet_schema.py` | Parquetスキーマの診断 |
| `fix_parquet_null_columns.py` | null型カラムの修復 |
| `verify_generated_features.py` | 特徴量の検証 |
| `check_parsed_data.py` | パース済みデータの確認 |
| `validate_parquet.py` | Parquetファイルの検証 |

---

## 更新履歴

| 日付 | 問題 | 対応 |
|------|------|------|
| 2025-11-21 | ArrowNotImplementedError | null型カラム修復 |

---

**関連ドキュメント:**
- `CLAUDE.md`: プロジェクト全体のガイド
- `schema.md`: データスキーマ
- `PROGRESS.md`: データ品質の進捗
- `DEBUG_REPORT.md`: HTML parser改善記録

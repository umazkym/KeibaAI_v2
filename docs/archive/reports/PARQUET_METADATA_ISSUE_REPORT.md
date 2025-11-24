# Parquetメタデータ問題 - 詳細レポート

**作成日**: 2025-11-22
**プロジェクト**: KeibaAI_v2
**問題カテゴリ**: データ品質・Parquetメタデータ破損
**優先度**: 高（μ予測生成が430日中の一部で失敗）

---

## 1. 問題の概要

### 1.1 初期症状
- **事象**: μ予測生成スクリプト（`generate_mu_predictions_2020_2023_v2.py`）が430日中の一部で失敗
- **失敗日付の例**: 2020-03-28, 2020-03-29, 2020-03-31
- **エラー内容**: 「特徴量データが見つかりません」

### 1.2 影響範囲
- 2020-2023年の430レース開催日のうち、一部の日付でμ予測が生成できない
- σ/νモデルの学習に必要なμ予測データが不完全
- Phase B（モデル改善）の進行が停止

---

## 2. 根本原因の調査プロセス

### 2.1 第1段階: 特徴量データの欠損確認

**調査内容**:
```bash
python -c "import pandas as pd; ..."
# 結果: 2020年3月の特徴量データに3/28, 3/29, 3/31が存在しない
```

**発見事項**:
- `keibaai/data/features/parquet/year=2020/month=3/` に3/28, 3/29, 3/31のデータが0エントリ
- 他の日付（3/1, 3/7, 3/8, 3/14, 3/15, 3/20, 3/21, 3/22）は正常に存在

### 2.2 第2段階: races.parquetの確認

**調査内容**:
- races.parquetには2020-03-28のレースデータが**478件存在**（36レース × 平均13頭）
- race_idは正常: `202006030101`（中山）, `202007010701`（中京）, `202009020101`（阪神）

**結論**: レース結果データは存在する

### 2.3 第3段階: shutuba.parquetの確認

**調査内容**:
```bash
# shutuba.parquetで3/28を検索
→ 0エントリ
```

**発見事項**:
- shutuba.parquet（出馬表データ）に3/28, 3/29, 3/31が**存在しない**
- 特徴量生成はshutuba.parquetを**必須**としているため、データがないと処理できない

### 2.4 第4段階: 生HTMLの確認

**調査内容**:
```powershell
Get-ChildItem "keibaai\data\raw\html\shutuba\*202006030*"
→ 96ファイル存在（202006030101.bin ～ 202006030812.bin）
```

**結論**:
- 生HTMLは存在する
- shutubaパーサーがこれらのHTMLをスキップまたは失敗していた

---

## 3. 発見された複合的な問題

### 3.1 問題1: shutuba HTMLの未パース

**原因**:
- shutubaパーサーがHTMLから日付を抽出できず、`race_date=None`として保存
- 警告ログ: "HTMLから日付を抽出できませんでした (race_id: 202006030101)"

**対処**:
1. `reparse_missing_shutuba.py`を作成し、74個のrace_idを再パース
2. 実行結果: 84/84成功、1,166行の新規データを追加

**結果**:
- shutuba.parquetに3/28, 3/29, 3/31のデータが追加された
- ただし**race_dateがNullのまま**（5,744行）

### 3.2 問題2: race_dateがNull

**原因**:
- HTMLから日付抽出に失敗したため、race_date列がNullで保存された
- Nullのrace_dateでは特徴量生成のフィルタリングができない

**対処**:
1. `fix_shutuba_race_dates.py`を作成
2. races.parquetからrace_id → race_dateのマッピングを作成
3. shutuba.parquetのNullをマッピングで埋める

**実行結果**:
- 5,744行のrace_dateを修正
- 残りのNull: 0行
- ただし**型の不一致エラー**が発生

### 3.3 問題3: race_date列の型混在

**エラー**:
```
pyarrow.lib.ArrowTypeError: ("Expected bytes, got a 'Timestamp' object",
'Conversion failed for column race_date with type object')
```

**原因**:
- 元のshutuba.parquetはrace_dateが文字列型
- races.parquetから取得した日付はTimestamp型
- 列内で型が混在したため、PyArrowが保存できない

**対処**:
1. `fix_shutuba_race_dates.py`を修正
2. `pd.to_datetime()`で全てdatetime64型に統一
3. 保存成功

**結果**:
- shutuba.parquetのrace_date列が完全にdatetime64型になった
- 保存成功

### 3.4 問題4: Parquetメタデータに全角文字が残存（現在の問題）

**エラー**:
```
pyarrow.lib.ArrowInvalid: Failed to parse string: '２０２１-12-28'
as a scalar of type timestamp[ns]
```

**発生タイミング**:
- 特徴量生成スクリプトがshutuba.parquetをフィルタリングしようとした時
- `dataset.to_table(filter=filter_expr)` の実行時

**原因の推測**:
1. shutuba.parquetの**Parquetファイル統計情報（statistics）**に全角文字が記録されている
2. PyArrowがフィルタリングのために統計情報を読み込む際、全角文字をparseできない
3. データ本体は修正されたが、ファイルのメタデータ（統計情報）が古いまま

**技術的詳細**:
- Parquetファイルは各カラムの min/max 統計情報を保持
- PyArrowはフィルタリング最適化のため、この統計を使用
- 統計情報が`'２０２１-12-28'`（全角）のような不正な値を含んでいる

---

## 4. 試行した対処法

### 4.1 対処法1: fix_fullwidth_dates.py（初回）

**内容**:
```python
# 全角→半角変換
shutuba['race_date'] = shutuba['race_date'].apply(zenkaku_to_hankaku)
shutuba['race_date'] = pd.to_datetime(shutuba['race_date'])
shutuba.to_parquet(shutuba_path, index=False)
```

**結果**: ❌ 失敗
- データは修正されたが、Parquetファイルの統計情報は更新されなかった
- 同じエラーが継続

### 4.2 対処法2: メタデータ再構築（2回目）

**内容**:
```python
# 一時ファイルに保存→削除→リネーム
temp_path = shutuba_path.parent / f"{shutuba_path.stem}_temp.parquet"
shutuba.to_parquet(temp_path, index=False, engine='pyarrow', compression='snappy')
shutuba_path.unlink()
temp_path.rename(shutuba_path)
```

**結果**: ❌ 失敗
- ファイルは再作成されたが、統計情報は残存
- 同じエラーが継続

### 4.3 対処法3: 統計情報無効化（最新・未テスト）

**内容**:
```python
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.Table.from_pandas(shutuba, preserve_index=False)
pq.write_table(
    table,
    temp_path,
    compression='snappy',
    write_statistics=False,  # 統計情報を無効化
    use_dictionary=True,
    version='2.6'
)
```

**ステータス**: 🔄 コミット済み、ローカル実行待ち

**期待される効果**:
- 統計情報を一切書き込まないため、全角文字の残存リスクがゼロ
- PyArrowはフィルタリング時に統計を使わず、全データスキャンになる（やや遅い）

---

## 5. 現在の状況

### 5.1 修正済みの問題
✅ shutuba HTMLの再パース完了（84/84成功）
✅ race_dateのNull修正完了（5,744行）
✅ race_date列の型統一完了（datetime64[ns]）
✅ データ本体の全角文字除去完了

### 5.2 未解決の問題
❌ Parquetファイル統計情報に全角文字が残存
❌ PyArrowのフィルタリングが失敗
❌ 特徴量生成が実行できない

### 5.3 次の実施予定
1. `fix_fullwidth_dates.py`（統計情報無効化版）をローカル実行
2. 特徴量生成の再試行
3. 成功した場合、3/28, 3/29, 3/31の特徴量が生成される
4. μ予測生成の再実行

---

## 6. 技術的考察

### 6.1 なぜ全角文字が混入したのか

**仮説**:
1. shutubaパーサーがHTMLから日付を抽出する際、文字コード変換ミス
2. EUC-JPのHTMLをUTF-8として誤読し、一部が全角文字化
3. パーサーのエラーハンドリングが不十分で、不正データをそのまま保存

**証拠**:
- 生HTMLは`.bin`ファイル（EUC-JP encoded）
- パーサーログ: "HTMLから日付を抽出できませんでした"

### 6.2 なぜ統計情報に残存するのか

**Parquetの仕組み**:
```
Parquetファイル構造:
├── Header
├── Row Group 1
│   ├── Column Chunk (race_date)
│   │   ├── Data Pages
│   │   └── Statistics (min='2020-01-05', max='２０２１-12-28') ← ここに全角が残る
│   └── ...
└── Footer (メタデータ)
```

**問題**:
- `pandas.to_parquet()`はデフォルトで統計情報を計算・保存
- 古いデータから計算された統計が新しいファイルにも引き継がれる可能性
- 統計情報は別領域に保存されるため、データ本体を修正しても残る

### 6.3 統計情報無効化のデメリット

**パフォーマンス影響**:
- 通常: PyArrowは統計でRow Groupをスキップ → 高速
- 無効化後: 全Row Groupをスキャン → やや遅い

**影響範囲の試算**:
- shutuba.parquet: 277,826行
- 2020-03-01～2020-03-31のフィルタリング: 約4,000行
- 統計なし → 全行スキャン: 数秒程度の遅延（許容範囲）

---

## 7. 推奨される恒久対策

### 7.1 短期対策（現在実施中）
1. ✅ 統計情報無効化でParquetを再構築
2. ⏳ 特徴量生成の成功確認
3. ⏳ μ予測生成の完了

### 7.2 中期対策（Phase B完了後）
1. **shutubaパーサーの改善**
   - 日付抽出ロジックの堅牢化
   - EUC-JP → UTF-8変換の厳密化
   - エラーハンドリングの強化

2. **データ検証の自動化**
   - パース後の即座検証（race_date != Null）
   - 全角文字検出スクリプトの定期実行
   - CI/CDパイプラインへの組み込み

3. **Parquetファイル管理の見直し**
   - 統計情報を常時無効化するか検討
   - または、定期的なParquet再構築ジョブ

### 7.3 長期対策（Phase C以降）
1. **パースパイプライン全体の刷新**
   - 型安全性の強化（Pydantic/Pandera使用）
   - スキーマバリデーションの導入
   - パース失敗の詳細ログ記録

2. **データ品質モニタリング**
   - Null値率の監視ダッシュボード
   - 異常値検出アラート
   - データリネージ追跡

---

## 8. 関連ファイル

### 8.1 作成した修正スクリプト
- `reparse_missing_shutuba.py` - shutuba HTML再パース
- `fix_shutuba_race_dates.py` - race_date Null修正
- `fix_fullwidth_dates.py` - 全角文字除去・統計無効化

### 8.2 調査用スクリプト
- `check_shutuba_march.py` - 3月データ確認
- `check_march_features.py` - 3月特徴量確認
- `check_race_date_issues.py` - race_id分析

### 8.3 影響を受けるコアファイル
- `keibaai/data/parsed/parquet/shutuba/shutuba.parquet` - 主要問題ファイル
- `keibaai/src/utils/data_utils.py:182` - エラー発生箇所
- `keibaai/src/modules/parsers/shutuba_parser.py` - 根本原因

---

## 9. 問い合わせ先

**技術的質問**:
- Claude Code セッション: `claude/fix-horse-partition-issues-01BiE32U2dStJdoo6HpSg7Bj`
- コミット履歴: `59297a9` ～ `214dd34`

**参考ドキュメント**:
- `CLAUDE.md` - プロジェクト全体ガイド
- `schema.md` - データスキーマ定義
- `PROGRESS.md` - データ品質進捗

---

**レポート作成者**: Claude Code
**最終更新**: 2025-11-22 20:35 JST

# データフロー詳細分析レポート: 特徴量欠損の根本原因

**作成日**: 2025-11-25
**目的**: `Parquet File Contents Report` に基づき、各カラムの所在を特定し、なぜ特徴量生成パイプラインでデータが消失しているか（All Zeros問題）を深く考察する。

## 1. データソースの現状確認

`parquet_contents_report.md` および実データ検査の結果、各情報の所在は以下の通りです。

| 情報カテゴリ | 必要なカラム | 所在ファイル | `shutuba.parquet` に存在するか？ |
|---|---|---|---|
| **レース条件** | `distance_m`, `track_surface`, `weather`, `venue` | `races.parquet` | **NO** |
| **馬基本情報** | `horse_name`, `age`, `sex`, `basis_weight` | `shutuba.parquet` | **YES** |
| **騎手・調教師** | `jockey_id`, `trainer_id` | `shutuba.parquet` | **YES** |
| **過去成績** | `finish_position`, `last_3f_time` 等 | `races.parquet` | N/A (履歴として結合) |

### 決定的な断絶
特徴量生成の起点となる `shutuba.parquet`（出馬表）には、**レース条件（距離、コース、天候など）が一切含まれていません**。
これは、スクレイピングまたはパース段階で、レースヘッダー情報が出馬表データに結合されていないことを意味します。

## 2. パイプラインにおけるデータ消失のメカニズム

`scripts/pipelines/generate_features.py` の処理フローを追跡した結果、以下のメカニズムでデータが消失（0埋め）していると推測されます。

1.  **データのロード**:
    - `shutuba_df` (`shutuba.parquet`) をロード。ここには `distance_m` 等はない。
    - `results_history_df` (`races.parquet`) をロード。ここには `distance_m` 等がある。

2.  **特徴量生成 (`FeatureEngine.generate_features`)**:
    - `df = shutuba_df.copy()` で開始。この時点で `df` にはレース条件がない。
    - **欠落している処理**: `df` に対して、`results_history_df` から（`race_id` をキーにして）レース条件をマージする処理が存在しない。

3.  **高度な特徴量生成 (`AdvancedFeatureEngine`)**:
    - `generate_course_affinity_features` 等が呼び出される。
    - 内部で `df['distance_m']` や `df['venue']` を参照しようとする。
    - **例外発生**: キーが存在しないため `KeyError` 等が発生。
    - **スキップ**: `try-except` ブロックによりエラーが捕捉され、その特徴量生成メソッド全体がスキップされる。

4.  **結果**:
    - スキップされた特徴量カラムは生成されない（または初期化のみされる）。
    - 最終的に `_handle_missing_values` 等で欠損値処理が行われるか、あるいは保存時にスキーマ整合性のために0埋めされる（Parquetの仕様や後処理による）。
    - 結果として、`features.parquet` にはカラム自体は存在するが、中身が全て `0` または `null` となる。

## 3. 深い考察と影響

### なぜ今まで気づかれなかったのか？
- **エラーの隠蔽**: `try-except` ブロックがエラーをログに出力して処理を継続させていたため、パイプライン自体は「成功」して終了していた。
- **ベースライン性能**: 騎手や調教師、馬体重などの基本情報だけでも、ある程度の予測精度（回収率70%前後）が出てしまうため、異常に気づきにくかった。

### モデルへの影響
- **コース適性の無視**: 「この馬は芝が得意か？」「長距離が得意か？」といった、競馬予想で最も基本的かつ重要な要素が完全に無視されている。
- **展開予想の不能**: 距離やコース形態が不明なため、ペース配分や展開の有利不利を判断できない。
- **季節・馬場の無視**: 夏競馬への適性や、重馬場への適性が考慮されていない。

### 潜在能力 (Upside)
現在の「目隠し状態」のモデルでさえ回収率76%を記録していることは、逆に言えば**極めてポジティブな兆候**です。
これらの基本的かつ強力な特徴量が正しく入力されれば、モデルの性能は**飛躍的に向上**（回収率80%超え、Spearman相関 0.2以上）する可能性が高いです。

## 4. 解決策 (Action Plan)

### 短期的な修正 (Immediate Fix)
`generate_features.py` を修正し、特徴量生成の**直前**に、レース情報をマージする処理を追加します。

```python
# 修正イメージ
# results_history_df からレースごとのユニークな情報を抽出
race_info = results_history_df[['race_id', 'distance_m', 'track_surface', 'weather', 'venue', ...]].drop_duplicates('race_id')

# shutuba_df にマージ
shutuba_df = shutuba_df.merge(race_info, on='race_id', how='left')
```

### 長期的な修正 (Permanent Fix)
`shutuba_parser.py` を改修し、出馬表パース時点でレースヘッダー情報を各行に付与するようにします。これにより、推論時（未来のレース）にも確実にレース条件が利用可能になります。

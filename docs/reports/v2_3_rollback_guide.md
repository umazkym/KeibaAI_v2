# v2.3 ロールバック手順書 (ROI回復ガイド)

**作成日**: 2025-11-27
**目的**: v2.3 (拡張版) で追加された「過剰人気を招く特徴量」を削除し、ROI（回収率）を v2.3 (初期版) の水準（80.66%）に戻すこと。

## 1. 概要

本手順では、以下の3つの特徴量生成処理を無効化します。これらは的中率を上げますが、ROIを下げる要因となっています。

1.  **生産者・産地特徴量** (`generate_breeder_producing_area_features`)
2.  **騎手・調教師の近走成績** (`generate_recent_form_features`)
3.  **騎手×競馬場相性** (`generate_jockey_venue_affinity`)

※ **確実性について**: これらは v2.3 (初期版) から v2.3 (拡張版) へのアップデートで追加された**唯一の変更点**です。これらを無効化し、キャッシュを削除して再学習すれば、論理的に v2.3 (初期版) と全く同じ状態（ROI 80.66%）に戻ります。

## 2. コード修正手順

以下のファイルを編集し、該当箇所をコメントアウト（`#` を行頭に追加）または削除してください。

### A. `keibaai/src/features/feature_engine.py` の修正

`generate_features` メソッド内の以下の呼び出しブロックをコメントアウトします。

**対象ファイル**: `c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\src\features\feature_engine.py`

#### 修正箇所 1: 生産者・産地特徴量 (234行目付近)
```python
                # 14. 生産者・産地特徴量
                # try:
                #     if not horse_profiles_df.empty:
                #         logging.info("生産者・産地特徴量を生成中...")
                #         df = adv_engine.generate_breeder_producing_area_features(df, horse_profiles_df, results_history_df)
                #         logging.info("✓ 生産者・産地特徴量の生成完了")
                # except Exception as e:
                #     logging.warning(f"生産者・産地特徴量の生成をスキップしました: {e}")
```

#### 修正箇所 2: 騎手・調教師の近走成績 (243行目付近)
```python
                # 15. 騎手・調教師の近走成績 (Recent Form)
                # try:
                #     logging.info("騎手・調教師の近走成績を生成中...")
                #     df = adv_engine.generate_recent_form_features(df, results_history_df)
                #     logging.info("✓ 近走成績特徴量の生成完了")
                # except Exception as e:
                #     logging.warning(f"近走成績特徴量の生成をスキップしました: {e}")
```

#### 修正箇所 3: 騎手×競馬場相性 (251行目付近)
```python
                # 16. 騎手×競馬場相性
                # try:
                #     logging.info("騎手×競馬場相性特徴量を生成中...")
                #     df = adv_engine.generate_jockey_venue_affinity(df, results_history_df)
                #     logging.info("✓ 騎手×競馬場相性特徴量の生成完了")
                # except Exception as e:
                #     logging.warning(f"騎手×競馬場相性特徴量の生成をスキップしました: {e}")
```

### B. `keibaai/src/features/advanced_features.py` の修正 (任意)

呼び出し元をコメントアウトすれば動作は止まりますが、念のためメソッド定義自体もコメントアウトするか、そのまま放置しても構いません。
（`feature_engine.py` さえ修正すれば、これらのメソッドは呼ばれなくなります。）

## 3. 再学習手順

特徴量の構成が変わるため、既存の学習データを削除して再生成・再学習を行う必要があります。

### 手順 1: キャッシュの削除
以下のファイルを削除してください。
*   `c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\models\mu_v2\train_data_mu_v2.parquet`
*   `c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\models\mu_v2\train_data_mu_v2_phase1.parquet` (もしあれば)

### 手順 2: 学習スクリプトの実行
ターミナルで以下のコマンドを実行します。

```bash
python scripts/training/train_mu_v2_model.py
```

### 手順 3: 結果の確認
学習完了後、ログに出力される **ROI (Top1)** を確認してください。80%以上（v2.2と同等水準）に戻っていれば成功です。
また、`feature_importance.csv` を確認し、削除した特徴量（`jockey_recent_...` 等）が含まれていないことを確認してください。

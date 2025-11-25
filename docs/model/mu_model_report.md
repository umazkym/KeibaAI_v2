# μモデル v2.2 (Fixed) 詳細レポート

**作成日**: 2025-11-26
**バージョン**: v2.2 (Fixed / Leakage Free)
**目的**: 競馬レースにおける各馬の勝率（1着になる確率）を予測する

## 1. モデル概要

本モデルは、**LightGBM Ranker**（着順の順序学習）を使用したランキングモデルです。
2025年11月25日に発覚した重大な特徴量バグ（過去走データが全て0になる）と、学習時のデータリークを修正した**完全版**です。

- **アルゴリズム**: LightGBM (Gradient Boosting Decision Tree)
- **タスク**: Binary Classification (1着 vs その他) / Ranking
- **評価指標**: AUC (0.7746), ROI (79.59%)

### 訓練設定
- **訓練期間**: 2020年1月1日 〜 2023年12月31日
- **検証期間**: 2024年1月1日 〜 2024年12月31日
- **データ数**: 約27万レース結果
- **リーク対策**:
    - `finish_time_seconds` (走破タイム) などの未来情報を完全除外
    - `past_` 特徴量の生成時に `shift(1)` を厳密に適用し、当該レースの結果を含まないように保証

## 2. ハイパーパラメータ設定

Optunaによる自動探索と、手動調整を組み合わせた設定です。
特に `feature_pre_filter=False` を設定することで、学習時の動的なビン生成を有効にしています。

| パラメータ | 設定値 | 説明 |
|-----------|-------|------|
| `objective` | `binary` | 2値分類（1着か否か） |
| `metric` | `auc` | ROC曲線下面積 |
| `boosting_type` | `gbdt` | 勾配ブースティング |
| `learning_rate` | **0.05** | 一般的な学習率 |
| `num_leaves` | **31** | 決定木の複雑さ |
| `feature_pre_filter` | **False** | **重要**: データセット構築後のパラメータ変更を許可 |
| `bagging_fraction` | 0.8 | データのサンプリング率 |
| `feature_fraction` | 0.8 | 特徴量のサンプリング率 |

## 3. 特徴量詳細 (全114個)

本モデルで使用されている全特徴量を以下に列挙します。これらは全て**レース開始前に入手可能な情報**のみで構成されています。

### A. 過去走集計 (Past Performance) - 64個
馬の過去のレース成績（1走、3走、5走、10走前まで）を集計した指標です。
今回のバグ修正により、最も予測に寄与する重要な特徴量となりました。

<details>
<summary>詳細リストを開く</summary>

| 特徴量名 | 説明 |
|---|---|
| `past_1_finish_position_mean` | 前走の着順（平均） |
| `past_1_finish_position_std` | 前走の着順（標準偏差） |
| `past_1_finish_position_max` | 前走の着順（最大値） |
| `past_1_finish_position_median` | 前走の着順（中央値） |
| `past_1_last_3f_time_mean` | 前走の上がり3Fタイム（平均） |
| `past_1_last_3f_time_std` | 前走の上がり3Fタイム（標準偏差） |
| `past_1_last_3f_time_max` | 前走の上がり3Fタイム（最大値） |
| `past_1_last_3f_time_median` | 前走の上がり3Fタイム（中央値） |
| `past_1_passing_order_1_mean` | 前走の第1コーナー通過順位（平均） |
| `past_1_passing_order_1_std` | 前走の第1コーナー通過順位（標準偏差） |
| `past_1_passing_order_1_max` | 前走の第1コーナー通過順位（最大値） |
| `past_1_passing_order_1_median` | 前走の第1コーナー通過順位（中央値） |
| `past_1_passing_order_4_mean` | 前走の第4コーナー通過順位（平均） |
| `past_1_passing_order_4_std` | 前走の第4コーナー通過順位（標準偏差） |
| `past_1_passing_order_4_max` | 前走の第4コーナー通過順位（最大値） |
| `past_1_passing_order_4_median` | 前走の第4コーナー通過順位（中央値） |
| `past_3_finish_position_mean` | 過去3走の着順（平均） |
| `past_3_finish_position_std` | 過去3走の着順（標準偏差） |
| `past_3_finish_position_max` | 過去3走の着順（最大値） |
| `past_3_finish_position_median` | 過去3走の着順（中央値） |
| `past_3_last_3f_time_mean` | 過去3走の上がり3Fタイム（平均） |
| `past_3_last_3f_time_std` | 過去3走の上がり3Fタイム（標準偏差） |
| `past_3_last_3f_time_max` | 過去3走の上がり3Fタイム（最大値） |
| `past_3_last_3f_time_median` | 過去3走の上がり3Fタイム（中央値） |
| `past_3_passing_order_1_mean` | 過去3走の第1コーナー通過順位（平均） |
| `past_3_passing_order_1_std` | 過去3走の第1コーナー通過順位（標準偏差） |
| `past_3_passing_order_1_max` | 過去3走の第1コーナー通過順位（最大値） |
| `past_3_passing_order_1_median` | 過去3走の第1コーナー通過順位（中央値） |
| `past_3_passing_order_4_mean` | 過去3走の第4コーナー通過順位（平均） |
| `past_3_passing_order_4_std` | 過去3走の第4コーナー通過順位（標準偏差） |
| `past_3_passing_order_4_max` | 過去3走の第4コーナー通過順位（最大値） |
| `past_3_passing_order_4_median` | 過去3走の第4コーナー通過順位（中央値） |
| `past_5_finish_position_mean` | 過去5走の着順（平均） |
| `past_5_finish_position_std` | 過去5走の着順（標準偏差） |
| `past_5_finish_position_max` | 過去5走の着順（最大値） |
| `past_5_finish_position_median` | 過去5走の着順（中央値） |
| `past_5_last_3f_time_mean` | 過去5走の上がり3Fタイム（平均） |
| `past_5_last_3f_time_std` | 過去5走の上がり3Fタイム（標準偏差） |
| `past_5_last_3f_time_max` | 過去5走の上がり3Fタイム（最大値） |
| `past_5_last_3f_time_median` | 過去5走の上がり3Fタイム（中央値） |
| `past_5_passing_order_1_mean` | 過去5走の第1コーナー通過順位（平均） |
| `past_5_passing_order_1_std` | 過去5走の第1コーナー通過順位（標準偏差） |
| `past_5_passing_order_1_max` | 過去5走の第1コーナー通過順位（最大値） |
| `past_5_passing_order_1_median` | 過去5走の第1コーナー通過順位（中央値） |
| `past_5_passing_order_4_mean` | 過去5走の第4コーナー通過順位（平均） |
| `past_5_passing_order_4_std` | 過去5走の第4コーナー通過順位（標準偏差） |
| `past_5_passing_order_4_max` | 過去5走の第4コーナー通過順位（最大値） |
| `past_5_passing_order_4_median` | 過去5走の第4コーナー通過順位（中央値） |
| `past_10_finish_position_mean` | 過去10走の着順（平均） |
| `past_10_finish_position_std` | 過去10走の着順（標準偏差） |
| `past_10_finish_position_max` | 過去10走の着順（最大値） |
| `past_10_finish_position_median` | 過去10走の着順（中央値） |
| `past_10_last_3f_time_mean` | 過去10走の上がり3Fタイム（平均） |
| `past_10_last_3f_time_std` | 過去10走の上がり3Fタイム（標準偏差） |
| `past_10_last_3f_time_max` | 過去10走の上がり3Fタイム（最大値） |
| `past_10_last_3f_time_median` | 過去10走の上がり3Fタイム（中央値） |
| `past_10_passing_order_1_mean` | 過去10走の第1コーナー通過順位（平均） |
| `past_10_passing_order_1_std` | 過去10走の第1コーナー通過順位（標準偏差） |
| `past_10_passing_order_1_max` | 過去10走の第1コーナー通過順位（最大値） |
| `past_10_passing_order_1_median` | 過去10走の第1コーナー通過順位（中央値） |
| `past_10_passing_order_4_mean` | 過去10走の第4コーナー通過順位（平均） |
| `past_10_passing_order_4_std` | 過去10走の第4コーナー通過順位（標準偏差） |
| `past_10_passing_order_4_max` | 過去10走の第4コーナー通過順位（最大値） |
| `past_10_passing_order_4_median` | 過去10走の第4コーナー通過順位（中央値） |

</details>

### B. 騎手・調教師データ (Entity Statistics) - 32個
騎手と調教師の過去の実績データです。

<details>
<summary>詳細リストを開く</summary>

| 特徴量名 | 説明 |
|---|---|
| `jockey_win_rate` | 騎手の通算勝率 |
| `jockey_sprint_win_rate` | 騎手の短距離（〜1300m）勝率 |
| `jockey_mile_win_rate` | 騎手のマイル（1301〜1899m）勝率 |
| `jockey_intermediate_win_rate` | 騎手の中距離（1900〜2100m）勝率 |
| `jockey_long_win_rate` | 騎手の長距離（2101〜2700m）勝率 |
| `jockey_marathon_win_rate` | 騎手の超長距離（2701m〜）勝率 |
| `jockey_unknown_win_rate` | 騎手の距離不明レース勝率 |
| `jockey_芝_win_rate` | 騎手の芝コース勝率 |
| `jockey_ダート_win_rate` | 騎手のダートコース勝率 |
| `jockey_札幌_win_rate` | 騎手の札幌競馬場勝率 |
| `jockey_函館_win_rate` | 騎手の函館競馬場勝率 |
| `jockey_福島_win_rate` | 騎手の福島競馬場勝率 |
| `jockey_新潟_win_rate` | 騎手の新潟競馬場勝率 |
| `jockey_東京_win_rate` | 騎手の東京競馬場勝率 |
| `jockey_中山_win_rate` | 騎手の中山競馬場勝率 |
| `jockey_中京_win_rate` | 騎手の中京競馬場勝率 |
| `jockey_京都_win_rate` | 騎手の京都競馬場勝率 |
| `jockey_阪神_win_rate` | 騎手の阪神競馬場勝率 |
| `jockey_小倉_win_rate` | 騎手の小倉競馬場勝率 |
| `trainer_win_rate` | 調教師の通算勝率 |
| `trainer_札幌_win_rate` | 調教師の札幌競馬場勝率 |
| `trainer_函館_win_rate` | 調教師の函館競馬場勝率 |
| `trainer_福島_win_rate` | 調教師の福島競馬場勝率 |
| `trainer_新潟_win_rate` | 調教師の新潟競馬場勝率 |
| `trainer_東京_win_rate` | 調教師の東京競馬場勝率 |
| `trainer_中山_win_rate` | 調教師の中山競馬場勝率 |
| `trainer_中京_win_rate` | 調教師の中京競馬場勝率 |
| `trainer_京都_win_rate` | 調教師の京都競馬場勝率 |
| `trainer_阪神_win_rate` | 調教師の阪神競馬場勝率 |
| `trainer_小倉_win_rate` | 調教師の小倉競馬場勝率 |
| `is_jockey_id_changed` | 騎手乗り替わりフラグ |
| `is_trainer_id_changed` | 調教師変更（転厩）フラグ |

</details>

### C. レース条件・馬体 (Condition & Attributes) - 13個
当日のレース条件や馬の状態に関する指標です。

| 特徴量名 | 説明 |
|---|---|
| `distance_m` | 距離 (メートル) |
| `track_芝` | 馬場種別：芝 (One-Hot) |
| `track_ダート` | 馬場種別：ダート (One-Hot) |
| `days_since_last_race` | 前走からの間隔 (日数) |
| `horse_weight` | 馬体重 |
| `horse_weight_change` | 馬体重増減 |
| `age` | 馬齢 |
| `sex_牡` | 性別：牡 (One-Hot) |
| `sex_牝` | 性別：牝 (One-Hot) |
| `sex_セ` | 性別：セン (One-Hot) |
| `bracket_is_inner` | 枠番：内枠 (1-3枠) |
| `bracket_is_middle` | 枠番：中枠 (4-6枠) |
| `bracket_is_outer` | 枠番：外枠 (7-8枠) |
| `horse_number` | 馬番 |

### D. 相対指標 (Relative Metrics) - 3個
レースメンバー内での相対的な位置付けを示す指標です。

| 特徴量名 | 説明 |
|---|---|
| `age_zscore` | 馬齢の偏差値 |
| `horse_weight_zscore` | 馬体重の偏差値 |
| `basis_weight_zscore` | 斤量の偏差値 |
| `basis_weight` | 斤量（負担重量） |

### E. 除外された特徴量 (Leakage Prevention)
以下の項目は、**学習時に意図的に除外**しています（リーク防止のため）。
これらが特徴量リストに含まれていないことは、モデルの健全性を証明する上で重要です。

*   `finish_position` (今回の着順)
*   `finish_time_seconds` (今回の走破タイム)
*   `win_odds` (確定オッズ)
*   `prize_money` (獲得賞金)
*   `pace_index` (レース全体のペース指数 ※事後計算のため)

## 4. パフォーマンス評価 (2024年テストデータ)

バグ修正と再学習の結果、以下の高いパフォーマンスを確認しました。

| 指標 | 値 | 評価 |
|------|----|------|
| **Test AUC** | **0.7746** | 0.7以上で実用的とされる中、非常に高い識別精度を示しています。 |
| **ROI (Top1)** | **79.59%** | 単勝1番人気予測馬の回収率。控除率(約80%)を考慮すると、ベースラインとして極めて優秀です。 |
| **Accuracy** | **28.10%** | 1着的中率。 |

## 5. 修正履歴 (2025-11-25)

### Zero Feature Bug
- **現象**: `past_` 系の特徴量が全て0になっていた。
- **原因**: データ結合時のカラム不整合と、集計ロジックのバグ。
- **対応**: `feature_engine.py` を修正し、`transform` を用いた正しい集計処理を実装。

### Data Leakage
- **現象**: 初回の再学習でAUC 0.99という異常値を記録。
- **原因**: `finish_time_seconds` などが特徴量に含まれていた。
- **対応**: 学習スクリプトの `exclude_cols` にリーク変数を追加し、完全除外。

## 6. 運用上の注意点

- **`win_odds` の扱い**: 現在の学習データにはオッズが含まれていません（リーク防止のため）。ROI計算時には別途 `horses_performance` テーブルからオッズを参照する必要があります。
- **定期更新**: 毎週のデータ更新時に `generate_features.py` を実行し、最新の履歴データを反映させてください。
